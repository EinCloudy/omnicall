import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from omnicall.gemini import LiveEvent
from omnicall.media import CallMediaSession, _OutputItem


def make_session() -> CallMediaSession:
    return CallMediaSession(
        call_id="test-call",
        websocket=AsyncMock(),
        settings=SimpleNamespace(),
        database=AsyncMock(),
        gemini=AsyncMock(),
        finish_callback=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_audio_is_gated_until_live_opening_finishes() -> None:
    session = make_session()
    session._started = True

    await session._handle_audio(b"\x00" * 3_200)
    assert session._input_queue.empty()

    session._opening_complete = True
    await session._handle_audio(b"\x00" * 3_200)
    assert await session._input_queue.get() == b"\x00" * 3_200


@pytest.mark.asyncio
async def test_live_interruption_flushes_asterisk_and_stale_audio() -> None:
    session = make_session()
    session._output_transcript_parts.append("Angefangene Antwort")
    session._output_queue.put_nowait(
        _OutputItem(kind="audio", generation=0, data=b"stale")
    )
    session._speaking = True

    await session._handle_interruption()

    assert session._generation == 1
    assert session._output_queue.empty()
    assert session._playback_interrupted.is_set()
    assert session._speaking is False
    payload = json.loads(session.websocket.send.await_args.args[0])
    assert payload == {"command": "FLUSH_MEDIA"}
    session.database.add_turn.assert_awaited_once_with(
        "test-call", "agent", "Angefangene Antwort"
    )
    session.database.add_event.assert_awaited_once_with(
        "test-call", "agent_interrupted", {"generation": 0}
    )


@pytest.mark.asyncio
async def test_interrupted_farewell_ends_call_without_repeating_it() -> None:
    session = make_session()
    session._output_transcript_parts.append("Alles klar, danke. Auf Wiederhören.")
    session._speaking = True

    await session._handle_interruption()

    assert session._finish_started is True
    assert session.database.add_event.await_args_list[1].args == (
        "test-call",
        "finish_fallback",
        {"reason": "interrupted_spoken_farewell"},
    )
    session.finish_callback.assert_awaited_once_with("test-call", "end", None)


@pytest.mark.asyncio
async def test_agent_transcript_waits_for_callee_turn() -> None:
    class FakeLive:
        async def events(self):
            yield LiveEvent(kind="output_transcript", text="Antwort", finished=True)
            yield LiveEvent(kind="input_transcript", text="Frage", finished=True)
            yield LiveEvent(kind="turn_complete")

    session = make_session()

    await session._live_receiver(FakeLive())

    assert [call.args for call in session.database.add_turn.await_args_list] == [
        ("test-call", "callee", "Frage"),
        ("test-call", "agent", "Antwort"),
    ]


@pytest.mark.asyncio
async def test_finish_tool_runs_after_output_turn() -> None:
    session = make_session()
    session._opening_complete = True
    session._pending_finish = (
        0,
        LiveEvent(
            kind="finish",
            action="needs_hermes",
            hermes_question="Welcher Termin?",
            internal_note="Rückfrage nötig",
        ),
    )

    await session._finish_output_turn(0)

    session.database.add_event.assert_awaited_once_with(
        "test-call", "agent_note", {"note": "Rückfrage nötig"}
    )
    session.finish_callback.assert_awaited_once_with(
        "test-call", "needs_hermes", "Welcher Termin?"
    )


@pytest.mark.asyncio
async def test_finish_tool_runs_when_it_arrives_after_turn_complete() -> None:
    session = make_session()
    session._opening_complete = True
    first_turn_processed = asyncio.Event()
    original_finish_output_turn = session._finish_output_turn

    async def finish_output_turn(generation: int) -> None:
        await original_finish_output_turn(generation)
        first_turn_processed.set()

    session._finish_output_turn = finish_output_turn
    await session._output_queue.put(_OutputItem(kind="turn_end", generation=0))
    writer = asyncio.create_task(session._output_writer())
    await first_turn_processed.wait()
    session.finish_callback.assert_not_awaited()

    class FakeLive:
        async def events(self):
            yield LiveEvent(kind="finish", action="end")

    await session._live_receiver(FakeLive())
    await asyncio.wait_for(writer, timeout=0.1)

    session.finish_callback.assert_awaited_once_with("test-call", "end", None)


@pytest.mark.parametrize("farewell", ["Auf Wiederhören.", "Tschüss."])
@pytest.mark.asyncio
async def test_spoken_farewell_ends_call_without_finish_tool(farewell: str) -> None:
    session = make_session()
    session._opening_complete = True
    session._last_output_transcript = f"Vielen Dank und {farewell}"

    await session._finish_output_turn(0)

    session.database.add_event.assert_awaited_once_with(
        "test-call", "finish_fallback", {"reason": "spoken_farewell"}
    )
    session.finish_callback.assert_awaited_once_with("test-call", "end", None)

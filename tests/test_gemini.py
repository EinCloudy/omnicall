from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from google.genai import types

from omnicall.gemini import GeminiClient, GeminiLiveSession


def live_settings(
    *,
    disclosure_enabled: bool = True,
    consent_question_enabled: bool = True,
) -> SimpleNamespace:
    return SimpleNamespace(
        gemini_voice="Kore",
        gemini_vad_prefix_padding_ms=100,
        gemini_vad_silence_ms=650,
        disclosure_enabled=disclosure_enabled,
        consent_question_enabled=consent_question_enabled,
    )


async def run_request(_operation: str, request, **_kwargs):
    return await request()


def test_live_config_enables_native_audio_transcripts_and_interruption() -> None:
    client = GeminiClient.__new__(GeminiClient)
    client.settings = live_settings()

    config = client.build_live_config(
        {
            "objective": "Vereinbare einen Termin.",
            "context": {"city": "Mannheim"},
            "opening_message": None,
        }
    )

    assert config.response_modalities == [types.Modality.AUDIO]
    assert config.input_audio_transcription is not None
    assert config.output_audio_transcription is not None
    assert (
        config.realtime_input_config.activity_handling
        == types.ActivityHandling.START_OF_ACTIVITY_INTERRUPTS
    )
    assert (
        config.realtime_input_config.automatic_activity_detection.silence_duration_ms == 650
    )
    assert config.speech_config.voice_config.prebuilt_voice_config.voice_name == "Kore"
    assert config.tools[0].function_declarations[0].name == "finish_call"
    assert "Vereinbare einen Termin" in config.system_instruction
    assert "Sind Sie damit einverstanden?" in config.system_instruction
    assert "ausdrücklicher Zustimmung" in config.system_instruction


def test_live_config_without_consent_question_starts_objective_directly() -> None:
    client = GeminiClient.__new__(GeminiClient)
    client.settings = live_settings(consent_question_enabled=False)

    config = client.build_live_config(
        {
            "objective": "Vereinbare einen Termin.",
            "context": {"city": "Mannheim"},
            "opening_message": None,
        }
    )

    instruction = config.system_instruction
    assert "Dieses Gespräch wird zur Bearbeitung transkribiert." in instruction
    assert "einverstanden" not in instruction
    assert "ausdrücklicher Zustimmung" not in instruction
    assert "Stelle keine Einwilligungsfrage." in instruction
    assert "direkt und ohne Warten" in instruction


def test_live_config_without_disclosure_omits_notice_and_consent() -> None:
    client = GeminiClient.__new__(GeminiClient)
    client.settings = live_settings(disclosure_enabled=False)

    config = client.build_live_config(
        {
            "objective": "Vereinbare einen Termin.",
            "context": {"city": "Mannheim"},
            "opening_message": None,
        }
    )

    instruction = config.system_instruction
    assert "Dieses Gespräch wird zur Bearbeitung transkribiert." not in instruction
    assert "Sind Sie damit einverstanden?" not in instruction
    assert "Beginne direkt" in instruction
    assert "transkript" not in instruction.casefold()
    assert "einwilligung" not in instruction.casefold()


@pytest.mark.asyncio
async def test_live_session_reuses_connection_for_audio_until_shutdown() -> None:
    raw = AsyncMock()
    live = GeminiLiveSession(raw)

    await live.start_conversation()
    await live.send_audio(b"first")
    await live.send_audio(b"second")
    await live.end_audio()
    await live.end_audio()

    assert raw.send_realtime_input.await_count == 4
    assert raw.send_realtime_input.await_args_list[0].kwargs["text"].startswith("Beginne")
    assert raw.send_realtime_input.await_args_list[1].kwargs["audio"].data == b"first"
    assert raw.send_realtime_input.await_args_list[2].kwargs["audio"].data == b"second"
    assert raw.send_realtime_input.await_args_list[3].kwargs == {"audio_stream_end": True}


@pytest.mark.asyncio
async def test_live_session_start_conversation_prompt_matches_consent_mode() -> None:
    raw = AsyncMock()
    await GeminiLiveSession(raw, consent_required=True).start_conversation()
    prompt = raw.send_realtime_input.await_args.kwargs["text"]
    assert "Einwilligungsfrage" in prompt

    raw = AsyncMock()
    await GeminiLiveSession(raw, consent_required=False).start_conversation()
    prompt = raw.send_realtime_input.await_args.kwargs["text"]
    assert "Einwilligungsfrage" not in prompt
    assert "direkt zum eigentlichen Anliegen" in prompt

    raw = AsyncMock()
    await GeminiLiveSession(raw, disclosure_enabled=False).start_conversation()
    prompt = raw.send_realtime_input.await_args.kwargs["text"]
    assert "direkt mit dem eigentlichen Anliegen" in prompt
    assert "Begrüßung" not in prompt


@pytest.mark.asyncio
async def test_live_session_decodes_audio_transcripts_and_turn_state() -> None:
    raw = AsyncMock()
    live = GeminiLiveSession(raw)
    response = SimpleNamespace(
        server_content=SimpleNamespace(
            model_turn=SimpleNamespace(
                parts=[
                    SimpleNamespace(
                        inline_data=SimpleNamespace(
                            data=b"pcm", mime_type="audio/pcm;rate=24000"
                        )
                    )
                ]
            ),
            input_transcription=SimpleNamespace(text="Ja", finished=True),
            output_transcription=SimpleNamespace(text="Danke", finished=True),
            interrupted=True,
            turn_complete=True,
        ),
        tool_call=None,
        tool_call_cancellation=None,
        go_away=None,
    )

    events = [event async for event in live._decode_response(response)]

    assert [event.kind for event in events] == [
        "input_transcript",
        "output_transcript",
        "interrupted",
        "audio",
        "turn_complete",
    ]
    assert events[0].text == "Ja"
    assert events[3].data == b"pcm"


@pytest.mark.asyncio
async def test_live_session_receives_multiple_turns_on_one_connection() -> None:
    class FakeRawSession:
        def __init__(self) -> None:
            self.receive_calls = 0

        def receive(self):
            self.receive_calls += 1
            call = self.receive_calls

            async def responses():
                if call <= 2:
                    yield SimpleNamespace(
                        server_content=SimpleNamespace(
                            model_turn=None,
                            input_transcription=None,
                            output_transcription=None,
                            interrupted=False,
                            turn_complete=True,
                        ),
                        tool_call=None,
                        tool_call_cancellation=None,
                        go_away=None,
                    )

            return responses()

    raw = FakeRawSession()
    live = GeminiLiveSession(raw)

    events = [event async for event in live.events()]

    assert [event.kind for event in events] == ["turn_complete", "turn_complete"]
    assert raw.receive_calls == 3


@pytest.mark.asyncio
async def test_live_session_acknowledges_finish_tool() -> None:
    raw = AsyncMock()
    live = GeminiLiveSession(raw)
    response = SimpleNamespace(
        server_content=None,
        tool_call=SimpleNamespace(
            function_calls=[
                SimpleNamespace(
                    id="tool-1",
                    name="finish_call",
                    args={
                        "action": "needs_hermes",
                        "hermes_question": "Welcher Termin?",
                        "internal_note": "Rückfrage nötig",
                    },
                )
            ]
        ),
        tool_call_cancellation=None,
        go_away=None,
    )

    events = [event async for event in live._decode_response(response)]

    assert len(events) == 1
    assert events[0].kind == "finish"
    assert events[0].action == "needs_hermes"
    assert events[0].hermes_question == "Welcher Termin?"
    raw.send_tool_response.assert_awaited_once()


@pytest.mark.asyncio
async def test_summary_falls_back_when_primary_response_is_truncated() -> None:
    client = GeminiClient.__new__(GeminiClient)
    client.settings = SimpleNamespace(
        gemini_summary_model="gemini-3.7-flash",
        gemini_summary_fallback_model="gemini-3.6-flash",
    )
    generate_content = AsyncMock(
        side_effect=[
            SimpleNamespace(text='{"summary":"**Zusammen"}'),
            SimpleNamespace(
                text=(
                    '{"summary":"Der Systemtest war erfolgreich und das Gespräch wurde '
                    'regulär beendet."}'
                )
            ),
        ]
    )
    client._aio = SimpleNamespace(models=SimpleNamespace(generate_content=generate_content))
    client._with_retry = run_request

    summary = await client.summarize(
        {"objective": "Systemtest", "status": "completed"},
        [{"speaker": "callee", "text": "Test"}],
    )

    assert summary.startswith("Der Systemtest war erfolgreich")
    assert [call.kwargs["model"] for call in generate_content.await_args_list] == [
        "gemini-3.7-flash",
        "gemini-3.6-flash",
    ]

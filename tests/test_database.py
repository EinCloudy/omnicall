from pathlib import Path

import pytest

from omnicall.database import CallDatabase


@pytest.fixture
async def database(tmp_path: Path):
    db = CallDatabase(str(tmp_path / "calls.db"))
    await db.open()
    try:
        yield db
    finally:
        await db.close()


async def test_persists_call_transcript_and_cursor(database: CallDatabase) -> None:
    call = await database.create_call(
        call_id="call-1",
        phone_number="+4962112345",
        objective="Einen Termin abstimmen",
        context={"name": "Test"},
        opening_message=None,
        parent_call_id=None,
        max_duration_seconds=300,
    )
    await database.set_status("call-1", "active")
    await database.add_turn("call-1", "callee", "Dienstag passt.")
    await database.add_turn("call-1", "agent", "Dann um zehn Uhr.")
    await database.set_status("call-1", "completed")
    await database.set_summary("call-1", "Termin am Dienstag um zehn Uhr.")

    stored = await database.get_call("call-1", include_transcript=True)
    assert call["context"] == {"name": "Test"}
    assert stored is not None
    assert stored["status"] == "completed"
    assert stored["summary"] == "Termin am Dienstag um zehn Uhr."
    assert [turn["sequence"] for turn in stored["transcript"]] == [1, 2]

    first_page = await database.get_events(after_event_id=0, limit=3)
    assert len(first_page["events"]) == 3
    second_page = await database.get_events(after_event_id=first_page["next_event_id"], limit=100)
    assert second_page["events"]
    assert all(event["event_id"] > first_page["next_event_id"] for event in second_page["events"])


async def test_long_poll_times_out_without_events(database: CallDatabase) -> None:
    result = await database.get_events(after_event_id=0, wait_seconds=0.01)
    assert result == {"events": [], "next_event_id": 0}


async def test_recovers_interrupted_calls(database: CallDatabase) -> None:
    await database.create_call(
        call_id="call-2",
        phone_number="12345",
        objective="Test",
        context={},
        opening_message=None,
        parent_call_id=None,
        max_duration_seconds=60,
    )
    await database.set_status("call-2", "dialing")

    assert await database.recover_interrupted_calls() == 1
    call = await database.get_call("call-2")
    assert call is not None
    assert call["status"] == "failed"
    assert "restarted" in call["error"]

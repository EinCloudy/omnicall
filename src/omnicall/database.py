from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiosqlite

TERMINAL_STATUSES = frozenset({"completed", "needs_input", "failed", "cancelled"})
VALID_STATUSES = frozenset({"queued", "dialing", "ringing", "active", *TERMINAL_STATUSES})


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


class CallDatabase:
    def __init__(self, path: str) -> None:
        self.path = path
        self.connection: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()
        self._events_changed = asyncio.Condition()

    async def open(self) -> None:
        path = Path(self.path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = await aiosqlite.connect(path)
        self.connection.row_factory = aiosqlite.Row
        await self.connection.execute("PRAGMA journal_mode=WAL")
        await self.connection.execute("PRAGMA synchronous=NORMAL")
        await self.connection.execute("PRAGMA foreign_keys=ON")
        await self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS calls (
                id TEXT PRIMARY KEY,
                phone_number TEXT NOT NULL,
                objective TEXT NOT NULL,
                context_json TEXT NOT NULL DEFAULT '{}',
                opening_message TEXT,
                parent_call_id TEXT REFERENCES calls(id),
                status TEXT NOT NULL,
                summary TEXT,
                hermes_question TEXT,
                error TEXT,
                max_duration_seconds INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                started_at TEXT,
                answered_at TEXT,
                ended_at TEXT
            );

            CREATE TABLE IF NOT EXISTS turns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                call_id TEXT NOT NULL REFERENCES calls(id) ON DELETE CASCADE,
                sequence INTEGER NOT NULL,
                speaker TEXT NOT NULL,
                text TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(call_id, sequence)
            );

            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                call_id TEXT NOT NULL REFERENCES calls(id) ON DELETE CASCADE,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_calls_status_updated
                ON calls(status, updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_turns_call_sequence
                ON turns(call_id, sequence);
            CREATE INDEX IF NOT EXISTS idx_events_call_id
                ON events(call_id, id);
            """
        )
        await self.connection.commit()

    async def close(self) -> None:
        if self.connection is not None:
            await self.connection.close()
            self.connection = None

    def _db(self) -> aiosqlite.Connection:
        if self.connection is None:
            raise RuntimeError("database is not open")
        return self.connection

    async def create_call(
        self,
        *,
        call_id: str,
        phone_number: str,
        objective: str,
        context: dict[str, Any],
        opening_message: str | None,
        parent_call_id: str | None,
        max_duration_seconds: int,
    ) -> dict[str, Any]:
        now = utc_now()
        payload = {
            "status": "queued",
            "phone_number": phone_number,
            "objective": objective,
            "parent_call_id": parent_call_id,
        }
        async with self._lock:
            db = self._db()
            await db.execute(
                """
                INSERT INTO calls (
                    id, phone_number, objective, context_json, opening_message,
                    parent_call_id, status, max_duration_seconds, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?)
                """,
                (
                    call_id,
                    phone_number,
                    objective,
                    json.dumps(context, ensure_ascii=False),
                    opening_message,
                    parent_call_id,
                    max_duration_seconds,
                    now,
                    now,
                ),
            )
            await self._insert_event(db, call_id, "call_created", payload, now)
            await db.commit()
        await self._notify_events()
        call = await self.get_call(call_id)
        assert call is not None
        return call

    async def set_status(
        self,
        call_id: str,
        status: str,
        *,
        error: str | None = None,
        question: str | None = None,
        event_payload: dict[str, Any] | None = None,
    ) -> None:
        if status not in VALID_STATUSES:
            raise ValueError(f"invalid call status: {status}")
        now = utc_now()
        timestamp_column = {
            "dialing": "started_at",
            "active": "answered_at",
            "completed": "ended_at",
            "needs_input": "ended_at",
            "failed": "ended_at",
            "cancelled": "ended_at",
        }.get(status)
        assignments = ["status = ?", "updated_at = ?"]
        values: list[Any] = [status, now]
        if timestamp_column:
            assignments.append(f"{timestamp_column} = COALESCE({timestamp_column}, ?)")
            values.append(now)
        if error is not None:
            assignments.append("error = ?")
            values.append(error)
        if question is not None:
            assignments.append("hermes_question = ?")
            values.append(question)
        values.append(call_id)

        payload = {"status": status, **(event_payload or {})}
        if error:
            payload["error"] = error
        if question:
            payload["question"] = question
        async with self._lock:
            db = self._db()
            cursor = await db.execute(
                f"UPDATE calls SET {', '.join(assignments)} WHERE id = ?", values
            )
            if cursor.rowcount != 1:
                raise KeyError(f"call not found: {call_id}")
            await self._insert_event(db, call_id, "status_changed", payload, now)
            await db.commit()
        await self._notify_events()

    async def set_summary(self, call_id: str, summary: str) -> None:
        now = utc_now()
        async with self._lock:
            db = self._db()
            await db.execute(
                "UPDATE calls SET summary = ?, updated_at = ? WHERE id = ?",
                (summary, now, call_id),
            )
            await self._insert_event(db, call_id, "summary_ready", {"summary": summary}, now)
            await db.commit()
        await self._notify_events()

    async def add_turn(self, call_id: str, speaker: str, text: str) -> dict[str, Any]:
        if speaker not in {"agent", "callee", "system"}:
            raise ValueError(f"invalid speaker: {speaker}")
        now = utc_now()
        async with self._lock:
            db = self._db()
            cursor = await db.execute(
                "SELECT COALESCE(MAX(sequence), 0) + 1 FROM turns WHERE call_id = ?",
                (call_id,),
            )
            sequence = int((await cursor.fetchone())[0])
            await db.execute(
                """
                INSERT INTO turns(call_id, sequence, speaker, text, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (call_id, sequence, speaker, text, now),
            )
            payload = {"sequence": sequence, "speaker": speaker, "text": text}
            await self._insert_event(db, call_id, "transcript_turn", payload, now)
            await db.execute("UPDATE calls SET updated_at = ? WHERE id = ?", (now, call_id))
            await db.commit()
        await self._notify_events()
        return {**payload, "created_at": now}

    async def add_event(self, call_id: str, event_type: str, payload: dict[str, Any]) -> None:
        now = utc_now()
        async with self._lock:
            db = self._db()
            await self._insert_event(db, call_id, event_type, payload, now)
            await db.commit()
        await self._notify_events()

    async def _insert_event(
        self,
        db: aiosqlite.Connection,
        call_id: str,
        event_type: str,
        payload: dict[str, Any],
        created_at: str,
    ) -> None:
        await db.execute(
            """
            INSERT INTO events(call_id, event_type, payload_json, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (call_id, event_type, json.dumps(payload, ensure_ascii=False), created_at),
        )

    async def _notify_events(self) -> None:
        async with self._events_changed:
            self._events_changed.notify_all()

    async def get_call(
        self, call_id: str, *, include_transcript: bool = False
    ) -> dict[str, Any] | None:
        cursor = await self._db().execute("SELECT * FROM calls WHERE id = ?", (call_id,))
        row = await cursor.fetchone()
        if row is None:
            return None
        result = self._call_row(row)
        if include_transcript:
            result["transcript"] = await self.get_turns(call_id)
        return result

    async def get_turns(self, call_id: str) -> list[dict[str, Any]]:
        cursor = await self._db().execute(
            """
            SELECT sequence, speaker, text, created_at
            FROM turns WHERE call_id = ? ORDER BY sequence
            """,
            (call_id,),
        )
        return [dict(row) for row in await cursor.fetchall()]

    async def list_calls(
        self, *, status: str | None = None, limit: int = 20
    ) -> list[dict[str, Any]]:
        if status is not None and status not in VALID_STATUSES:
            raise ValueError(f"invalid call status: {status}")
        if status:
            cursor = await self._db().execute(
                "SELECT * FROM calls WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                (status, limit),
            )
        else:
            cursor = await self._db().execute(
                "SELECT * FROM calls ORDER BY created_at DESC LIMIT ?", (limit,)
            )
        return [self._call_row(row) for row in await cursor.fetchall()]

    async def get_events(
        self,
        *,
        after_event_id: int,
        call_id: str | None = None,
        limit: int = 100,
        wait_seconds: float = 0,
    ) -> dict[str, Any]:
        events = await self._query_events(after_event_id, call_id, limit)
        if not events and wait_seconds > 0:
            async with self._events_changed:
                events = await self._query_events(after_event_id, call_id, limit)
                if not events:
                    try:
                        await asyncio.wait_for(
                            self._events_changed.wait(), timeout=min(wait_seconds, 25)
                        )
                    except TimeoutError:
                        pass
            events = await self._query_events(after_event_id, call_id, limit)
        cursor = events[-1]["event_id"] if events else after_event_id
        return {"events": events, "next_event_id": cursor}

    async def _query_events(
        self, after_event_id: int, call_id: str | None, limit: int
    ) -> list[dict[str, Any]]:
        if call_id:
            cursor = await self._db().execute(
                """
                SELECT id, call_id, event_type, payload_json, created_at
                FROM events WHERE id > ? AND call_id = ? ORDER BY id LIMIT ?
                """,
                (after_event_id, call_id, limit),
            )
        else:
            cursor = await self._db().execute(
                """
                SELECT id, call_id, event_type, payload_json, created_at
                FROM events WHERE id > ? ORDER BY id LIMIT ?
                """,
                (after_event_id, limit),
            )
        return [
            {
                "event_id": row["id"],
                "call_id": row["call_id"],
                "type": row["event_type"],
                "payload": json.loads(row["payload_json"]),
                "created_at": row["created_at"],
            }
            for row in await cursor.fetchall()
        ]

    async def recover_interrupted_calls(self) -> int:
        cursor = await self._db().execute(
            "SELECT id FROM calls WHERE status IN ('dialing', 'ringing', 'active')"
        )
        call_ids = [row[0] for row in await cursor.fetchall()]
        for call_id in call_ids:
            await self.set_status(
                call_id,
                "failed",
                error="service restarted while the call was in progress",
            )
        return len(call_ids)

    def _call_row(self, row: aiosqlite.Row) -> dict[str, Any]:
        result = dict(row)
        result["context"] = json.loads(result.pop("context_json"))
        return result

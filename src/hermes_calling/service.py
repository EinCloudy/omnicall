from __future__ import annotations

import asyncio
import json
import logging
import re
import uuid
from typing import Any

from .ari import AriClient, AriError
from .config import Settings
from .database import TERMINAL_STATUSES, CallDatabase
from .gemini import GeminiClient
from .media import MediaServer

logger = logging.getLogger(__name__)
PHONE_PATTERN = re.compile(r"^\+?[0-9]{3,20}$")


class CallService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.database = CallDatabase(settings.database_path)
        self.gemini = GeminiClient(settings)
        self.ari = AriClient(settings, self.handle_ari_event)
        self.media = MediaServer(
            settings,
            self.database,
            self.gemini,
            self.finish_from_media,
            self.fail_from_media,
        )
        self._semaphore = asyncio.Semaphore(settings.max_concurrent_calls)
        self._jobs: dict[str, asyncio.Task[None]] = {}
        self._waiters: dict[str, asyncio.Future[None]] = {}
        self._bridges: dict[str, tuple[str, str]] = {}
        self._known_calls: set[str] = set()
        self._summary_tasks: set[asyncio.Task[Any]] = set()
        self._summarizing: set[str] = set()
        self._closing = False

    async def start(self) -> None:
        await self.database.open()
        recovered = await self.database.recover_interrupted_calls()
        if recovered:
            logger.warning("marked %s interrupted call(s) as failed", recovered)
        await self.media.start()
        await self.ari.start()
        queued = await self.database.list_calls(status="queued", limit=1000)
        for call in reversed(queued):
            self._schedule(call["id"])

    async def close(self) -> None:
        self._closing = True
        await self.media.close()
        await self.ari.close()
        for task in self._jobs.values():
            task.cancel()
        if self._jobs:
            await asyncio.gather(*self._jobs.values(), return_exceptions=True)
        if self._summary_tasks:
            await asyncio.gather(*self._summary_tasks, return_exceptions=True)
        await self.gemini.close()
        await self.database.close()

    async def start_call(
        self,
        *,
        phone_number: str,
        objective: str,
        context: dict[str, Any] | None = None,
        opening_message: str | None = None,
        max_duration_seconds: int | None = None,
        parent_call_id: str | None = None,
    ) -> dict[str, Any]:
        number = normalize_phone_number(phone_number)
        objective = objective.strip()
        if not objective:
            raise ValueError("objective must not be empty")
        if len(objective) > 4000:
            raise ValueError("objective must not exceed 4000 characters")
        context = context or {}
        if len(json.dumps(context, ensure_ascii=False)) > 32_000:
            raise ValueError("context must not exceed 32 KB")
        duration = max_duration_seconds or self.settings.max_call_duration
        duration = min(max(30, duration), self.settings.max_call_duration)
        call_id = str(uuid.uuid4())
        call = await self.database.create_call(
            call_id=call_id,
            phone_number=number,
            objective=objective,
            context=context,
            opening_message=opening_message.strip() if opening_message else None,
            parent_call_id=parent_call_id,
            max_duration_seconds=duration,
        )
        self._schedule(call_id)
        return call

    async def follow_up_call(
        self,
        *,
        previous_call_id: str,
        instructions: str,
        phone_number: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        previous = await self.database.get_call(previous_call_id, include_transcript=True)
        if previous is None:
            raise KeyError(f"call not found: {previous_call_id}")
        inherited = {
            "previous_call": {
                "id": previous_call_id,
                "objective": previous["objective"],
                "summary": previous["summary"],
                "open_question": previous["hermes_question"],
                "transcript": previous["transcript"][-30:],
            },
            **(context or {}),
        }
        objective = f"Folgeanruf zum vorherigen Gespräch. Neue Anweisung: {instructions.strip()}"
        return await self.start_call(
            phone_number=phone_number or previous["phone_number"],
            objective=objective,
            context=inherited,
            parent_call_id=previous_call_id,
        )

    async def cancel_call(self, call_id: str) -> dict[str, Any]:
        call = await self.database.get_call(call_id)
        if call is None:
            raise KeyError(f"call not found: {call_id}")
        if call["status"] in TERMINAL_STATUSES:
            return call
        await self.database.set_status(call_id, "cancelled")
        await self.media.interrupt_call(call_id)
        if call["status"] != "queued":
            try:
                await self.ari.hangup(call_id)
            except AriError:
                logger.warning("could not hang up cancelled call %s", call_id)
        await self._cleanup_resources(call_id)
        self._complete_waiter(call_id)
        self._schedule_summary(call_id)
        result = await self.database.get_call(call_id)
        assert result is not None
        return result

    async def finish_from_media(self, call_id: str, action: str, question: str | None) -> None:
        call = await self.database.get_call(call_id)
        if call is None or call["status"] in TERMINAL_STATUSES:
            return
        status = "needs_input" if action == "needs_hermes" else "completed"
        await self.database.set_status(call_id, status, question=question)
        try:
            await self.ari.hangup(call_id)
        except AriError:
            logger.warning("could not hang up completed call %s", call_id)

    async def fail_from_media(self, call_id: str, error: str) -> None:
        call = await self.database.get_call(call_id)
        if call is None or call["status"] in TERMINAL_STATUSES:
            return
        await self.database.set_status(call_id, "failed", error=f"media failure: {error}")
        try:
            await self.ari.hangup(call_id)
        except AriError:
            pass

    async def handle_ari_event(self, event: dict[str, Any]) -> None:
        event_type = event.get("type")
        channel = (
            event.get("peer")
            if event_type == "Dial" and event.get("peer")
            else event.get("channel") or {}
        )
        channel_id = channel.get("id", "")
        if channel_id.startswith("media-"):
            return
        if channel_id not in self._known_calls:
            return

        if event_type == "ChannelStateChange":
            state = channel.get("state")
            if state in {"Ring", "Ringing"}:
                await self._set_status_if_current(call_id=channel_id, status="ringing")
        elif event_type == "StasisStart":
            await self._on_answered(channel_id)
        elif event_type in {"ChannelDestroyed", "StasisEnd"}:
            await self._on_channel_ended(channel_id, event)
        elif event_type == "Dial":
            dial_status = event.get("dialstatus")
            if dial_status in {"BUSY", "NOANSWER", "CHANUNAVAIL", "CONGESTION"}:
                await self._fail_call(channel_id, f"dial failed: {dial_status}")

    async def _on_answered(self, call_id: str) -> None:
        call = await self.database.get_call(call_id)
        if call is None or call["status"] in TERMINAL_STATUSES or call_id in self._bridges:
            return
        await self.database.set_status(call_id, "active")
        try:
            self._bridges[call_id] = await self.ari.create_call_media(call_id)
        except Exception as exc:
            await self._fail_call(call_id, f"could not connect call audio: {exc}")

    async def _on_channel_ended(self, call_id: str, event: dict[str, Any]) -> None:
        call = await self.database.get_call(call_id)
        if call is None:
            return
        await self.media.interrupt_call(call_id)
        if call["status"] not in TERMINAL_STATUSES:
            if call["status"] == "active":
                await self.database.set_status(call_id, "completed")
            else:
                cause = event.get("cause_txt") or event.get("cause") or "call ended"
                await self.database.set_status(call_id, "failed", error=str(cause))
        self._complete_waiter(call_id)
        await self._cleanup_resources(call_id)
        self._schedule_summary(call_id)

    async def _fail_call(self, call_id: str, error: str) -> None:
        call = await self.database.get_call(call_id)
        if call is None or call["status"] in TERMINAL_STATUSES:
            return
        await self.database.set_status(call_id, "failed", error=error)
        try:
            await self.ari.hangup(call_id)
        except AriError:
            pass
        self._complete_waiter(call_id)
        await self._cleanup_resources(call_id)
        self._schedule_summary(call_id)

    async def _set_status_if_current(self, *, call_id: str, status: str) -> None:
        call = await self.database.get_call(call_id)
        if call and call["status"] not in TERMINAL_STATUSES and call["status"] != status:
            await self.database.set_status(call_id, status)

    def _schedule(self, call_id: str) -> None:
        if call_id in self._jobs:
            return
        self._known_calls.add(call_id)
        task = asyncio.create_task(self._execute_call(call_id), name=f"call-{call_id}")
        self._jobs[call_id] = task
        task.add_done_callback(lambda _: self._jobs.pop(call_id, None))

    async def _execute_call(self, call_id: str) -> None:
        try:
            async with self._semaphore:
                call = await self.database.get_call(call_id)
                if call is None or call["status"] != "queued" or self._closing:
                    return
                waiter = asyncio.get_running_loop().create_future()
                self._waiters[call_id] = waiter
                await self.database.set_status(call_id, "dialing")
                await self.ari.wait_ready()
                dial_number = f"{self.settings.sip_dial_prefix}{call['phone_number']}"
                await self.ari.originate(
                    call_id=call_id,
                    number=dial_number,
                    caller_id=self.settings.sip_caller_id,
                    timeout_seconds=self.settings.default_call_timeout,
                )
                await waiter
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("call %s failed", call_id)
            await self._fail_call(call_id, str(exc))
        finally:
            self._waiters.pop(call_id, None)

    def _complete_waiter(self, call_id: str) -> None:
        waiter = self._waiters.get(call_id)
        if waiter and not waiter.done():
            waiter.set_result(None)

    async def _cleanup_resources(self, call_id: str) -> None:
        resources = self._bridges.pop(call_id, None)
        if not resources:
            return
        bridge_id, media_id = resources
        await asyncio.gather(
            self.ari.hangup(media_id),
            self.ari.destroy_bridge(bridge_id),
            return_exceptions=True,
        )

    def _schedule_summary(self, call_id: str) -> None:
        if call_id in self._summarizing:
            return
        self._summarizing.add(call_id)
        task = asyncio.create_task(self._summarize(call_id), name=f"summary-{call_id}")
        self._summary_tasks.add(task)
        task.add_done_callback(
            lambda finished: (
                self._summary_tasks.discard(finished),
                self._summarizing.discard(call_id),
            )
        )

    async def _summarize(self, call_id: str) -> None:
        call = await self.database.get_call(call_id)
        if call is None or call.get("summary"):
            return
        turns = await self.database.get_turns(call_id)
        if not turns:
            summary = call.get("error") or "Es kam zu keinem transkribierten Gespräch."
        else:
            try:
                summary = await self.gemini.summarize(call, turns)
            except Exception:
                logger.exception("could not summarize call %s", call_id)
                summary = " | ".join(f"{turn['speaker']}: {turn['text']}" for turn in turns[-8:])
        await self.database.set_summary(call_id, summary)


def normalize_phone_number(value: str) -> str:
    number = re.sub(r"[\s()./-]", "", value.strip())
    if number.startswith("00"):
        number = f"+{number[2:]}"
    if not PHONE_PATTERN.fullmatch(number):
        raise ValueError("phone_number must contain 3 to 20 digits and may start with '+'")
    return number

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import parse_qs, urlparse

from websockets.asyncio.server import Server, ServerConnection, serve

from .audio import StreamingPcmResampler
from .config import Settings
from .database import CallDatabase
from .gemini import GeminiClient, GeminiLiveSession, LiveEvent

logger = logging.getLogger(__name__)
FAREWELL_PHRASES = ("auf wiederhören", "auf wiedersehen", "tschüss", "tschüs")
FinishCallback = Callable[[str, str, str | None], Awaitable[None]]
FailureCallback = Callable[[str, str], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class _OutputItem:
    kind: Literal["audio", "turn_end"]
    generation: int
    data: bytes = b""


class MediaServer:
    def __init__(
        self,
        settings: Settings,
        database: CallDatabase,
        gemini: GeminiClient,
        finish_callback: FinishCallback,
        failure_callback: FailureCallback,
    ) -> None:
        self.settings = settings
        self.database = database
        self.gemini = gemini
        self.finish_callback = finish_callback
        self.failure_callback = failure_callback
        self._server: Server | None = None
        self._sessions: dict[str, CallMediaSession] = {}

    async def start(self) -> None:
        self._server = await serve(
            self._handle_connection,
            self.settings.media_host,
            self.settings.media_port,
            subprotocols=["media"],
            compression=None,
            max_size=1_048_576,
            ping_interval=20,
            ping_timeout=20,
        )
        logger.info(
            "media WebSocket listening on %s:%s",
            self.settings.media_host,
            self.settings.media_port,
        )

    async def close(self) -> None:
        sessions = list(self._sessions.values())
        await asyncio.gather(*(session.close() for session in sessions), return_exceptions=True)
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def interrupt_call(self, call_id: str) -> None:
        session = self._sessions.get(call_id)
        if session:
            await session.close()

    async def _handle_connection(self, websocket: ServerConnection) -> None:
        path = websocket.request.path
        query = parse_qs(urlparse(path).query)
        call_id = query.get("call_id", [""])[0]
        if not call_id:
            logger.warning("rejected media WebSocket without call_id")
            await websocket.close(code=1008, reason="call_id is required")
            return
        call = await self.database.get_call(call_id)
        if call is None or call["status"] not in {"active", "ringing"}:
            logger.warning("rejected media WebSocket for unknown/inactive call %s", call_id)
            await websocket.close(code=1008, reason="call is not active")
            return
        if call_id in self._sessions:
            logger.warning("rejected duplicate media WebSocket for call %s", call_id)
            await websocket.close(code=1013, reason="call media is already connected")
            return

        session = CallMediaSession(
            call_id=call_id,
            websocket=websocket,
            settings=self.settings,
            database=self.database,
            gemini=self.gemini,
            finish_callback=self.finish_callback,
        )
        self._sessions[call_id] = session
        try:
            await session.run()
        except Exception as exc:
            logger.exception("media session failed for call %s", call_id)
            await self.failure_callback(call_id, str(exc))
        finally:
            if self._sessions.get(call_id) is session:
                self._sessions.pop(call_id, None)
            await session.close()


class CallMediaSession:
    def __init__(
        self,
        *,
        call_id: str,
        websocket: ServerConnection,
        settings: Settings,
        database: CallDatabase,
        gemini: GeminiClient,
        finish_callback: FinishCallback,
    ) -> None:
        self.call_id = call_id
        self.websocket = websocket
        self.settings = settings
        self.database = database
        self.gemini = gemini
        self.finish_callback = finish_callback
        self._started = False
        self._closing = False
        self._closed_event = asyncio.Event()
        self._close_lock = asyncio.Lock()
        self._failure: BaseException | None = None
        self._tasks: set[asyncio.Task[Any]] = set()
        self._live_task: asyncio.Task[Any] | None = None
        self._duration_task: asyncio.Task[Any] | None = None
        self._live: GeminiLiveSession | None = None
        self._input_queue: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=100)
        self._input_buffer = bytearray()
        self._input_ended = asyncio.Event()
        self._output_queue: asyncio.Queue[_OutputItem] = asyncio.Queue()
        self._resampler = StreamingPcmResampler()
        self._generation = 0
        self._playback_generation: int | None = None
        self._buffered_bytes = 0
        self._playback_deadline = 0.0
        self._playback_interrupted = asyncio.Event()
        self._opening_complete = False
        self._speaking = False
        self._finish_started = False
        self._pending_finish: tuple[int, LiveEvent] | None = None
        self._input_transcript_parts: list[str] = []
        self._output_transcript_parts: list[str] = []
        self._last_input_transcript = ""
        self._last_output_transcript = ""
        self._send_lock = asyncio.Lock()
        self._can_send = asyncio.Event()
        self._can_send.set()
        self._audio_messages = 0
        self._audio_bytes = 0

    async def run(self) -> None:
        try:
            async for message in self.websocket:
                if isinstance(message, str):
                    await self._handle_control(message)
                else:
                    await self._handle_audio(message)
        finally:
            await self.close()
        if self._failure:
            raise RuntimeError("Gemini Live media task failed") from self._failure

    async def close(self) -> None:
        owner = False
        async with self._close_lock:
            if not self._closing:
                self._closing = True
                owner = True
        if not owner:
            await self._closed_event.wait()
            return

        try:
            if self._opening_complete and self._input_buffer:
                self._queue_input(bytes(self._input_buffer))
            self._input_buffer.clear()
            self._queue_input(None)
            if self._live_task:
                try:
                    await asyncio.wait_for(self._input_ended.wait(), timeout=1.0)
                    await asyncio.sleep(1.0)
                except TimeoutError:
                    pass

            current = asyncio.current_task()
            tasks = [task for task in self._tasks if task is not current]
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            await self._flush_transcript("input")
            await self._flush_transcript("output")
            self._playback_interrupted.set()
            await self.websocket.close()
            logger.info(
                "media session closed for call %s after %s messages / %s bytes",
                self.call_id,
                self._audio_messages,
                self._audio_bytes,
            )
        finally:
            self._closed_event.set()

    async def _handle_control(self, raw: str) -> None:
        try:
            message = json.loads(raw)
            event = str(message.get("event", ""))
        except json.JSONDecodeError:
            parts = raw.split()
            event = parts[0] if parts else ""
            message = {"correlation_id": parts[1] if len(parts) > 1 else ""}

        if event == "MEDIA_START" and not self._started:
            self._started = True
            logger.info(
                "media started for call %s: format=%s ptime=%sms optimal=%s",
                self.call_id,
                message.get("format"),
                message.get("ptime"),
                message.get("optimal_frame_size"),
            )
            self._duration_task = self._spawn(self._duration_limit(), "duration-limit")
            self._spawn(self._output_writer(), "output-writer")
            self._live_task = self._spawn(self._run_live(), "gemini-live")
        elif event == "MEDIA_XOFF":
            logger.info("media flow paused for call %s", self.call_id)
            self._can_send.clear()
        elif event == "MEDIA_XON":
            logger.info("media flow resumed for call %s", self.call_id)
            self._can_send.set()

    async def _handle_audio(self, data: bytes) -> None:
        self._audio_messages += 1
        self._audio_bytes += len(data)
        if self._audio_messages == 1:
            logger.info("first inbound media for call %s: %s bytes", self.call_id, len(data))
        if not self._started or not self._opening_complete or self._closing:
            return
        self._input_buffer.extend(data)
        while len(self._input_buffer) >= 3_200:
            chunk = bytes(self._input_buffer[:3_200])
            del self._input_buffer[:3_200]
            await self._input_queue.put(chunk)

    async def _run_live(self) -> None:
        call = await self._require_call()
        manager = self.gemini.connect_live(call)
        live = await asyncio.wait_for(
            manager.__aenter__(),
            timeout=self.settings.gemini_live_connect_timeout_seconds,
        )
        self._live = live
        sender: asyncio.Task[Any] | None = None
        receiver: asyncio.Task[Any] | None = None
        try:
            logger.info(
                "connected Gemini Live for call %s using %s",
                self.call_id,
                self.settings.gemini_live_model,
            )
            await live.start_conversation()
            sender = asyncio.create_task(
                self._input_sender(live), name=f"live-input-{self.call_id}"
            )
            receiver = asyncio.create_task(
                self._live_receiver(live), name=f"live-output-{self.call_id}"
            )
            done, pending = await asyncio.wait(
                {sender, receiver}, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                exception = task.exception()
                if exception:
                    raise exception
            if receiver in done and not self._closing:
                raise RuntimeError("Gemini Live response stream ended unexpectedly")
            await asyncio.gather(*pending)
        finally:
            for task in (sender, receiver):
                if task and not task.done():
                    task.cancel()
            await asyncio.gather(
                *(task for task in (sender, receiver) if task),
                return_exceptions=True,
            )
            self._live = None
            await manager.__aexit__(None, None, None)

    async def _input_sender(self, live: GeminiLiveSession) -> None:
        try:
            while True:
                chunk = await self._input_queue.get()
                if chunk is None:
                    await live.end_audio()
                    return
                await live.send_audio(chunk)
        finally:
            self._input_ended.set()

    async def _live_receiver(self, live: GeminiLiveSession) -> None:
        async for event in live.events():
            if event.kind == "audio" and event.data:
                pcm = self._resampler.convert(event.data, event.mime_type)
                if pcm:
                    await self._output_queue.put(
                        _OutputItem(kind="audio", generation=self._generation, data=pcm)
                    )
            elif event.kind == "input_transcript":
                await self._append_transcript("input", event.text or "", event.finished)
            elif event.kind == "output_transcript":
                await self._append_transcript("output", event.text or "", False)
            elif event.kind == "interrupted":
                if self._speaking:
                    await self._handle_interruption()
            elif event.kind == "finish":
                self._pending_finish = (self._generation, event)
                await self._output_queue.put(
                    _OutputItem(kind="turn_end", generation=self._generation)
                )
            elif event.kind == "finish_cancelled":
                if (
                    self._pending_finish
                    and self._pending_finish[1].tool_call_id == event.tool_call_id
                ):
                    self._pending_finish = None
            elif event.kind == "turn_complete":
                await self._flush_transcript("input")
                await self._flush_transcript("output")
                await self._output_queue.put(
                    _OutputItem(kind="turn_end", generation=self._generation)
                )
            elif event.kind == "go_away":
                raise RuntimeError("Gemini Live requested session termination")

    async def _output_writer(self) -> None:
        while True:
            item = await self._output_queue.get()
            if item.generation != self._generation:
                continue
            if item.kind == "audio":
                if self._playback_generation != item.generation:
                    self._playback_generation = item.generation
                    self._buffered_bytes = 0
                    self._playback_deadline = asyncio.get_running_loop().time()
                    self._playback_interrupted = asyncio.Event()
                    self._speaking = True
                for offset in range(0, len(item.data), 32_000):
                    if item.generation != self._generation or self._closing:
                        break
                    await self._can_send.wait()
                    async with self._send_lock:
                        chunk = item.data[offset : offset + 32_000]
                        await self.websocket.send(chunk)
                    now = asyncio.get_running_loop().time()
                    self._playback_deadline = max(self._playback_deadline, now) + len(
                        chunk
                    ) / (16_000 * 2)
                    self._buffered_bytes += len(chunk)
            else:
                await self._finish_output_turn(item.generation)
                if self._finish_started:
                    return

    async def _finish_output_turn(self, generation: int) -> None:
        if generation != self._generation:
            return
        if self._playback_generation == generation:
            mark_id = str(uuid.uuid4())
            await self._send_control("MARK_MEDIA", correlation_id=mark_id)
            expected_seconds = self._buffered_bytes / (16_000 * 2)
            remaining_seconds = max(
                0.0,
                self._playback_deadline - asyncio.get_running_loop().time(),
            )
            logger.info(
                "streamed %.2fs Gemini Live audio for call %s (%.2fs still queued)",
                expected_seconds,
                self.call_id,
                remaining_seconds,
            )
            try:
                if remaining_seconds:
                    await asyncio.wait_for(
                        self._playback_interrupted.wait(),
                        timeout=remaining_seconds,
                    )
            except TimeoutError:
                pass
            finally:
                if generation == self._generation:
                    self._playback_generation = None
                    self._buffered_bytes = 0
                    self._playback_deadline = 0.0
                    self._speaking = False
            if generation != self._generation:
                return

        if not self._opening_complete:
            self._opening_complete = True
            logger.info("Gemini Live opening completed for call %s", self.call_id)

        if (
            self._pending_finish
            and self._pending_finish[0] == generation
            and not self._finish_started
        ):
            self._finish_started = True
            event = self._pending_finish[1]
            if event.internal_note:
                await self.database.add_event(
                    self.call_id,
                    "agent_note",
                    {"note": event.internal_note},
                )
            await self.finish_callback(
                self.call_id,
                event.action or "end",
                event.hermes_question,
            )
        elif not self._finish_started and any(
            phrase in self._last_output_transcript.casefold() for phrase in FAREWELL_PHRASES
        ):
            self._finish_started = True
            await self.database.add_event(
                self.call_id,
                "finish_fallback",
                {"reason": "spoken_farewell"},
            )
            logger.info("ending call %s after spoken farewell", self.call_id)
            await self.finish_callback(self.call_id, "end", None)

    async def _handle_interruption(self) -> None:
        interrupted_generation = self._generation
        await self._flush_transcript("output")
        self._generation += 1
        self._pending_finish = None
        self._resampler.reset()
        while True:
            try:
                self._output_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        self._playback_generation = None
        self._buffered_bytes = 0
        self._playback_deadline = 0.0
        self._speaking = False
        self._playback_interrupted.set()
        self._can_send.set()
        await self._send_control("FLUSH_MEDIA")
        await self.database.add_event(
            self.call_id,
            "agent_interrupted",
            {"generation": interrupted_generation},
        )
        logger.info("flushed interrupted Gemini audio for call %s", self.call_id)

    async def _append_transcript(self, speaker: str, text: str, finished: bool) -> None:
        parts = (
            self._input_transcript_parts
            if speaker == "input"
            else self._output_transcript_parts
        )
        if text:
            parts.append(text)
        if finished:
            await self._flush_transcript(speaker)

    async def _flush_transcript(self, speaker: str) -> None:
        if speaker == "input":
            parts = self._input_transcript_parts
            last = self._last_input_transcript
            role = "callee"
        else:
            parts = self._output_transcript_parts
            last = self._last_output_transcript
            role = "agent"
        text = "".join(parts).strip()
        parts.clear()
        if not text or text == last:
            return
        await self.database.add_turn(self.call_id, role, text)
        if speaker == "input":
            self._last_input_transcript = text
        else:
            self._last_output_transcript = text

    async def _send_control(self, command: str, **parameters: str) -> None:
        if self._closing:
            return
        payload = {"command": command, **parameters}
        async with self._send_lock:
            await self.websocket.send(json.dumps(payload))

    async def _duration_limit(self) -> None:
        call = await self._require_call()
        duration = min(call["max_duration_seconds"], self.settings.max_call_duration)
        await asyncio.sleep(duration)
        if not self._closing and not self._finish_started:
            self._finish_started = True
            await self.database.add_event(
                self.call_id,
                "duration_limit_reached",
                {"seconds": duration},
            )
            await self.finish_callback(self.call_id, "end", None)

    async def _require_call(self) -> dict[str, Any]:
        call = await self.database.get_call(self.call_id)
        if call is None:
            raise RuntimeError(f"call disappeared: {self.call_id}")
        return call

    def _queue_input(self, value: bytes | None) -> None:
        try:
            self._input_queue.put_nowait(value)
        except asyncio.QueueFull:
            self._input_queue.get_nowait()
            self._input_queue.put_nowait(value)

    def _spawn(self, coroutine: Awaitable[Any], name: str) -> asyncio.Task[Any]:
        task = asyncio.create_task(coroutine, name=f"{name}-{self.call_id}")
        self._tasks.add(task)

        def done(finished: asyncio.Task[Any]) -> None:
            self._tasks.discard(finished)
            if finished.cancelled() or self._closing:
                return
            exception = finished.exception()
            if exception:
                self._failure = exception
                logger.error(
                    "%s task failed for call %s: %s",
                    name,
                    self.call_id,
                    exception,
                )
                asyncio.create_task(
                    self.websocket.close(code=1011, reason="call media task failed")
                )

        task.add_done_callback(done)
        return task

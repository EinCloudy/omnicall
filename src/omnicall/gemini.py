from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Literal, TypeVar

from google import genai
from google.genai import errors, types

from .config import Settings

logger = logging.getLogger(__name__)
T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class LiveEvent:
    kind: Literal[
        "audio",
        "input_transcript",
        "output_transcript",
        "interrupted",
        "turn_complete",
        "finish",
        "finish_cancelled",
        "go_away",
    ]
    data: bytes | None = None
    mime_type: str | None = None
    text: str | None = None
    finished: bool = False
    tool_call_id: str | None = None
    action: str | None = None
    hermes_question: str | None = None
    internal_note: str | None = None


class GeminiLiveSession:
    def __init__(self, session: Any, *, consent_required: bool = True) -> None:
        self._session = session
        self._consent_required = consent_required
        self._audio_ended = False

    async def start_conversation(self) -> None:
        if self._consent_required:
            prompt = (
                "Beginne jetzt das Telefonat mit der vorgeschriebenen transparenten "
                "Begrüßung und Einwilligungsfrage."
            )
        else:
            prompt = (
                "Beginne jetzt das Telefonat mit der vorgeschriebenen transparenten "
                "Begrüßung und gehe danach direkt zum eigentlichen Anliegen über."
            )
        await self._session.send_realtime_input(text=prompt)

    async def send_audio(self, pcm: bytes) -> None:
        if self._audio_ended:
            return
        await self._session.send_realtime_input(
            audio=types.Blob(data=pcm, mime_type="audio/pcm;rate=16000")
        )

    async def end_audio(self) -> None:
        if self._audio_ended:
            return
        self._audio_ended = True
        await self._session.send_realtime_input(audio_stream_end=True)

    async def events(self) -> AsyncIterator[LiveEvent]:
        while True:
            received = False
            async for response in self._session.receive():
                received = True
                async for event in self._decode_response(response):
                    yield event
            if not received or self._audio_ended:
                return

    async def _decode_response(self, response: Any) -> AsyncIterator[LiveEvent]:
        content = response.server_content
        if content:
            if content.input_transcription:
                transcript = content.input_transcription
                yield LiveEvent(
                    kind="input_transcript",
                    text=transcript.text or "",
                    finished=bool(transcript.finished),
                )
            if content.output_transcription:
                transcript = content.output_transcription
                yield LiveEvent(
                    kind="output_transcript",
                    text=transcript.text or "",
                    finished=bool(transcript.finished),
                )
            if content.interrupted:
                yield LiveEvent(kind="interrupted")
            if content.model_turn:
                for part in content.model_turn.parts or []:
                    blob = part.inline_data
                    if blob and blob.data:
                        yield LiveEvent(
                            kind="audio",
                            data=blob.data,
                            mime_type=blob.mime_type or "audio/pcm;rate=24000",
                        )
            if content.turn_complete:
                yield LiveEvent(kind="turn_complete")

        if response.tool_call:
            function_responses: list[types.FunctionResponse] = []
            for function_call in response.tool_call.function_calls or []:
                call_id = function_call.id
                name = function_call.name or ""
                if not call_id:
                    raise RuntimeError("Gemini function call did not contain an ID")
                if name != "finish_call":
                    function_responses.append(
                        types.FunctionResponse(
                            id=call_id,
                            name=name,
                            response={"error": f"unknown function: {name}"},
                        )
                    )
                    continue
                arguments = function_call.args or {}
                action = str(arguments.get("action", "end"))
                if action not in {"end", "needs_hermes"}:
                    action = "end"
                question = str(arguments.get("hermes_question", "")).strip() or None
                note = str(arguments.get("internal_note", "")).strip() or None
                function_responses.append(
                    types.FunctionResponse(
                        id=call_id,
                        name=name,
                        response={"output": "The call will end after queued speech is played."},
                    )
                )
                yield LiveEvent(
                    kind="finish",
                    tool_call_id=call_id,
                    action=action,
                    hermes_question=question,
                    internal_note=note,
                )
            if function_responses:
                await self._session.send_tool_response(function_responses=function_responses)

        if response.tool_call_cancellation:
            for call_id in response.tool_call_cancellation.ids or []:
                yield LiveEvent(kind="finish_cancelled", tool_call_id=call_id)

        if response.go_away:
            yield LiveEvent(kind="go_away")


class GeminiClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._client = genai.Client(
            api_key=settings.gemini_api_key,
            http_options=types.HttpOptions(
                api_version="v1beta",
                timeout=int(settings.gemini_timeout_seconds * 1000),
                retry_options=types.HttpRetryOptions(attempts=1),
            ),
        )
        self._aio = self._client.aio

    async def close(self) -> None:
        await self._aio.aclose()
        self._client.close()

    @asynccontextmanager
    async def connect_live(self, call: dict[str, Any]) -> AsyncIterator[GeminiLiveSession]:
        config = self.build_live_config(call)
        async with self._aio.live.connect(
            model=self.settings.gemini_live_model,
            config=config,
        ) as session:
            yield GeminiLiveSession(
                session,
                consent_required=self.settings.consent_question_enabled,
            )

    def build_live_config(self, call: dict[str, Any]) -> types.LiveConnectConfig:
        context = json.dumps(call["context"], ensure_ascii=False, indent=2)
        opening = call.get("opening_message") or "(keine besondere Formulierung vorgegeben)"
        if self.settings.consent_question_enabled:
            opening_rules = """
- Deine allererste Äußerung lautet wortgetreu: "Guten Tag, ich bin ein KI-gestützter digitaler
  Assistent. Dieses Gespräch wird zur Bearbeitung transkribiert. Sind Sie damit einverstanden?"
- Die erste Äußerung endet unmittelbar nach der Einwilligungsfrage. Stelle darin keine zweite Frage,
  beginne das eigentliche Anliegen noch nicht und ergänze keinen Smalltalk.
- Fahre erst nach ausdrücklicher Zustimmung mit dem eigentlichen Ziel fort.
- Bei abgelehnter Transkription entschuldigst du dich, verabschiedest dich und rufst danach
  finish_call mit action=end auf.
""".strip()
        else:
            opening_rules = """
- Deine allererste Äußerung beginnt wortgetreu mit: "Guten Tag, ich bin ein KI-gestützter digitaler
  Assistent. Dieses Gespräch wird zur Bearbeitung transkribiert."
- Stelle keine Einwilligungsfrage. Gehe nach dieser Eröffnung direkt und ohne Warten auf eine
  Zustimmung zum eigentlichen Anliegen über.
""".strip()
        instruction = f"""
Du führst im Auftrag von OmniCall einen echten Telefonanruf auf Deutsch.

Ziel des Anrufs:
{call["objective"]}

Relevanter Kontext (JSON):
{context}

Optionale gewünschte Eröffnung:
{opening}

Regeln:
- Sprich natürlich, knapp und freundlich. Stelle jeweils nur eine Hauptfrage.
- Erfinde keine Fakten, Zusagen, Identitäten oder Befugnisse.
- Nenne Zahlen, Namen und Termine eindeutig und bestätige kritische Angaben.
{opening_rules}
- Behaupte niemals, dass Audio aufgezeichnet oder zur Verbesserung ausgewertet wird. Es werden
  ausschließlich Transkripte zur Bearbeitung erstellt.
- Wenn das Ziel erreicht ist oder die Person das Gespräch beenden möchte, verabschiedest du dich
  zuerst hörbar, beendest deine Verabschiedung mit dem Satz "Auf Wiederhören." und rufst danach
  finish_call mit action=end auf.
- Falls zwingend Informationen oder eine Entscheidung des Auftraggebers fehlen, erkläre die
  Rücksprache,
  verabschiede dich und rufe danach finish_call mit action=needs_hermes und einer präzisen
  hermes_question auf.
- Rufe finish_call nie auf, bevor dein abschließender gesprochener Satz erzeugt wurde.
""".strip()
        finish_tool = types.Tool(
            function_declarations=[
                types.FunctionDeclaration(
                    name="finish_call",
                    description=(
                        "Ends the physical phone call after the final spoken response has played."
                    ),
                    parameters_json_schema={
                        "type": "object",
                        "properties": {
                            "action": {
                                "type": "string",
                                "enum": ["end", "needs_hermes"],
                            },
                            "hermes_question": {"type": "string"},
                            "internal_note": {"type": "string"},
                        },
                        "required": ["action", "hermes_question", "internal_note"],
                        "additionalProperties": False,
                    },
                )
            ]
        )
        return types.LiveConnectConfig(
            response_modalities=[types.Modality.AUDIO],
            system_instruction=instruction,
            speech_config=types.SpeechConfig(
                voice_config=types.VoiceConfig(
                    prebuilt_voice_config=types.PrebuiltVoiceConfig(
                        voice_name=self.settings.gemini_voice
                    )
                )
            ),
            input_audio_transcription=types.AudioTranscriptionConfig(),
            output_audio_transcription=types.AudioTranscriptionConfig(),
            realtime_input_config=types.RealtimeInputConfig(
                automatic_activity_detection=types.AutomaticActivityDetection(
                    disabled=False,
                    start_of_speech_sensitivity=types.StartSensitivity.START_SENSITIVITY_LOW,
                    end_of_speech_sensitivity=types.EndSensitivity.END_SENSITIVITY_LOW,
                    prefix_padding_ms=self.settings.gemini_vad_prefix_padding_ms,
                    silence_duration_ms=self.settings.gemini_vad_silence_ms,
                ),
                activity_handling=types.ActivityHandling.START_OF_ACTIVITY_INTERRUPTS,
                turn_coverage=types.TurnCoverage.TURN_INCLUDES_ONLY_ACTIVITY,
            ),
            tools=[finish_tool],
        )

    async def summarize(self, call: dict[str, Any], turns: list[dict[str, Any]]) -> str:
        transcript = "\n".join(f"{turn['speaker']}: {turn['text']}" for turn in turns)
        prompt = f"""
Fasse dieses Telefonat für den OmniCall-Auftraggeber knapp und sachlich auf Deutsch zusammen.
Nenne Ergebnis, getroffene Vereinbarungen, bestätigte Kontaktdaten/Termine und offene Punkte.
Keine Spekulationen, keine Überschrift und kein Markdown. Maximal 120 Wörter.

Ziel: {call["objective"]}
Status: {call["status"]}
Gespräch:
{transcript or "(kein Gespräch zustande gekommen)"}
""".strip()

        async def request(model: str) -> str:
            response = await self._aio.models.generate_content(
                model=model,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0.1,
                    max_output_tokens=800,
                    thinking_config=types.ThinkingConfig(
                        thinking_level=types.ThinkingLevel.LOW
                    ),
                    response_mime_type="application/json",
                    response_json_schema={
                        "type": "object",
                        "properties": {"summary": {"type": "string"}},
                        "required": ["summary"],
                        "additionalProperties": False,
                    },
                    automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True),
                ),
            )
            try:
                payload = json.loads(response.text or "")
                summary = str(payload["summary"]).strip()
            except (json.JSONDecodeError, KeyError, TypeError) as exc:
                raise RuntimeError(f"{model} returned an invalid summary") from exc
            if len(summary) < 30 or summary[-1] not in ".!?":
                raise RuntimeError(f"{model} returned an incomplete summary")
            return summary

        try:
            summary = await self._with_retry(
                "summary",
                lambda: request(self.settings.gemini_summary_model),
                attempts=2,
            )
        except RuntimeError:
            logger.warning(
                "%s failed for summary; falling back to %s",
                self.settings.gemini_summary_model,
                self.settings.gemini_summary_fallback_model,
            )
            summary = await self._with_retry(
                "fallback summary",
                lambda: request(self.settings.gemini_summary_fallback_model),
                attempts=2,
            )
        return summary

    async def _with_retry(
        self,
        operation: str,
        request: Callable[[], Awaitable[T]],
        *,
        attempts: int = 4,
    ) -> T:
        for attempt in range(attempts):
            try:
                return await request()
            except errors.APIError as exc:
                code = exc.code or 0
                retryable = code in {408, 409, 429} or code >= 500
                if not retryable or attempt == attempts - 1:
                    raise RuntimeError(f"Gemini {operation} failed: {exc.message}") from exc
                delay = min(8.0, 0.75 * (2**attempt))
                logger.warning(
                    "Gemini %s temporarily failed (HTTP %s), retrying in %.2fs",
                    operation,
                    code,
                    delay,
                )
                await asyncio.sleep(delay)
            except (TimeoutError, ConnectionError) as exc:
                if attempt == attempts - 1:
                    raise RuntimeError(f"Gemini {operation} timed out") from exc
                await asyncio.sleep(min(8.0, 0.75 * (2**attempt)))
        raise AssertionError("retry loop exhausted")

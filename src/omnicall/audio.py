from __future__ import annotations

import audioop
import io
import re
import wave

SAMPLE_RATE = 16_000
SAMPLE_WIDTH = 2
CHANNELS = 1


class StreamingPcmResampler:
    def __init__(self, target_rate: int = SAMPLE_RATE) -> None:
        self.target_rate = target_rate
        self._source_rate: int | None = None
        self._state: tuple | None = None
        self._pending = b""

    def convert(self, data: bytes, mime_type: str | None) -> bytes:
        source_rate = _sample_rate_from_mime(mime_type) or 24_000
        if self._source_rate not in {None, source_rate}:
            self.reset()
        self._source_rate = source_rate
        pcm = self._pending + data
        complete = len(pcm) - (len(pcm) % SAMPLE_WIDTH)
        self._pending = pcm[complete:]
        pcm = pcm[:complete]
        if not pcm or source_rate == self.target_rate:
            return pcm
        converted, self._state = audioop.ratecv(
            pcm,
            SAMPLE_WIDTH,
            CHANNELS,
            source_rate,
            self.target_rate,
            self._state,
        )
        return converted

    def reset(self) -> None:
        self._source_rate = None
        self._state = None
        self._pending = b""


def pcm_to_wav(pcm: bytes, sample_rate: int = SAMPLE_RATE) -> bytes:
    output = io.BytesIO()
    with wave.open(output, "wb") as wav:
        wav.setnchannels(CHANNELS)
        wav.setsampwidth(SAMPLE_WIDTH)
        wav.setframerate(sample_rate)
        wav.writeframes(pcm)
    return output.getvalue()


def normalize_tts_audio(data: bytes, mime_type: str | None) -> bytes:
    sample_rate = _sample_rate_from_mime(mime_type) or 24_000
    pcm = data
    if data.startswith(b"RIFF"):
        with wave.open(io.BytesIO(data), "rb") as wav:
            sample_rate = wav.getframerate()
            channels = wav.getnchannels()
            width = wav.getsampwidth()
            pcm = wav.readframes(wav.getnframes())
        if width != SAMPLE_WIDTH:
            pcm = audioop.lin2lin(pcm, width, SAMPLE_WIDTH)
        if channels == 2:
            pcm = audioop.tomono(pcm, SAMPLE_WIDTH, 0.5, 0.5)
        elif channels != 1:
            raise ValueError(f"unsupported TTS channel count: {channels}")
    if len(pcm) % SAMPLE_WIDTH:
        pcm = pcm[: -(len(pcm) % SAMPLE_WIDTH)]
    if sample_rate != SAMPLE_RATE:
        pcm, _ = audioop.ratecv(pcm, SAMPLE_WIDTH, CHANNELS, sample_rate, SAMPLE_RATE, None)
    return pcm


def _sample_rate_from_mime(mime_type: str | None) -> int | None:
    if not mime_type:
        return None
    match = re.search(r"rate=(\d+)", mime_type, flags=re.IGNORECASE)
    return int(match.group(1)) if match else None

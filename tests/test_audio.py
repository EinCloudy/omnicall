import io
import struct
import wave

from hermes_calling.audio import StreamingPcmResampler, normalize_tts_audio, pcm_to_wav


def test_pcm_to_wav_has_expected_format() -> None:
    pcm = b"\x01\x00" * 160
    result = pcm_to_wav(pcm)

    with wave.open(io.BytesIO(result), "rb") as wav:
        assert wav.getnchannels() == 1
        assert wav.getsampwidth() == 2
        assert wav.getframerate() == 16_000
        assert wav.readframes(160) == pcm


def test_normalize_tts_audio_resamples_raw_24khz_pcm() -> None:
    source = b"".join(struct.pack("<h", index % 1000) for index in range(2_400))
    result = normalize_tts_audio(source, "audio/L16;codec=pcm;rate=24000")

    assert 3_190 <= len(result) <= 3_210
    assert len(result) % 2 == 0


def test_streaming_resampler_matches_whole_buffer_across_odd_chunks() -> None:
    source = b"".join(struct.pack("<h", index % 1000) for index in range(2_400))
    expected = normalize_tts_audio(source, "audio/pcm;rate=24000")
    converter = StreamingPcmResampler()

    result = b"".join(
        [
            converter.convert(source[:101], "audio/pcm;rate=24000"),
            converter.convert(source[101:2003], "audio/pcm;rate=24000"),
            converter.convert(source[2003:], "audio/pcm;rate=24000"),
        ]
    )

    assert result == expected

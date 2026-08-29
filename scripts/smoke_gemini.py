import asyncio

from dotenv import load_dotenv

from hermes_calling.config import Settings
from hermes_calling.gemini import GeminiClient


async def main() -> None:
    load_dotenv()
    settings = Settings.from_env()
    client = GeminiClient(settings)
    audio_bytes = 0
    transcript: list[str] = []
    summary = ""
    try:
        async with asyncio.timeout(30):
            async with client.connect_live(
                {
                    "objective": "Führe einen kurzen Live-Audiotest durch.",
                    "context": {},
                    "opening_message": None,
                }
            ) as live:
                await live.start_conversation()
                async for event in live.events():
                    if event.kind == "audio" and event.data:
                        audio_bytes += len(event.data)
                    elif event.kind == "output_transcript" and event.text:
                        transcript.append(event.text)
                    elif event.kind == "turn_complete":
                        await live.end_audio()
                        break
        summary = await client.summarize(
            {"objective": "Live-Audiotest", "status": "completed"},
            [
                {"speaker": "agent", "text": "Bitte sagen Sie Test."},
                {"speaker": "callee", "text": "Test"},
            ],
        )
        print(
            {
                "model": settings.gemini_live_model,
                "audio_bytes": audio_bytes,
                "transcript": "".join(transcript).strip(),
                "summary": summary,
            }
        )
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())

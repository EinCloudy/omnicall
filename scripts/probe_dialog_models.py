import asyncio
import os
import time

from dotenv import load_dotenv
from google import genai
from google.genai import types

MODELS = [
    "gemini-3.7-flash",
    "gemini-3.6-flash",
    "gemini-3.5-flash",
    "gemini-3.1-flash-lite",
    "gemini-3-flash-preview",
    "gemini-2.5-flash",
]


async def main() -> None:
    load_dotenv()
    client = genai.Client(
        api_key=os.environ["GEMINI_API_KEY"],
        http_options=types.HttpOptions(
            api_version="v1beta",
            timeout=20_000,
            retry_options=types.HttpRetryOptions(attempts=1),
        ),
    )
    try:
        for model in MODELS:
            started = time.monotonic()
            try:
                response = await client.aio.models.generate_content(
                    model=model,
                    contents="Antworte knapp mit einem freundlichen Hallo.",
                    config=types.GenerateContentConfig(
                        max_output_tokens=300,
                        thinking_config=types.ThinkingConfig(thinking_level="LOW"),
                        response_mime_type="application/json",
                        response_json_schema={
                            "type": "object",
                            "properties": {"speech": {"type": "string"}},
                            "required": ["speech"],
                            "additionalProperties": False,
                        },
                    ),
                )
                print(model, round(time.monotonic() - started, 2), response.text)
            except Exception as exc:
                print(model, round(time.monotonic() - started, 2), type(exc).__name__, exc)
    finally:
        await client.aio.aclose()
        client.close()


if __name__ == "__main__":
    asyncio.run(main())

import argparse
import asyncio
import json
from typing import Any

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

TERMINAL_STATUSES = {"completed", "needs_input", "failed", "cancelled"}


def result_value(result: Any) -> Any:
    if result.structuredContent:
        value = result.structuredContent
        return value.get("result", value)
    return json.loads(result.content[0].text)


DEFAULT_OBJECTIVE = (
    "Führe einen kurzen Systemtest durch. Bitte nach der verpflichtenden KI- und "
    "Transkriptionsaufklärung um Zustimmung und anschließend darum, einmal das Wort "
    "'Test' zu sagen. Bestätige die Erkennung und beende das Gespräch danach freundlich."
)


async def main(phone_number: str, objective: str) -> None:
    async with streamablehttp_client("http://127.0.0.1:8090/mcp") as streams:
        read_stream, write_stream, _ = streams
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            started = result_value(
                await session.call_tool(
                    "start_call",
                    arguments={
                        "phone_number": phone_number,
                        "objective": objective,
                        "max_duration_seconds": 120,
                    },
                )
            )
            call_id = started["call_id"]
            print({"call_id": call_id, "status": started["status"]})

            last_status = None
            terminal_polls = 0
            for _ in range(75):
                call = result_value(
                    await session.call_tool(
                        "get_call",
                        arguments={"call_id": call_id, "include_transcript": True},
                    )
                )
                if call["status"] != last_status:
                    print({"status": call["status"], "error": call.get("error")})
                    last_status = call["status"]
                if call["status"] in TERMINAL_STATUSES:
                    terminal_polls += 1
                if call["status"] in TERMINAL_STATUSES and (
                    call.get("summary") or terminal_polls >= 8
                ):
                    print(
                        json.dumps(
                            {
                                "status": call["status"],
                                "summary": call.get("summary"),
                                "error": call.get("error"),
                                "transcript": call.get("transcript", []),
                            },
                            ensure_ascii=False,
                            indent=2,
                        )
                    )
                    return
                await asyncio.sleep(2)
            raise TimeoutError(f"call {call_id} did not finish within 150 seconds")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("phone_number")
    parser.add_argument("--objective", default=DEFAULT_OBJECTIVE)
    args = parser.parse_args()
    asyncio.run(main(args.phone_number, args.objective))

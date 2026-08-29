from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from mcp.server.fastmcp import FastMCP

from .config import Settings
from .database import VALID_STATUSES
from .service import CallService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)
_service: CallService | None = None
_bootstrap = Settings.from_env(require_secrets=False)


@asynccontextmanager
async def lifespan(_: FastMCP):
    global _service
    settings = Settings.from_env()
    service = CallService(settings)
    await service.start()
    _service = service
    logger.info("Hermes calling service started")
    try:
        yield
    finally:
        _service = None
        await service.close()
        logger.info("Hermes calling service stopped")


mcp = FastMCP(
    "Hermes Calling",
    instructions=(
        "Use start_call for outbound phone tasks, then poll get_updates with the returned "
        "event cursor. Use get_call for the full persisted transcript and follow_up_call "
        "when a completed call requires another phone conversation. Never invent a phone "
        "number or call objective."
    ),
    lifespan=lifespan if _bootstrap.mcp_transport == "stdio" else None,
    host=_bootstrap.mcp_host,
    port=_bootstrap.mcp_port,
    stateless_http=True,
)


@asynccontextmanager
async def http_lifespan(_: Any):
    async with lifespan(mcp):
        async with mcp.session_manager.run():
            yield


async def run_http() -> None:
    app = mcp.streamable_http_app()
    app.router.lifespan_context = http_lifespan
    config = uvicorn.Config(
        app,
        host=_bootstrap.mcp_host,
        port=_bootstrap.mcp_port,
        log_level="info",
    )
    await uvicorn.Server(config).serve()


def service() -> CallService:
    if _service is None:
        raise RuntimeError("calling service is not ready")
    return _service


@mcp.tool()
async def start_call(
    phone_number: str,
    objective: str,
    context: dict[str, Any] | None = None,
    opening_message: str | None = None,
    max_duration_seconds: int | None = None,
) -> dict[str, Any]:
    """Queue an outbound AI phone call and return its persistent call ID.

    Args:
        phone_number: Destination number, preferably in E.164 format such as +4962112345.
        objective: Concrete outcome and boundaries for the phone conversation.
        context: Relevant facts the calling agent may use; omit secrets that are not needed.
        opening_message: Optional wording for the greeting after mandatory AI disclosure.
        max_duration_seconds: Optional duration cap, bounded by the server maximum.
    """
    call = await service().start_call(
        phone_number=phone_number,
        objective=objective,
        context=context,
        opening_message=opening_message,
        max_duration_seconds=max_duration_seconds,
    )
    return {
        "call_id": call["id"],
        "status": call["status"],
        "created_at": call["created_at"],
        "next_step": "Poll get_updates(after_event_id=0, call_id=call_id).",
    }


@mcp.tool()
async def follow_up_call(
    previous_call_id: str,
    instructions: str,
    phone_number: str | None = None,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Start a follow-up call with the previous transcript and result as context.

    Args:
        previous_call_id: Persistent ID of the earlier call.
        instructions: New facts, answer, or objective for this follow-up.
        phone_number: Optional different destination; defaults to the previous number.
        context: Optional additional structured facts.
    """
    call = await service().follow_up_call(
        previous_call_id=previous_call_id,
        instructions=instructions,
        phone_number=phone_number,
        context=context,
    )
    return {
        "call_id": call["id"],
        "parent_call_id": previous_call_id,
        "status": call["status"],
    }


@mcp.tool()
async def get_call(call_id: str, include_transcript: bool = True) -> dict[str, Any]:
    """Get current call state, result, open Hermes question, and persisted transcript."""
    call = await service().database.get_call(call_id, include_transcript=include_transcript)
    if call is None:
        raise ValueError(f"call not found: {call_id}")
    return call


@mcp.tool()
async def get_updates(
    after_event_id: int = 0,
    call_id: str | None = None,
    wait_seconds: float = 0,
    limit: int = 100,
) -> dict[str, Any]:
    """Poll persistent call events after a cursor, optionally using a short long-poll.

    Save next_event_id and pass it as after_event_id on the next call. Events include status
    changes, transcript turns, summaries, and questions that require Hermes input.
    """
    if after_event_id < 0:
        raise ValueError("after_event_id must not be negative")
    if not 1 <= limit <= 500:
        raise ValueError("limit must be between 1 and 500")
    return await service().database.get_events(
        after_event_id=after_event_id,
        call_id=call_id,
        wait_seconds=max(0, min(wait_seconds, 25)),
        limit=limit,
    )


@mcp.tool()
async def list_calls(status: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
    """List recent persisted calls, optionally filtered by status."""
    if status is not None and status not in VALID_STATUSES:
        raise ValueError(f"status must be one of: {', '.join(sorted(VALID_STATUSES))}")
    if not 1 <= limit <= 100:
        raise ValueError("limit must be between 1 and 100")
    return await service().database.list_calls(status=status, limit=limit)


@mcp.tool()
async def cancel_call(call_id: str) -> dict[str, Any]:
    """Cancel a queued or active call. Completed calls remain unchanged."""
    return await service().cancel_call(call_id)


def main() -> None:
    if _bootstrap.mcp_transport == "streamable-http":
        asyncio.run(run_http())
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()

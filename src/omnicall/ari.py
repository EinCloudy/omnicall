from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import aiohttp

from .config import Settings

logger = logging.getLogger(__name__)
EventHandler = Callable[[dict[str, Any]], Awaitable[None]]


class AriError(RuntimeError):
    pass


class AriClient:
    def __init__(self, settings: Settings, event_handler: EventHandler) -> None:
        self.settings = settings
        self.event_handler = event_handler
        self.ready = asyncio.Event()
        self._session: aiohttp.ClientSession | None = None
        self._event_task: asyncio.Task[None] | None = None
        self._stopping = False

    async def start(self) -> None:
        timeout = aiohttp.ClientTimeout(total=15, connect=5)
        self._session = aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(self.settings.ari_user, self.settings.ari_password),
            timeout=timeout,
        )
        self._event_task = asyncio.create_task(self._event_loop(), name="ari-events")

    async def close(self) -> None:
        self._stopping = True
        self.ready.clear()
        if self._event_task:
            self._event_task.cancel()
            await asyncio.gather(self._event_task, return_exceptions=True)
        if self._session:
            await self._session.close()
            self._session = None

    async def wait_ready(self, timeout_seconds: float = 20) -> None:
        try:
            await asyncio.wait_for(self.ready.wait(), timeout=timeout_seconds)
        except TimeoutError as exc:
            raise AriError("Asterisk ARI is unavailable") from exc

    async def originate(
        self,
        *,
        call_id: str,
        number: str,
        caller_id: str,
        timeout_seconds: int,
    ) -> dict[str, Any]:
        params: dict[str, str | int] = {
            "endpoint": f"PJSIP/{number}@{self.settings.sip_endpoint}",
            "app": self.settings.ari_app,
            "appArgs": f"call:{call_id}",
            "channelId": call_id,
            "timeout": timeout_seconds,
        }
        if caller_id:
            params["callerId"] = caller_id
        return await self.request("POST", "/channels", params=params)

    async def create_call_media(self, call_id: str) -> tuple[str, str]:
        bridge_id = f"bridge-{call_id}"
        media_id = f"media-{call_id}"
        await self.request(
            "POST",
            "/bridges",
            params={"type": "mixing,proxy_media", "bridgeId": bridge_id},
        )
        try:
            await self.request(
                "POST",
                "/channels/externalMedia",
                params={
                    "channelId": media_id,
                    "app": self.settings.ari_app,
                    "data": f"media:{call_id}",
                    "external_host": self.settings.media_connection,
                    "transport": "websocket",
                    "encapsulation": "none",
                    "format": "slin16",
                    "transport_data": f"f(json)v(call_id={call_id})",
                },
            )
            retry_delays = (0.05, 0.1, 0.2, 0.4, 0.8, 1.0)
            for attempt in range(len(retry_delays) + 1):
                try:
                    await self.request(
                        "POST",
                        f"/bridges/{bridge_id}/addChannel",
                        params={"channel": f"{call_id},{media_id}"},
                    )
                    break
                except AriError as exc:
                    if "HTTP 422" not in str(exc) or attempt == len(retry_delays):
                        raise
                    await asyncio.sleep(retry_delays[attempt])
        except BaseException:
            await asyncio.gather(
                self.hangup(media_id),
                self.destroy_bridge(bridge_id),
                return_exceptions=True,
            )
            raise
        return bridge_id, media_id

    async def hangup(self, channel_id: str) -> None:
        try:
            await self.request("DELETE", f"/channels/{channel_id}")
        except AriError as exc:
            if "404" not in str(exc):
                raise

    async def destroy_bridge(self, bridge_id: str) -> None:
        try:
            await self.request("DELETE", f"/bridges/{bridge_id}")
        except AriError as exc:
            if "404" not in str(exc):
                raise

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str | int] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if self._session is None:
            raise AriError("ARI client is not started")
        url = f"{self.settings.ari_url.rstrip('/')}/{path.lstrip('/')}"
        try:
            async with self._session.request(
                method, url, params=params, json=json_body
            ) as response:
                if response.status >= 400:
                    detail = (await response.text())[:500]
                    raise AriError(
                        f"ARI {method} {path} failed with HTTP {response.status}: {detail}"
                    )
                if response.status == 204 or response.content_length == 0:
                    return {}
                return await response.json()
        except aiohttp.ClientError as exc:
            raise AriError(f"ARI {method} {path} failed: {exc}") from exc

    async def _event_loop(self) -> None:
        assert self._session is not None
        delay = 0.5
        while not self._stopping:
            params = {
                "app": self.settings.ari_app,
                "api_key": f"{self.settings.ari_user}:{self.settings.ari_password}",
                "subscribeAll": "false",
            }
            try:
                async with self._session.ws_connect(
                    self.settings.ari_ws_url,
                    params=params,
                    heartbeat=20,
                    receive_timeout=45,
                ) as websocket:
                    logger.info("connected to Asterisk ARI")
                    self.ready.set()
                    delay = 0.5
                    async for message in websocket:
                        if message.type == aiohttp.WSMsgType.TEXT:
                            try:
                                await self.event_handler(message.json())
                            except Exception:
                                logger.exception("failed to handle ARI event")
                        elif message.type in {
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.CLOSE,
                            aiohttp.WSMsgType.ERROR,
                        }:
                            break
            except (aiohttp.ClientError, TimeoutError):
                if not self._stopping:
                    logger.warning("Asterisk ARI disconnected; retrying in %.1fs", delay)
            finally:
                self.ready.clear()
            if not self._stopping:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 10)

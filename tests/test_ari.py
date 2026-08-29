from unittest.mock import AsyncMock, call

import pytest

from omnicall.ari import AriClient, AriError
from omnicall.config import Settings


@pytest.fixture
def ari() -> AriClient:
    return AriClient(Settings.from_env(require_secrets=False), AsyncMock())


async def test_create_call_media_retries_until_channel_enters_stasis(
    ari: AriClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    ari.request = AsyncMock(
        side_effect=[
            {},
            {},
            AriError("HTTP 422: Channel not in Stasis application"),
            {},
        ]
    )
    sleep = AsyncMock()
    monkeypatch.setattr("omnicall.ari.asyncio.sleep", sleep)

    resources = await ari.create_call_media("call-id")

    assert resources == ("bridge-call-id", "media-call-id")
    assert ari.request.await_args_list[-2:] == [
        call(
            "POST",
            "/bridges/bridge-call-id/addChannel",
            params={"channel": "call-id,media-call-id"},
        ),
        call(
            "POST",
            "/bridges/bridge-call-id/addChannel",
            params={"channel": "call-id,media-call-id"},
        ),
    ]
    sleep.assert_awaited_once_with(0.05)


async def test_create_call_media_cleans_up_after_failure(ari: AriClient) -> None:
    ari.request = AsyncMock(
        side_effect=[
            {},
            {},
            AriError("HTTP 400: Channel not found"),
            {},
            {},
        ]
    )

    with pytest.raises(AriError, match="Channel not found"):
        await ari.create_call_media("call-id")

    assert call("DELETE", "/channels/media-call-id") in ari.request.await_args_list
    assert call("DELETE", "/bridges/bridge-call-id") in ari.request.await_args_list

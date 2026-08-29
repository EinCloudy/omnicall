from __future__ import annotations

import os
from dataclasses import dataclass


def _int(name: str, default: int) -> int:
    value = int(os.getenv(name, default))
    if value < 1:
        raise ValueError(f"{name} must be positive")
    return value


def _float(name: str, default: float) -> float:
    value = float(os.getenv(name, default))
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean")


@dataclass(frozen=True, slots=True)
class Settings:
    gemini_api_key: str
    gemini_live_model: str
    gemini_summary_model: str
    gemini_summary_fallback_model: str
    gemini_voice: str
    database_path: str
    ari_url: str
    ari_ws_url: str
    ari_user: str
    ari_password: str
    ari_app: str
    sip_caller_id: str
    sip_endpoint: str
    sip_dial_prefix: str
    media_host: str
    media_port: int
    media_connection: str
    mcp_host: str
    mcp_port: int
    mcp_transport: str
    max_concurrent_calls: int
    default_call_timeout: int
    max_call_duration: int
    consent_question_enabled: bool
    gemini_vad_prefix_padding_ms: int
    gemini_vad_silence_ms: int
    gemini_live_connect_timeout_seconds: float
    gemini_timeout_seconds: float

    @classmethod
    def from_env(cls, *, require_secrets: bool = True) -> Settings:
        api_key = os.getenv("GEMINI_API_KEY", "")
        ari_password = os.getenv("ARI_PASSWORD", "")
        if require_secrets and not api_key:
            raise ValueError("GEMINI_API_KEY is required")
        if require_secrets and not ari_password:
            raise ValueError("ARI_PASSWORD is required")

        transport = os.getenv("MCP_TRANSPORT", "streamable-http")
        if transport not in {"streamable-http", "stdio"}:
            raise ValueError("MCP_TRANSPORT must be 'streamable-http' or 'stdio'")
        return cls(
            gemini_api_key=api_key,
            gemini_live_model=os.getenv(
                "GEMINI_LIVE_MODEL", "gemini-3.1-flash-live-preview"
            ),
            gemini_summary_model=os.getenv("GEMINI_SUMMARY_MODEL", "gemini-3.7-flash"),
            gemini_summary_fallback_model=os.getenv(
                "GEMINI_SUMMARY_FALLBACK_MODEL", "gemini-3.6-flash"
            ),
            gemini_voice=os.getenv("GEMINI_VOICE", "Kore"),
            database_path=os.getenv("DATABASE_PATH", "/data/calls.db"),
            ari_url=os.getenv("ARI_URL", "http://127.0.0.1:8088/ari"),
            ari_ws_url=os.getenv("ARI_WS_URL", "ws://127.0.0.1:8088/ari/events"),
            ari_user=os.getenv("ARI_USER", "omnicall"),
            ari_password=ari_password,
            ari_app=os.getenv("ARI_APP", "omnicall"),
            sip_caller_id=os.getenv("SIP_CALLER_ID", ""),
            sip_endpoint=os.getenv("SIP_ENDPOINT", "fritzbox"),
            sip_dial_prefix=os.getenv("SIP_DIAL_PREFIX", ""),
            media_host=os.getenv("MEDIA_HOST", "127.0.0.1"),
            media_port=_int("MEDIA_PORT", 8765),
            media_connection=os.getenv("MEDIA_CONNECTION", "omnicall_media"),
            mcp_host=os.getenv("MCP_HOST", "127.0.0.1"),
            mcp_port=_int("MCP_PORT", 8090),
            mcp_transport=transport,
            max_concurrent_calls=_int("MAX_CONCURRENT_CALLS", 2),
            default_call_timeout=_int("DEFAULT_CALL_TIMEOUT", 45),
            max_call_duration=_int("MAX_CALL_DURATION", 900),
            consent_question_enabled=_bool("CONSENT_QUESTION_ENABLED", True),
            gemini_vad_prefix_padding_ms=_int("GEMINI_VAD_PREFIX_PADDING_MS", 100),
            gemini_vad_silence_ms=_int("GEMINI_VAD_SILENCE_MS", 650),
            gemini_live_connect_timeout_seconds=_float(
                "GEMINI_LIVE_CONNECT_TIMEOUT_SECONDS", 15.0
            ),
            gemini_timeout_seconds=_float("GEMINI_TIMEOUT_SECONDS", 60.0),
        )

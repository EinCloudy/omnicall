# OmniCall

OmniCall is a persistent MCP service for outbound AI phone calls through a local FritzBox
SIP account. Asterisk handles SIP, call states, RTP, and audio timing. The Python service
provides the OmniCall MCP server, maintains one Gemini Live audio session per call, and
stores the call history.

## Features

- Streamable HTTP MCP endpoint at `http://127.0.0.1:8090/mcp`
- Concurrent, bounded call queue with clean cancellation and restart recovery
- `gemini-3.1-flash-live-preview` for native bidirectional audio conversations
- One persistent Gemini Live session for the full duration of each call
- `gemini-3.7-flash` with a `gemini-3.6-flash` fallback for summaries
- 16 kHz PCM media path through Asterisk `chan_websocket`
- Gemini VAD with real barge-in and immediate Asterisk `FLUSH_MEDIA`
- Stateful streaming resampling from 24 kHz Gemini PCM to 16 kHz telephone PCM
- SQLite in WAL mode for calls, transcripts, summaries, and event cursors
- No audio files stored on disk; only metadata and transcripts are persisted
- Configurable AI and transcription notice, with an optional consent question

## Architecture

```text
MCP agent -- MCP/HTTP --> OmniCall -- Gemini API
                              |       |
                              |       +-- SQLite /data/calls.db
                              |
                              +-- ARI + PCM/WebSocket --> Asterisk -- SIP/RTP --> FritzBox
```

The containers use host networking on Linux. This avoids SIP and RTP NAT issues and lets
Asterisk reach the FritzBox directly on the LAN. ARI and the media WebSocket bind only to
`127.0.0.1`. The MCP endpoint also binds to `127.0.0.1` by default. Set
`MCP_HOST=0.0.0.0` in `.env` to expose it to the entire LAN at
`http://<host-ip>:8090/mcp`. The endpoint has no authentication, so any device on the
network can initiate calls when it is exposed.

After a call is answered, the service opens exactly one Gemini Live connection. Incoming
telephone audio is sent continuously in 100 ms blocks, while Gemini's native audio response
is forwarded to Asterisk as it is generated. If Gemini detects the other party speaking
during a response, the service discards every frame that has not yet been played. Input
remains blocked during the mandatory initial consent question so that echo cannot interrupt
the announcement.

## Getting Started

The local `.env` file is intentionally excluded through `.gitignore`. It contains the
FritzBox, Gemini, and internal ARI configuration. Create it from `.env.example` and provide
the required credentials before starting the services.

```bash
docker compose up -d --build
docker compose ps
```

Check SIP registration and logs:

```bash
docker compose exec asterisk asterisk -rx "pjsip show registrations"
docker compose exec asterisk asterisk -rx "pjsip show endpoint fritzbox"
docker compose logs -f omnicall-mcp asterisk
```

The FritzBox must allow outbound calls for the IP phone account and assign the desired
outbound phone number to it. If the Docker host cannot resolve `fritz.box`, set
`SIP_REGISTRAR` to the FritzBox LAN IP address.

## Hermes Agent

Hermes reads MCP server definitions from `~/.hermes/config.yaml`:

```yaml
mcp_servers:
  omnicall:
    url: http://127.0.0.1:8090/mcp
    connect_timeout: 15
    timeout: 60
    supports_parallel_tool_calls: false
```

Restart Hermes after changing the configuration, or run `/reload-mcp` in an active
session. The tools are exposed with the Hermes prefix `mcp_omnicall_*`.

## MCP Tools

| Tool | Purpose |
| --- | --- |
| `start_call` | Queue a call with a target, context, and optional opening statement |
| `get_updates` | Retrieve new states, transcript turns, questions, and summaries by cursor |
| `get_call` | Retrieve the complete persisted call, including its transcript |
| `follow_up_call` | Start a follow-up call with the previous history and new information |
| `list_calls` | List recent calls, optionally filtered by status |
| `cancel_call` | End a queued or active call |

Typical workflow:

1. The agent calls `start_call` and stores the returned `call_id`.
2. The agent calls `get_updates(after_event_id=0, call_id=...)`.
3. Each subsequent request uses `next_event_id` as its new cursor.
4. The terminal status is `completed`, `needs_input`, `failed`, or `cancelled`.
5. For `needs_input`, the call contains `hermes_question`; after receiving an answer, the
   agent starts `follow_up_call`.

`get_updates` supports up to 25 seconds of long polling through `wait_seconds`. Events are
persisted in SQLite and survive service restarts.

## Operations

Important variables in `.env.example`:

| Variable | Default and purpose |
| --- | --- |
| `MAX_CONCURRENT_CALLS` | `2`; adjust to the FritzBox and line capacity |
| `DEFAULT_CALL_TIMEOUT` | Ringing timeout, 45 seconds by default |
| `MAX_CALL_DURATION` | Hard call duration limit, 900 seconds by default |
| `DISCLOSURE_ENABLED` | `true`: announce AI use and transcription. `false`: omit both the notice and consent question and begin the call directly |
| `CONSENT_QUESTION_ENABLED` | When disclosure is enabled, `true` asks for transcription consent and `false` continues directly after the notice |
| `GEMINI_LIVE_MODEL` | Native audio model, `gemini-3.1-flash-live-preview` by default |
| `GEMINI_VAD_PREFIX_PADDING_MS` | Minimum duration before speech start, 100 ms by default |
| `GEMINI_VAD_SILENCE_MS` | Silence before turn end, 650 ms by default |
| `GEMINI_LIVE_CONNECT_TIMEOUT_SECONDS` | Live connection timeout, 15 seconds by default |
| `SIP_DIAL_PREFIX` | Optional FritzBox or PBX dialing prefix |

Back up the call history:

```bash
docker compose stop omnicall-mcp
docker run --rm -v omnicall_call-data:/data -v "$PWD":/backup \
  busybox cp /data/calls.db /backup/calls.db
docker compose start omnicall-mcp
```

## Tests

```bash
uv sync --extra dev
uv run pytest
uv run ruff check .
uv run python scripts/smoke_gemini.py
docker compose config --quiet
docker compose build
```

A real end-to-end call requires a reachable FritzBox, an available external line, and valid
permissions for the configured Gemini models.

## Privacy

By default, the service discloses the use of AI and transcription at the beginning of each
call and asks for consent. `CONSENT_QUESTION_ENABLED=false` disables only the question, while
`DISCLOSURE_ENABLED=false` disables both the notice and the question. Operators remain
responsible for verifying whether disabling these safeguards is lawful for their use case,
including applicable consent requirements, disclosure obligations, retention periods, and
deletion processes. The SQLite database contains personal conversation data and must be
protected accordingly.

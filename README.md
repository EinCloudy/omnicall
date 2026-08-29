# Hermes Calling MCP

Ein langlebiger MCP-Dienst für ausgehende KI-Telefonate über einen lokalen
FritzBox-SIP-Benutzer. Asterisk übernimmt SIP, Rufzustände, RTP und Audio-Timing. Der
Python-Dienst übernimmt Hermes-MCP, eine persistente Gemini-Live-Audiositzung pro Anruf
und die persistente Historie.

## Eigenschaften

- Streamable HTTP MCP unter `http://127.0.0.1:8090/mcp`
- Parallele, begrenzte Anrufwarteschlange mit sauberem Abbruch und Neustart-Recovery
- `gemini-3.1-flash-live-preview` für native, bidirektionale Audiogespräche
- Eine Gemini-Live-Sitzung über die gesamte Dauer jedes Telefonats
- `gemini-3.7-flash` mit `gemini-3.6-flash`-Fallback für Zusammenfassungen
- 16-kHz-PCM-Medienpfad über Asterisk `chan_websocket`
- Gemini-VAD mit echtem Barge-in und sofortigem Asterisk `FLUSH_MEDIA`
- Zustandsbehaftetes Streaming-Resampling von Gemini-PCM mit 24 kHz auf Telefon-PCM mit 16 kHz
- SQLite im WAL-Modus mit Anrufen, Transkript, Zusammenfassung und Event-Cursor
- Keine Audiodateien auf Platte; gespeichert werden Metadaten und Transkripte
- KI- und Transkriptionshinweis in der ersten Äußerung, optional mit Einwilligungsfrage
  (`CONSENT_QUESTION_ENABLED`)

## Architektur

```text
Hermes Agent -- MCP/HTTP --> Calling Service -- Gemini API
                              |       |
                              |       +-- SQLite /data/calls.db
                              |
                              +-- ARI + PCM/WebSocket --> Asterisk -- SIP/RTP --> FritzBox
```

Die Container verwenden unter Linux Host-Networking. Das vermeidet SIP-/RTP-NAT-Probleme
und lässt Asterisk die FritzBox direkt im LAN erreichen. ARI und Medien-WebSocket binden
ausschließlich an `127.0.0.1`. Der MCP-Endpunkt bindet standardmäßig ebenfalls an
`127.0.0.1`; mit `MCP_HOST=0.0.0.0` in der `.env` ist er im gesamten LAN unter
`http://<Host-IP>:8090/mcp` erreichbar. Der Endpunkt hat keine Authentifizierung – jedes
Gerät im Netz kann dann Anrufe auslösen.

Nach der Rufannahme öffnet der Dienst genau eine Gemini-Live-Verbindung. Eingehendes
Telefon-Audio wird kontinuierlich in 100-ms-Blöcken gesendet; Geminis native Audioantwort
wird bereits während der Erzeugung an Asterisk weitergeleitet. Erkennt Gemini Sprache des
Gesprächspartners während einer Antwort, verwirft der Dienst alle noch nicht abgespielten
Frames. Während der verpflichtenden ersten Einwilligungsfrage bleibt der Eingang gesperrt,
damit Echo diese Ansage nicht unterbrechen kann.

## Start

Die lokale `.env` ist absichtlich per `.gitignore` ausgeschlossen. Sie enthält die
FritzBox-, Gemini- und interne ARI-Konfiguration.

```bash
docker compose up -d --build
docker compose ps
```

Registrierung und Logs prüfen:

```bash
docker compose exec asterisk asterisk -rx "pjsip show registrations"
docker compose exec asterisk asterisk -rx "pjsip show endpoint fritzbox"
docker compose logs -f calling-mcp asterisk
```

Die FritzBox muss den IP-Telefon-Benutzer für ausgehende Gespräche freigeben und ihm die
gewünschte ausgehende Rufnummer zuweisen. Wenn `fritz.box` auf dem Docker-Host nicht
auflösbar ist, wird `SIP_REGISTRAR` auf die LAN-IP der FritzBox gesetzt.

## Hermes Agent

Hermes liest MCP-Server aus `~/.hermes/config.yaml`:

```yaml
mcp_servers:
  hermes-calling:
    url: http://127.0.0.1:8090/mcp
    connect_timeout: 15
    timeout: 60
    supports_parallel_tool_calls: false
```

Nach einer Konfigurationsänderung Hermes neu starten oder in einer laufenden Sitzung
`/reload-mcp` ausführen. Die Tools erscheinen mit dem Hermes-Präfix
`mcp_hermes-calling_*`.

## MCP-Tools

| Tool | Zweck |
| --- | --- |
| `start_call` | Anruf mit Ziel, Kontext und optionaler Eröffnung einreihen |
| `get_updates` | Neue Zustände, Transkriptbeiträge, Fragen und Zusammenfassungen per Cursor abrufen |
| `get_call` | Vollständigen persistenten Anruf einschließlich Transkript lesen |
| `follow_up_call` | Folgeanruf mit Historie und neuen Angaben starten |
| `list_calls` | Letzte Anrufe, optional nach Status gefiltert, auflisten |
| `cancel_call` | Wartenden oder laufenden Anruf beenden |

Typischer Ablauf:

1. Hermes ruft `start_call` auf und merkt sich `call_id`.
2. Hermes ruft `get_updates(after_event_id=0, call_id=...)` auf.
3. Weitere Abfragen verwenden jeweils `next_event_id` als neuen Cursor.
4. Der Endstatus ist `completed`, `needs_input`, `failed` oder `cancelled`.
5. Bei `needs_input` enthält der Anruf `hermes_question`; mit der Antwort startet Hermes
   `follow_up_call`.

`get_updates` unterstützt mit `wait_seconds` bis zu 25 Sekunden Long-Polling. Events sind
in SQLite persistent und gehen bei einem Neustart nicht verloren.

## Betrieb

Wichtige Variablen in `.env.example`:

| Variable | Standard/Bedeutung |
| --- | --- |
| `MAX_CONCURRENT_CALLS` | `2`; an FritzBox- und Anschlusskapazität anpassen |
| `DEFAULT_CALL_TIMEOUT` | Klingeldauer, standardmäßig 45 Sekunden |
| `MAX_CALL_DURATION` | Harte Gesprächsgrenze, standardmäßig 900 Sekunden |
| `CONSENT_QUESTION_ENABLED` | `true` (Standard): Einwilligungsfrage zur Transkription vor Gesprächsbeginn. `false`: direkter Einstieg nach dem KI-/Transkriptionshinweis |
| `GEMINI_LIVE_MODEL` | Native-Audio-Modell, standardmäßig `gemini-3.1-flash-live-preview` |
| `GEMINI_VAD_PREFIX_PADDING_MS` | Mindestdauer bis zum Sprachbeginn, standardmäßig 100 ms |
| `GEMINI_VAD_SILENCE_MS` | Pause bis zum Turn-Ende, standardmäßig 650 ms |
| `GEMINI_LIVE_CONNECT_TIMEOUT_SECONDS` | Zeitlimit für den Live-Verbindungsaufbau, standardmäßig 15 s |
| `SIP_DIAL_PREFIX` | Optionales FritzBox-/Anlagen-Präfix |

Backup der Historie:

```bash
docker compose stop calling-mcp
docker run --rm -v hermes-calling_call-data:/data -v "$PWD":/backup \
  busybox cp /data/calls.db /backup/calls.db
docker compose start calling-mcp
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

Ein echter End-to-End-Anruf setzt eine erreichbare FritzBox, eine freie Amtsleitung und
gültige Gemini-Model-Berechtigungen voraus.

## Datenschutz

Der Dienst informiert zu Gesprächsbeginn über KI-Einsatz und Transkription und bittet um
Zustimmung (sofern `CONSENT_QUESTION_ENABLED=true` gesetzt ist; auch ohne die Frage bleibt
der Hinweis bestehen). Trotzdem muss der Betreiber die für seinen Anwendungsfall geltenden Vorgaben,
Einwilligungen, Informationspflichten, Aufbewahrungsfristen und Löschprozesse selbst
prüfen. Die SQLite-Datenbank enthält personenbezogene Gesprächsinhalte und ist entsprechend
zu schützen.
# omnicall

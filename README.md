# courier

Courier is a generic bi-directional gateway between external chat applications and [herdr](https://herdr.dev)-driven coding agents. Each external message becomes a durable row in a SQLite ledger; courier delivers a bounded pointer to the agent, the agent reads and answers through MCP tools, and courier posts the reply back to the exact conversation it came from. Unanswered messages are redelivered until they are settled — by design, an agent can never silently drop a message.

One Go binary, three subcommands:

- `courier serve` — the daemon: SQLite ledger, connector ingestion, ordered dispatch through herdr's socket API, HTTP/unix IPC, reply posting, redelivery, and a shadow observation mode.
- `courier mcp` — per-session stdio MCP shim. It fetches the daemon's manifest and forwards tool calls; it does not deliver messages itself.
- `courier version` — binary version.

Supported connectors: **Mattermost**, **Gmail**, **Telegram**, and **Kaneo** (webhook-based project tracker events). Courier is connector-agnostic at its core: a connector only needs to normalize events into the ledger and post replies.

## Requirements

- Go 1.25+ (or [mise](https://mise.jdx.dev), which pins the toolchain in `mise.toml`)
- A reachable herdr session (socket API)
- Credentials for whichever external applications you want to bridge

## Build and test

```sh
mise run build   # or: go build -o courier .
mise run test    # go test -race ./...
```

## Run the daemon

```sh
export COURIER_ORG=example          # ledger namespace; also names the sqlite file
export COURIER_TARGET=my-agent      # the herdr agent label messages route to
courier serve
```

Courier connects to herdr through `HERDR_SOCKET_PATH` or `HERDR_SESSION`; otherwise it resolves the default herdr session socket. The daemon binds `127.0.0.1:8788`, stores its ledger at `./data/$COURIER_ORG.sqlite`, and ticks every 15 seconds.

Core options (every `COURIER_*` name also accepts its legacy `CHANNEL_*` spelling; setting both to different values fails at boot rather than choosing silently):

| Variable | Default | Purpose |
|---|---:|---|
| `COURIER_ORG` | required | Ledger namespace and sqlite filename. |
| `COURIER_TARGET` | required | herdr agent label that receives messages. |
| `COURIER_DATA_DIR` | `./data` | Ledger and runtime data root. |
| `COURIER_DB_PATH` | `$DATA_DIR/$ORG.sqlite` | Explicit SQLite path. |
| `COURIER_BIND` / `COURIER_PORT` | `127.0.0.1` / `8788` | TCP IPC bind. |
| `COURIER_SOCKET` | unset | Unix IPC socket; overrides TCP listening. |
| `COURIER_PROMPT_TIMEOUT_MS` | `120000` | herdr prompt timeout. |
| `COURIER_TICK_MS` | `15000` | Sweep, reply-retry, and dispatch cadence; `0` disables ticks. |
| `COURIER_CONNECTORS` | inferred | Comma-separated connector allowlist. |
| `COURIER_ENVELOPE_PREVIEW` | on | Include the bounded one-line preview in `<msg>`. |
| `COURIER_SHADOW` | off | Ingest and observe without prompts, replies, typing, or settlement. |
| `COURIER_SHADOW_HEARTBEAT_MS` | `900000` | Shadow health log cadence; `0` disables it. |
| `COURIER_REDELIVER_GRACE_MS` | `300000` | Base redelivery backoff. |
| `COURIER_REDELIVER_MAX_BACKOFF_MS` | `1800000` | Redelivery backoff cap. |
| `COURIER_REDELIVER_READ_FACTOR` | `4` | Extra backoff for read-but-unsettled messages. |

### Connector activation

Without `COURIER_CONNECTORS`, the presence of each connector's required configuration activates it. With an allowlist, only named connectors may activate; a named connector with missing requirements logs a warning and stays inactive.

**Mattermost** (websocket ingest, posting, thread routing):

```sh
export MATTERMOST_URL=https://mattermost.example.com
export MATTERMOST_BOT_TOKEN=...
# optional: MATTERMOST_BOT_USER_ID, MATTERMOST_ATTACHMENT_DIR
```
Channel mentions start or join a thread. Once mentioned, Courier durably follows
that thread and delivers every later message so the agent can decide whether a
reply is useful. For DMs, `chat_reply` can preserve the incoming location or set
`reply_mode` to `root`/`thread`.


**Gmail** (per-account polling via historyId):

```sh
export GMAIL_ACCOUNTS_FILE=/run/secrets/gmail-accounts.json   # or GMAIL_ACCOUNTS_JSON
# optional: GMAIL_POLL_SECONDS, GMAIL_ATTACHMENT_DIR
```

The accounts file is a JSON array; each entry names an `email` and a `token_command` — a shell command that prints a fresh OAuth access token as its last stdout line (55-minute cache, single-flight refresh).

**Telegram** (webhook ingest; activation requires the listen port plus its credentials):

```sh
export TELEGRAM_LISTEN_PORT=7784
export TELEGRAM_BOT_TOKEN=...
export TELEGRAM_WEBHOOK_SECRET=...
export TELEGRAM_ALLOWED_USER_IDS=42          # and/or TELEGRAM_ALLOWED_CHAT_IDS=-100xxxx
# optional: TELEGRAM_BOT_USERNAME, TELEGRAM_BOT_USER_ID, TELEGRAM_GROUP_REQUIRE_MENTION,
#           TELEGRAM_ATTACHMENT_DIR, TELEGRAM_CLEAR_DISABLED, TELEGRAM_CLEAR_ACK,
#           TELEGRAM_DISCONNECT_NOTICE
```

Telegram reaches the webhook on the loopback listener; put your own TLS-terminating proxy or tunnel in front of it. `TELEGRAM_WEBHOOK_SECRET` is validated against `X-Telegram-Bot-Api-Secret-Token` on every request.

**Kaneo** (signed webhooks; activation requires the listen port plus all three credentials):

```sh
export KANEO_LISTEN_PORT=8790
export KANEO_CHANNEL_WEBHOOK_SECRET=...
export KANEO_API_BASE=https://kaneo.example.com
export KANEO_BOT_KEY=...
# optional: KANEO_WORKSPACE_ID, KANEO_BOT_ACTOR
```

Secrets belong in environment injection, never command arguments.

## Run the MCP shim

An agent session config launches:

```sh
COURIER_AGENT=my-agent \
COURIER_HOST_URL=http://127.0.0.1:8788 \
courier mcp
```

`COURIER_AGENT` is required; the shim attaches it to every tool call and never lets the model supply its own. `COURIER_HOST_URL` defaults to `http://127.0.0.1:8788`. The shim retries `/manifest`, caches the last valid manifest under `$XDG_CACHE_HOME/courier` (or `~/.cache/courier`), serves the manifest's tools verbatim over stdio, and exits cleanly when stdin closes.

The daemon IPC surface is:

- `GET /health`
- `GET /manifest`
- `POST /tool/{name}`
- `POST /handled`

## The message contract (`courier/1`)

Incoming events reach the agent as bounded `<msg>` pointers:

```text
<msg delivery_id="d-123" conversation_id="channel-7:thread-9" user="Dana" connector="mattermost" redelivery="0" trigger="dm" schema="courier/1">
Can you check whether the Friday batch went out…
</msg>
```

The pointer is not the message. The agent must call `read_message` with the `delivery_id`, then settle exactly once: `chat_reply` (ids passed back unchanged) when a visible reply serves the sender, or `mark_handled` when none is warranted. Reading is a receipt, not settlement; doing neither causes redelivery with an explicit banner. The complete schema — attribute order, preview bounds, escaping rules, redelivery wording, and the tool workflow — is defined in [`SKILL.md`](./SKILL.md). Install that file wherever your agents read skills.

## Lifecycle

Startup is ordered: open/migrate the ledger, reclaim stale dispatches, reconcile the herdr target, start configured connectors, bind IPC, then drain. Shutdown stops tickers, drains connector shutdown (including in-flight webhooks), shuts down IPC, closes herdr, and closes SQLite.

## License

MIT — see [LICENSE](./LICENSE).

# courier

Generic bi-directional gateway between external applications (Mattermost, Gmail, Telegram, Kaneo)
and herdr agents. Go, flat `package main`, module `github.com/Orange-County-AI/courier`.
Ported from an earlier TypeScript implementation — the semantics recorded in that
implementation's file headers are contractual, not historical.

- `courier serve` — the gateway daemon: sqlite ledger, connectors, dispatcher, HTTP IPC
- `courier mcp` — per-session stdio MCP shim (manifest fetched from the daemon)
- `courier push` — reference `courier.ingest/1` sender (signs and POSTs one event)

Rules:

1. **THE INVARIANT**: only unexported `handle()` in store.go writes `handled_at` /
   `status='handled'`; its only callers are `CompleteAfterPost` (refusing on nil
   posted_at) and `MarkHandled`. `read_at` is written only by `MarkRead`.
   `invariant_test.go` enforces this by text scan + AST; it must stay green.
2. `mise run test` (`go test -race ./...`) green before any commit to main.
3. The envelope schema `courier/1` is defined in `SKILL.md`; changing it is an interface
   change to every agent that receives messages.
   Likewise the ingest wire schema `courier.ingest/1` is defined in `spec/ingest-1.md`; it is
   what third-party integrations are written against, so changing a limit, status code, header
   or field there is a breaking change to code this repo cannot see. Additive optional fields
   are the only compatible change; anything else takes a new schema name.
4. Secrets come from env, never argv; never print a secret value.
5. herdr is driven over its socket API (newline-JSON), not by exec'ing the CLI.

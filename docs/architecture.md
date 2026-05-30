# Architecture Notes

## Initial workspace

The repository starts as a Rust workspace with two crates:

- `ask-core` for shared data types and reusable logic
- `ask-server` for the long-running service process and HTTP or MCP entrypoints

## Direction

The current setup is intentionally small.

The expected evolution is:

1. add storage and indexing abstractions to `ask-core`
2. add service runtime, configuration, and API layers to `ask-server`
3. package the service and supporting dependencies together through Docker
4. optionally add MCP and a minimal frontend as separate workspace members later

## Database migrations

`ask-server` owns database startup and migration application.

- SQLite is the initial storage layer
- the SQLite file location is derived from `ASK_SERVER_DATA_DIR`
- the default path is `.data/ask.sqlite3`, which matches the ignored local data directory
- migration definitions are embedded in the binary from `ask-server/migrations/`
- a `migrations` table tracks applied versions in ascending order
- each applied row stores `version` and optional `required_actions`
- the server creates the tracking table if needed, then applies any missing migrations in version order during startup

## Embedding provider configuration

`ask-server` reads one of two embedding backends from environment variables:

- `ASK_SERVER_EMBEDDING_MODE=tei` targets a local OpenAI-compatible TEI service
- `ASK_SERVER_EMBEDDING_MODE=openai` targets an external OpenAI-compatible provider
- `ASK_SERVER_EMBEDDING_AUTH_TOKEN` is only required in `openai` mode

## Robustness

To keep the project stable as it grows:

- keep shared contracts in `ask-core`
- keep process and transport concerns in `ask-server`
- prefer fallible APIs over panics
- add tests close to the code that defines behavior

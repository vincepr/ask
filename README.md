# ask

`ask` is a Rust workspace for a minimal, robust knowledge query and storage system.

## Workspace layout

- `ask-core`: shared domain types and reusable logic.
- `ask-server`: main daemon and API process.
- `docs/`: project notes and architecture decisions.

## Database

`ask-server` uses SQLite and applies embedded migrations on startup.

- database path env var: `ASK_SERVER_SQLITE_PATH`
- default database path: `data/ask.sqlite3`
- example override: `ASK_SERVER_SQLITE_PATH=/data/ask/ask.sqlite3`
- applied migration metadata is tracked in the `migrations` table by version

This fits the intended Docker deployment model where a host-mounted directory can hold both the
SQLite database file and additional markdown knowledge files.

## Goals

- keep the codebase small and maintainable
- build toward a single Docker-deployable stack
- favor explicit error handling and stable interfaces
- leave room for a future MCP surface and a minimal frontend wrapper

## Getting started

```bash
cargo test
cargo run -p ask-server
```

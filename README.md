# ask

`ask` is a Rust workspace for a minimal, robust knowledge query and storage system.

## Workspace layout

- `ask-core`: shared domain types and reusable logic.
- `ask-server`: main daemon and API process.
- `docs/`: project notes and architecture decisions.

## Database

`ask-server` uses SQLite and applies embedded migrations on startup.

- data directory env var: `ASK_SERVER_DATA_DIR`
- default data directory: `.data`
- derived default database path: `.data/ask.sqlite3`
- example override: `ASK_SERVER_DATA_DIR=/data`
- applied migration metadata is tracked in the `migrations` table by version

This fits the intended Docker deployment model where a host-mounted directory can hold both the
SQLite database file and additional markdown knowledge files.

## HTTP

`ask-server` exposes a minimal health endpoint.

- `GET /health` returns `{"status":"healthy"}`
- bind host env var: `ASK_SERVER_BIND_HOST`
- bind port env var: `ASK_SERVER_BIND_PORT`
- default bind address: `0.0.0.0:3000`

## Embeddings

`ask-server` now supports two embedding provider modes through environment configuration.

- mode env var: `ASK_SERVER_EMBEDDING_MODE`
- supported modes: `tei`, `openai`
- TEI base URL env var: `ASK_SERVER_EMBEDDING_BASE_URL`
- TEI default base URL: `http://tei:80`
- OpenAI mode requires both `ASK_SERVER_EMBEDDING_BASE_URL` and `ASK_SERVER_EMBEDDING_AUTH_TOKEN`

## Docker Compose

`docker-compose.yml` is driven by `.env` values and includes `ask-server` with:

- host port `ASK_SERVER_EXPOSE_PORT` mapped to `ASK_SERVER_BIND_PORT`
- a host-mounted `./${ASK_DATA_DIR}` directory at `/data`
- SQLite stored at `/data/ask.sqlite3`
- optional `tei` service behind the Compose profile `tei`

Use TEI mode:

```bash
docker compose --profile tei up --build
```

Use external OpenAI-compatible mode:

```bash
docker compose up --build
```

## Goals

- keep the codebase small and maintainable
- build toward a single Docker-deployable stack
- favor explicit error handling and stable interfaces
- leave room for a future MCP surface and a minimal frontend wrapper

## Local development (debugging)

Run the server directly with `cargo` (no Docker needed). The server auto-loads
`.env` from the project root on startup.

```bash
# 1. Copy the env template and adjust as needed
cp .env.example .env

# 2. Run tests
cargo test

# 3. Start the server (auto-rebuilds on source changes)
cargo run -p ask-server
```

The `.env` is loaded automatically — no need to `source` or `export` it manually.
Override any env var inline for quick iterations:

```bash
ASK_SERVER_BIND_PORT=3001 cargo run -p ask-server
```

The server expects a reachable embedding backend at `ASK_SERVER_EMBEDDING_BASE_URL`.
Point it at a local TEI instance, an OpenAI-compatible API, or skip embedding-dependent
features during initial development.

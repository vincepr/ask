# ask

`ask` is a Rust workspace for a minimal, robust knowledge query and storage system.

## Layout

- `ask-core`: shared domain types and reusable logic.
- `ask-server`: main daemon and API process.
- `docs/features/`: planned feature work and implementation notes.
- `docs/improvements/`: design follow-ups and cleanup ideas.
- `docs/issues/`: temporary issue notes when present.
- `.env.example`: local configuration example.
- `docker-compose.yml`: local container stack.

## Config

Copy `.env.example` to `.env` and adjust values there for local runs.
For containerized runs, also see [docker-compose.yml](/home/vince/ask/docker-compose.yml).

## Goals

- keep the codebase small and maintainable
- build toward a single Docker-deployable stack
- favor explicit error handling and stable interfaces
- leave room for a future MCP surface and a minimal frontend wrapper

## Local Development

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

The `.env` is loaded automatically. Override values inline for quick
experiments:

```bash
ASK_SERVER_BIND_PORT=3001 cargo run -p ask-server
```

The server exposes a health endpoint at `GET /health`.

## Docker

Use the local TEI-backed stack:

```bash
docker compose --profile tei up --build
```

Use an external OpenAI-compatible embeddings backend:

```bash
docker compose up --build
```

## Ingesting documents

After the server is running, tell it what to index by POSTing to `/ingest`:

```bash
# Ingest the entire resource directory (the project root, mounted at /resources).
curl -X POST http://localhost:13000/ingest \
  -H "Content-Type: application/json" \
  -d '{"root_path": "/resources"}'

# Ingest the entire resource directory (the project root, mounted at /data). For now only the sqlite db lives here. But in the future might become a ai-managed folder for knowledge
curl -X POST http://localhost:13000/ingest \
  -H "Content-Type: application/json" \
  -d '{"root_path": "/data"}'

# Ingest only a subdirectory.
curl -X POST http://localhost:13000/ingest \
  -H "Content-Type: application/json" \
  -d '{"root_path": "/resources/crates"}'

# Ingest only markdown files.
curl -X POST http://localhost:13000/ingest \
  -H "Content-Type: application/json" \
  -d '{"root_path": "/resources", "file_pattern": "**/*.md"}'
```

The `file_pattern` is an optional glob pattern. When omitted, all files under
`root_path` are indexed. The `root_path` must be a subdirectory of
`ASK_SERVER_RESOURCE_DIR` (`/resources` in Docker, `.` in local dev).

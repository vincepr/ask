# Docker Environment Config Surface

The docker-compose.yml and `.env` file share responsibility for configuration,
but some variables are wired inconsistently between the two.

| Variable | `.env` (local) | docker-compose.yml | Effective in Docker |
|---|---|---|---|
| `ASK_SERVER_EMBEDDING_BASE_URL` | `http://localhost:18080` | `http://tei:80` | correct override |
| `ASK_SERVER_EMBEDDING_DIMENSIONS` | absent | absent | default `768` (should be `1024`) |
| `ASK_SERVER_RESOURCE_DIR` | `.` | `/resources` | correct override |
| `ASK_SERVER_EMBEDDING_MODE` | `tei` | `tei` (fallback) | correct |
| `ASK_SERVER_EMBEDDING_AUTH_TOKEN` | absent | `${...:-}` | correct |

Variables not explicitly set in docker-compose fall through to defaults defined
in `config.rs`, not to `.env` values. Docker Compose loads `.env` for compose
variable substitution (`${ASK_SERVER_EXPOSE_PORT:-13000}`), but this does not
automatically set the corresponding container environment variable.

## Questions

- Should docker-compose explicitly list every configurable env var (becoming a
  source of truth), or should it only override the values that differ between
  local and Docker (minimal diff)?
- If the latter, how does a user discover that `ASK_SERVER_EMBEDDING_DIMENSIONS`
  needs to be set for Docker but is not mentioned in docker-compose.yml?
- Should there be a Docker-specific env file (e.g., `.env.docker`) that is
  loaded only by docker-compose, keeping `.env` clean for local runs?
- Is the current split between compose-level variables (for compose
  substitution) and container env vars (for the application) documented clearly
  enough for someone new to the project?
- What is the minimum set of env vars needed in docker-compose.yml to make the
  stack work correctly without surprises?

---
_This document captures problems observed during exploration. Update or close when the corresponding implementation resolves the underlying concern._

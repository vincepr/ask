# Startup State Flow: Silent Empty State

After deleting the old DB and restarting, the server starts, creates a fresh
schema, registers the embedding model, and sits idle — no documents, no jobs,
no errors. From the outside, everything looks healthy: the HTTP server responds
to `/health`, the worker polls every 5s and finds nothing, logs are clean.

There is no indication that the system is effectively inert until an external
action (an ingest request) is taken.

## How We Got Here

The full startup flow (`crates/ask-server/src/bin/ask-server.rs`):

1. **Config load** — reads env vars, picks defaults for anything unset.
2. **Database init** — opens SQLite, runs pending migrations (creating tables
   if empty).
3. **Model lookup/insert** (`ask-server.rs:35-57`):
   - If a row exists in `embedding_models` with `name='default'`: use it
   - If not: insert one, then call `backfill_pending_for_model()` which
     iterates `list_documents()` and enqueues `EmbedDocument` jobs
4. **Vector index setup** — creates/rebuilds the sqlite-vec virtual table.
5. **Worker spawn** — starts a background task that polls `job_queue` every 5s.
6. **HTTP server** — serves endpoints, including `POST /ingest`.

The critical point: step 3's backfill only produces jobs if documents already
exist in the `documents` table. If the DB is fresh (zero documents), backfill
is a no-op. The worker runs forever finding nothing.

Documents only enter the system through `POST /ingest`, which enqueues an
`IngestFolder` job. That job walks the directory, upserts documents, and
enqueues `EmbedDocument` jobs. Only then does embedding actually happen.

## The Unspoken Precondition

The design assumes that either:
- The user knows to call `POST /ingest` after startup, or
- The DB is a pre-seeded artifact (from a previous run or backup)

Neither of these is discoverable from the server's runtime behavior. Logs show
"applied pending migrations applied_count=0" and "ensured sqlite-vec search
index ... backfilled=0" — accurate, but silent on what the user should do next.

## Questions on State Flow Design

**Entry points for data:**
- Should there be an auto-ingest at startup that walks the configured
  `resource_dir` and ingests everything? This would make first-time setup
  seamless but might be surprising if `resource_dir` is large or slow.
- If auto-ingest exists, should it be opt-in (env var flag) or the default
  behavior?
- Should the `IngestFolderHandler` be invoked at startup for the configured
  `resource_dir`, enqueuing the same job that `POST /ingest` would create?

**Discoverability:**
- Should the health endpoint or a status endpoint report "no documents, no
  embeddings" as a warning state?
- Should the startup log contain an explicit message like
  "resource directory is empty or no documents found; use POST /ingest to
  begin"?

**Backfill semantics:**
- `backfill_pending_for_model` only runs for new models. Once a model exists,
  re-ingesting documents or changing the resource directory requires manual
  API calls. Should the server detect changes to the resource directory on
  restart and re-ingest automatically?
- If a model row exists but `document_embeddings` is empty for that model,
  should that be treated the same as "new model" and trigger backfill?
  (Currently it would not — the model lookup matches by name and returns the
  existing row.)

**Orchestration vs simplicity:**
- The current design is simple: a background worker polls a queue, the HTTP
  server enqueues jobs, no cron, no watchers, no filesystem monitoring.
- Adding auto-ingest, health-based state detection, or startup recovery adds
  complexity. Where is the right balance for this project?
- For a single-user, single-model, single-directory deployment (which appears
  to be the primary use case), could the startup be simplified to:
  - Walk the resource dir on startup
  - Upsert all documents
  - Backfill all pending embeddings
  - Block until embedding completes
  - Then serve?
  This would eliminate the job queue entirely for this use case.

**Failure modes:**
- The user deletes the DB to "reset" but doesn't know to re-ingest.
- The user changes `resource_dir` in config but the old paths in the DB are
  stale.
- The user expects the server to "just work" pointing at a directory of files,
  but nothing happens until they discover the API.

---
_This document captures problems observed during exploration. Update or close when the corresponding implementation resolves the underlying concern._

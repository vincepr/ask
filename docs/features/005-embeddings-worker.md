# 005: Embeddings Worker

## Context

Documents are ingested and their embedding rows are created in `Pending` state, but no worker actually computes the embedding vectors. Without this step the search index remains empty. Additionally, there is no mechanism to re-embed documents whose embeddings are `Stale`.

## Problem

1. Pending embedding rows are never processed — they stay `Pending` forever.
2. Stale embeddings are never re-computed, so marking documents stale has no effect.
3. No parallel processing — even if a single-threaded worker existed, it would be too slow for large corpora.
4. No API to start/stop embedding work — embeddings processing is either always on or always off.

## Required Feature

### Part 1: Repository method to seed embed jobs

A `seed_embed_jobs` repository method that:

1. Queries all documents with `Pending` or `Stale` embedding rows.
2. Enqueues one `EmbedDocument` job per document (deduplicated by the unique index on `(job_type, payload)`).
3. Stale rows are freed (replaced) once new embeddings are committed.

### Part 2: EmbeddingWorker and parallel pool

A new `EmbeddingWorker` that:

1. Receives a document, reads its content from disk, splits it into chunks using the configured model's `chunk_size` and `chunk_overlap`.
2. Calls an embedding interface (initially returns a hardcoded vector).
3. Inserts new embedding rows, deletes old rows for that document+model, deletes the job queue entry, and commits atomically.

**Parallel pool** (not always running):

- `POST /embeddings/start` spawns a configurable number of parallel workers (default 3) that claim `EmbedDocument` jobs via a shared channel.
- `POST /embeddings/stop` signals running workers to shut down gracefully (in-flight job completes first).

### Part 3: Real embedding API calls

Replace the hardcoded vector with a call to an OpenAI-compatible API:

1. Configurable via `base_url` and `auth_token` (supports both OpenAI and locally-hosted TEI containers).
2. Sends `POST {base_url}/embeddings` with the chunk text.
3. Stores the returned vector as embedding bytes.
4. Falls back to hardcoded embedding on API failure (degraded but not broken).

## Required Sub-tasks

### Part 1
- [ ] Add `seed_embed_jobs` to repository
- [ ] Add `JobType::EmbedDocument` variant

### Part 2
- [ ] Implement `EmbeddingWorker` with chunking + hardcoded embedding
- [ ] Add `POST /embeddings/start` endpoint with configurable parallelism
- [ ] Add `POST /embeddings/stop` endpoint
- [ ] Manage worker lifecycle (spawn/join handles, graceful shutdown)
- [ ] Atomic commit: insert new embeddings + delete old ones + delete job

### Part 3
- [ ] Add embedding client abstraction in configuration
- [ ] Implement OpenAI-compatible HTTP client (reqwest)
- [ ] Wire real embeddings into `EmbeddingWorker`
- [ ] Add fallback to hardcoded embedding on error

## Why Now

Without this feature, embeddings are created in `Pending` state and never computed. The entire search feature depends on this worker existing.

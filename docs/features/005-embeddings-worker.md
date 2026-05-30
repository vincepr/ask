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

1. Queries all `(document_id, model_id)` pairs that have at least one `Pending` or
   `Stale` embedding row.
2. Enqueues one `EmbedDocument` job per `(document_id, model_id)` pair
   (deduplicated by the unique index on `(job_type, payload)`).
3. Leaves existing rows untouched during seeding. Replacement happens only after the
   worker has produced a complete fresh embedding set inside a transaction.

### Part 2: EmbeddingWorker and parallel pool

A new `EmbeddingWorker` that:

1. Receives a payload containing `document_id` and `model_id`.
2. Loads the document and model from the database, reads the file from disk, and
   splits the content using that model's `chunk_size` and `chunk_overlap`.
3. Calls an embedding interface. The first version can use a deterministic mock
   provider for tests and local plumbing.
4. Replaces the old embedding rows for that exact `(document_id, model_id)` pair,
   deletes the job queue entry, and commits atomically.

**Parallel pool** (not always running):

- `POST /embeddings/workers` starts a configurable number of parallel workers
  (default 3).
- `DELETE /embeddings/workers` signals running workers to shut down gracefully
  (in-flight job completes first).

Do not add a second in-memory work queue between the API and the database. The
database-backed job queue already provides deduplication, crash recovery, and
claim/retry behavior. Each embedding worker should claim jobs directly from the same
queue mechanism used by other background jobs.

### Part 3: Real embedding API calls

Replace the mock provider with a call to an OpenAI-compatible API:

1. Configurable via `base_url` and `auth_token` (supports both OpenAI and locally-hosted TEI containers).
2. Sends `POST {base_url}/embeddings` with the chunk text.
3. Verifies the returned vector length matches the configured model dimensions.
4. Stores the returned vector as embedding bytes.
5. Fails the job on API/network/shape errors so it can be retried later. Do not
   silently fall back to a fake embedding in production, because that would poison
   search quality while looking like success.

## Required Sub-tasks

### Part 1
- [ ] Add `seed_embed_jobs` to repository
- [ ] Add `JobType::EmbedDocument` variant
- [ ] Add an `EmbedDocumentPayload { document_id, model_id }`

### Part 2
- [ ] Implement `EmbeddingWorker` with chunking + deterministic mock provider
- [ ] Add `POST /embeddings/workers` endpoint with configurable parallelism
- [ ] Add `DELETE /embeddings/workers` endpoint
- [ ] Manage worker lifecycle (spawn/join handles, graceful shutdown)
- [ ] Atomic commit: replace embeddings for one `(document_id, model_id)` pair and
  delete the job

### Part 3
- [ ] Add embedding client abstraction in configuration
- [ ] Implement OpenAI-compatible HTTP client (reqwest)
- [ ] Wire real embeddings into `EmbeddingWorker`
- [ ] Fail and retry jobs on provider errors instead of storing fake vectors

## Why Now

Without this feature, embeddings are created in `Pending` state and never computed. The entire search feature depends on this worker existing.

# 007: EmbedDocument Worker

## Context

Feature 006 creates `EmbedDocument` jobs for distinct `(document_id, model_id)`
pairs that need work. This feature is the execution half of that design.

## Problem

1. `Pending` and `Stale` rows still never become vectors.
2. The current worker only knows how to process `IngestFolder`.
3. The draft design proposed a second in-memory queue and HTTP endpoints to
   start or stop workers. That does not fit the current architecture.

## Decision

Extend the existing queue worker and dispatcher. Do not add:

- a second in-memory work queue
- `POST /embeddings/workers`
- `DELETE /embeddings/workers`

Workers should start with the server and claim jobs directly from `job_queue`,
just like `IngestFolder` does today. If parallelism is needed, make it a normal
startup configuration or process-count setting, not an HTTP-controlled runtime
feature.

## Required Feature

Implement `EmbedDocumentHandler` as another `JobHandler`:

1. Decode `EmbedDocumentPayload { document_id, model_id }`.
2. Load the document and model.
3. Read the source file and derive the chunk boundaries for that model.
4. Call the configured embedding provider for the filename chunk and content
   chunks.
5. Replace the embedding rows for exactly that `(document_id, model_id)` pair
   inside one transaction.
6. Leave the queue row claimed on failure so existing retry/stale behavior still
   applies until retry semantics are redesigned.

## Important Design Corrections

1. `model_id` must come from the embed job payload, not from worker-global
   state.
2. The database-backed queue remains the only scheduler.
3. Row replacement must be atomic for one `(document_id, model_id)` pair.
4. Production errors from the provider must fail the job. Do not silently store
   fake vectors outside tests.

## Required Sub-tasks

- [ ] Add `EmbedDocumentHandler` to `worker.rs`
- [ ] Extend `resolve_handler` for `JobType::EmbedDocument`
- [ ] Add an embedding-client abstraction over the existing TEI/OpenAI config
- [ ] Add deterministic test provider coverage
- [ ] Add transaction-scoped "replace embeddings for one document/model pair"
      repository logic
- [ ] Add tests for success, provider failure, and malformed payload behavior

## Acceptance Criteria

1. `EmbedDocument` jobs are claimed from the same queue as `IngestFolder`.
2. Each job embeds exactly one `(document_id, model_id)` pair.
3. Successful jobs replace rows atomically and then complete the queue row.
4. Failed jobs do not write fake vectors or partially replace a pair's rows.

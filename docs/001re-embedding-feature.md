# 001: Re-embedding of Stale Documents

## Context

Documents can have their embeddings marked as `'stale'` via the
`POST /documents/stale` endpoint (or programmatically through
`repository::mark_documents_stale`). Once stale, the embeddings are no longer
usable for search, but they are not automatically re-computed.

## Problem

There is no process that picks up stale embeddings and re-computes them. Over
time stale embeddings accumulate, reducing the effective search corpus.

## Required Feature

A background worker (analogous to the existing `IngestFolder` job handler) that:

1. Polls the `document_embeddings` table for rows where `state = 'stale'`.
2. For each batch of stale rows, re-computes the embedding vector using the
   model specified by the document's model association.
3. Updates the row's `state` to `'embedded'` and stores the new vector in the
   `embedding` column.
4. Runs as a recurring job — either scheduled at a fixed interval or triggered
   when new stale rows appear.

## Required Sub-tasks

- [ ] Add a new `JobType::ReEmbed` variant to the `JobType` enum
- [ ] Implement a `ReEmbedHandler` that implements `JobHandler` in `worker.rs`
- [ ] Add a `pending_stale_embeddings` (or similar) query to `repository.rs`
  that returns stale embeddings grouped by model (this replaces the removed
  `pending_embeddings_for_model` function)
- [ ] Wire the new handler into `dispatch_job` in `worker.rs`
- [ ] Add an embedding client abstraction in `ask-core` (or `ask-server`) that
  can call the configured embedding provider (TEI / OpenAI)
- [ ] Write integration tests that verify stale → embedded transition
- [ ] Decide on scheduling: cron-like timer vs. event-driven trigger

## Why Now

The `mark_documents_stale` endpoint exists and works, but without a consumer
for stale embeddings the feature is a dead end. Adding the re-embed worker
closes the loop and makes the stale-marking feature actually useful.

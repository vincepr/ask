# 006: Seed Embed Jobs for Pending and Stale Rows

## Context

The database already records two kinds of embedding work:

- freshly queued rows in `Pending` state
- existing rows marked `Stale`

Neither state causes execution by itself. The queue only knows about
`IngestFolder` jobs today.

## Problem

1. `Pending` rows created during ingest never turn into executable jobs.
2. `POST /documents/stale` changes row state, but does not schedule any follow-up
   work.
3. A separate `ReEmbed` job type would duplicate queue semantics that the
   existing job table already provides.

## Decision

Do **not** add a second worker that polls `document_embeddings` directly.

Keep one queue-driven execution model:

- distinct `(document_id, model_id)` pairs that need work become
  `EmbedDocument` jobs
- workers consume those jobs from `job_queue`

This means re-embedding is not a special worker. It is the same embed pipeline
applied to rows in either `Pending` or `Stale` state.

## Required Feature

1. Add `JobType::EmbedDocument`.
2. Add `EmbedDocumentPayload { document_id, model_id }`.
3. Add a repository method that finds distinct `(document_id, model_id)` pairs
   with at least one `Pending` or `Stale` row and enqueues one embed job per
   pair.
4. Call that seeding path after:
   - folder ingest creates pending rows
   - `mark_documents_stale` marks rows stale
   - model backfill creates pending rows for existing documents

## Why This Split Matters

This feature is only about turning database state into queue work.

The actual execution of `EmbedDocument` jobs belongs in feature 007.

## Required Sub-tasks

- [ ] Add `JobType::EmbedDocument`
- [ ] Add `EmbedDocumentPayload`
- [ ] Add `seed_embed_jobs` repository functionality
- [ ] Invoke seeding after ingest, stale-marking, and model backfill paths
- [ ] Rely on the existing `(job_type, payload)` uniqueness rule for deduplication
- [ ] Add tests proving repeated seeding does not create duplicate jobs

## Acceptance Criteria

1. Re-embedding uses the same queue as first-time embedding.
2. There is no separate `ReEmbed` worker that bypasses `job_queue`.
3. One `(document_id, model_id)` pair becomes at most one queued embed job.
4. Marking rows stale creates a path to execution instead of a dead-end state.

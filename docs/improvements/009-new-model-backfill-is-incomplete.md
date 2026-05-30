# New Model Backfill Misses Document Content

## Problem

Registering a new embedding model only queues filename embeddings for existing documents.

## Evidence

- `crates/ask-core/src/repository.rs:321-329` loops over all documents and inserts only
  `ChunkType::Filename`.
- `crates/ask-server/src/worker.rs:250-280` shows that normal ingestion queues both filename and content
  chunks.

## Why This Is Risky

- A newly added model sees an incomplete corpus.
- Queries against that model will underperform or miss documents whose content was never queued.
- The system behavior differs depending on whether a document existed before or after model creation.

## Simplest Stable Fix

- Backfill the same chunk plan that normal ingestion would create.
- If the system does not yet persist chunk plans, enqueue a reingest-style rebuild for each existing
  document instead of pretending filename-only coverage is complete.
- Add a regression test that creates documents first, then creates a model, and verifies both filename
  and content rows are queued.

## Human review:
- Implement that.
- Check if it makes sense to have shared logic/method etc. If able to do without bloating complexity/lines of code do it.
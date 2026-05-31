# 005: Document Ingest Should Commit Atomically

## Problem

Each file ingest is implemented as a series of independent writes, so mid-file failure can leave partial
state behind.

## Evidence

- `crates/ask-server/src/worker.rs:237-280` inserts the document row, then filename embedding rows,
  then content embedding rows, all without an explicit transaction.
- `crates/ask-core/src/repository.rs:196-226` inserts embeddings one row at a time, so failure can
  happen after partial success.

## Why This Is Risky

- A document row can exist without the expected embedding work rows.
- Filename embeddings can be queued while content embeddings are missing.
- Retrying later becomes harder because the database no longer represents a clean before/after state.

## Simplest Stable Fix

- Process one document inside one SQL transaction.
- Keep the repository API coarse enough to express one atomic operation, for example
  `upsert_document_and_queue_embeddings(...)`.
- If content chunk generation fails before any database write should be committed, abort the whole
  transaction.
- Add regression tests for an injected failure between document creation and embedding queue creation.

# Embedding Model Contract and Dimension Validation

The TEI provider (Qwen3-Embedding-0.6B-ONNX) outputs 1024-dimensional vectors.
The code defaults to 768 (`crates/ask-server/src/config.rs:123`).

When a job runs, the response is validated against `model.dimensions` at
`crates/ask-server/src/embeddings.rs:117-123` and rejected when they don't
match. Every `EmbedDocument` job fails, no embeddings get stored.

## Questions

## Quickfix Applied

- Added `ASK_SERVER_EMBEDDING_DIMENSIONS=1024` to `.env` and
  `docker-compose.yml`.
- Deleted `./data/ask.sqlite3` so the model row is re-created with the correct
  dimensions.

This is a band-aid. If the env var is ever removed or a different model is
used, the mismatch will reappear. A proper solution should handle this
automatically.

## Questions

- Should dimensions be statically configured, auto-detected from the provider at
  startup, or left as a tunable parameter with a fail-fast check?
- If auto-detected, what does that imply for provider availability during
  startup (the TEI container may not be ready yet)?
- The DB row for this model already has `dimensions=768`. If dimensions change,
  should a new model row be created (with its own backfill), or should the
  existing row be updated (requiring re-embedding of every document)?
- What should happen when a user changes the env var and restarts — expect them
  to delete the old model row manually, or automate the migration?
- Is there a simpler approach that avoids adding startup-time network calls or
  migration complexity, given this is fundamentally a one-time config fix?

## Recommended Direction

- Treat the embedding model contract as an explicit tuple, not an implicit
  mutable row. At minimum that contract includes:
  - provider kind
  - provider model identifier
  - output dimensions
  - chunk size
  - chunk overlap
- Do not update an existing model row in place when the contract changes.
  Create a new row and let re-embedding happen against the new contract. This
  keeps old embeddings unambiguous and avoids silent drift.
- Separate the application's stable model key from the provider's concrete model
  id. The current `name='default'` pattern is too ambiguous once validation and
  provider interoperability matter.

## Implementation Notes

- Prefer fail-fast validation before the worker starts claiming jobs.
- Keep validation minimal:
  - if provider metadata is available, verify configured dimensions and model id
    at startup
  - if provider metadata is unavailable, require explicit config and fail on the
    first mismatching response without mutating persistent model state
- Avoid a design where startup rewrites existing rows opportunistically. That
  makes rollback and debugging harder.
- If a new model contract is registered, startup should enqueue or backfill
  pending embeddings for that new model automatically.

## Dependencies and Sequencing

- This should land before request batching and provider-readiness work because
  those features need a trustworthy model contract.
- This is also a prerequisite for making search stable across provider changes.

## Test Expectations

- Config/model registration test for first startup with a fresh DB.
- Regression test proving a mismatched provider dimension does not silently
  reuse an incompatible existing row.
- Test that a changed model contract creates a distinct model row and triggers
  backfill semantics.

## Detailed Plan

- See the implementation plan at
  [embedding-model-contract-implementation-plan.md](/tmp/embedding-model-contract-implementation-plan.md).

---
_This document captures problems observed during exploration. Update or close when the corresponding implementation resolves the underlying concern._

# Embedding Dimension Mismatch

The TEI provider (Qwen3-Embedding-0.6B-ONNX) outputs 1024-dimensional vectors.
The code defaults to 768 (`crates/ask-server/src/config.rs:123`).

When a job runs, the response is validated against `model.dimensions` at
`crates/ask-server/src/embeddings.rs:117-123` and rejected when they don't
match. Every `EmbedDocument` job fails, no embeddings get stored.

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

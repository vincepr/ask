# Embedding Configuration Needs Real Validation

## Problem

Startup configuration only checks whether numeric fields parse, not whether the values are meaningful.

## Evidence

- `crates/ask-server/src/config.rs:124-150` accepts any `i64` for dimensions, chunk size, and chunk
  overlap.
- `crates/ask-server/src/worker.rs:261-265` casts `model.chunk_size` and `model.chunk_overlap` from
  `i64` to `usize`.
- `crates/ask-server/migrations/0002_create_domain_tables.sql:11-18` stores model dimensions and chunk
  settings without range constraints.

## Why This Is Risky

- A negative chunk size becomes a huge `usize` after cast.
- `chunk_overlap >= chunk_size` currently degrades into special behavior instead of a rejected config.
- Negative dimensions or zero chunk sizes are accepted into persistent state even though they do not
  describe a usable embedding model.

## Simplest Stable Fix

- Validate at load time:
  - `embedding_dimensions > 0`
  - `embedding_chunk_size > 0`
  - `embedding_chunk_overlap >= 0`
  - `embedding_chunk_overlap < embedding_chunk_size`
- Mirror the same rules in the database with `CHECK` constraints.
- Replace raw `i64` settings with validated newtypes or a small `EmbeddingSettings` constructor so the
  rest of the code cannot observe invalid values.

## Human review:
- Question postgres can't do usize properly, so even if sqlite can handle it better, its probably not worth switching
- So just add some validation for now.

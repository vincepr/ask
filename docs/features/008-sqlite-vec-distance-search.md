# 008: sqlite-vec Feasibility on the Current SQLite Stack

## Context

The project has embedding rows but no vector search implementation yet.
`document_embeddings.embedding` is currently stored as a SQLite `BLOB`.

If the project wants to stay on SQLite, `sqlite-vec` is the most plausible path
to native-ish vector search.

## Feasibility

Feasible, with moderate schema and synchronization work.

This is **not** a drop-in "add one crate and keep everything else as-is"
change.

## Corrections to the Earlier Draft

1. Extension registration is only one small part of the work.
2. Search needs new schema, new write-path synchronization, and new query logic.
3. `sqlite-vec` is pre-v1, and its official Rust bindings are not covered by a
   stable semver contract.
4. `vec0` columns have fixed dimensions. The current schema allows multiple
   embedding models with different `dimensions`, so one universal vec table is
   not enough unless the product constrains itself to one active search model.

## Practical Design

If this route is chosen, the safest first version is:

1. Search only one configured embedding model at a time.
2. Create a vec table whose row ids map to `document_embeddings.id`.
3. Populate or replace vec rows only when an embedding row reaches
   `state = 'embedded'`.
4. Join vec-search results back to `document_embeddings` and then to
   `documents`.

That avoids inventing dynamic table creation per request. If multi-model search
is required later, then per-model or per-dimension vec tables need an explicit
design.

## Required Work

- [ ] Add `sqlite-vec` dependency and register the extension before pool warmup
      or otherwise guarantee every connection sees it
- [ ] Add migration(s) for vec table(s) and their mapping to embedding rows
- [ ] Define the supported search model strategy: one active model first, or
      explicit per-model tables
- [ ] Update embed-write paths so vec rows stay in sync with embedded rows
- [ ] Add repository search queries that join vec results back to documents
- [ ] Add backfill tooling for existing embedded rows
- [ ] Add integration tests for insert, update, delete, and query behavior

## Constraints

- `sqlite-vec` currently uses brute-force search rather than mature ANN indexes
- vec tables duplicate part of the logical state and therefore need careful
  transactional sync
- this path is an alternative to the PostgreSQL migration path, not a prerequisite for it

## Acceptance Criteria

1. Vector search works without loading all embeddings into Rust memory.
2. Search and embed-write paths stay consistent when rows are updated or deleted.
3. The chosen design handles model dimensions explicitly instead of assuming one
   fixed vector width everywhere.

# qmd Repository Overview

Source inspected: [tobi/qmd](https://github.com/tobi/qmd) at commit
`3f751cd0f0fcc9336095abe050947403d32fcc25`, cloned locally to
`temp/qmd-upstream`.

This note summarizes the architecture. The ingestion-specific details are in
[`01-ingestion-and-vector-search.md`](01-ingestion-and-vector-search.md), and
the direct lessons for this Rust repository are in
[`02-lessons-for-ask.md`](02-lessons-for-ask.md).

## What qmd Is

`qmd` is a TypeScript CLI, SDK, and MCP server for local document search. It is
optimized for personal notes, documentation, meeting notes, and codebases. Its
search stack combines:

- SQLite document metadata and content storage.
- SQLite FTS5 for BM25 lexical search.
- `sqlite-vec` for local vector nearest-neighbor search.
- `node-llama-cpp` for local embedding, query expansion, and reranking.
- Optional AST-aware code chunking through `web-tree-sitter`.

The published package is `@tobilu/qmd`; the package manifest declares Node 22+,
`better-sqlite3`, `sqlite-vec`, `node-llama-cpp`, `fast-glob`, `picomatch`,
`yaml`, and tree-sitter grammar packages. The README describes the system as
"on-device" and model-backed, with models downloaded into `~/.cache/qmd/models/`.

## Major Source Files

The core implementation is concentrated in a small number of files:

| File | Role |
|---|---|
| `src/store.ts` | Database initialization, document indexing, chunking, embedding persistence, FTS search, vector search, RRF fusion, hybrid search, retrieval helpers. |
| `src/llm.ts` | Local model resolution, embedding/query/rerank formatting, llama.cpp lifecycle, batching, parallel contexts, truncation. |
| `src/ast.ts` | Optional tree-sitter language detection and AST break point extraction for code chunking. |
| `src/db.ts` | Cross-runtime SQLite adapter for Node and Bun, including `sqlite-vec` extension loading. |
| `src/index.ts` | SDK facade around the internal store. |
| `src/cli/qmd.ts` | CLI command parsing, progress output, status, doctor, update, embed, search, query. |
| `src/collections.ts` | YAML and inline collection config handling. |

The important design decision is that the store owns the retrieval pipeline.
The CLI, MCP server, and SDK are thin frontends around the same functions.

## Storage Model

`qmd` stores documents in content-addressed form:

- `content`: unique document bodies keyed by content hash.
- `documents`: collection/path/title metadata pointing to a content hash.
- `documents_fts`: FTS5 virtual table for filepath, title, and body.
- `content_vectors`: per-content-hash chunk metadata, including chunk sequence,
  source position, model, embedding fingerprint, expected chunk count, and
  embedding timestamp.
- `vectors_vec`: `sqlite-vec` virtual table keyed by `hash_seq`.
- `store_collections`: DB copy of configured collections.
- `store_config`: metadata and migration state.
- `llm_cache`: query expansion and rerank cache.

This is different from this repo's current `document_embeddings` schema. qmd
keys vectors by content hash, not only by document id. Identical content across
collections can share vector rows, while document paths stay separate.

## User-Facing Workflows

qmd exposes two distinct phases:

1. `qmd update`: scan collections, read files, compute hashes/titles, write
   content, document metadata, and FTS rows.
2. `qmd embed`: find active content hashes that do not have complete vectors
   for the active embedding fingerprint, chunk them, embed chunk text, and
   write vector rows.

Search has three main modes:

- `search`: BM25/FTS only.
- `vsearch`: vector-only semantic search.
- `query`: hybrid search with typed query expansion, FTS, vector retrieval, RRF
  fusion, chunk selection, and reranking.

The SDK exposes the same split through `store.update()`, `store.embed()`,
`store.searchLex()`, `store.searchVector()`, and `store.search()`.

## Model Strategy

The default model URIs are defined in `src/llm.ts`:

- Embedding: `embeddinggemma-300M-Q8_0`.
- Reranking: `qwen3-reranker-0.6b-q8_0`.
- Query expansion: `qmd-query-expansion-1.7B-q4_k_m`.

qmd supports alternate embedding models via config or `QMD_EMBED_MODEL`.
Embedding rows carry both `model` and an embedding fingerprint derived from:

- model URI,
- query embedding prompt format,
- document embedding prompt format,
- chunk token size,
- chunk overlap.

That fingerprint is a practical stale-vector strategy. If prompt formatting or
chunk parameters change, old rows are treated as pending without relying only on
model name.

## High-Level Retrieval Pipeline

The hybrid pipeline in `src/store.ts` is:

1. Probe BM25 for the original query.
2. If the BM25 signal is strong and no intent override is supplied, skip query
   expansion.
3. Expand the query into typed `lex`, `vec`, and `hyde` variants.
4. Route `lex` variants to FTS5.
5. Route original, `vec`, and `hyde` variants to vector search.
6. Fuse result lists using weighted reciprocal rank fusion.
7. Re-chunk candidate document bodies and pick one best chunk per candidate.
8. Rerank those chunks, not full documents.
9. Blend retrieval rank and reranker score with position-aware weights.
10. Deduplicate by file, filter by score, and return.

qmd is therefore not just "vector search over chunks". It treats vector search
as one candidate generator in a larger retrieval system.

## Notable Engineering Choices

The strongest implementation lessons are:

- Keep indexing and embedding separate. qmd can update text metadata without
  synchronously loading models.
- Persist enough vector metadata to detect partial and stale embeddings.
- Use lexical search as a first-class backend, not a fallback.
- Avoid reranking full documents. qmd explicitly reranks selected chunks to
  avoid a token-cost trap.
- Batch vector query embedding in hybrid/structured search.
- Use a two-step `sqlite-vec` query when the extension cannot safely handle
  joins in the vector MATCH query.
- Treat model identity as model plus formatting plus chunking parameters.
- Add diagnostics for vector health, embedding freshness, mixed fingerprints,
  grammar availability, and runtime issues.

## Caution

qmd is a fast-moving TypeScript project. This analysis reflects the inspected
commit. The README and changelog mention fixes for partial embeddings, vector
table state, CJK FTS normalization, sqlite-vec join behavior, model drift, and
cross-platform llama.cpp issues. Those are useful signals: the ingestion and
retrieval design has been iterated against real operational failures.

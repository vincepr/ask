# Lessons From qmd for ask

Source inspected: [tobi/qmd](https://github.com/tobi/qmd) at commit
`3f751cd0f0fcc9336095abe050947403d32fcc25`.

This note compares qmd's ingestion and retrieval design with the current Rust
repo state. It assumes the current `ask` implementation inspected on
2026-06-04.

## Current ask Baseline

`ask` already has a lean vector pipeline:

1. `crates/ask-server/src/worker/ingest.rs` walks a requested root, optionally
   using git-tracked files, registers document metadata, and plans pending
   embeddings.
2. `crates/ask-server/src/worker/embed_document.rs` re-reads each document,
   prepares the same filename/content chunks, calls the embedding provider, and
   replaces embedding rows.
3. `crates/ask-core/src/repository.rs` stores `document_embeddings` rows with
   `chunk_type`, byte offsets, state, and vector bytes.
4. `crates/ask-server/src/vector_index.rs` keeps one active `sqlite-vec` table
   for the configured embedding model.
5. `search_documents_by_embedding()` queries that vector table and joins results
   back to documents.

This is simpler than qmd, but the gap is now clear: qmd has a retrieval system;
`ask` currently has vector search over byte slices plus filename embeddings.

## Biggest Differences

| Area | qmd | ask today | Lesson |
|---|---|---|---|
| Chunk unit | Content-hash chunk rows with sequence, position, model, fingerprint, expected count. | `document_embeddings` rows keyed by document/model/type/start. | Add stable chunk records and complete-coverage checks before adding more retrieval features. |
| Chunking | Markdown-aware, code-fence-aware, optional AST boundaries, token-validated. | Fixed byte windows with overlap. | Replace byte chunking before evaluating retrieval quality. |
| Embedding identity | Model plus prompt format plus chunk token settings. | Model row has name, dimensions, chunk size, overlap. | Extend identity to include embedding input format and chunker version. |
| Lexical search | FTS5 body/title/path BM25 is first-class. | No content FTS search yet. | Add FTS5 before reranking or generated context. |
| Hybrid retrieval | Typed expansion, BM25, vector, RRF, reranking chunks. | Vector-only search. | Build candidate-generation layers before answer-generation or reranker layers. |
| Vector table | Metadata table plus `sqlite-vec`; vector MATCH is isolated from joins. | Single active `document_embedding_vec`; current search uses MATCH inside a CTE then joins. | Keep testing sqlite-vec join behavior; consider qmd's two-step approach if hangs appear. |
| Failure recovery | Retry failed chunks, remove partial coverage, diagnose fingerprints. | Embedding job replaces document/model atomically, but provider failures can leave pending work stuck depending on job behavior. | Track chunk-level failures and provider readiness more explicitly. |

## Recommended Direction

The qmd lessons support the direction already hinted by existing docs:

- `docs/issues/003-text-decoding-beyond-utf8.md`
- `docs/backlog/sqlite-fuzzy-or-hybrid-search.md`
- `docs/temp/anthropic-contextual-retrieval-analysis.md`

The most useful path is not to copy qmd wholesale. The right path is to adopt
the smallest durable pieces in Rust:

1. Create a shared chunk planner.
2. Persist chunk text or chunk references separately from model-specific
   embeddings.
3. Add FTS5 lexical indexing over chunk text, title/path, and maybe decoded
   body.
4. Add RRF fusion between FTS and vector candidates.
5. Add reranking only after chunking and FTS are measurable.

## Lesson 1: Stop Using Byte Windows as the Retrieval Contract

Current `ask` chunking is in `worker/ingest.rs`:

```rust
while start < len {
    let end = std::cmp::min(start + chunk_size, len);
    chunks.push((start, end));
    if end >= len {
        break;
    }
    start += step;
}
```

`embed_document.rs` then slices `content.as_bytes()[start..end]` and converts
with `String::from_utf8_lossy()`.

This is robust enough to avoid some panics during embedding, but it is a poor
semantic retrieval unit:

- It can split in the middle of a UTF-8 scalar boundary.
- It can split headings from the paragraphs they introduce.
- It can split code declarations mid-function.
- It forces ingest and embed to re-derive chunks from mutable file contents.
- It stores byte offsets but no chunk text, title, heading path, or chunk
  fingerprint.

qmd's chunking strategy is a better target:

- choose natural markdown break points,
- protect fenced code blocks,
- optionally add AST break points for source code,
- enforce token limits with the actual embedding tokenizer,
- store chunk sequence and source position,
- store expected total chunk count.

For `ask`, the first increment should be a UTF-8-safe, line/markdown-aware chunk
planner in Rust. It does not need tree-sitter on day one. It should produce
stable chunks with:

```text
document_id
chunk_ordinal
chunk_type
char_start or byte_start plus a UTF-8-safe invariant
char_end or byte_end plus a UTF-8-safe invariant
heading_path
text_hash
text
created_at
```

Persisting text is important because embedding jobs should not re-read a file
and hope it still matches the pending offsets. qmd avoids this class of drift by
embedding content stored in SQLite.

## Lesson 2: Separate Chunks From Embeddings

qmd's `content_vectors` is still model-specific, but qmd's content table is not.
The next `ask` schema should make this separation clearer:

- `documents`: file metadata.
- `document_contents` or content fields: decoded body hash and text.
- `document_chunks`: model-independent chunk identity and chunk text.
- `document_embeddings`: model-specific vector for a chunk.
- `document_chunk_fts`: FTS5 over chunk text plus path/title/heading metadata.

This prevents every retrieval improvement from being encoded as a new embedding
row shape. It also makes it possible to add:

- lexical-only search when embedding provider is down,
- hybrid search,
- contextual text,
- different embedding models,
- chunker version migrations,
- retrieval evaluation fixtures.

## Lesson 3: Make Embedding Completeness Explicit

qmd considers a content hash pending if stored chunk count is lower than the
expected total chunk count. It also removes partial vectors after failed embed
runs.

`ask` currently replaces all embeddings for a document/model in one transaction
after the provider returns every vector. That is good: it avoids partial
document replacement. But the pending rows are planned separately from the final
replacement rows, and the queue operates at document/model granularity.

For larger files and slower providers, qmd's chunk-level failure accounting is
worth adapting:

- persist expected chunk count for each chunking strategy,
- store per-chunk failure reason and attempts if embedding fails,
- make document/model searchable only when the chunk set is complete,
- keep stale or pending chunks visible in status endpoints,
- retry failed chunks without hiding unrecovered failures behind logs.

The old provider-readiness note was removed after transient embedding retry
behavior became good enough for the current queue model.

## Lesson 4: Add FTS5 Before Reranking

qmd gets a lot of value from FTS5:

- exact symbols,
- filenames,
- phrases,
- error strings,
- headings,
- negated lexical queries,
- a cheap strong-signal bypass before loading query expansion/rerank models.

`ask` should add FTS5 before any LLM reranker. A minimal first version could:

1. Add `document_chunks`.
2. Add an FTS5 table over `(filepath, chunk_text, heading_path)`.
3. Query FTS5 with BM25.
4. Query vector search as today.
5. Fuse with RRF.
6. Collapse to documents only at the API boundary.

This matches the existing Anthropic contextual retrieval note: contextual BM25
and vector search are both more useful when chunk text is durable.

## Lesson 5: Delay Query Expansion Until There Is an Evaluation Harness

qmd's query expansion is sophisticated: it generates typed lexical, vector, and
HyDE-style variants, then routes them to different backends. That is valuable,
but it depends on:

- a local generation model,
- caches,
- validation of generated query syntax,
- metrics to know whether expansion helps or hurts.

For `ask`, adding query expansion before FTS and chunk-level evaluation would be
premature. The qmd lesson is not "add a query LLM now"; it is "when expansion is
added, make expansions typed and backend-routed."

The useful intermediate shape is:

```text
SearchRequest {
    query: String,
    mode: Vector | Lexical | Hybrid,
    limit: usize,
    min_score: Option<f32>,
}
```

Later:

```text
ExpandedQuery {
    kind: Lex | Vec | Hyde,
    query: String,
}
```

## Lesson 6: Treat Embedding Input Format as Model Identity

qmd's fingerprint includes formatted query and document probes. `ask` currently
stores embedding model name, dimensions, chunk size, and overlap. That catches
some drift, but not all of it.

The embedding identity should eventually include:

- provider/model name,
- dimensions,
- chunker name and version,
- chunk size and overlap,
- document input template version,
- query input template version,
- optional normalization/decoding version.

This matters if `ask` changes from embedding raw chunk text to embedding:

```text
path: crates/ask-server/src/worker/ingest.rs
heading: plan_pending_embeddings_for_document
text: ...
```

Old vectors should become stale automatically.

## Lesson 7: Preserve qmd's Two-Step sqlite-vec Caution

qmd explicitly avoids joining `sqlite-vec` MATCH directly with document tables
because it observed hangs. The current `ask` query uses a `nearest` CTE and then
joins `document_embeddings` and `documents`.

That may be fine with the Rust `sqlite-vec` binding and current SQLite version,
but it deserves a regression test with a large enough fixture and a timeout. If
search ever hangs or degrades, qmd's safer pattern is:

1. Query only `document_embedding_vec` for rowids and distances.
2. Fetch metadata in a second SQL statement using those rowids.
3. Join/deduplicate in Rust or normal SQL without MATCH.

This is a pragmatic reliability lesson, not a required immediate rewrite.

## Lesson 8: Keep One Active Vec Table Until There Is a Need for More

`ask` currently has one active vector-search table configured by
`embedding_search_state`. qmd also effectively recreates `vectors_vec` for the
active dimensionality, while preserving model/fingerprint metadata separately.

Do not rush to multiple sqlite-vec tables. One active table keeps the system
small. The higher-value change is making the table rebuild and backfill clearly
diagnosable:

- active model id,
- dimensions,
- number of backfilled rows,
- number of missing embeddings,
- number of stale embeddings,
- number of chunks without FTS rows.

## Concrete Implementation Order

Recommended order for `ask`, based on qmd:

1. Build a shared UTF-8-safe chunk planner used by both ingest and embed.
2. Persist chunk rows and chunk text before embedding.
3. Make embedding jobs consume persisted chunks rather than re-reading files.
4. Add chunker/template fingerprint fields to embedding identity.
5. Add an FTS5 chunk index.
6. Add lexical search endpoint/mode.
7. Add hybrid search with RRF over FTS and vector candidates.
8. Add chunk-level status and partial/failure diagnostics.
9. Add optional AST break points for Rust/Python/TS code only if evaluation
   shows fixed markdown/line chunking is insufficient.
10. Add reranking after the candidate pipeline is stable.

## What Not To Copy

Do not copy these parts yet:

- TypeScript runtime model management through `node-llama-cpp`.
- Local query expansion model.
- MCP-specific query affordances.
- Tree-sitter dependency stack as a first step.
- Complex reranker blending before there is an FTS/vector benchmark.

Those features are coherent inside qmd, but they would add too much surface area
to this repo before the chunk and FTS foundation exists.

## Bottom Line

qmd's ingestion strategy is mature because it treats retrieval as a pipeline:
file scan, durable content, semantic chunking, embedding fingerprinting,
complete vector coverage, FTS, vector search, rank fusion, selected-chunk
reranking, and diagnostics.

The main lesson for `ask` is to make chunks durable and semantically meaningful
before adding more model calls. Once chunks are persisted and FTS5 exists, the
rest of qmd's retrieval strategy can be adopted incrementally without turning
the Rust codebase into a large framework.

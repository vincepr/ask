# Anthropic Contextual Retrieval Analysis

## Scope

This note analyzes Anthropic's
["Introducing Contextual Retrieval"](https://www.anthropic.com/engineering/contextual-retrieval),
published on 2024-09-19, against the current `ask` repository as inspected on
2026-06-01.

Anthropic's method has two parts:

1. Generate a short, chunk-specific explanation using the whole source
   document as context.
2. Prepend that explanation to the chunk before both embedding and lexical
   indexing.

Anthropic calls the resulting techniques Contextual Embeddings and Contextual
BM25. The article reports lower retrieval failure rates at recall@20 when they
are combined, with a further improvement from reranking. Those measurements are
useful evidence that the approach is worth evaluating. They are not evidence
that Anthropic's chunk sizes, top-k values, providers, or reranker settings are
optimal for `ask`.

## Current `ask` Retrieval Path

`ask` already has a lean vector-search pipeline:

1. [`worker/ingest.rs`](../../crates/ask-server/src/worker/ingest.rs) registers
   each file and plans a filename chunk plus fixed-size content chunks.
2. [`worker/embed_document.rs`](../../crates/ask-server/src/worker/embed_document.rs)
   re-reads the file, embeds the filename and content slices, and stores vector
   bytes.
3. [`vector_index.rs`](../../crates/ask-server/src/vector_index.rs) maintains
   one active `sqlite-vec` table.
4. [`repository.rs`](../../crates/ask-core/src/repository.rs) runs vector KNN
   search and joins matching embedding rows back to documents.
5. [`http.rs`](../../crates/ask-server/src/http.rs) embeds the query, retrieves
   an over-fetched vector result list, collapses it to one hit per document, and
   returns filepaths with optional byte offsets.

The current schema in
[`0002_create_domain_tables.sql`](../../crates/ask-core/migrations/0002_create_domain_tables.sql)
stores document metadata, chunk byte offsets, state, and vectors. It does not
persist:

- original chunk text
- retrieval-only contextual text
- a lexical content index
- a context-generation strategy or version
- a reranker score
- a stable chunk identity independent of an embedding model

The existing embedding client in
[`embeddings.rs`](../../crates/ask-server/src/embeddings.rs) only supports
OpenAI-compatible embedding requests. There is no text-generation client or
prompt-cache integration.

## Fit Assessment

| Anthropic technique | Current `ask` state | Assessment |
|---|---|---|
| Contextual Embeddings | Content chunks are embedded without document context. Filename chunks are embedded separately. | Applicable, especially for chunks whose meaning depends on their filepath, heading, module, type, or neighboring declarations. |
| Contextual BM25 | No lexical content index exists. | Applicable after persisted chunk text and SQLite FTS5 are added. This is a high-value prerequisite even without generated context. |
| Rank fusion | Search is vector-only. | Applicable after FTS5 exists. Fuse lexical and vector ranks before collapsing to one result per document. |
| Reranking | No reranker stage exists. | Potentially useful, but it adds another runtime provider call and should follow evaluation of cheaper indexing improvements. |
| Prompt caching during contextualization | No generation provider exists. | Optional optimization for a future LLM contextualizer. It should not be a prerequisite for the first hybrid-search release. |
| Passing top-20 chunks to answer generation | `/search` returns documents, not an answer-generation context bundle. | Not directly applicable until the API exposes chunk-oriented retrieval or a later answer-generation layer. |

## Key Repository Implications

### 1. Persist Original Chunks Before Adding Generated Context

Contextual Retrieval needs a durable chunk record. Add a normal
`document_chunks` table with fields such as:

```text
id
document_id
chunk_type
chunk_ordinal
byte_start
byte_end
original_text
retrieval_context
context_strategy
context_updated_at
```

`document_embeddings` should reference the stable chunk ID. Keep original text
separate from retrieval context:

- embed and lexical-index `retrieval_context + original_text`
- return `original_text`, filepath, and source offsets to callers
- retain the generated context for debugging and retrieval evaluation

Do not prepend generated context destructively into the stored source text. It
would pollute snippets and make it harder to distinguish source evidence from
LLM-generated metadata.

This prerequisite overlaps with the recommendations in
[`gapvec-retrieval-comparison.md`](gapvec-retrieval-comparison.md).

### 2. Add Hybrid Search Before Paying For LLM Contextualization

Anthropic's own conclusion is that embeddings plus BM25 outperform embeddings
alone. `ask` should first add SQLite FTS5 over persisted chunks and fuse
lexical and vector candidates in Rust.

Suggested query flow:

1. Embed the query and retrieve an over-fetched vector candidate list.
2. Query FTS5 and retrieve an over-fetched BM25-ranked candidate list.
3. Fuse ranks with reciprocal rank fusion.
4. Collapse to documents only after fusion for the current API mode.
5. Add a chunk-oriented response mode when downstream grounding needs it.

This improves exact identifier retrieval for code, filenames, configuration
keys, and error strings without introducing a generation dependency. Use the
normal FTS tokenizer for content. Keep the trigram filepath mode proposed in
[`009-sqlite-fuzzy-search.md`](../features/009-sqlite-fuzzy-search.md) as a
separate fallback search mode.

### 3. Fix Chunk Planning Before Measuring Contextual Retrieval

The configured chunk size is described as tokens in
[`config.rs`](../../crates/ask-server/src/config.rs), but
[`worker/ingest.rs`](../../crates/ask-server/src/worker/ingest.rs) currently
splits by byte count. The existing
[`005-utf8-safe-chunking.md`](../improvements/005-utf8-safe-chunking.md) note
also identifies unsafe UTF-8 boundary behavior.

Generated context cannot compensate for poor chunk boundaries. Introduce one
shared planner used by ingest planning and embed-time materialization. At
minimum it must be UTF-8-safe. Prefer structure-aware boundaries for markdown
and code, with bounded line or character-safe fallbacks.

Anthropic reports experiments using chunks of a few hundred tokens and gives an
800-token costing example. Treat those as evaluation inputs, not defaults for a
byte-based chunker.

### 4. Version Context As Part Of Embedding Identity

[`EmbeddingIdentity`](../../crates/ask-core/src/models/embedding_model.rs)
currently includes model name, dimensions, chunk size, and overlap. Once the
embedded input includes contextual text, reuse is only valid when the
contextualization strategy is also unchanged.

Add a stable identity component such as:

```text
context_strategy = "none"
context_strategy = "deterministic-code-v1"
context_strategy = "llm:<provider>:<model>:<prompt-version>"
```

A prompt change, contextualizer model change, or deterministic formatter change
must create a new embedding identity and trigger backfill. Otherwise the active
index can silently mix vectors produced from different retrieval inputs.

### 5. Start With Deterministic Context For Code And Resource Trees

Anthropic generates 50-100 tokens of context from the whole document. For
`ask`, a deterministic first version is cheaper and simpler:

```text
Path: crates/ask-core/src/repository.rs
File type: Rust source
Section: search_documents_by_embedding
```

Useful deterministic context can include:

- normalized relative filepath
- basename and extension
- markdown heading path
- code symbol or nearby declaration when reliably extractable
- document category

This is particularly suitable for source trees, where filepath and symbol
names carry retrieval meaning. It also avoids making ingestion depend on a
generation API while the job retry semantics in
[`004-embedding-provider-readiness.md`](../issues/004-embedding-provider-readiness.md)
remain unresolved.

LLM-generated context can then be evaluated as an optional strategy for prose,
notes, and documents where deterministic metadata is insufficient.

### 6. Treat Full-Document Contextualization As A Bounded Job

The article's prompt includes the whole document for each chunk, with prompt
caching proposed to reduce repeated cost. `ask` currently reads whole files and
already has an open size-guard concern in
[`006-file-filtering-and-size-guards.md`](../improvements/006-file-filtering-and-size-guards.md).

An LLM contextualizer therefore needs explicit bounds:

- maximum contextualizable document size
- maximum generated context length
- provider timeout and retry policy
- persisted failure state distinct from embedding failure
- deterministic fallback when contextualization is unavailable
- optional prompt caching when supported by the chosen provider

Do not place generation inline inside the existing `embed_document` call
without separate state. A transient contextualizer outage would otherwise
block all embeddings and inherit the queue's current long stale-claim behavior.

### 7. Defer Reranking Until The Candidate Pipeline Is Measurable

Anthropic reports its best result by retrieving a broad candidate set,
reranking it, and selecting a smaller final set. That maps naturally to `ask`,
but reranking should come after chunk persistence, FTS5, rank fusion, and a
retrieval oracle exist.

The current `/search` endpoint returns at most 100 documents and internally
over-fetches vector hits by a factor of four. A future chunk-oriented pipeline
can independently configure:

- vector candidate count
- BM25 candidate count
- fused candidate count
- reranker input count
- final chunk count
- final document count

Do not copy Anthropic's top-150 to top-20 values without measuring the local
corpus and latency budget.

## Recommended Delivery Order

1. Implement one UTF-8-safe shared chunk planner and persist stable chunk rows.
2. Add SQLite FTS5 content indexing and reciprocal-rank fusion.
3. Add an `ask` retrieval oracle covering code, markdown, exact identifiers,
   error strings, and semantic paraphrases.
4. Add deterministic retrieval context from filepath and structural metadata.
5. Extend embedding identity with the context strategy version.
6. Evaluate an optional LLM contextualizer for prose-heavy corpora.
7. Add reranking only if measured recall or ranking quality still justifies the
   runtime dependency.

## Evaluation Requirements

Measure each stage independently:

| Variant | Purpose |
|---|---|
| vector-only baseline | Quantify the current system. |
| BM25-only baseline | Measure exact-match strength. |
| vector + BM25 fusion | Isolate the value of hybrid retrieval. |
| deterministic context + fusion | Measure cheap code/resource-tree context. |
| LLM context + fusion | Measure the incremental value of generated context. |
| chosen context + fusion + reranker | Measure whether runtime reranking earns its cost. |

Include regression queries for:

- exact symbols such as `search_documents_by_embedding`
- configuration names such as `ASK_SERVER_EMBEDDING_MAX_BATCH_SIZE`
- errors such as `active embedding_search_state row is missing`
- chunks with ambiguous local text but distinctive filepath or heading context
- duplicate terminology across different modules
- multibyte UTF-8 around chunk boundaries
- changed context strategy versions and required backfill behavior
- contextualizer outages and deterministic fallback behavior

Track recall at k, reciprocal rank, indexing latency, provider cost, database
growth, and query latency. Report chunk-level and document-level metrics
separately because the current API collapses multiple matching chunks into one
document.

## Conclusion

Contextual Retrieval is a strong fit for `ask`, but the first valuable change
is not an Anthropic API integration. The repository should first persist clean
chunk records, add SQLite FTS5 and rank fusion, and repair chunk planning. Those
steps enable Contextual BM25 immediately and create the evaluation surface
needed to judge deterministic and LLM-generated contextual embeddings.

For this repository's source-code-heavy workload, deterministic filepath and
structural context should be the first contextualization strategy. Add
LLM-generated context and reranking only as measured, versioned strategies with
bounded failure behavior.

## External Sources

- [Anthropic: Introducing Contextual Retrieval](https://www.anthropic.com/engineering/contextual-retrieval)
- [SQLite: FTS5 Extension](https://sqlite.org/fts5.html)

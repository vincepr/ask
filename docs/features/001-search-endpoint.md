# Search Endpoint

## Problem

The system builds a vector index of document chunks (sqlite-vec with cosine
distance). The repository layer has `search_documents_by_embedding()` that takes
a query vector and returns nearest chunks. But there is no way to actually
query the index — no HTTP endpoint, no CLI, nothing.

## Primary Audience: AI Citizens

The endpoint is designed for AI agents (LLM tool calls, automated reasoning
pipelines), not human eyeballs. Every field in the response should be directly
useful to an AI reading it. No internal IDs, no implementation artifacts.

## What Exists

- `repository::search_documents_by_embedding()` at
  `crates/ask-core/src/repository.rs:545` — returns `DocumentSearchResult`
  with fields like `document_id`, `embedding_id`, `model_id`, `chunk_type`,
  `chunk_start`, `chunk_end`, `distance`, etc.
- `HttpEmbeddingClient` at `crates/ask-server/src/embeddings.rs:59` — can
  embed text via the same provider TEI uses.
- No HTTP endpoint exposes either.

## Design Constraints for AI Consumption

**Minimal, self-explanatory fields.** An AI reading the response should be able
to use the filepath directly. No IDs that require a second lookup.

**Helpful score.** The raw cosine distance from sqlite-vec is an implementation
detail. What matters to an AI is relative ordering and a sense of confidence.
A score that is intuitive (e.g., higher = better, 0..1) is preferable.

**Chunk text inline.** The AI needs the actual content of the matching chunk.
Pushing file I/O to the AI (by returning only byte offsets) adds a second
round-trip and wastes context. The server should read the file and include the
text.

**Ordered by relevance.** Results must be returned in descending relevance
(most relevant first). The AI should not need to re-sort.

## Questions

**Response shape:**
- What is the minimum set of per-result fields an AI needs?
  - Filepath (for source attribution and re-reading)
  - Chunk text (the actual content)
  - Relevance score (high = good match, low = weak match)
  - Maybe a snippet of surrounding context?
- Should the response include the original query for echo-verification?
- Should the response include a summary of what was searched
  (e.g., total indexed chunks)?

**Relevance score:**
- Cosine distance is 0..2 (0 = identical, 1 = orthogonal, 2 = opposite).
  A raw distance is unintuitive for an AI. Should we transform it to a
  similarity score, e.g., `1 - distance` or `(2 - distance) / 2`?
- Should the score be returned as `score` (higher = better) or `distance`
  (lower = better)? Using `score` is more intuitive.

**Chunk text extraction:**
- Reading files at chunk offsets has the same path resolution problem as
  issue #02/#07. Should search include a workaround, or wait for those to
  be fixed?
- If a file is missing or unreadable, should the result be omitted entirely
  (so the AI only gets useful results), or included with empty text?

**Endpoint style:**
- `POST /search` with `{ "query": "...", "limit": 10 }` is standard for JSON
  APIs. Is a `GET /search?q=...` also needed for simpler AI tool definitions
  (many AI frameworks prefer GET for tool calls)?
- If both, how is consistency maintained?

**Error responses:**
- What should the AI see when the index is empty — an empty results list, or
  a clear message like "no documents have been embedded yet"?
- What should the AI see when the embedding provider is down — a standard
  error, or an empty list (graceful degradation)?

**Relation to existing:**
- `search_documents_by_embedding` returns DB IDs and internal types. A
  response type for AI consumption means mapping fields. Should the mapping
  happen in the HTTP handler, or should there be a new query function that
  returns AI-friendly types directly?
- The embedding provider readiness problem (issue #09) also affects search.
  Should search handle provider downtime differently (e.g., return a clear
  error message the AI can report), or fail the same as other endpoints?

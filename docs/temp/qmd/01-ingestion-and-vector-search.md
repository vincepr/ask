# qmd Ingestion and Vector Search Deep Dive

Source inspected: [tobi/qmd](https://github.com/tobi/qmd) at commit
`3f751cd0f0fcc9336095abe050947403d32fcc25`.

This file focuses on how qmd ingests content, splits files, embeds chunks, and
uses the vector index.

## Phase 1: Collection Indexing

The indexing entry point is `reindexCollection()` in `src/store.ts`. It receives
a collection path, glob pattern, collection name, and optional ignore patterns.

The scan strategy is:

1. Build default excludes for `node_modules`, `.git`, `.cache`, `vendor`,
   `dist`, and `build`.
2. Merge configured ignore patterns.
3. Use `fast-glob` with:
   - `cwd` set to the collection path,
   - `onlyFiles: true`,
   - `followSymbolicLinks: false`,
   - `dot: false`,
   - merged ignore patterns.
4. Filter any path segment that starts with `.`.
5. Read each remaining file with `readFileSync(filepath, "utf-8")`.
6. Skip files that throw during the filesystem read and skip empty/whitespace
   files.
7. Hash the complete content.
8. Extract a title from content or filename.
9. Upsert content and document metadata.
10. Deactivate active documents from that collection that were not seen.
11. Clean orphaned content.

The path stored in `documents.path` is the literal collection-relative path with
normalized separators. qmd intentionally does not "handelize" or display-normalize
the path at index time because that breaks path fidelity.

Important caveat: this is not a binary-file detector. The default collection
glob is `**/*.md`, so PNGs and other non-markdown assets are normally excluded
before the read step. If a collection is configured with a broad glob such as
`**/*`, qmd does not appear to reject files by MIME type or extension in
`reindexCollection()`. A binary file can be decoded as replacement-character
UTF-8 text and indexed if the read itself succeeds.

### What It Stores During Indexing

The database schema is created in `initializeDatabase()`:

- `content(hash, doc, created_at)`: one copy of each unique body.
- `documents(id, collection, path, title, hash, created_at, modified_at, active)`.
- `documents_fts(filepath, title, body)`: FTS5 virtual table.

The document row is the path layer. The content row is the body layer. This lets
qmd share vector work for duplicate bodies and keep multiple collection paths
attached to the same content hash.

### FTS Population

qmd has FTS triggers, but the production indexing path also writes/rebuilds FTS
rows so it can normalize CJK text before SQLite's `unicode61` tokenizer sees it.

The FTS index includes:

- collection-relative filepath,
- title,
- full body.

BM25 is weighted as `bm25(documents_fts, 1.5, 4.0, 1.0)`, so title matches are
more important than body matches. Query execution uses a CTE that forces FTS5 to
produce candidates before collection filtering, because combining MATCH and the
collection predicate directly caused slow full scans in large collections.

## Phase 2: Selecting Documents for Embedding

Embedding starts in `generateEmbeddings()`.

If `force` is true, qmd clears existing vectors. Otherwise it selects pending
content hashes through `getPendingEmbeddingDocs()`. A document is pending when:

- it is active,
- no vector rows exist for its content hash and active model fingerprint, or
- the number of stored chunk rows is less than the stored expected chunk count.

The query groups by `d.hash`, so the embedding unit is unique content, not
document path. It also orders by a sample path for stable progress.

qmd processes pending docs in document batches with two caps:

- maximum documents per batch, default 64,
- maximum input bytes per batch, default 64 MB.

Within a document batch, it creates chunk items and embeds those in sub-batches
of 32 chunk texts.

## Embedding Fingerprint

qmd does not treat model name alone as sufficient vector identity.

`getEmbeddingFingerprint()` hashes:

- model URI,
- formatted query probe text,
- formatted document probe text,
- chunk token size,
- chunk overlap.

Only six hex characters are persisted, but the key idea is strong: changing
prompt formatting or chunking invalidates old vectors. This prevents stale
vectors from silently surviving semantic changes.

## Chunk Parameters

The primary chunk constants are:

- target chunk size: 900 tokens,
- overlap: 15 percent, currently 135 tokens,
- break search window: 200 tokens.

There are also character approximations for sync and first-pass chunking:

- 3600 chars for a 900-token chunk,
- 540 chars for overlap,
- 800 chars for the break search window.

The actual embedding path uses token-aware chunking, so the character values are
not trusted as the final contract.

## Regex Markdown Chunking

The base chunker scans text for break points. Break points have a position,
score, and type. Higher score means better split location.

The regex scores are:

| Boundary | Score |
|---|---:|
| H1 heading | 100 |
| H2 heading | 90 |
| H3 heading | 80 |
| H4 heading | 70 |
| H5 heading | 60 |
| H6 heading | 50 |
| Code fence marker | 80 |
| Horizontal rule | 60 |
| Blank line | 20 |
| Unordered list item | 5 |
| Ordered list item | 5 |
| Any newline | 1 |

The core algorithm walks forward by target chunk size. When the target cut is
not at the end of the document, it searches backward within the break window.
For every candidate break point, it applies squared distance decay:

```text
multiplier = 1.0 - (distance / window)^2 * 0.7
final_score = base_score * multiplier
```

This means a heading reasonably far behind the target can still beat a low-value
newline near the exact target. The split is natural rather than arbitrary.

Code fences are detected separately, and break points inside a fenced region are
ignored. If no good point exists, qmd falls back to the target position.

After emitting a chunk, it backs up by the overlap. If overlap would fail to
make progress, it advances to the end of the previous chunk.

## AST-Aware Code Chunking

When `chunkStrategy` is `auto`, `chunkDocumentAsync()` merges regex break points
with AST break points from `src/ast.ts`.

Supported extensions:

- TypeScript and TSX: `.ts`, `.tsx`, `.mts`, `.cts`, `.jsx`
- JavaScript: `.js`, `.mjs`, `.cjs`
- Python: `.py`
- Go: `.go`
- Rust: `.rs`

AST capture scores are aligned with the markdown scoring scale:

| AST boundary | Score |
|---|---:|
| class, interface, struct, trait, impl, module | 100 |
| export, function, method, decorated definition | 90 |
| type alias, enum | 80 |
| import/use declaration | 60 |

Tree-sitter failures are not fatal. Unsupported files, missing grammars, parse
errors, or explicit `regex` strategy all fall back to regex-only chunking.

This is a conservative implementation: qmd uses AST nodes only as smarter break
points. It does not yet persist symbol metadata or build a symbol index.

## Token-Aware Correction Pass

`chunkDocumentByTokens()` is the embedding-time chunker.

It first runs character chunking with a conservative estimate of three
characters per token. Then it tokenizes each chunk with the active embedding
model. If a chunk is within the target token limit, it is accepted with its
actual token count.

If a chunk is too large:

1. Compute the observed chars-per-token ratio.
2. Compute a smaller safe character limit with a 0.95 safety factor.
3. Re-run the chunker on the oversized text.
4. Recurse into subchunks.
5. If a pathological single-line blob still does not shrink, split it in half.
6. If even that fails, truncate to the maximum token count and detokenize.

The final output is a list of `{ text, pos, tokens }`.

This is a major ingestion lesson: qmd uses natural text boundaries when possible,
but it enforces provider/model token constraints before embedding.

## Document Embedding Text

qmd formats documents differently based on the embedding model family.

For the default embeddinggemma/nomic-style format:

```text
title: {title_or_none} | text: {chunk_text}
```

For Qwen3-Embedding:

```text
{title}
{chunk_text}
```

or just raw text if there is no title.

Query embedding text is also model-aware:

```text
task: search result | query: {query}
```

for the default format, and:

```text
Instruct: Retrieve relevant documents for the given query
Query: {query}
```

for Qwen3-Embedding.

This is why the embedding fingerprint includes formatted probe strings.

## Embedding Execution

`generateEmbeddings()` opens a long-lived LLM session with a default 30-minute
duration. It then:

1. Builds pending document batches.
2. Loads document bodies for the current batch.
3. Extracts the title for each document.
4. Builds chunk items containing hash, path, title, text, sequence, position,
   token count, byte count, and expected total chunk count.
5. Embeds the first chunk once to discover vector dimensions.
6. Ensures `vectors_vec` exists with those dimensions.
7. Embeds chunk texts in batches of 32 through `session.embedBatch()`.
8. Inserts each vector into both metadata and vec tables.
9. Falls back to single-chunk embedding when a batch fails.
10. Tracks failures and retries outstanding chunks.
11. Removes partial vectors for any document whose expected chunk coverage was
    not completed.

The partial-vector cleanup matters. Without it, a crashed embed run can make a
document look embedded while only some chunks are searchable.

## Vector Persistence

qmd keeps two vector-related tables:

```text
content_vectors(hash, seq, pos, model, embed_fingerprint, total_chunks, embedded_at)
vectors_vec(hash_seq, embedding)
```

`hash_seq` is formatted as `{hash}_{seq}`. qmd inserts the metadata row first,
then deletes and inserts the vector row. The delete-plus-insert is intentional:
the project notes that `sqlite-vec` virtual tables silently ignore `INSERT OR
REPLACE` conflict behavior in this path.

The metadata-first order is described as crash-safe for pending selection. qmd
then repairs incomplete coverage with `removeIncompleteEmbeddings()`.

## Vector Search

`searchVec()` performs vector search in two steps:

1. Query `vectors_vec` only:

   ```sql
   SELECT hash_seq, distance
   FROM vectors_vec
   WHERE embedding MATCH ? AND k = ?
   ```

2. Join the returned `hash_seq` values back to `content_vectors`, `documents`,
   and `content` in a normal SQL query.

The code warns not to combine the vector MATCH and joins in one query because
sqlite-vec can hang indefinitely with joins in the same query shape.

qmd over-fetches vector rows by `limit * 3`, joins active document rows, applies
an optional collection filter, deduplicates by filepath, keeps the best chunk
distance per file, sorts ascending by distance, and returns document results.

Vector score is:

```text
1 - cosine_distance
```

The returned result carries `chunkPos`, the source character offset of the best
matching embedded chunk.

## Vector-Only Search Command

`vectorSearchQuery()` is not a bare single embedding lookup. It:

1. Expands the query.
2. Filters out lexical variants.
3. Runs the original query plus `vec` and `hyde` variants through vector search.
4. Deduplicates by filepath, keeping the maximum score.
5. Sorts, filters by minimum score, and limits.

This means even "vector-only" mode can still use local LLM query expansion.

## Hybrid Search

The main `query` path is `hybridQuery()`:

1. Run an initial FTS probe.
2. If the top BM25 score is at least `0.85` and exceeds the second score by at
   least `0.15`, skip expensive query expansion unless an intent was provided.
3. Expand the query into typed variants.
4. Run lexical variants through FTS.
5. Batch embed all vector queries in one `embedBatch()` call.
6. Run vector lookups with the precomputed query embeddings.
7. Fuse ranked lists with weighted RRF.
8. Keep the top candidates.
9. Re-chunk candidate bodies and choose one best chunk per document using query
   and intent term overlap.
10. Rerank only those selected chunks.
11. Blend reranker score with retrieval position.

RRF uses `k = 60`, doubles original-query FTS and vector lists, and adds a top
rank bonus of `0.05` for rank 1 and `0.02` for ranks 2-3.

Position-aware blending uses:

- top 1-3 retrieval candidates: 75 percent retrieval, 25 percent reranker,
- top 4-10: 60 percent retrieval, 40 percent reranker,
- rank 11+: 40 percent retrieval, 60 percent reranker.

The core retrieval insight is that qmd protects strong exact retrieval signals
from being destroyed by a reranker, while still allowing reranking to improve
deeper candidates.

## Diagnostics and Recovery

The changelog and code show several hard-earned recovery features:

- Pending status requires complete chunk coverage.
- Scoped force embedding clears only vectors exclusively owned by that collection.
- Legacy content-vector columns are lazily repaired.
- Mixed embedding fingerprints are diagnosed.
- Vector table dimensions are recreated from the active model.
- Chunk/session failures are tracked and retried.
- Oversized embedding inputs are truncated before llama.cpp can crash.
- Windows CUDA parallelism is reduced by default due to driver instability.

These details are not decorative. They are the operational glue that keeps a
local vector index trustworthy after interrupted runs, model swaps, runtime
changes, and large heterogeneous corpora.

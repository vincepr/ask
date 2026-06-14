# gapvec Retrieval Comparison

## Scope

This report compares `ask` with
[`andunn/gapvec`](https://gitlab.com/andunn/gapvec) as inspected on
2026-06-01 at commit
[`393872ad7b480463843e35c1ffaf23cbb7e759cb`](https://gitlab.com/andunn/gapvec/-/tree/393872ad7b480463843e35c1ffaf23cbb7e759cb).
The comparison focuses on structure-aware chunking, hybrid search, and date
pre-filtering. Section expansion is included because it depends on the same
chunk metadata.

The two repositories have different target workloads:

- `gapvec` is a local CLI for chronological `.txt` and `.md` notes, journals,
  and logs.
- `ask` is a server that ingests broader resource trees, including source code,
  and exposes HTTP search over an active embedding model.

The useful outcome is an `ask`-specific retrieval design, not a direct port.

## Current Comparison

| Capability | `gapvec` | `ask` | Assessment |
|---|---|---|---|
| Chunk boundaries | Splits at recognized date headers and horizontal rules, then groups paragraphs up to a target character count. | Splits every readable UTF-8 file into fixed overlapping byte ranges. | `gapvec` has better semantic boundaries for chronological prose. `ask` needs a general structure-aware splitter with safe fallback behavior because its corpus is broader. |
| Persisted chunk metadata | Stores chunk text, basename, ordinal, and `section_date` next to the vector. | Stores document ID, model ID, chunk type, byte offsets, state, and vector bytes. | `ask` lacks the chunk text and section identity needed for lexical search, snippets, date filtering, and section expansion. |
| Vector search | LanceDB vector search. | `sqlite-vec` `vec0` KNN search. | Both are suitable. `ask` does not need LanceDB to adopt the retrieval ideas. |
| Lexical search | Builds a Tantivy FTS index through LanceDB and runs hybrid vector plus full-text search. | No lexical content search. The only implemented search path embeds the query and runs vector KNN. | Add SQLite FTS5 over persisted chunks and fuse ranks in Rust. |
| Rank fusion | LanceDB hybrid execution uses reciprocal rank fusion (RRF). | No fusion step. Results are vector-distance ordered, then collapsed to one result per document. | RRF is a small, storage-independent improvement once FTS candidates exist. |
| Date metadata | Parses date headers into `section_date`. | Stores filesystem `file_modified_at`, but no date describing the content of a chunk. | File modification time is not an adequate substitute for a section date. |
| Query date filter | Extracts ISO dates, year ranges, quarters, month/year phrases, and years from the query. Applies a date filter before retrieval and retries unfiltered if the filtered search returns no hits. | No query date parsing and no search filters. | Add a typed temporal filter and expose fallback behavior explicitly. |
| Context expansion | Fetches sibling chunks from the same apparent section after top-k retrieval. | Returns a filepath and optional offsets for the best chunk per document. | Add only after stable section IDs and chunk-oriented result contracts exist. |

## Source Evidence

### `gapvec`

- Structure-aware splitting is implemented in
  [`chunking.rs`](https://gitlab.com/andunn/gapvec/-/blob/393872ad7b480463843e35c1ffaf23cbb7e759cb/crates/gapvec-core/src/chunking.rs):
  it locates date headers and rules, creates sections, and then groups
  paragraphs.
- Temporal extraction is implemented in
  [`dates.rs`](https://gitlab.com/andunn/gapvec/-/blob/393872ad7b480463843e35c1ffaf23cbb7e759cb/crates/gapvec-core/src/dates.rs).
- The LanceDB schema in
  [`index.rs`](https://gitlab.com/andunn/gapvec/-/blob/393872ad7b480463843e35c1ffaf23cbb7e759cb/crates/gapvec-core/src/index.rs)
  stores `text`, `vector`, `doc_name`, `chunk_index`, and `section_date`, then
  creates an FTS index on `text`.
- The query pipeline in
  [`query.rs`](https://gitlab.com/andunn/gapvec/-/blob/393872ad7b480463843e35c1ffaf23cbb7e759cb/crates/gapvec-core/src/query.rs)
  applies the date predicate before search, invokes hybrid search, retries
  without the date predicate on an empty result, and expands sibling chunks.
- The
  [`README`](https://gitlab.com/andunn/gapvec/-/blob/393872ad7b480463843e35c1ffaf23cbb7e759cb/README.md)
  says these defaults came from a 36-configuration benchmark sweep. The
  benchmark corpus and runnable benchmark are not present in the inspected
  repository, so the reported outcomes are useful evidence for `gapvec`, not
  proof that the same defaults fit `ask`.

### `ask`

- [`worker/ingest.rs`](../../crates/ask-server/src/worker/ingest.rs) plans
  filename embeddings plus fixed byte-offset content chunks.
- [`worker/embed_document.rs`](../../crates/ask-server/src/worker/embed_document.rs)
  re-reads the file and materializes those byte ranges with
  `String::from_utf8_lossy`.
- [`vector_index.rs`](../../crates/ask-server/src/vector_index.rs) creates
  `document_embedding_vec` with only `embedding float[N]`.
- [`repository.rs`](../../crates/ask-core/src/repository.rs) performs vector-only
  KNN against that table and joins hits back to `document_embeddings` and
  `documents`.
- [`http.rs`](../../crates/ask-server/src/http.rs) exposes only query text,
  result limit, and optional byte locations. It collapses vector hits to one
  result per document.
- [`0002_create_domain_tables.sql`](../../crates/ask-core/migrations/0002_create_domain_tables.sql)
  has document modification timestamps but no persisted section or chunk-text
  table.
- Existing notes already identify related work in
  [`sqlite-fuzzy-or-hybrid-search.md`](../backlog/sqlite-fuzzy-or-hybrid-search.md).

## Improvements Applicable To `ask`

### 1. Replace Byte Windows With A Shared Chunk Plan

This is the prerequisite for the other improvements.

Add a shared chunk planner that emits a typed record such as:

```text
document_id
section_id
section_date_epoch: Option<i64>
chunk_ordinal
byte_start
byte_end
text
```

The planner should:

1. Detect optional structure boundaries for supported text, starting with
   markdown headings, paragraphs, and chronological date headings.
2. Preserve date headings as metadata and preferably include useful heading
   text in the embedded input.
3. Split oversized sections with bounded paragraph, line, then character-safe
   fallback rules.
4. Emit valid UTF-8 boundaries and keep byte offsets only as source-location
   metadata.
5. Be called by both ingest planning and embed-time materialization so those
   paths cannot drift.
6. Keep filename embeddings as a separate chunk kind because they are useful
   for resource-tree search.

Do not apply only a date-header splitter. `ask` indexes code and general
resources, so chronological splitting must be one strategy inside a broader
planner.

### 2. Persist Searchable Chunks And Add SQLite FTS5

Add a normal `document_chunks` table keyed by a stable chunk ID. Persist chunk
text, offsets, section ID, optional section date, ordinal, and chunk kind.
Reference that stable row from `document_embeddings`.

Add an FTS5 index over chunk text. Use the normal prose tokenizer for content
search. Keep the trigram tokenizer proposed in
[`sqlite-fuzzy-or-hybrid-search.md`](../backlog/sqlite-fuzzy-or-hybrid-search.md) for the
separate filepath-search mode, where substring matching is the intended
behavior.

At query time:

1. Retrieve an over-fetched vector candidate list.
2. Retrieve an over-fetched FTS5 candidate list ranked by BM25.
3. Fuse the two ranked lists in Rust with RRF.
4. Collapse or expand results only after fusion.

This keeps the existing SQLite deployment model and avoids adding LanceDB.
SQLite documents FTS5 as a built-in full-text virtual table module with
relevance ranking and external-content options:
[`sqlite.org/fts5.html`](https://sqlite.org/fts5.html).

### 3. Add True Date Pre-Filtering With `sqlite-vec` Metadata

Store `section_date_epoch` as a nullable integer on `document_chunks`. Mirror a
non-null filterable representation into `document_embedding_vec`, for example
an integer date plus a boolean `has_section_date`.

The current `sqlite-vec = "0.1.9"` dependency can support this design without a
new vector database. `vec0` metadata columns can participate in KNN `WHERE`
constraints, so the date range can be applied during KNN calculation rather
than after top-k retrieval:
[`sqlite-vec vec0 metadata documentation`](https://alexgarcia.xyz/sqlite-vec/features/vec0.html#metadata-columns).

Use metadata columns first, not a per-day partition key. The upstream docs warn
that partition keys can over-shard the index and recommend hundreds of vectors
per distinct partition value. A coarser partition such as month should be
considered only after measuring a representative corpus.

Keep content dates distinct from `documents.file_modified_at`. A file timestamp
may be a useful optional search filter, but it does not answer queries such as
"what happened in March 2026" inside a long-lived notes file.

### 4. Add Deterministic Temporal Parsing

Implement query temporal parsing as a small, tested module returning a typed
date range. ISO dates, explicit month/year phrases, quarters, and explicit year
ranges are a reasonable first scope.

Do not copy `gapvec` date parsing verbatim:

- Validate dates with `chrono::NaiveDate`; do not persist impossible dates.
- Avoid assigning the current year to headings that omit a year unless the
  containing document provides an explicit year context. Re-indexing the same
  file in a different calendar year must not silently change metadata.
- Represent missing dates as `None`, not an empty string.
- Report whether an unfiltered fallback was used. Silent fallback can surprise
  clients that intended a strict time range.

### 5. Add Section Expansion Only With Stable Identity

Once chunk rows exist, add bounded sibling expansion for chunk-oriented
responses. Use a persisted `section_id`, not `(filepath, section_date)`.

This avoids merging unrelated sections that happen to share a date. It also
avoids a weakness in `gapvec`: the inspected implementation stores only a file
basename as `doc_name`, then uses `(section_date, doc_name)` for expansion.
Different directories can contain the same basename, and one document can
contain multiple sections with the same date.

Keep the current compact document-oriented response as one mode. Add a
chunk/snippet-oriented response mode for callers that need grounding context.

## gapvec Behaviors Not To Copy

The `gapvec` design is useful, but its implementation should not be imported
unchanged:

- A single paragraph larger than the target size remains oversized because
  there is no final line or character fallback.
- Optional overlap slices a Rust string at a calculated byte offset and can
  panic on non-ASCII text. Its default overlap of zero avoids the path, but the
  API still accepts unsafe values.
- Date-header parsing formats captured values without validating calendar
  dates.
- Missing years default to the current year, making indexing non-deterministic
  across years.
- Basename-only identity can collide during replacement and section expansion.
- Expansion by `(section_date, doc_name)` is weaker than an explicit section
  identifier.
- The query layer constructs filter strings. `ask` should keep user-derived
  filter values bound as SQL parameters where SQLite APIs allow it.

## Suggested Delivery Order

1. Implement UTF-8-safe shared chunk planning and persist stable chunk rows.
2. Add SQLite FTS5 chunk indexing and RRF fusion, with retrieval regression
   tests over an `ask`-specific corpus.
3. Add validated section-date extraction and `sqlite-vec` metadata
   pre-filtering.
4. Add explicit date-filter fallback semantics to the HTTP request and response.
5. Add bounded section expansion for snippet-oriented search results.
6. Benchmark defaults such as chunk size, overlap, vector/FTS candidate counts,
   RRF constant, and expansion limit before making them configuration defaults.

## Test And Evaluation Requirements

Add tests for:

- markdown date headers, ordinary headings, code files, and unstructured text
- oversized paragraphs and lines
- multibyte UTF-8 near every boundary and overlap path
- deterministic date parsing, invalid dates, leap years, quarters, and missing
  years
- repeated section dates in one document and identical basenames in different
  directories
- vector-only, FTS-only, and fused ranking
- date pre-filtering before KNN and explicit unfiltered fallback behavior
- transactional synchronization between chunk rows, FTS rows, embedding rows,
  and `vec0` rows

Create a small checked-in retrieval oracle representative of `ask`: source
paths, Rust code, markdown docs, chronological notes, exact identifiers, and
semantic paraphrases. Measure recall at k and ranking quality before adopting
`gapvec` defaults such as 750 characters, zero overlap, and top-k 10.

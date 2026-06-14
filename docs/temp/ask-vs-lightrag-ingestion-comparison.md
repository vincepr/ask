# ask vs HKUDS/LightRAG: Ingestion and Pre-processing Comparison

Scope: compare this repo's data-ingestion and embedding-adjacent preprocessing flow against `HKUDS/LightRAG`, focusing on the stage where document chunks are prepared and embedded. I am intentionally not comparing LLM choice/prompting except where it directly shapes preprocessing behavior.

Compared against:
- `ask` local repo in `D:\coding\ask`
- `LightRAG` cloned from `https://github.com/HKUDS/LightRAG` at `1da5814fecdadb823ddfda6bd0470e9512d4086a`

## Executive summary

`ask` is a much narrower and simpler ingestion system:
- it walks files from disk
- stores only document metadata plus pending chunk offsets
- rereads the file later to compute embeddings
- uses one byte-window chunking scheme tied to the embedding model

`LightRAG` is a broader document-processing pipeline:
- it persists full document content and per-document chunking options up front
- supports multiple chunking strategies per document
- performs chunk embedding through the vector-storage layer at commit time
- can do embedding-driven chunk boundary detection before the final chunk embeddings are written

If the question is specifically "where does embedding happen and what preprocessing leads into it?", the main contrast is:

- `ask`: preprocessing produces `(chunk_type, start, end)` records first, embedding happens later in a separate worker job.
- `LightRAG`: preprocessing produces full chunk payloads first, then vector storage batches and embeds those payloads during its flush/commit step.

## 1. Ingestion entry point and source of truth

### `ask`

- Ingestion starts from filesystem roots submitted to `/ingest` or `/ingest/git`, then the worker either walks directories or enumerates git-tracked files: `crates/ask-server/src/worker/ingest.rs:31-116`, `135-157`, `160-218`.
- It normalizes candidate paths relative to the requested root and optionally filters them by a regex: `crates/ask-server/src/ingest.rs:7-39`, `crates/ask-server/src/worker/ingest.rs:228-239`.
- It stores document metadata in SQLite (`filepath`, `file_type`, `file_modified_at`, `file_size`) but not the full text itself: `crates/ask-server/src/worker/ingest.rs:271-311`, `crates/ask-core/src/repository.rs:43-123`.
- The file on disk remains the source of truth for later embedding. The embed worker rereads that file by path: `crates/ask-server/src/worker/embed_document.rs:55-75`, `126-175`.

Implication:
- `ask` keeps storage small and simple, but embedding correctness depends on the file still existing and still matching the metadata snapshot when the later embed job runs.

### `LightRAG`

- Enqueue persists document content and document status up front into `full_docs` and `doc_status`: `temp/_compare_lightrag/lightrag/pipeline.py:222-293`, `875-915`.
- The processing stage explicitly rereads content from `full_docs`, not from the original file path or queue payload: `temp/_compare_lightrag/lightrag/pipeline.py:1884-1892`.
- `LightRAG` supports multiple input formats:
  - raw text
  - "lightrag" structured documents
  - pending-parse documents for later parser execution
  See `temp/_compare_lightrag/lightrag/pipeline.py:244-270`, `365-427`.

Implication:
- `LightRAG` is heavier, but the ingest pipeline is more restartable and less dependent on the original file still being present after enqueue.

## 2. File selection and upfront filtering

### `ask`

- Normal ingestion walks all regular files under the root. Git-mode avoids some obvious binary/media/archive extensions through a small denylist, but plain walk mode does not: `crates/ask-server/src/worker/ingest.rs:18-21`, `524-533`.
- The repo already documents that there are no real size guards or binary sniffing yet: `docs/issues/004-file-filtering-and-size-guards.md`.

Implication:
- `ask` is currently optimistic: it tries to read almost everything as UTF-8 text and only falls back after the read fails.

### `LightRAG`

- The upstream system is not built around raw directory walking alone. Its pipeline expects already supplied raw text or parser-managed documents, and persists per-document processing metadata at enqueue time: `temp/_compare_lightrag/lightrag/pipeline.py:244-270`, `397-427`, `875-909`.
- In practice, this means preprocessing policy is more document-centric than filesystem-centric.

Implication:
- `LightRAG` does more work before chunk embedding, but it also has a richer contract for document state, parser choice, and chunking strategy.

## 3. Chunk planning / preprocessing before embeddings

### `ask`

- During ingest, `ask` immediately plans pending embedding rows for each file by reading it as UTF-8 text and computing chunk offsets: `crates/ask-server/src/worker/ingest.rs:301-311`, `553-617`.
- The chunk plan always includes a filename embedding row plus zero or more content rows: `crates/ask-server/src/worker/ingest.rs:557-583`.
- Chunking is a simple overlapping byte window:
  - max bytes = `model.chunk_size`
  - overlap bytes = `model.chunk_overlap`
  - output = `(start, end)` byte offsets
  See `crates/ask-server/src/worker/ingest.rs:586-617`.
- The chunking parameters are part of embedding model identity, so changing chunk size/overlap creates a logically new embedding model lineage: `crates/ask-core/src/models/embedding_model.rs:1-19`, `crates/ask-core/src/repository.rs:394-426`.

Implication:
- Preprocessing is deterministic and cheap, but simplistic. The chunker is tied directly to model metadata and does not understand tokens, syntax, headings, or semantic boundaries.

### `LightRAG`

- `LightRAG` persists per-document `chunk_options` at enqueue time and dispatches chunking later from `process_single_document`: `temp/_compare_lightrag/lightrag/pipeline.py:255-270`, `1930-2050`.
- It supports four chunking strategies selected via `process_options`:
  - `F`: fixed-token
  - `R`: recursive-character
  - `V`: semantic-vector
  - `P`: paragraph-semantic
  See `temp/_compare_lightrag/lightrag/pipeline.py:1913-2029`.
- The default fixed-token chunker is token-aware, not byte-aware, and can preserve exact source spans: `temp/_compare_lightrag/lightrag/chunker/token_size.py:1-22`, `50-93`, `114-234`.
- After chunking, it applies a final hard split to ensure no chunk exceeds the embedding model's max token limit: `temp/_compare_lightrag/lightrag/pipeline.py:2183-2207`, `temp/_compare_lightrag/lightrag/utils.py:1943-2016`.
- It then builds chunk payload objects containing actual content, token counts, order, doc id, file path, and cache metadata before persistence: `temp/_compare_lightrag/lightrag/pipeline.py:2238-2277`, `temp/_compare_lightrag/lightrag/utils_pipeline.py:45-96`.

Implication:
- `LightRAG` has a much richer preprocessing contract. It carries full chunk content and metadata forward, not just offsets.

## 4. Where embeddings actually happen

### `ask`

- Ingestion does not compute vectors. It only inserts pending rows and seeds `embed_document` jobs: `crates/ask-core/src/repository.rs:145-192`, `460-499`, `699-749`.
- The later `EmbedDocumentHandler` rereads the file, reconstructs chunk text, calls the embedding provider, validates vector count, and atomically replaces rows in SQLite: `crates/ask-server/src/worker/embed_document.rs:18-115`.
- Provider calls are HTTP batched by `max_batch_size` inside `HttpEmbeddingClient::embed`: `crates/ask-server/src/embeddings.rs:66-86`, `166-205`.

Implication:
- `ask` splits ingestion into two phases:
  1. metadata + chunk plan
  2. materialized embeddings

That is operationally simple and gives a clean retry boundary, but it also means double file reads and duplicate chunk-preparation logic across ingest and embed phases.

### `LightRAG`

- After chunking, `process_single_document` writes chunk payloads to both `chunks_vdb` and `text_chunks` before KG extraction: `temp/_compare_lightrag/lightrag/pipeline.py:2251-2279`.
- The default Nano vector storage does not embed in `upsert`. It buffers records and defers embedding to `index_done_callback`: `temp/_compare_lightrag/lightrag/kg/nano_vector_db_impl.py:269-318`, `633-670`.
- During that flush, Nano batches pending chunk contents and calls `embedding_func(batch, context="document")`, then materializes vectors into storage: `temp/_compare_lightrag/lightrag/kg/nano_vector_db_impl.py:334-389`.

Implication:
- `LightRAG` pushes the actual document-embedding step down into the vector storage layer, not a separate document-embedding worker.
- This is closer to "chunk persistence and vector materialization happen together at commit time" than to `ask`'s queued document-embedding job model.

## 5. Embedding-aware preprocessing differences

This is the biggest conceptual gap.

### `ask`

- Chunk boundaries are computed without any tokenization or embedding awareness.
- The embedding model only sees whatever byte-window slices were planned earlier.

### `LightRAG`

- The `V` semantic-vector chunker uses embeddings during chunk formation itself. It embeds sentence windows to choose semantic breakpoints before the final chunk vectors are even written: `temp/_compare_lightrag/lightrag/chunker/semantic_vector.py:1-25`, `55-87`, `192-260`.
- Then the final chunks still get embedded again for vector retrieval storage through `chunks_vdb`.

Implication:
- `LightRAG` can use embeddings twice in one ingestion flow:
  1. to decide chunk boundaries (`V`)
  2. to embed the final chunks for retrieval

`ask` never uses embeddings to influence chunk boundaries.

## 6. Text decoding and UTF-8 behavior

### `ask`

- Both ingest chunk planning and embed-time chunk preparation read full raw file bytes before decoding or chunk preparation.
- Non-UTF-8 or unreadable files degrade to filename-only embeddings.
- The repo already tracks non-UTF-8 decoding as a known limitation and explicitly wants a shared decoding helper later: `docs/issues/003-text-decoding-beyond-utf8.md`.

Implication:
- `ask` currently has a correctness risk around multibyte boundaries and a recall loss for decodable non-UTF-8 text files.

### `LightRAG`

- Its default F-path is token-based rather than byte-based, and its chunk payloads are actual strings rather than deferred byte slices: `temp/_compare_lightrag/lightrag/chunker/token_size.py:114-234`.
- It also enforces a token-limit fallback split before final embedding: `temp/_compare_lightrag/lightrag/utils.py:1943-2016`.

Implication:
- `LightRAG`'s preprocessing is materially safer for natural-language text and better aligned with embedding model constraints.

## 7. Storage model for chunk data

### `ask`

- Stores:
  - document metadata in `documents`
  - chunk identity + offsets + state in `document_embeddings`
  - vector bytes in the same table once embedded
- It does not store original document content in the database.

Net effect:
- compact
- easy to reason about
- weak introspection for preprocessing artifacts

### `LightRAG`

- Stores:
  - full document content in `full_docs`
  - chunk payloads in `text_chunks`
  - chunk vectors in `chunks_vdb`
  - later KG outputs separately

Net effect:
- more storage overhead
- much stronger observability and resumability
- preprocessing artifacts are first-class persisted data

## 8. Bottom line

For the narrow scope of "data ingestion and preprocessing where embeddings happen":

- `ask` is a minimal two-stage pipeline:
  - discover file
  - plan byte-offset chunks
  - queue pending rows
  - reread file later
  - embed batches
  - replace rows atomically

- `LightRAG` is a richer document pipeline:
  - persist full document and chunking config
  - choose per-document chunking strategy
  - produce chunk payload objects with metadata
  - optionally use embeddings to decide chunk boundaries
  - flush chunk embeddings through vector storage commit
  - then continue into KG extraction

If you want the shortest practical characterization:

- `ask` is "filesystem-first, metadata-first, async embed-later".
- `LightRAG` is "document-state-first, chunk-object-first, vector-store-committed embedding".

## 9. Most relevant improvement ideas if `ask` wants to move toward LightRAG's strengths

The highest-signal gaps are:

1. Replace byte-based chunking with token-aware chunking.
2. Introduce one shared text-decoding helper used by both ingest planning and embed execution.
3. Add file-size / binary guards before full reads.
4. Consider persisting chunk text or a normalized preprocessing artifact if resumability and post-hoc inspection matter.
5. If chunk quality matters more than simplicity, add at least a structural chunker before considering LightRAG-style semantic chunking.

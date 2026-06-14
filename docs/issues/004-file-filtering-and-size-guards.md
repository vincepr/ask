# File Filtering, Size Guards, and Streaming Reads

## Problem

The worker attempts to read every matched regular file into memory before it can
decide how much content should become embeddings.

## Evidence

- `crates/ask-server/src/worker/ingest.rs` reads full raw bytes with
  `std::fs::read`.
- `plan_pending_embeddings_for_bytes` only chunks content after the full file is
  already resident in memory.
- The current fallback for non-UTF-8 content is deliberate filename-only
  indexing, but the read path still pays the full memory cost first.

## Why This Is Risky

- Large files can create avoidable memory spikes.
- Binary files are still fully loaded before being rejected for content chunking.
- The service has no explicit supported-file contract, which makes operations less predictable.

## Investigation

Before implementing size limits, deeply check whether the ingestion path should
move from full-file reads to a parser/reader that consumes files in chunks.

Questions to answer:

- Can a streaming reader cleanly feed the existing chunking strategy, which
  currently expects a complete UTF-8 `str` and returns byte spans?
- If streaming is used, should the chunk planner become incremental, or should a
  bounded prefix be collected and then passed to the current planner?
- How should decoding handle buffer boundaries for UTF-8 and future fallback
  encodings without splitting a multi-byte sequence incorrectly?
- Is a maximum embedded byte budget enough, where large files are represented by
  filename plus the first bounded amount of decoded text?
- Should the limit be derived from the active model chunk size and overlap, with
  an absolute upper cap?

## Likely Minimal Fix

- Define a small supported-text-file policy and enforce it before content
  chunking.
- Add a maximum content-ingestion byte budget.
- Read a small prefix first to reject obvious binary content.
- For files above the content budget, embed only a bounded prefix plus the
  filename, rather than loading the whole file.
- Keep filename-only indexing as a deliberate fallback.

## Acceptance Criteria

- Large files cannot force unbounded memory use during ingest.
- Binary files are rejected or downgraded before full-file loading.
- The chosen design explicitly explains why it uses either full-file bounded
  reads or an incremental parser.
- Tests cover binary files, files above the size limit, and multibyte text near
  read or chunk boundaries.

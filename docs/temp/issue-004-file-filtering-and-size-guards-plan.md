# File Filtering And Size Guards Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent folder ingestion and embedding jobs from loading arbitrary file contents into
memory while preserving document identity, filename indexing, and current UTF-8 chunk semantics.

**Architecture:** Keep the existing chunk planner as the only content chunking implementation, but
feed it only a bounded UTF-8 prefix. Hash full files with streaming I/O so document change
detection still covers the entire file without materializing it. Treat binary or unsupported text
content as filename-only indexing before full content reads.

**Tech Stack:** Rust 2024, `std::fs::File`, `std::io::{BufReader, Read}`, existing `sha2`,
`ignore`, `regex`, `rusqlite`, and current worker integration tests.

---

## Findings

- Issue file: `docs/issues/004-file-filtering-and-size-guards.md`.
- Primary ingest path: `crates/ask-server/src/worker/ingest.rs`.
- Primary embedding path: `crates/ask-server/src/worker/embed_document.rs`.
- Chunking contract: `crates/ask-server/src/worker/chunking.rs` returns byte spans into a UTF-8
  `str`. Later embedding slices the original file content with those stored byte spans.
- Current memory risk is larger than the issue evidence says:
  - `ingest_candidate_file` uses `std::fs::read` for full-file hash and content planning.
  - `queue_pending_embeddings_for_document` uses `std::fs::read` when backfilling a new model.
  - `EmbedDocumentHandler::process` uses `std::fs::read` to verify the hash and prepare chunks.
  - `replan_document_from_bytes` assumes the full changed file is already in memory.
- Existing tests already cover normal ingest, non-UTF-8 filename-only behavior, large text chunking,
  backfill, hash-based replan, and chunk UTF-8 boundary safety.
- `GIT_IGNORED_FILE_EXTENSIONS` applies only to git-filtered candidates. Plain folder ingest still
  records binary files as documents and currently downgrades them only after full-file loading.
- There is no config field for content-ingestion byte limits today. Adding an environment setting
  would be more surface area than the issue requires unless operators need runtime tuning.

## Design Decision

Use bounded full-prefix planning, not a streaming chunk planner.

Reasons:

- The existing planner deliberately returns byte spans into a complete UTF-8 string.
- Embedding rows persist only byte ranges, not copied content.
- A truly incremental planner would need a new cross-buffer boundary model, overlap handling, and
  possibly persisted normalization state for future encodings.
- The issue can be solved with simpler bounded reads: full-file hash streams over the file, content
  planning receives only the first supported text bytes, and all stored content spans remain valid
  offsets into the original file because the prefix starts at byte 0.

## Proposed Policy

- Always record matched regular files as documents unless the current traversal/filter logic skips
  them.
- Always compute `file_size`, `file_modified_at`, and `file_hash` against the full file.
- Attempt content chunking only when the file passes a cheap text-content probe.
- Read at most a fixed content byte budget for chunk planning.
- If the file is larger than the content budget, index filename plus bounded prefix content.
- If the content probe or prefix decoding finds binary/non-UTF-8 data, index filename only.
- Store metadata that makes this behavior explicit:
  - `strategy`
  - `planned_chunk_count`
  - `content_utf8`
  - `content_truncated`
  - `content_bytes_indexed`
  - `content_byte_budget`

Initial constants should live in `ingest.rs`:

```rust
const CONTENT_PROBE_BYTES: usize = 8 * 1024;
const CONTENT_BYTE_BUDGET: usize = 1024 * 1024;
const UTF8_MAX_SCALAR_BYTES: usize = 4;
```

The 1 MiB budget is intentionally conservative: it is much larger than normal source/document
chunks, small enough to bound per-file memory, and avoids configuration churn. It can become config
later if operational evidence justifies it.

## High-Level Implementation Plan

- [ ] Add a small internal `ContentReadPlan` or equivalent helper in
  `crates/ask-server/src/worker/ingest.rs`.
  - It should hold the bounded bytes, `content_utf8`, `content_truncated`,
    `content_bytes_indexed`, and `content_byte_budget`.
  - It should not own full-file bytes.

- [ ] Replace full-file hashing in ingest paths with streaming hashing.
  - Add `hash_file(path: &Path) -> Result<String>` using `File`, `BufReader`, and a reusable buffer.
  - Keep `hash_bytes` for tests and small in-memory paths that still need it.
  - Use `hash_file` in `ingest_candidate_file`.

- [ ] Add bounded content prefix reading.
  - Open the file once for content planning.
  - Read up to `CONTENT_BYTE_BUDGET + UTF8_MAX_SCALAR_BYTES - 1` bytes.
  - Treat embedded NUL bytes in the probe/prefix as binary and return filename-only content policy.
  - Use `std::str::from_utf8`.
  - If UTF-8 fails with an incomplete scalar at the end, truncate to `valid_up_to`.
  - If UTF-8 fails because invalid bytes appear inside the prefix, return filename-only policy.
  - Floor the retained prefix to at most `CONTENT_BYTE_BUDGET` at a UTF-8 character boundary.

- [ ] Change `plan_pending_embeddings_for_bytes` into a bounded-content planner.
  - Prefer a signature like:

```rust
fn plan_pending_embeddings_for_content(
    path: &Path,
    content: Option<&str>,
    content_truncated: bool,
    content_byte_budget: usize,
    model: &EmbeddingModel,
) -> PlannedEmbeddings
```

  - Keep filename rows unconditional.
  - For `None` or empty content, return filename-only metadata.
  - For `Some(content)`, run existing `chunking::plan_chunks`.
  - Ensure all returned content spans are within the retained prefix.

- [ ] Update backfill to use the same bounded planner.
  - `queue_pending_embeddings_for_document` should not call `std::fs::read`.
  - It should read only bounded content bytes and then insert filename/content rows using the same
    planner as fresh ingest.

- [ ] Update changed-document replanning.
  - Replace `replan_document_from_bytes` with a path-based function that streams hash, reads bounded
    content, and upserts the document.
  - Update `EmbedDocumentHandler::process` so a hash mismatch does not require a full-file byte
    vector before replanning.

- [ ] Update embed preparation to avoid full-file reads.
  - Stream the current full-file hash first.
  - If the hash matches and only filename rows are pending, do not read content bytes.
  - If content rows are pending, read only through the maximum pending `chunk_end` plus the small
    UTF-8 boundary allowance.
  - Decode with the same strict UTF-8 boundary rules used during planning.
  - Keep stored range validation exactly as strict as it is today.

- [ ] Add regression tests.
  - Ingesting a large text file above the budget stores full `file_size` and full `file_hash`, but
    content rows never end past the budget.
  - A binary file with an early NUL byte gets filename-only indexing and metadata states
    `content_utf8: false`.
  - A valid UTF-8 file whose budget lands inside a multibyte scalar keeps valid content chunk spans
    and never marks the file as non-UTF-8 just because the read stopped mid-scalar.
  - New-model backfill uses the same bounded content behavior as fresh ingest.
  - Hash-mismatch replanning in `embed_document` keeps using full-file hash identity without
    loading the full file into memory.

- [ ] Update existing tests whose expectations assume unbounded chunking.
  - `ingest_large_file_produces_many_chunks` should either use content smaller than the budget or be
    replaced with a budget-specific regression test.
  - Existing non-UTF-8 tests should continue to pass, but metadata assertions should be extended.

- [ ] Run verification.
  - `cargo fmt --check`
  - `cargo clippy --workspace --all-targets -- -D warnings`
  - `cargo test`

## Risks And Notes

- Memory cannot be proved by ordinary integration tests alone. The strongest practical guarantee is
  code structure: no `std::fs::read` remains in worker ingest/embed paths for arbitrary documents.
- Keeping full-file hashing means very large files still cost I/O time, but not unbounded memory.
- Filename-only indexing for binary content preserves current behavior for plain folder ingest while
  making the downgrade happen before full content loading.
- The fixed content budget avoids adding config now. If operators need tuning, add an environment
  variable in a later issue with config tests.

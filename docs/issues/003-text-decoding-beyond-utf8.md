# Text Decoding Beyond UTF-8

## Goal

Support common text files that are not valid UTF-8.

The system should keep treating binary files as non-text, but it should stop
 degrading common human-readable encodings into filename-only embeddings just
 because they are not UTF-8.

## Problem

Today the ingest and embed paths use `std::fs::read_to_string()`.

That gives three behaviors:

- UTF-8 text files get content chunks and content embeddings.
- Binary or non-text files fall back to filename-only handling.
- Text files encoded as Latin-1, Windows-1252, UTF-16, or similar are treated
  the same as binary files because decoding fails early.

The repo now has explicit filepath invariants, so the next missing capability is
 text decoding, not path resolution. Human-readable content should not be lost
 just because its bytes are not UTF-8.

## Decision

- Introduce a small shared text-decoding step for document reads.
- Treat UTF-8 as the fast path.
- Detect and decode a small set of common text encodings for local files.
- Keep filename-only behavior for content that is still not decodable as text.
- Use the same decoding behavior in both ingest chunk planning and embed-time
  chunk preparation.
- Do not persist original text content in SQLite for this feature.

## Why This Design

This is the smallest change that materially improves ingestion quality.

- It preserves the current architecture: files remain the source of truth.
- It keeps text handling consistent between ingest and embed execution.
- It avoids turning the worker into a binary-format parser.
- It improves recall for real-world notes, exports, and legacy documents without
  widening scope into document conversion.

## Non-Goals

- No OCR, PDF extraction, or rich-document parsing.
- No schema change to store decoded content.
- No attempt to decode arbitrary binary payloads as text.
- No user-configurable encoding override in this feature.
- No full charset-detection framework for every legacy encoding in existence.

## Implementation Plan

1. Add one shared helper that reads raw file bytes and returns:
   - decoded text when the file looks like supported text
   - `None` when the file is not decodable as supported text
2. Use UTF-8 as the first decode path.
3. Add support for a constrained set of common fallback encodings:
   - UTF-16 with BOM
   - Windows-1252
   - ISO-8859-1 only if needed after Windows-1252 choice is evaluated
4. Replace direct `read_to_string()` usage in both:
   - pending chunk planning during ingest
   - embed-time chunk preparation
5. Preserve filename-only behavior when the helper returns `None`.
6. Keep missing/inaccessible path failures unchanged: those are still normal I/O
   failures, not decoding fallbacks.

## Implementation Notes

- Prefer a small maintained crate such as `encoding_rs` rather than handwritten
  decoding tables.
- Keep the helper focused on one responsibility: decode supported text bytes.
- If heuristic detection is needed beyond BOM handling, keep it conservative.
  False positives that turn binary blobs into junk text are worse than
  filename-only fallback.
- Normalize decoded output to Rust `String` before chunking so the existing
  chunk pipeline stays unchanged.
- The ingest path and embed path must not drift; they should call the same
  decoding helper.

## Test Plan

- Unit test UTF-8 input stays unchanged.
- Unit test UTF-16 text with BOM is decoded and chunked as text.
- Unit test Windows-1252 text is decoded and chunked as text.
- Regression test that clearly binary content still produces filename-only
  embeddings.
- Integration test that ingest and embed produce matching chunk boundaries for
  the same non-UTF-8 text file.
- Regression test that missing files still fail clearly with filepath context.

## Acceptance Criteria

- Supported non-UTF-8 text files produce content chunks and content embeddings.
- Binary files still fall back to filename-only handling.
- Ingest and embed use the same decoding behavior.
- No filepath fallback logic or persisted-content workaround is introduced.

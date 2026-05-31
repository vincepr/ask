# 001: Document Identity and Upserts

## Problem

The current ingest path treats every changed file as a brand-new document row instead of updating the
existing row.

## Evidence

- `crates/ask-server/migrations/0002_create_domain_tables.sql:1-9` creates `documents` without a
  unique constraint on `filepath`. There is currently no `UNIQUE(filepath)` and no equivalent unique
  index.
- `crates/ask-core/src/repository.rs:15-30` only exposes `insert_document`, not an update or upsert.
- `crates/ask-server/src/worker.rs:230-248` skips a file only when `file_modified_at` and `file_size`
  match; otherwise it inserts a fresh document row for the same path.
- `crates/ask-core/src/repository.rs:33-58` then reads by `filepath` without `ORDER BY`, so once
  duplicates exist the returned row is not deterministic.
- `crates/ask-server/tests/integration.rs:452-512` only proves unchanged files are skipped. There is
  not yet a regression test proving that a changed file keeps a single `documents` row.

## Why This Is Risky

- Re-ingesting a modified file creates duplicate `documents` rows for one real file.
- Old embedding rows remain attached to stale document ids instead of being replaced or marked stale in
  a controlled way.
- Any future logic that expects one row per file path will eventually behave inconsistently.
- `find_document_by_path` can return an older row and make later maintenance code operate on the wrong
  document.

## Simplest Stable Fix

- Make the persisted file identity unique. The simplest version is a canonical absolute path stored in a
  unique column.
- Replace `insert_document` plus `find_document_by_path` with one `upsert_document(...) -> DocumentId`
  repository function.
- When file metadata changes, update the existing row and mark that document's embeddings stale in the
  same transaction.
- Add regression tests for:
  - ingesting the same unchanged file twice
  - ingesting the same file after content changes
  - resolving the same directory through syntactic path variants

## Human review:
- Q: Re-ingesting a modified file creates duplicate `documents` rows for one real file. - At the
  moment this is possible because `documents.filepath` is not unique in the schema. If we want one row
  per canonical file identity, that uniqueness constraint still needs to be added.
- But we should still have sound upsert logic. So ensure that through and through.
- File changes will come later. And a a big todo later with filewachers/tracking modified (if mac is problematic as i think it will be)/ or worst case doing it via hashes

# 002: Recursive Folder Ingestion

## Context

When ingesting a folder, only top-level files are currently added. Files in subdirectories are skipped entirely, which means users must point at every subfolder individually — or miss documents entirely.

## Problem

1. Only files directly inside `root_path` are indexed; nested files are ignored.
2. Users cannot ingest a project tree in one call — they must know its structure and issue multiple requests.
3. No clear design for whether recursion should spawn sub-jobs or happen in a single worker.

## Decision

Recursion happens in a **single worker** — no sub-queuing. This avoids concurrency issues (two workers accidentally processing overlapping roots) and is simpler to implement: one `walkdir` traversal, one transaction.

## Required Feature

1. `IngestFolderHandler` walks the entire directory tree recursively, not just the top level.
2. Hidden directories (names starting with `.`) are skipped (`.git`, `.hidden`, etc.).
3. Symlinks are canonicalized and deduplicated by real path.
4. The entire traversal runs in one worker without enqueuing additional jobs.

## Required Sub-tasks

- [ ] Update the file-walking logic in `IngestFolderHandler` to recurse into subdirectories
- [ ] Add skip logic for dot-prefixed directories
- [ ] Handle symlinks: canonicalize, deduplicate by real path
- [ ] Add/update tests verifying nested files are ingested
- [ ] Verify no performance regression on shallow directories

## Why Now

Without recursion the ingest feature is incomplete — users must script their own recursive calls or miss content. This is a small change with high impact.

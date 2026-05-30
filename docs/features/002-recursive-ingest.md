# 002: Recursive Folder Ingestion

## Context

When ingesting a folder, only top-level files are currently added. Files in subdirectories are skipped entirely, which means users must point at every subfolder individually — or miss documents entirely.

## Problem

1. Only files directly inside `root_path` are indexed; nested files are ignored.
2. Users cannot ingest a project tree in one call — they must know its structure and issue multiple requests.
3. No clear design for whether recursion should spawn sub-jobs or happen in a single worker.

## Decision

Recursion happens in a **single worker** — no sub-queuing. This avoids concurrency
issues (two workers accidentally processing overlapping roots) and is simpler to
implement.

Use the `ignore` crate's walker instead of hand-rolled recursion or `walkdir` plus
separate git-ignore checks. It already handles recursive traversal efficiently and
can honor `.gitignore`, `.ignore`, and parent ignore files when present.

## Required Feature

1. `IngestFolderHandler` walks the entire directory tree recursively, not just the top level.
2. Traversal stays inside a single worker without enqueuing additional jobs.
3. Ignore handling comes from the walker configuration rather than ad hoc
   filename checks.
4. Symlinks are **not** followed by default. That avoids cycles, duplicate content,
   and escaping the requested root unexpectedly.

Do not require one giant database transaction for the whole tree. A long-lived write
transaction would increase lock contention and make recovery worse on very large
directories. One traversal is required; one monolithic transaction is not.

## Required Sub-tasks

- [ ] Replace top-level `read_dir` logic with recursive traversal
- [ ] Use `ignore::WalkBuilder` (or equivalent) as the traversal primitive
- [ ] Configure traversal to avoid following symlinks
- [ ] Add/update tests verifying nested files are ingested
- [ ] Verify no performance regression on shallow directories
- [ ] Verify ignored directories are skipped by traversal configuration, not by
  blanket dot-prefix filtering

## Why Now

Without recursion the ingest feature is incomplete — users must script their own recursive calls or miss content. This is a small change with high impact.

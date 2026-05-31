# 002: Recursive Folder Ingestion

## Context

`IngestFolderHandler` currently reads only the top level of `root_path`.
Nested files are skipped entirely.

## Problem

1. Only files directly inside `root_path` are indexed.
2. Users must know the directory structure in advance and enqueue multiple
   roots to cover one tree.
3. The current traversal logic does not compose cleanly with ignore files.

## Decision

Traversal stays inside one claimed `IngestFolder` job. Do not create sub-jobs
for child directories.

Use the `ignore` crate as the traversal primitive. It already handles recursive
walking efficiently and can honor `.gitignore`, `.ignore`, and parent ignore
files when configured.

Do not follow symlinks by default.

## Required Feature

1. Replace the top-level `read_dir` loop with recursive traversal.
2. Keep the current "best effort" behavior: unreadable entries should be logged
   and skipped rather than aborting the whole job.
3. Preserve per-file canonicalization before database writes so deduplication
   still works on real paths.
4. Keep traversal in one worker; no nested queueing and no tree-wide write
   transaction.

## Required Sub-tasks

- [ ] Replace `std::fs::read_dir` iteration with `ignore::WalkBuilder`
- [ ] Configure recursive traversal without following symlinks
- [ ] Keep only regular files as ingest candidates
- [ ] Preserve current per-file warning-and-continue behavior on IO failures
- [ ] Add tests proving nested files are ingested
- [ ] Add tests proving nested ignored directories are skipped once feature 003
      enables path filters

## Acceptance Criteria

1. One `IngestFolder` job can ingest an entire directory tree.
2. Nested files are discovered without additional API calls.
3. Symlink traversal is disabled by default.
4. The worker still behaves predictably on partial filesystem failure.

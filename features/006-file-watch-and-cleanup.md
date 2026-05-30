# 006: File System Watching and Orphan Cleanup

## Context

The server stores `documents` rows that correspond to files on disk. Currently,
documents are only added or updated when the user explicitly calls
`POST /ingest`. If a file is modified externally after ingestion, the
document's embeddings remain `'embedded'` and return stale search results until
the user re-ingests manually or hits `POST /documents/stale`. If a file is
deleted externally, the corresponding document row (and its embeddings) stay in
the database forever, creating orphans.

Neither of these states is automatically detected or handled.

## Dependencies

**Blocked by [001: Re-embedding of Stale Documents](001re-embedding-feature.md).**
Feature A (stale-on-modify) is only useful once the re-embed worker exists to
consume stale embeddings; without it, the watcher would pile up stale rows with
no consumer. Implementation of this feature SHOULD NOT begin until 001 is
complete.

## Problem

1. **Stale-on-modify**: When a file changes on disk after ingestion, its
   embeddings immediately become stale, but nothing marks them as such. Users
   must remember to re-ingest the changed file.
2. **Orphan-on-delete**: When a file is removed from disk after ingestion, the
   corresponding `documents` row and its `document_embeddings` rows remain in
   the database. These orphans bloat the search index and may cause confusing
   search results (pointing to paths that no longer exist).
3. **No periodic reconciliation**: There is no mechanism to periodically audit
   the database against the real file system and clean up inconsistencies.

## Required Features

### Feature A: Real-time File Modification Watching

Use `inotify` (Linux) or a general-purpose file watcher crate (e.g.
`notify`) to observe the resource directory for file change events.

- On a `Modify` or `Create` event for a known file path, call
  `repository::mark_documents_stale` for the affected document(s).
- The watcher should be configurable (watch directory, debounce interval).
- Start the watcher as a background task on server boot (spawned via
  `tokio::spawn`).
- Add a CLI flag or config setting to enable/disable the watcher.

### Feature B: Periodic Orphan Cleanup

Run a background sweep on a configurable interval (default: e.g. 5 minutes)
that:

1. Queries all `documents` rows whose `filepath` starts with the configured
   resource directory prefix.
2. For each batch, checks if the file still exists on disk (`std::fs::metadata`).
3. For documents whose file no longer exists:
   a. Delete the `document_embeddings` rows (CASCADE will handle this if FK
      is `ON DELETE CASCADE`, otherwise delete explicitly).
   b. Delete the `documents` row.
4. Log a summary of how many orphans were removed each cycle.
5. Respect a configurable batch size to avoid long transactions.

### Feature C (optional): On-Boot Scan

As a one-shot complement to the periodic sweep, scan all documents on server
startup and remove orphans immediately. This prevents a window where the server
restarts and stale orphans persist until the first sweep interval elapses.

## Architecture

Both features should live in a new module, e.g.
`crates/ask-server/src/fs_watch.rs`, rather than being stuffed into
`worker.rs` or `http.rs`.

```
crates/ask-server/src/
├── fs_watch.rs          # <-- new: watcher + sweeper
├── http.rs
├── worker.rs
└── lib.rs
```

The module exports:

```rust
pub(crate) fn start_background_tasks(
    pool: SqlitePool,
    resource_dir: PathBuf,
    watch_enabled: bool,
    sweep_interval: Duration,
) -> Vec<JoinHandle<()>>;
```

The HTTP server (`lib.rs`) calls `start_background_tasks` during `serve` and
stores the join handles so they are cancelled on shutdown.

## Interaction with Re-Embedding (001)

Once Feature A marks a document's embeddings as stale, the re-embed worker
(from 001) will pick them up and recompute them. The two features form a
pipeline:

```
File modified on disk
  → FS watcher detects event
    → mark_documents_stale(doc_id)
      → re-embed worker polls stale embeddings
        → recomputes and sets state = 'embedded'
```

Feature B (orphan cleanup) is independent — it removes documents entirely
rather than marking them stale.

## Required Sub-tasks

- [ ] Add `notify` crate to `ask-server`'s `Cargo.toml`
- [ ] Create `crates/ask-server/src/fs_watch.rs` with:
  - `watch_resource_dir(pool, resource_dir)` — sets up the `notify` watcher,
    debounces events, calls `mark_documents_stale`
  - `orphan_sweeper(pool, resource_dir, interval)` — periodic sweep loop
  - `remove_orphans(pool, resource_dir, batch_size)` — batch delete logic
- [ ] Wire `start_background_tasks` into `lib.rs::serve`
- [ ] Add config fields: `watch.enabled`, `watch.sweep_interval_secs`,
      `watch.debounce_ms`
- [ ] Write integration tests:
  - Modify a watched file and verify embeddings become stale
  - Delete a file from disk and verify the orphan sweep removes it
  - Verify on-boot scan removes orphans
- [ ] Update `config.rs` tests for new config fields

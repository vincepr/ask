# 003: Watch `resource_dir` and Clean Up Orphans

## Context

The server already has a configured `resource_dir`, but documents are only
added or updated when a user explicitly calls `POST /ingest`.

Two gaps remain:

- indexed files can change on disk without their embeddings becoming stale
- deleted files can leave orphaned rows behind

## Scope Correction

The earlier draft implicitly assumed there is one watched root for every
ingested path. That is not true in the current codebase.

Today the server has one configured `resource_dir`, while `/ingest` can still
accept arbitrary directories. So this feature should only watch and reconcile
documents that live under `config.resource_dir`.

Files indexed outside that tree remain manually managed until the product grows
a real root-registry concept.

## Dependencies

- Stale-on-modify depends on features 006 and 007 so that "mark stale" leads to
  actual embed work.
- Orphan cleanup is independently useful and can ship even before re-embedding.

## Decision

Use `notify` for cross-platform watching of `config.resource_dir`.

Behavior:

1. On `Modify` for an already indexed file under `resource_dir`, mark the
   document stale and seed an `EmbedDocument` job.
2. On `Create`, do nothing in the first version. New files still require an
   explicit ingest request.
3. On `Remove`, rely on the orphan sweeper instead of trying to fully reconcile
   deletes from watcher events alone.

Add a periodic sweep for documents whose `filepath` is inside `resource_dir` but
no longer exists on disk.

## Architecture

Put the watcher and sweeper in a dedicated module such as
`crates/ask-server/src/fs_watch.rs`.

Start them from the actual server boot path in
`crates/ask-server/src/bin/ask-server.rs` or from a small startup helper called
there. Do not document wiring through `lib.rs::serve`, because that function
does not exist today.

## Required Sub-tasks

- [ ] Add `notify` to `ask-server`
- [ ] Create `fs_watch.rs` for watcher and sweeper code
- [ ] Watch `config.resource_dir`, not arbitrary historical ingest roots
- [ ] Mark indexed files stale on modify and seed embed jobs
- [ ] Add periodic orphan removal for database rows under `resource_dir`
- [ ] Add config for enable/disable, debounce, and sweep interval
- [ ] Wire startup from the real server bootstrap path
- [ ] Add integration tests for modify, delete, and startup sweep behavior

## Acceptance Criteria

1. Indexed files under `resource_dir` become stale when modified on disk.
2. Deleted files under `resource_dir` are eventually removed from the database.
3. New files are not auto-ingested in the first version.
4. The feature does not pretend to watch arbitrary ingest roots that the server
   does not currently track.

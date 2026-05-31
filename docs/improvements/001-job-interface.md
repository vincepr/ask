# 001: Job Contract and Liveness

## Context

`crates/ask-server/src/worker.rs` already has a useful internal job abstraction:

- `JobHandler`
- `JobContext`
- handler-owned payload decoding
- fallible dispatch with queue rows left claimed on failure

That means the original "introduce a trait" work is mostly done already. The
remaining gap is liveness. Long-running jobs never refresh `claimed_at`, so the
queue cannot distinguish "still running" from "worker died halfway through"
until the coarse stale timeout expires.

## Problem

1. A long-running job can sit claimed for up to 24 hours even if the process
   dies immediately after claiming it.
2. There is no shared contract for how handlers report liveness while they work.
3. Future handlers should not call repository heartbeat functions directly.
4. The current shared `JobContext` still carries worker-global data
   (`model_id`) that should eventually move into job payloads.

## Decision

Keep the existing `JobHandler` shape. Do not redesign the dispatcher around a
new trait hierarchy.

Extend the existing job contract with a worker-owned liveness handle that the
handler can call during long-running work:

```rust
pub(crate) struct JobContext<'a> {
    pub(crate) pool: &'a DbPool,
    pub(crate) entry: &'a JobQueueEntry,
    pub(crate) liveness: &'a JobLiveness,
}

pub(crate) struct JobLiveness {
    job_id: i64,
}

impl JobLiveness {
    pub(crate) fn beat(&self, conn: &rusqlite::Connection, now: i64) -> anyhow::Result<()>;
}
```

The important part is ownership, not the exact type. Handlers may call
`ctx.liveness.beat(...)`, but they must not call repository heartbeat updates
directly. This keeps the queue lifecycle centralized even if handlers decide
when to refresh during their own loops.

Use `claimed_at` as the liveness timestamp. Do not add a second heartbeat
column.

## Scope

This feature is intentionally smaller than the original draft.

It does **not** require:

- a new public trait
- a background heartbeat thread
- full shutdown/cancellation plumbing
- retry policy redesign

Those can be added later if needed. The immediate goal is to make long-running
jobs observable and reclaimable without rewriting the current worker model.

## Required Work

- [ ] Add a repository method that refreshes `claimed_at` for one claimed job
- [ ] Add a worker-owned liveness helper to `JobContext`
- [ ] Update `IngestFolderHandler` to call the liveness helper periodically
      during traversal
- [ ] Keep dispatcher completion/failure behavior unchanged for now
- [ ] Remove worker-global assumptions from new job types; job-specific ids
      belong in payloads, not shared context

## Required Tests

- [ ] A test handler can invoke the liveness helper and prove `claimed_at`
      advances while the job is still running
- [ ] Existing malformed-payload and failing-handler tests still pass
- [ ] `IngestFolderHandler` regression tests prove heartbeat calls do not
      change success/failure semantics

## Acceptance Criteria

1. The worker still dispatches jobs through `JobHandler::process`.
2. Long-running handlers can refresh their own claim through a shared worker
   abstraction.
3. No handler calls a repository heartbeat function directly.
4. The queue can observe forward progress without waiting only on the 24-hour
   stale timeout.

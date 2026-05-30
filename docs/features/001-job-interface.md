# 001: Job Handler Trait Abstraction

## Context

Background jobs are currently dispatched by matching on `JobType` and calling job-specific logic from the worker. `IngestFolder` already needs heartbeat updates while it runs. Future jobs such as `ReEmbed` and `EmbedDocument` will need the same lifecycle behavior.

The goal is not just to rename `process_ingest_folder` into a trait method. The worker needs a real job execution contract that gives every job the same lifecycle: claim, decode payload, run with context, refresh liveness, report failure, and complete or retry according to policy.

## Problems To Avoid

1. A trait like `fn process(&self, pool: &DbPool)` is too weak. It hides the job row, payload, job id, shutdown behavior, and error semantics from the handler.
2. Handlers must remain fallible. Logging and returning `()` makes data loss indistinguishable from success.
3. Liveness refresh failures are not harmless. They must be logged with job id and enough context, and future retry/staleness behavior must be able to reason about them.
4. Payload parsing should not be half-owned by the central dispatcher. Either the handler/factory owns typed payload decoding, or a registry owns job-type-to-handler construction.
5. Tests must prove heartbeat behavior directly. They should not rely on a large ingest taking long enough for a timer to fire.

## Proposed Design

### Job Context

Introduce a context object that carries all shared execution facilities:

```rust
pub(crate) struct JobContext<'a> {
    pub(crate) pool: &'a DbPool,
    pub(crate) entry: &'a JobQueueEntry,
    pub(crate) shutdown: &'a ShutdownToken,
}
```

`ShutdownToken` can start as a small wrapper around an atomic flag or channel receiver.
It does not need to support full process shutdown immediately, but the trait should
not block adding it later.

Do not put job-specific identifiers such as `model_id` into `JobContext`. Those belong
in the typed payload for the concrete job. Otherwise the shared interface bakes in
assumptions that only apply to some job types.

### Handler Trait

Keep handlers fallible and context-aware:

```rust
pub(crate) trait JobHandler: Send + Sync {
    fn job_type(&self) -> JobType;

    fn process(&self, ctx: JobContext<'_>) -> anyhow::Result<()>;
}
```

This means every job implementation receives the claimed queue entry and shared runtime
context. Future handlers can inspect `ctx.entry.id`, deserialize their payload, access
the pool, and check shutdown state without expanding the trait signature again.

### Payload Ownership

Use one of these two approaches. Prefer the first until the number of job types grows.

**Option A: Handler-owned payload parsing**

Each handler parses `ctx.entry.payload` into its typed payload at the start of `process`:

```rust
let payload: IngestFolderPayload = serde_json::from_str(&ctx.entry.payload)?;
```

This keeps the dispatcher generic and avoids job-specific parsing in `dispatch_job`.

**Option B: Registry/factory boundary**

If handler construction needs typed dependencies later, create a registry:

```rust
pub(crate) trait JobHandlerFactory: Send + Sync {
    fn build(&self, entry: &JobQueueEntry) -> anyhow::Result<Box<dyn JobHandler>>;
}
```

The dispatcher asks the registry for a handler. The dispatcher still does not parse payloads directly.

### Liveness Lifecycle

Move liveness refresh logic into a worker-owned guard:

```rust
struct HeartbeatGuard {
    stop_tx: std::sync::mpsc::Sender<()>,
    join_handle: std::thread::JoinHandle<anyhow::Result<()>>,
}
```

The guard starts before `process` and stops after `process` returns. It uses `recv_timeout(interval)` instead of `sleep` so shutdown wakes immediately.

The current queue schema uses `claimed_at` as the stale-job clock, so the guard should
refresh that field (or a small repository wrapper around it) rather than reintroducing
a second heartbeat column. Liveness update failures should be returned from the guard
thread and logged by the dispatcher with `job_id`. Do not silently ignore them.

### Dispatcher Flow

The dispatcher should become:

1. Receive a claimed `JobQueueEntry`.
2. Resolve a handler from `entry.job_type` without parsing job-specific payload in the dispatcher.
3. Start a liveness guard for `entry.id`.
4. Call `handler.process(JobContext { ... })`.
5. Stop the guard and surface stop errors.
6. Complete the job only after handler processing returns.
7. Return/log the handler result and completion result with job id and job type.

Initial behavior can continue deleting jobs after handler failure if that is current policy, but the error must still be returned/logged. A later retry feature can change completion policy without changing every handler.

## Required Implementation Steps

1. Add `JobContext` and crate-private `JobHandler` trait in `worker.rs`.
2. Refactor `IngestFolderHandler` to implement `JobHandler::process(ctx)`.
3. Move `IngestFolderPayload` parsing into `IngestFolderHandler::process` or into an explicit handler factory.
4. Introduce `HeartbeatGuard` and use it in `dispatch_job`.
5. Keep `dispatch_job` fallible: `pub fn dispatch_job(...) -> anyhow::Result<()>`.
6. Preserve current behavior for job completion, but never swallow `complete_job` failures.
7. Add a repository method dedicated to refreshing a claimed job's liveness timestamp.
8. Replace direct `println!` / `eprintln!` in new code with structured logging when a logging crate is available.

## Required Tests

1. A custom blocking test handler proves liveness refreshes while the handler is still running.
2. A failing handler proves errors are returned/logged and completion behavior is explicit.
3. A malformed payload test proves payload decode failures are visible and do not silently look like success.
4. `IngestFolderHandler` regression tests prove existing ingest behavior is preserved.
5. A future `EmbedDocument` skeleton carrying `(document_id, model_id)` in its payload can implement the trait without changing shared worker code.

## Acceptance Criteria

1. No job handler calls a repository liveness-update function directly.
2. Every handler implements the same `process(ctx) -> Result<()>` contract.
3. Dispatcher-owned liveness refresh is directly tested with a controllable handler.
4. Handler failures remain visible to the worker loop.
5. The dispatcher contains no job-specific payload parsing beyond handler lookup.

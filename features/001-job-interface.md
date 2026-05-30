# 001: Job Handler Trait Abstraction

## Context

Currently there is a loose function `process_ingest_folder` that handles ingest jobs. Each new job type (e.g. calculating embeddings) would require reimplementing the critical heartbeat logic, which is error-prone and duplicates code.

## Problem

1. The heartbeat update loop is coupled to the ingest handler — any new handler must re-implement it.
2. There is no enforced interface guaranteeing that every job handler provides a `process` method.
3. Adding a new background job type requires copying boilerplate instead of implementing a trait.

## Required Feature

Define a `JobHandler` trait in `ask-core` (or `ask-server`/`ask-core`) that:

1. Enforces a `process` method with a consistent signature including a heartbeat sender/callback.
2. The worker dispatcher manages the heartbeat loop on a timer, not each handler.
3. Existing `IngestFolderHandler` is refactored to implement the trait.
4. New handlers (e.g. `CalculateEmbeddingHandler`) implement the trait and get heartbeat for free.

The trait should look something like:

```rust
#[async_trait]
pub trait JobHandler: Send + Sync {
    async fn process(
        &self,
        job: JobQueueEntry,
        heartbeat: mpsc::Sender<()>,
        pool: DbPool,
    ) -> Result<(), JobError>;
}
```

The dispatcher spawns a heartbeat task that sends updates every N seconds while the handler runs, and cancels it when `process` returns.

## Required Sub-tasks

- [ ] Define `JobHandler` trait in `ask-core` (or a new module)
- [ ] Refactor `IngestFolderHandler` to implement `JobHandler`
- [ ] Move heartbeat loop from handler into the dispatcher
- [ ] Update `dispatch_job` to use the trait
- [ ] Verify all existing ingest behavior is preserved

## Why Now

Without this trait, every new background job duplicates the same heartbeat wiring. Defining it early prevents technical debt from accumulating.

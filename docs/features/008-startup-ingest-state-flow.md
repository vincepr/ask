# Startup State Flow

## Goal

Make startup explicit, predictable, and cheap.

A freshly started server should reconcile lightweight persisted state, surface
 whether it has any work to do, and then begin serving. Startup must not hide
 heavy ingest behavior, directory walks, or long-running maintenance behind a
 "healthy" process state.

## Problem

The current startup flow does the following:

1. load config
2. run DB migrations
3. ensure the configured embedding model exists
4. rebuild or confirm the active sqlite-vec index
5. start the worker
6. start the HTTP server

That leaves two gaps:

- On a fresh database with zero documents, the server starts cleanly but gives no
  actionable signal that nothing has been ingested yet.
- When the embedding model already exists, startup does not currently guarantee
  that recoverable pending work has been re-queued before the worker begins its
  normal polling loop.

The result is a process that can be technically healthy while still being
 operationally inert.

## Decision

- Keep ingest explicit. Startup must not auto-ingest the configured resource
  directory.
- Keep startup work cheap. Startup may run lightweight database reconciliation,
  but it must not walk the filesystem or perform embedding work.
- Run embedding-job reconciliation synchronously during startup.
- Emit an explicit startup summary after reconciliation so empty or idle states
  are obvious.
- Do not add a recurring scheduler, a long-lived maintenance worker, or a new
  queue job type for this feature.

## Why This Design

This is the smallest design that fixes the actual problem.

- It preserves the current architecture: explicit ingest, queued background
  work, and a small worker loop.
- It avoids hidden startup side effects such as unexpected directory scans.
- It avoids adding a maintenance-job abstraction for work that is only a cheap
  database reconciliation pass.
- It improves reliability and operator clarity without turning startup into a
  second orchestration system.

## Non-Goals

- No startup auto-ingest.
- No blocking startup on document indexing or embedding completion.
- No recurring maintenance scheduler.
- No new queue job type just to trigger cheap reconciliation logic.
- No `/health` redesign in this feature. Health remains a basic liveness check.
- No queue-model rewrite.

## Required Startup Behavior

Startup should perform these steps in order:

1. apply pending migrations
2. ensure the configured embedding model row exists
3. if the model is newly created, backfill pending rows for existing documents
4. seed missing `embed_document` jobs from pending or stale embedding rows
5. ensure the active sqlite-vec index matches the active model
6. inspect lightweight state and log an actionable startup summary
7. start the worker and HTTP server

The key constraint is that steps 3 through 6 are database reconciliation only.
 They must not scan the configured resource directory or perform embedding HTTP
 requests.

## Implementation Plan

1. Extract the startup state reconciliation into a small focused function or
   module so `main` is no longer responsible for manually sequencing every
   startup detail inline.
2. Reuse the existing repository primitive that seeds missing `embed_document`
   jobs from recoverable embedding rows.
3. Keep the "new model" path and the "existing model" path aligned so startup
   always ends with the same queue reconciliation behavior.
4. Add one lightweight startup-state query layer that can answer at least:
   - how many documents exist
   - how many recoverable document/model pairs exist
   - how many jobs were newly seeded during this startup pass
5. Emit explicit logs from startup based on those counts.
6. Keep worker startup unchanged after reconciliation is complete.

## Logging Expectations

Startup logs should make these states obvious:

- zero documents exist: the server is empty and the next action is to call
  `POST /ingest`
- documents exist and pending work was re-queued: recovery happened
- documents exist and no work is pending: the corpus is currently idle

The logs should be actionable, not noisy. One concise startup summary is enough.

## Implementation Notes

- The reconciliation function should stay synchronous because the work is only a
  few DB queries and enqueue operations.
- Do not introduce a new maintenance job just to move cheap logic out of `main`.
  That would increase queue surface area without reducing real complexity.
- If startup reconciliation ever becomes expensive, that is a signal that the
  design has drifted beyond this feature's intended scope.
- Manual ingest remains the only document-discovery mechanism.

## Test Plan

- Startup regression test for an existing model with pending or stale embedding
  rows and no queued jobs: recovery seeds the missing jobs before the worker
  starts.
- Startup regression test for a fresh database with zero documents: startup does
  not auto-ingest and emits the empty-state signal.
- Startup regression test for an existing fully idle corpus: no duplicate jobs
  are created.
- Regression test that startup reconciliation is safe to run repeatedly.

## Acceptance Criteria

- Startup performs cheap embedding-work reconciliation every time.
- A fresh empty database no longer looks silently complete; logs explain the next
  action.
- Startup does not scan the resource directory or auto-ingest files.
- No new scheduler or maintenance job abstraction is introduced.
- The startup path remains simple and cheap enough to run synchronously.

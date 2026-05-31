# Pending Embedding Recovery

## Goal

Recover missing embedding work automatically after restart.

If `document_embeddings` contains rows in a recoverable non-complete state but
 there is no runnable `embed_document` job for that document/model pair, startup
 should recreate the missing work without operator intervention.

## Decision

- Recovery runs on every startup.
- Recovery is derived from persistent database state.
- Recovery is idempotent.
- Recovery only recreates missing `embed_document` jobs.
- This feature does not redesign the queue model, retry policy, or failure
  backoff behavior.

## Why This Design

This is the smallest self-healing fix for the current failure mode.

- It repairs broken state caused by previous failed runs.
- It does not require new background daemons, retry counters, or queue
  abstractions.
- It makes restart behavior predictable for a persistent local `.data/`
  directory.
- It keeps the recovery rule explicit: pending work without a runnable job gets a
  job again.

## Non-Goals

- No queue architecture redesign.
- No retry scheduling or exponential backoff.
- No per-failure counters.
- No attempt to make `pending` impossible as a state.
- No special-case operator workflow through manual endpoints.

## Recovery Rule

At startup, for each distinct `(document_id, model_id)` pair with at least one
 `document_embeddings` row in a recoverable state such as `pending` or `stale`:

- if a runnable `embed_document` job already exists for that pair, do nothing
- otherwise enqueue one `embed_document` job for that pair

The recovery pass must be safe to run repeatedly.

## Implementation Plan

1. Extract a single repository query that finds distinct document/model pairs
   with recoverable embedding rows.
2. Extract or add a repository query that detects whether a runnable
   `embed_document` job already exists for a given document/model pair.
3. At startup, run one recovery pass that compares those two sets and enqueues
   only the missing jobs.
4. Reuse the same enqueue path already used for normal embedding work so the
   system has one job creation mechanism.
5. Keep the recovery call unconditional and idempotent so startup behavior does
   not depend on whether the model row was newly created in this boot.
6. Keep the scope at document/model granularity rather than trying to enqueue
   work per embedding row.

## Implementation Notes

- Prefer one shared code path over separate "new model backfill" and "restart
  recovery" mechanisms if the existing code can be simplified that way.
- Keep the recovery logic close to startup state reconciliation rather than
  inside the worker loop.
- Do not create duplicate jobs when one is already queued or claimed.
- Treat `pending` and `stale` as recoverable states only if that matches current
  worker semantics; do not invent new state meanings in this feature.

## Test Plan

- Regression test for startup with pending rows and no jobs: recovery enqueues
  one job per affected document/model pair.
- Regression test that running recovery twice does not create duplicate jobs.
- Regression test that an existing queued or claimed job prevents duplicate job
  creation.
- Regression test that completed embeddings do not create recovery jobs.

## Acceptance Criteria

- Restarting the server repairs orphaned pending embedding work automatically.
- Recovery can run on every startup without duplicate job creation.
- Recovery does not require manual use of a stale-documents endpoint.
- The implementation stays small and uses the existing queue model rather than
  introducing a second scheduling system.

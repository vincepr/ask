# 002: Job Retry and Failure Semantics

## Problem

A failed job keeps its claim until it becomes stale, which currently means a 24-hour delay before the
system can retry or cleanly classify the failure.

## Evidence

- `crates/ask-core/src/repository.rs:7-9` hard-codes a one-day stale timeout.
- `crates/ask-server/src/worker.rs:64-112` leaves failed jobs claimed and only logs the error.
- `crates/ask-server/src/worker.rs:141-153` treats a missing ingest directory as success, while
  malformed payloads and other permanent failures remain in the queue until the stale timeout.

## Why This Is Risky

- One malformed job can sit in the queue for a day even though it will never succeed.
- One transient database or filesystem fault also costs a day before retry.
- Operators cannot distinguish "retry later" from "dead forever" without reading logs.

## Simplest Stable Fix

- Add explicit job state fields such as `queued`, `running`, `failed`, and `completed`.
- Store `attempt_count`, `last_error`, and `next_attempt_at`.
- On permanent failures, mark the job failed immediately instead of holding the claim.
- On transient failures, release the claim with bounded backoff rather than waiting for a coarse stale
  timeout.


## Human review:
- As simplicity is the main goal for the queue system this seems fine for now. But for the future:
- Too much logic for what it currently does. Would rather go the route to pass in a lambda to update a heartbeet while the process is running. Can be also used to check if the db-entry can no longer be updated -> someone delted the entry -> we terminate this process. that heartbeat then allows to move the 24hour delay down to maybe an hour. (if heartbeat untouched for an hour we just assume its dead -> we then DELETE the old one so a still working process will then stop on its next heartbeat -> and create a duplicate with a new id) -> new worker picks it up. And transient falures can just write a heartbeat in the future (means something like retry after 3 h)
- BUT DEFINITLY NEEDS SOME BIG REWRITING, before coming a feature in that regard. Also really low prio

## Real-World Failure: Embedding Provider Not Ready

### What Happened

A fresh Docker Compose stack was started from scratch (empty DB, empty TEI
cache volume). The sequence:

1. TEI container begins: downloads model artifacts (`tokenizer.json`, ONNX
   weights ~93s), then warms up the model (~56s). Total: ~2.5 minutes before
   `/health` passes.
2. ask-server starts immediately after its container is created. The worker
   begins polling `job_queue` every 5 seconds.
3. User calls `POST /ingest` with `root_path: "/resources"`. This enqueues
   58 documents, producing 736 `EmbedDocument` jobs and ~736 pending embedding
   rows.
4. Worker picks up a job every 5s, tries `POST http://tei/embeddings`, gets
   `Connection refused` because TEI is still downloading. Fails, leaves claim
   in place.
5. Over ~3.5 minutes, the worker claims and fails 38 jobs. TEI finally becomes
   healthy, but all 38 jobs are claimed with a 24-hour stale timeout.
6. The remaining unclaimed jobs (~16) succeed once TEI is up, producing 32
   embedded chunks. The other 704 pending chunks are stuck behind the 38
   claimed jobs.
7. From the user's perspective: there was no error at ingest time, then some
   documents appear embedded, others remain pending with no feedback.

### Why This Happened

Three distinct problems combined:

1. **No provider readiness check**: The worker does not verify that the
   embedding provider is reachable before claiming a job. It claims first, then
   fails, burning the retry window.
2. **Claim-once semantics**: A claimed job is exclusively owned until the stale
   timeout (24h). No retry is possible without a restart.
3. **No transient vs permanent distinction**: "Connection refused" (transient,
   provider not ready) is treated the same as a malformed payload (permanent).
   Both block the queue for 24h.

### What a Fix Must Address

The stale timeout mechanism alone is insufficient. A fix for this class of
failure must consider the full lifecycle:

- **Before claiming**: Should the worker check provider health (e.g., a light
  `/health` ping) and skip the tick if the provider is unavailable? This
  prevents burning claims on transient infrastructure unavailability.
- **After transient failure**: Should the worker release the claim immediately
  on errors like "Connection refused", so the job is retried on the next tick?
  If so, how does the system avoid busy-looping against a down provider
  (hot-loop protection)?
- **Backoff**: If jobs are retried quickly, how does the system back off when
  the provider stays down (e.g., exponential backoff stored per job or
  globally)?
- **Hard vs soft claims**: The heartbeat approach from the human review above
  would naturally solve this — a short heartbeat timeout (minutes, not hours)
  would let a worker crash or a provider blip be recovered quickly without
  losing work.

See follow-up issue: `docs/issues/09-embedding-provider-readiness.md`

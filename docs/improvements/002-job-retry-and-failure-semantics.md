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

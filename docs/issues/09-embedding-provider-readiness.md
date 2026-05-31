# Embedding Provider Readiness and Transient Failures

## Problem

The worker does not account for the embedding provider being temporarily
unavailable. During the first startup of a fresh stack, the TEI container
needs to download model weights (~1-2 minutes) and warm up (~1 minute) before
it can serve requests.

The ask-server container typically starts before TEI is ready. Its worker
immediately begins claiming and failing `EmbedDocument` jobs, burning the
24-hour stale timeout on every claim. By the time TEI becomes healthy, most
jobs are stuck.

## Evidence

Fresh stack from empty state:
- 58 documents ingested → 736 `EmbedDocument` jobs enqueued
- Worker claims one job per 5s tick, all fail with `Connection refused`
- 38 jobs claimed and stuck for 24h before TEI finishes warming up
- 704/736 embedding chunks remain pending indefinitely
- Only 16 unclaimed jobs survive to succeed once TEI is healthy

The `STALE_JOB_AGE_SECS = 86400` (`repository.rs:12`) means claimed jobs are
not retried for 24 hours after failure. A simple `docker compose restart
ask-server` clears the in-memory claims and unblocks the queue, but this is
not discoverable.

## Root Causes

1. **No pre-claim health check**: The worker (`worker.rs:36-54`) claims a job
   before doing any work. It never checks whether the embedding provider is
   reachable first. If the provider is down, the claim is wasted.

2. **No transient failure handling**: All failures are treated equally —
   "Connection refused" (transient) gets the same 24h timeout as a corrupt
   payload (permanent). The worker logs the error and moves on
   (`worker.rs:148-156`).

3. **No backoff**: Even if claims were released on transient failure, the
   worker would re-claim and re-fail on every 5s tick, creating a hot loop
   against a down provider.

## Questions

**Provider health probe:**
- Should the worker ping the embedding provider's `/health` endpoint before
  claiming a job? If unhealthy, skip the tick entirely (sleep and retry).
- Where should this probe live — in the worker tick, in the
  `EmbeddingClient` trait, or in a separate health-checker task?
- How does the probe handle providers that don't expose a health endpoint?
  (OpenAI has no equivalent of TEI's `/health`.)

**Claim lifecycle for transient failures:**
- Should the worker release the claim on errors like "Connection refused" or
  timeout, making the job immediately available for retry?
- If released, how does the system avoid busy-looping? A global "provider is
  down, hold all claims" flag? Per-job `retry_after` timestamp?

**Backoff strategy:**
- Exponential backoff stored in the job row (`next_attempt_at`)?
- Fixed cooldown (e.g., 30s minimum between retries)?
- Heartbeat-based approach (from `002-job-retry-and-failure-semantics.md`):
  worker writes a heartbeat; if heartbeat is untouched for N minutes, another
  worker can delete the stale entry and create a fresh one.

**Startup ordering:**
- Should the ask-server container wait for TEI's healthcheck to pass before
  starting its worker? This could be done with a
  `depends_on: tei: condition: service_healthy` in docker-compose.
- Would this fully solve the issue, or are there other scenarios where the
  provider becomes unavailable mid-flight (e.g., network partition, OOM)?

**Simplicity constraint:**
- The project values lean code. Adding retry logic, health probes, and backoff
  is complexity. What is the smallest change that makes the system robust
  against this class of failure?
- Does the docker-compose healthcheck ordering (`depends_on` + `condition`)
  solve enough to defer the code-level changes?

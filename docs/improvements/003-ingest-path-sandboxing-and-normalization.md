# Ingest Paths Need Sandboxing and Normalization

## Problem

The HTTP API currently accepts any readable directory path on the host and uses the raw request string
as the deduplication key.

## Evidence

- `crates/ask-server/src/config.rs:63-82` defines `resource_dir`, but the ingest route does not use it.
- `crates/ask-server/src/http.rs:45-71` only checks `exists()` and `is_dir()` before queueing work.
- `crates/ask-core/src/repository.rs:241-258` deduplicates jobs by `(job_type, payload)`, which means
  `"/tmp/x"` and `"/tmp/./x"` are different jobs.
- `crates/ask-server/src/worker.rs:211-213` persists the path exactly as discovered, not as a canonical
  identity.

## Why This Is Risky

- Any client that can hit `POST /ingest` can make the service scan arbitrary host directories.
- The same directory can be ingested multiple times through path aliases, symlinks, or non-canonical
  spellings.
- Duplicate work at the job layer feeds directly into duplicate documents at the storage layer.

## Simplest Stable Fix

- Canonicalize the requested path before queueing anything.
- Reject paths outside a configured allowed root, most likely `resource_dir`.
- Store and compare canonical paths everywhere the code uses file identity.
- Deduplicate jobs on normalized structured data, not on raw JSON text.

# database is locked (SQLITE_BUSY)

- **Reported:** 2026-05-31
- **Severity:** Medium — causes ingest/search failures under concurrent load
- **Affected:** `ask-server` (all deployments sharing a single SQLite file)

---

## Symptoms

The server logs the error:

```
2026-05-31T18:21:09.690056Z ERROR database is locked
```

This is the `SQLITE_BUSY` error from rusqlite / libsqlite3. It manifests as:

- `/ingest` requests returning HTTP 409 Conflict with `"database error: ..."`
- `/documents/stale` requests returning HTTP 500
- Background worker ticks failing silently (logged at ERROR level)

Under steady concurrent load the error is reproducible within seconds of startup.

---

## Root Cause

Two interacting problems:

### 1. `tick()` holds a pool connection idle for the entire job duration

In `crates/ask-server/src/worker.rs`, the `tick()` function:

```rust
let mut conn = pool.get()?;          // acquires connection
let entry = repository::claim_job(&mut conn, now)?;
// ...
dispatch_job_with_resolver(&pool, &entry, ...)  // conn NOT dropped — held here!
```

The connection acquired for `claim_job()` was never explicitly dropped. It remained checked out from the pool for the entire lifetime of `dispatch_job_with_resolver()`, which includes:

- Reading a file from disk
- Calling an external HTTP embedding API
- Writing results back to the DB

For a document with many chunks, this can hold the connection idle for **multiple seconds**, needlessly reducing pool capacity from 4 → 3 (or fewer) for other consumers.

### 2. Short busy timeout (5 s)

The `busy_timeout` PRAGMA was 5000 ms. When multiple connections simultaneously hold `IMMEDIATE` transactions (used by `claim_job`, `upsert_document`, `replace_embeddings_for_document_model`, etc.), SQLite serialises writers. If a writer cannot acquire the reserved lock within 5 seconds, it bails with `SQLITE_BUSY`.

Given the reduced pool capacity from problem 1, transactions pile up and frequently exceed 5 s.

---

## Reproduction

1. Start the server with the default 4-connection pool.
2. Send concurrent requests: `/ingest` a directory with many files while simultaneously calling `/search` and `/documents/stale`.
3. Observe `"database is locked"` errors within seconds in server logs.

---

## Possible Fix - Validate other solutions first

Two changes in commit `???`:

### `crates/ask-server/src/worker.rs`

Insert `drop(conn)` after `claim_job` and before `dispatch_job_with_resolver` so the claim connection is returned to the pool immediately:

```rust
let mut conn = pool.get()?;
let entry = repository::claim_job(&mut conn, now)?;
let entry = match entry {
    Some(entry) => entry,
    None => return Ok(()),
};
drop(conn);   // ← added: release connection before long-running job

info!(job_id = entry.id, job_type = %entry.job_type, "claimed job");
dispatch_job_with_resolver(&pool, &entry, model_id, embedding_client, resolve_handler)
```

### `crates/ask-server/src/lib.rs`

Increase `busy_timeout` from 5000 → 30000 ms to give transient write conflicts more time to resolve:

```sql
PRAGMA busy_timeout=30000;
```

---

## Verification

- `cargo build` — compiles cleanly
- `cargo clippy -- -D warnings` — no warnings
- `cargo test` — all 139 tests pass
- After rebuilding with `docker compose --profile tei up --build`, the `"database is locked"` error no longer appears under the same concurrent load that previously triggered it.

---

## Notes

- The pool size of 4 is still relatively small. If concurrent load grows further, consider increasing `max_size` in `crates/ask-server/src/lib.rs:95`.
- The root cause was a resource-leak-by-omission: the `conn` binding had sufficient scope to survive the entire closure, but was only logically needed for the first few statements.

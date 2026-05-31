# Orphaned Pending Embeddings on Restart

When the embedding model already exists in the DB at startup,
`backfill_pending_for_model()` (`worker.rs:94-116`) is not called — it only
runs for newly inserted models (`ask-server.rs:35-57`).

After a failed run (TEI offline, dimension mismatch, etc.), the
`document_embeddings` table can have rows in `state=pending` with no
corresponding jobs in `job_queue`. There is no mechanism to re-enqueue them
automatically.

Observed state after the dimension mismatch failure:
- `document_embeddings state=pending`: 167 rows
- `job_queue`: empty

The `POST /documents/stale` endpoint can re-trigger embedding, but it requires
manual intervention and knowing which document IDs to pass.

## Questions

- Is this an edge case that only happens during development when providers are
  misconfigured, or is it a real failure mode that production deployments will
  encounter?
- If real, should the server detect orphaned pending embeddings on startup and
  re-enqueue jobs automatically? What would that check look like without adding
  expensive queries?
- Should the worker track failure counts and re-queue after a backoff, or is a
  simple stale-timeout mechanism sufficient?
- Should the `backfill_pending_for_model` logic be idempotent and safe to call
  even for existing models, eliminating the "only on insert" constraint?
- Is there a design where pending state cannot exist without a corresponding
  job (e.g., atomic enqueue + state change, or no pending state at all —
  just jobs)?
- Given that the user's `.data/` directory is persistent across restarts, what
  recovery semantics should the system guarantee?

---
_This document captures problems observed during exploration. Update or close when the corresponding implementation resolves the underlying concern._

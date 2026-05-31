# File Path Resolution in the Worker

Document filepaths are stored in the `documents.filepath` column. The worker's
`EmbedDocumentHandler` reads them back and passes them directly to
`std::fs::read_to_string()` (`worker.rs:229`, `worker.rs:483`).

Inside Docker:
- The volume mount places files at `/resources`
- The container WORKDIR is `/app` (from Dockerfile)
- Stored paths like `./Cargo.toml` resolve against CWD, becoming
  `/app/Cargo.toml` which does not exist

The `IngestFolderHandler` stores paths via `std::fs::canonicalize()` at
`worker.rs:386-397`, producing absolute paths. Yet the DB contains relative
paths (`./Cargo.toml`), suggesting they were inserted by a different code path
or an earlier version.

The worker's `JobContext` has no `resource_dir` field, so there is no way to
resolve a relative path against the configured resource root. The HTTP layer
(`http.rs:42`) does canonicalize `resource_dir` into a `resource_root`, but
that value is never passed to the worker.

## Questions

- Should stored filepaths always be absolute, or should they be relative to the
  resource root and resolved at read time? Each has different implications for
  portability, the ingest endpoint, and the data model.
- If paths should be absolute, how do we handle existing relative-path entries
  in the DB? A one-time migration? Ignore them and let re-ingest fix it?
- If paths should remain relative, how does `resource_dir` get threaded into
  the worker without adding complexity to `JobContext`, `spawn()`, `tick()`,
  and every handler? Is there a simpler approach (e.g., changing the container
  WORKDIR)?
- Does the worker even need to read files at all, or could chunk content be
  stored alongside the document embedding rows? That would trade storage for
  simplicity (no file I/O, no path resolution).
- What invariants should the `Document.filepath` field guarantee, and where
  should they be enforced — the DB schema, the repository layer, or the
  caller?

## Recommended Direction

- Make this a compatibility and cleanup feature, not the primary place where
  path semantics are defined.
- After `documents.filepath` is defined as canonical absolute paths, the worker
  should mostly just open the stored path directly.

## Implementation Notes

- If backward compatibility for existing relative rows is needed, thread the
  configured resource root into the worker only as a migration aid, not as the
  long-term steady-state contract.
- A pragmatic recovery flow is:
  - detect a relative stored path
  - resolve it against the configured resource root
  - if the resolved file exists, use it and repair the stored row
  - if not, fail clearly instead of silently reading from CWD
- Avoid changing container `WORKDIR` as the main fix. That hides the data-model
  ambiguity instead of removing it.

## Dependencies and Sequencing

- Depends on
  [006-document-filepath-invariants.md](/home/vince/ask/docs/features/006-document-filepath-invariants.md)
  for the steady-state invariant.
- Search endpoint work will benefit from the same read-path semantics.

## Test Expectations

- Regression test for a legacy relative DB path resolved against the resource
  root.
- Test that successful fallback repairs the stored path.
- Test that an unresolved relative path produces a deterministic error.

---
_This document captures problems observed during exploration. Update or close when the corresponding implementation resolves the underlying concern._

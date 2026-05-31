# Document Filepath Invariants

Document filepaths stored in `documents.filepath` are read by the worker and
passed directly to `std::fs::read_to_string()`. The current DB contains
relative paths (`./Cargo.toml`). The `IngestFolderHandler` stores absolute
paths via `canonicalize()` (`worker.rs:413`), but there is no enforcement of
this convention anywhere.

The `documents` table schema (`0002_create_domain_tables.sql`) declares
`filepath TEXT NOT NULL` — no constraint, no guidance on format.

A relative path is ambiguous without knowing the process working directory.
Inside Docker, where WORKDIR=/app but files live at /resources, `./Cargo.toml`
fails to resolve.

## Questions

- Should `filepath` be an absolute path (resolved against the resource root at
  ingest time), or a path relative to the resource root (resolved at read time)?
  What are the trade-offs for portability, debugging, and implementation
  complexity?
- If absolute, how do existing relative-path entries get fixed — migration,
  on-read resolution fallback, or manual re-ingest?
- If relative-to-resource-root, how does the worker discover the resource root
  without threading it through the entire job dispatch pipeline?
- Could the path format constraint be expressed in the schema (CHECK
  constraint), or is runtime validation more appropriate?
- Is storing filepaths at all necessary for the worker's job? Could the chunk
  content be persisted so the worker never needs to re-read files from disk?
  This would eliminate path resolution entirely at the cost of storage.
- What is the simplest set of invariants that makes the system work correctly
  without adding layers of path-normalization code?

## Recommended Direction

- Store canonical absolute paths in `documents.filepath`.
- Make the stored path be the exact path the worker will later open.
- Do not optimize for cross-machine portability of the SQLite file if that
  makes the runtime contract ambiguous. The current deployment model is a local
  mounted directory and a local database file.

## Implementation Notes

- Enforce the invariant at ingest boundaries, not at random read sites.
- Runtime validation is probably enough; a DB-level absolute-path constraint is
  harder to keep portable and does not replace canonicalization.
- Existing relative rows need a repair strategy. The least risky path is either:
  - a one-time migration/repair pass on startup, or
  - a backward-compatibility fallback in the worker that resolves and rewrites
    old relative rows once they are encountered
- Do not move to persisted chunk text yet just to avoid path handling. That is
  a larger storage and synchronization design change.

## Dependencies and Sequencing

- This should be decided before worker path resolution changes.
- Search result text extraction also depends on the same invariant.

## Test Expectations

- Test that ingest stores canonical absolute paths.
- Regression test for a previously relative path being repaired or rejected in a
  controlled way.
- Integration test for the Docker-like resource-root case.

---
_This document captures problems observed during exploration. Update or close when the corresponding implementation resolves the underlying concern._

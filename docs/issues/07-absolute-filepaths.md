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

---
_This document captures problems observed during exploration. Update or close when the corresponding implementation resolves the underlying concern._

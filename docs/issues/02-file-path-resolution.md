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

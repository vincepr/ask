# Document Filepath Invariants

## Goal

Make document file access boring and deterministic.

The system should store one canonical filepath format, use that exact path at read
 time, and fail clearly if the file is gone. Do not add compatibility layers,
 fallback path resolution, or migration logic for old database contents.

## Decision

- `documents.filepath` must store a canonical absolute path.
- Ingest is responsible for establishing that invariant.
- The worker must use the stored path directly.
- If the file no longer exists or cannot be read, the worker should return a
  normal file I/O error for that document.
- Existing databases with relative paths are out of scope. The database can be
  recreated instead of repaired.

## Why This Design

This is the smallest correct design for the current repo.

- It keeps path normalization in one place: ingest.
- It keeps worker logic simple.
- It avoids threading a resource-root concept through unrelated code.
- It avoids temporary compatibility paths that tend to become permanent.
- It matches the current deployment model: local files, local database, local or
  Docker-mounted resources.

## Non-Goals

- No startup repair pass for old relative rows.
- No worker fallback that resolves relative paths against a resource root.
- No schema-level path-format constraint beyond the existing column type.
- No persisted chunk text as a workaround for path handling.
- No portability goal for copying the SQLite database between different machines
  or filesystem layouts.

## Implementation Plan

1. Ingest canonicalizes every discovered file path before inserting or updating a
   `documents` row.
2. Any ingest path that cannot be canonicalized fails early and does not create a
   document row with an ambiguous path.
3. The worker reads `documents.filepath` exactly as stored.
4. The worker does not rewrite, normalize, or reinterpret stored paths.
5. File read failures are returned with clear context so the job failure explains
   which stored path could not be read.
6. Any tests or fixtures that still assume relative filepaths must be updated to
   use canonical absolute paths.

## Implementation Notes

- `std::fs::canonicalize()` should be the source of truth for normalization.
- Store the canonicalized absolute path string, not the originally supplied path.
- Keep the invariant at the boundary. Do not scatter additional path checks
  across downstream code.
- A missing file at embed time is a normal operational failure, not a signal to
  guess another path.
- If the application later needs true cross-machine portability, that should be
  designed as a new feature with a different storage contract rather than added
  as an exception to this one.

## Test Plan

- Unit test that ingest stores canonical absolute paths.
- Regression test that worker reads a stored absolute path without applying any
  resource-root or cwd-based fallback.
- Integration test covering the Docker-style case where the process working
  directory differs from the mounted resource location.
- Regression test that a deleted file produces a clear controlled failure rather
  than alternate path resolution.

## Acceptance Criteria

- New `documents.filepath` values are canonical absolute paths.
- Worker behavior does not depend on process working directory.
- The system does not contain dual path-resolution strategies.
- Deleted or unreadable files fail clearly and predictably.
- No migration or compatibility code is introduced for legacy relative-path
  database rows.

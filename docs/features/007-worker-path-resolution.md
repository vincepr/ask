# Worker Path Resolution

## Goal

Keep worker file access simple and deterministic.

The worker should read the stored document path exactly as written in the
 database, and its behavior must not depend on the process working directory,
 Docker `WORKDIR`, or any separately configured resource root.

## Decision

- This feature does not define path storage semantics. It assumes the existing
  ingest contract that `documents.filepath` is a canonical absolute path.
- The worker must treat `documents.filepath` as the exact absolute path to open.
- The worker must not resolve relative paths, prepend resource roots, or attempt
  path repair.
- If the file cannot be opened or read, the worker should fail the job with clear
  context about the path that failed.

## Why This Design

This keeps responsibility boundaries clear.

- Ingest owns path normalization.
- The worker owns file reading and error reporting.
- The runtime stays small because there is only one path interpretation model.
- Docker-specific filesystem layout does not need special handling once stored
  paths are canonical absolute paths.

## Non-Goals

- No resource-root field added to worker context.
- No compatibility handling for legacy relative paths.
- No fallback against current working directory.
- No read-time path rewriting or database repair.
- No container `WORKDIR` tricks as a substitute for a clean data contract.

## Implementation Plan

1. Review the worker read path and remove any logic that attempts to reinterpret
   stored filepaths.
2. Ensure the worker opens `documents.filepath` exactly as stored.
3. Add or tighten error context so job failures report the document id and the
   exact file path that could not be read.
4. Keep all path-format assumptions out of worker configuration and job context.
5. Update tests so worker behavior is validated against canonical absolute paths
   only.

## Implementation Notes

- The worker should rely on the invariant established by ingest rather than
  duplicating normalization logic.
- A missing file is an operational failure, not a prompt to guess alternate
  locations.
- If later features need a different storage contract, they should change the
  ingest-time invariant rather than adding another worker resolution strategy.

## Test Plan

- Regression test that the worker reads a stored canonical absolute path without
  consulting cwd or a resource root.
- Integration test covering a Docker-style layout where process cwd differs from
  the mounted resource location.
- Regression test that a missing file produces a clear deterministic failure.
- Regression test that worker behavior remains unchanged if cwd is moved to a
  different directory before processing a job.

## Acceptance Criteria

- Worker file access does not depend on process working directory.
- Worker code contains no alternate path resolution strategy.
- Job failures include useful path context when file reads fail.
- The worker remains a thin consumer of the ingest-established filepath
  invariant.

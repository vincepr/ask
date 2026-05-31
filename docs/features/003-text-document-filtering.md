# 003: Path-Based Ingest Filtering

## Context

The current ingest path has no explicit inclusion policy. Every regular file is
treated as a candidate, and the worker only discovers some bad inputs after
trying to read them as UTF-8.

That is not really "text document detection". It is just an implicit fallback.

## Problem

1. Users cannot control which files are eligible for ingest.
2. A regex on its own does not prove a file is text, but it can express a clear
   include policy.
3. The current API uses `IngestFolderPayload` directly as the HTTP request body,
   which makes queue-payload changes awkward.
4. Ignore-file handling should happen in traversal, not by shelling out.

## Decision

Treat this feature as **path-based inclusion**, not as a complete binary/text
classifier.

Add a separate request type for the HTTP API:

```rust
pub struct IngestRequest {
    pub root_path: String,
    pub file_pattern: Option<String>,
}
```

The queue payload stores the resolved pattern so retries are deterministic:

```rust
pub struct IngestFolderPayload {
    pub root_path: String,
    pub file_pattern: String,
}
```

If `file_pattern` is absent, resolve it to a default include regex before the
job is queued.

Match the regex against the normalized relative path from the ingest root using
forward slashes. This lets users filter by filename or by subdirectory layout.

## Non-Goals

This feature does **not** fully solve:

- binary-file sniffing
- large-file limits
- partial file reads

Those should be handled separately as file-size and content-safety guards.

## Required Feature

1. `POST /ingest` accepts an optional `file_pattern`.
2. The request path validates the regex before queueing the job.
3. `IngestFolderPayload` stores the resolved regex string.
4. The worker applies the regex before opening file content.
5. Recursive traversal uses ignore-file support from feature 002 instead of
   subprocess checks.

## Required Sub-tasks

- [ ] Add `IngestRequest` for HTTP input
- [ ] Extend `IngestFolderPayload` with `file_pattern`
- [ ] Define one default include regex constant
- [ ] Compile and validate user-supplied regexes in the request path
- [ ] Normalize candidate paths relative to the ingest root before matching
- [ ] Add tests for default matching, custom matching, and invalid regex input
- [ ] Add tests proving ignore files and regex matching compose correctly

## Acceptance Criteria

1. Ingest eligibility is driven by an explicit path filter.
2. Job retries use the same resolved filter as the original request.
3. Invalid regexes fail fast in the API instead of becoming poison jobs.
4. The feature does not claim to solve binary detection by regex alone.

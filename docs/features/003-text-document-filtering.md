# 003: Text Document Filtering

## Context

The ingest process needs a configurable definition of "text document". The current
behavior is too implicit: there is no user-supplied include pattern, and unreadable or
binary files are only avoided indirectly when UTF-8 decoding fails. Additionally,
ignored files (secrets, build artifacts, vendored dependencies) should not be indexed.

## Problem

1. Users cannot customize what gets indexed.
2. Relying on "can this file be read as UTF-8?" is not a real inclusion policy.
3. Git-ignored files (secrets, `target/`, `node_modules/`, build artifacts) are not excluded.
3. No mechanism to pass a filter pattern through the API into the job payload.

## Research: Identifying Text Documents

**Recommendation: regex, but only for inclusion rules**. Operating systems do not
provide a standard cross-platform "is this a text file?" heuristic that is both
reliable and project-configurable. The `file` command / `libmagic` can identify MIME
types, but that adds a dependency and still does not express project-specific policy
such as "index only source files and Markdown". A regex pattern is simpler and keeps
the rule explicit.

**Git-ignore exclusion**: do not shell out to `git check-ignore` per file. That would
be slow, harder to test, and incomplete unless every parent path is handled correctly.
Use the `ignore` crate walker so ignore handling happens as part of traversal. Outside
git repositories it naturally degrades to plain traversal plus the regex include rule.

## Required Feature

1. **API change**: `POST /ingest` accepts an optional `file_pattern` field (string
   regex). If absent, a default pattern is used:
   `(?i)\.(md|txt|rst|rs|py|js|ts|tsx|jsx|json|ya?ml|toml|ini|cfg|csv|sql)$`.
2. **Payload change**: `IngestFolderPayload` stores the resolved `file_pattern`
   string so worker retries are deterministic.
3. **Validation**: Compile the regex in the request path and reject invalid patterns
   early, rather than enqueueing jobs that are guaranteed to fail later.
4. **Handler change**: Match the regex against a normalized relative path, not just
   the basename. That gives users control over both filename and subdirectory layout.
5. **Ignore exclusion**: Let the recursive walker honor git-ignore and related ignore
   files during traversal.
6. **Fallback**: Outside a git repo, apply the regex filter with no special casing.

## Required Sub-tasks

- [ ] Add optional `file_pattern` field to `POST /ingest` request body
- [ ] Add `file_pattern` field to `IngestFolderPayload` (required once queued)
- [ ] Add default regex constant in configuration or code
- [ ] Validate and normalize the regex before enqueuing the job
- [ ] Implement relative-path regex matching in `IngestFolderHandler`
- [ ] Use traversal-layer ignore handling instead of per-file subprocess checks
- [ ] Handle non-git directories gracefully
- [ ] Update existing tests; add tests for regex matching and gitignore exclusion
- [ ] Add tests proving ignored files are skipped without shelling out to `git`

## Why Now

An explicit include pattern is more predictable than accidental UTF-8 sniffing, and
ignore-aware traversal prevents accidental indexing of secrets and build artifacts.

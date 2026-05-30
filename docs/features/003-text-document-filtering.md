# 003: Text Document Filtering

## Context

Currently the ingest process targets only `.md`, `.rs`, and `.cs` files via hardcoded extension checks. This is inflexible and does not adapt to different projects. Additionally, git-ignored files (secrets, build artifacts, vendored dependencies) are not excluded and may be indexed — leaking secrets and wasting storage.

## Problem

1. File matching is hardcoded to three extensions — users cannot customize what gets indexed.
2. Git-ignored files (secrets, `target/`, `node_modules/`, build artifacts) are not excluded.
3. No mechanism to pass a filter pattern through the API into the job payload.

## Research: Identifying Text Documents

**Recommendation: regex**. Operating systems do not provide a standard cross-platform "is this a text file?" heuristic that is reliable and configurable. The `file` command / `libmagic` can identify MIME types but adds a dependency and is language-agnostic — it cannot match against project-specific patterns (e.g. "only `*.py` files"). A regex pattern is simpler, more explicit, and gives the user full control.

**Git-ignore exclusion**: `git check-ignore` (via `git2` crate or shelling out) can reliably determine whether a file is git-ignored. This is straightforward for projects inside a git repo. For non-git directories, fall back to just the regex filter.

## Required Feature

1. **API change**: `POST /ingest` accepts an optional `file_pattern` field (string regex). If absent, a default pattern is used: `\.(md|rs|cs|txt|py|js|ts|json|yaml|toml)$`.
2. **Payload change**: `IngestFolderPayload` stores the `file_pattern` field (required for internal construction, optional for the HTTP call).
3. **Handler change**: When walking files, the handler matches each filename against the regex before inserting.
4. **Git-ignore exclusion**: Before inserting a matched file, check `git check-ignore` (if inside a git repo). If the file is git-ignored, skip it.
5. **Fallback**: Outside a git repo, apply only the regex filter.

## Required Sub-tasks

- [ ] Add optional `file_pattern` field to `POST /ingest` request body
- [ ] Add `file_pattern` field to `IngestFolderPayload` (required)
- [ ] Add default regex constant in configuration or code
- [ ] Implement regex matching in `IngestFolderHandler`
- [ ] Implement git-ignore check (research `git2` crate vs. shelling out to `git check-ignore`)
- [ ] Handle non-git directories gracefully (skip gitignore check)
- [ ] Update existing tests; add tests for regex matching and gitignore exclusion
- [ ] Update `.env.example` / config docs if needed

## Why Now

Hardcoded extensions are a constant source of friction — every new project type requires a code change. Git-ignore exclusion prevents accidental indexing of secrets and build artifacts, which is a security and correctness concern.

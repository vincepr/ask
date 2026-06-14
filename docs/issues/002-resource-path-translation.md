# Resource Path Translation

## Problem

Stored document paths can expose container-internal paths such as
`/resources/Cargo.toml`. That path is useful inside Docker, but it is often not
the path an MCP, CLI, or API caller expects to see.

## Goal

Return the most useful caller-facing path without changing the internal storage
model.

## Scope

- Keep canonical absolute paths in the database.
- Translate paths at response boundaries where the caller benefits from a
  resource-relative or host-relative path.
- Start with simple configured-prefix replacement based on the existing resource
  directory configuration.
- Avoid inventing a multi-root path registry for this issue.

## Acceptance Criteria

- Search and document-facing responses no longer expose container-only paths
  when a configured resource-root translation can produce a clearer path.
- Internal repository lookups continue to use canonical paths.
- Translation behavior is covered by tests for matching and non-matching paths.

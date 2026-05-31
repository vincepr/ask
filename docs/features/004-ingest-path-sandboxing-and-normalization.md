# 004: Ingest Path Sandboxing and Normalization

## Remaining Problem

The ingest path is now canonicalized, but the server still accepts any readable directory on the
container or host-visible filesystem.

## Done

- The API now canonicalizes the requested ingest directory before queueing work.
- New document rows now store canonical file paths instead of discovered path aliases.
- Path aliases such as `"/tmp/x"` and `"/tmp/./x"` now collapse onto the same queue payload.

## Remaining Evidence

- `crates/ask-server/src/config.rs` defines `resource_dir`, but the ingest route still does not use it
  to constrain requests.

## Why This Is Risky

- Any client that can hit `POST /ingest` can still make the service scan arbitrary readable
  directories.
- Historical non-canonical `documents.filepath` rows from older runs are not normalized automatically.

## Simplest Stable Fix

- Reject paths outside a configured allowed root, most likely `resource_dir`, if you want the API to
  be a narrow managed ingest surface instead of a generic filesystem scanner.
- Decide whether existing non-canonical document rows need a cleanup migration or can be tolerated.

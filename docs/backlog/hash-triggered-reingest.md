# Hash-Triggered Reingest

## Status

Mostly superseded by the current file-hash replanning implementation.

The system already stores a hash of raw file bytes and uses it to detect changed
content during ingest and embed-time recovery. Keep this note only for the
separate idea of an explicit repair or reingest command.

## Backlog Idea

Add a manual mode that scans documents and forces re-planning when stored hashes
or pending embedding rows look inconsistent.

This should not become default ingest behavior unless there is a demonstrated
need. The main design constraint is to keep implementation small and avoid
deleting working embeddings before replacement rows are ready.

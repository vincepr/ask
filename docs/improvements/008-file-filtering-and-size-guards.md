# Ingest Needs File-Type and Size Guards

## Problem

The worker attempts to read every file in the target directory into memory as UTF-8 text.

## Evidence

- `crates/ask-server/src/worker.rs:193-295` iterates every regular file, stores it as a document, and
  then calls `std::fs::read_to_string(&path)`.
- The only rejection path is the UTF-8 decode error after the full file read attempt.

## Why This Is Risky

- Large files can create avoidable memory spikes.
- Binary files are still fully loaded before being rejected for content chunking.
- The service has no explicit supported-file contract, which makes operations less predictable.

## Simplest Stable Fix

- Define a small supported-text-file policy and enforce it up front.
- Add a maximum file size for content ingestion. (otherwise just the first X bytes will get ingested!)
- Read a small prefix first to reject obvious binary content before loading the whole file.
- Keep filename-only indexing as a deliberate fallback rather than an accidental side effect.

## Human review:
- Implement that.
- added line about if > max filesize, just embedd only up to the limit
- limit should probably be const muliplicative of current models configured chunksize? with maybe a uper max (then take the chunksize time X below that). 

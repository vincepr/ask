# 009: Ingestion File Hash Replanning

## Problem

The current ingest path uses filesystem metadata to decide whether a document
changed. That can miss content changes when timestamp and size are preserved, and
the embed worker can later reread different bytes than the bytes used to plan
pending offsets.

## Design

Store a stable hash of the raw file bytes on the `documents` row. Ingest reads
the bytes once, hashes them, decodes UTF-8 only when content chunks can be
planned, and compares the new hash with the stored hash for the same canonical
path.

Schema additions:

- `documents.file_hash TEXT`
- `documents.metadata_json TEXT`

`metadata_json` is debug metadata. The first version stores the selected
strategy and planned chunk count. It does not affect model identity and should
not be used as a durable chunking contract.

## Replanning

When a file hash is unchanged, ingest skips document recalculation. When the hash
changes, ingest updates the document row, replaces pending embedding rows for the
active model, and stores the new metadata.

The embed worker rereads the file before embedding and verifies the current raw
byte hash against `documents.file_hash`.

- Missing file: delete the document row and rely on foreign keys to remove
  embedding rows.
- Hash mismatch: do not embed stale offsets. Replace pending rows by replanning
  from the current file bytes and leave/reseed the embed job for a later pass.
- Hash match: embed the stored pending/stale offsets for the requested
  document/model pair.

This keeps the storage model embedding-row-centric while making offset planning
recoverable and resistant to stale reads.

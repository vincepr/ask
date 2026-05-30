# Rust Review Findings

This directory contains separate review notes for the Rust codebase.

- `001-document-identity-and-upserts.md`: duplicate document rows and non-deterministic lookups
- `002-schema-constraints-and-strict-decoding.md`: weak database invariants and permissive enum decoding
- `003-ingest-path-sandboxing-and-normalization.md`: arbitrary host path ingestion and duplicate queue keys
- `004-job-retry-and-failure-semantics.md`: failed jobs remain poisoned for 24 hours
- `005-document-ingest-should-be-transactional.md`: partial document state can be committed
- `006-embedding-config-validation.md`: invalid numeric config is accepted and later misused
- `007-utf8-safe-chunking.md`: byte-based chunk boundaries are not safe for Unicode text
- `008-file-filtering-and-size-guards.md`: ingest reads unsupported or oversized files too eagerly
- `009-new-model-backfill-is-incomplete.md`: new embedding models only backfill filename chunks

# 008: Ingestion Chunking Interface

## Problem

Ingestion currently plans content chunks with byte-window arithmetic inside the
worker. That keeps the database small, but it couples chunking policy to one job
handler and makes it hard to add safer or document-aware splitting.

## Design

Introduce a shared chunk planner that returns byte spans only. The database
continues to store one row per embedding input and does not store document text,
chunk text, or a separate chunk table.

The initial planner surface is:

- `fixed_utf8`: fixed-size windows with overlap, adjusted to valid UTF-8
  boundaries.
- `structure`: Markdown-like splitting that prefers scored breakpoints near the
  target boundary and falls back to `fixed_utf8`.
- routing: a small selector that chooses a strategy for a document path and text.

The first routing version defaults to `structure` for all UTF-8 documents. The
route is still explicit so code, Markdown, and plain text can diverge later
without changing ingestion or embedding worker control flow.

## Structure Strategy

The structure planner scans possible breakpoints and searches backward from each
target chunk boundary within a bounded window. Candidate score uses the declared
base score and a squared distance decay factor of `0.7`; candidates farther from
the target are penalized more heavily.

Breakpoints:

- H1: 100
- H2: 90
- H3 and code fence markers: 80
- H4: 70
- H5 and horizontal rules: 60
- H6: 50
- blank lines: 20
- list items: 5
- newlines: 1

Breakpoints inside fenced code blocks are ignored. Code fence markers themselves
remain valid boundaries so large prose sections can split before or after code
blocks.

## Constraints

- Returned spans must always be valid byte ranges into the original UTF-8 text.
- Every loop must make forward progress, including when overlap is greater than
  or equal to the chunk size.
- Non-UTF-8 files remain filename-only because chunk spans are defined over
  valid Rust `str` content.
- Planner debug metadata is document metadata, not model identity. Chunking
  strategy/version is not stored in `embedding_models`.

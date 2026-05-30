# Content Chunking Is Not UTF-8 Safe

## Problem

The chunker splits strings by byte offsets and does not align chunk boundaries to UTF-8 character
boundaries.

## Evidence

- `crates/ask-server/src/worker.rs:299-325` computes chunks using `content.len()` and arithmetic on raw
  byte offsets.
- The existing multibyte test in `crates/ask-server/src/worker.rs` only covers one safe case and does
  not exercise a split inside a multibyte scalar.

## Why This Is Risky

- A string like `"éé"` with `chunk_size = 3` produces invalid slice boundaries.
- Any later code that slices `&content[start..end]` with those offsets can panic.
- Even without a panic, byte-oriented chunk boundaries are a poor contract for text embeddings.

## Simplest Stable Fix

- Make chunk boundaries character-safe at minimum.
- Prefer a token-aware or line-aware chunker if the goal is embeddings for human text.
- Add regression tests with multibyte characters where the naive byte split would land inside a scalar.

## Human review:
- REALLY LOW PRIO
- I know about that limitation. Also do we support other encodings than utf-8...
- Research & Brainstorm solutions here and draft possible ways we can handle this. For now we should probably just go the minimum effort way to harden this?
# Text Decoding Beyond UTF-8 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decode common non-UTF-8 text files into content chunks and embeddings while keeping binary
files on the filename-only path.

**Architecture:** Introduce a shared bounded text-decoding helper under the worker module. Ingest
planning and embed-time chunk preparation both call that helper, so supported legacy text encodings
produce the same decoded `String` before chunk slicing. Existing full-file streaming hash behavior
and canonical path storage remain unchanged.

**Tech Stack:** Rust 2024, `encoding_rs`, existing bounded file prefix reads, `sha2`, worker unit
tests, ask-server integration tests.

---

## Findings

- Issue file: `docs/issues/003-text-decoding-beyond-utf8.md`.
- Current bounded ingest code lives in `crates/ask-server/src/worker/ingest.rs`.
- Current embed-time chunk preparation lives in
  `crates/ask-server/src/worker/embed_document.rs`.
- `read_content_prefix_with_budget` currently accepts only UTF-8 text and rejects other encodings.
- `prepare_embedded_chunks` reads the same bounded prefix and slices by stored chunk offsets.
- No SQLite schema change is needed if chunk offsets remain offsets into the decoded `String` used
  by both ingest and embed execution.
- Add `encoding_rs` to keep decoding tables out of the codebase.

## Design Decision

Decode into a normalized Rust `String` before chunk planning, and use the same decoder during
embed-time slicing.

Supported encodings for this issue:

- UTF-8, including UTF-8 with BOM.
- UTF-16LE and UTF-16BE when a BOM is present.
- Windows-1252 as the conservative fallback for non-UTF-8 byte streams that still look like text.

Binary detection remains conservative:

- Reject content with NUL bytes before trying Windows-1252 fallback.
- Reject decoded output with a high ratio of control characters other than tab, carriage return,
  newline, and form feed.
- Reject Windows-1252 output when decoding reports replacements.

Metadata should continue to include existing issue-004 fields and add:

```json
{
  "content_encoding": "utf-8",
  "content_decoded": true
}
```

For filename-only binary fallback:

```json
{
  "content_encoding": null,
  "content_decoded": false
}
```

Keep `content_utf8` for compatibility. For Windows-1252 and UTF-16 content, `content_utf8` should be
`false` while `content_decoded` is `true`.

## File Structure

- Modify `crates/ask-server/Cargo.toml`
  - Add `encoding_rs = "0.8.35"`.
- Create `crates/ask-server/src/worker/text_decode.rs`
  - Own supported text decoding and binary/text heuristics.
- Modify `crates/ask-server/src/worker/mod.rs`
  - Add `mod text_decode;`.
  - Add unit tests for supported decoders.
- Modify `crates/ask-server/src/worker/ingest.rs`
  - Replace UTF-8-only prefix decoding with `text_decode`.
  - Extend metadata with `content_encoding` and `content_decoded`.
- Modify `crates/ask-server/src/worker/embed_document.rs`
  - Continue using `read_content_prefix_with_budget`; it will now return decoded text for supported
    encodings.
- Modify `crates/ask-server/tests/integration.rs`
  - Add ingest and embed regression coverage for UTF-16 BOM and Windows-1252 files.

## Task 1: Add Shared Decoder

**Files:**
- Create: `crates/ask-server/src/worker/text_decode.rs`
- Modify: `crates/ask-server/src/worker/mod.rs`
- Modify: `crates/ask-server/Cargo.toml`

- [ ] **Step 1: Write failing unit tests**

Add `mod text_decode;` in `crates/ask-server/src/worker/mod.rs`, then add tests in the existing
worker test module:

```rust
#[test]
fn text_decode_keeps_utf8_text_unchanged() {
    let decoded = text_decode::decode_supported_text("hello é".as_bytes(), false).unwrap();

    assert_eq!(decoded.text, "hello é");
    assert_eq!(decoded.encoding, "utf-8");
}

#[test]
fn text_decode_supports_utf16le_with_bom() {
    let bytes = [
        0xFF, 0xFE, b'h', 0, b'e', 0, b'l', 0, b'l', 0, b'o', 0,
    ];

    let decoded = text_decode::decode_supported_text(&bytes, false).unwrap();

    assert_eq!(decoded.text, "hello");
    assert_eq!(decoded.encoding, "utf-16le");
}

#[test]
fn text_decode_supports_windows_1252_text() {
    let bytes = [b'c', b'a', b'f', 0xE9];

    let decoded = text_decode::decode_supported_text(&bytes, false).unwrap();

    assert_eq!(decoded.text, "café");
    assert_eq!(decoded.encoding, "windows-1252");
}

#[test]
fn text_decode_rejects_binary_with_nul_bytes() {
    assert!(text_decode::decode_supported_text(b"abc\0def", false).is_none());
}
```

Run:

```bash
cargo test -p ask-server worker::tests::text_decode
```

Expected: fail because `text_decode` does not exist and `encoding_rs` is not yet added.

- [ ] **Step 2: Add dependency and implementation**

Add to `crates/ask-server/Cargo.toml`:

```toml
encoding_rs = "0.8.35"
```

Create `crates/ask-server/src/worker/text_decode.rs`:

```rust
use encoding_rs::{UTF_16BE, UTF_16LE, UTF_8, WINDOWS_1252};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DecodedText {
    pub(super) text: String,
    pub(super) encoding: &'static str,
}

pub(super) fn decode_supported_text(bytes: &[u8], read_truncated: bool) -> Option<DecodedText> {
    if bytes.is_empty() {
        return Some(DecodedText {
            text: String::new(),
            encoding: "utf-8",
        });
    }

    if bytes.iter().any(|byte| *byte == 0) && !has_utf16_bom(bytes) {
        return None;
    }

    if let Some(decoded) = decode_utf_bom(bytes) {
        return text_like(&decoded).then_some(decoded);
    }

    match std::str::from_utf8(bytes) {
        Ok(text) => {
            return text_like_str(text).then(|| DecodedText {
                text: strip_utf8_bom(text).to_string(),
                encoding: "utf-8",
            });
        }
        Err(err) if err.error_len().is_none() && read_truncated => {
            let text = std::str::from_utf8(&bytes[..err.valid_up_to()])
                .expect("valid_up_to must identify valid UTF-8");
            return text_like_str(text).then(|| DecodedText {
                text: strip_utf8_bom(text).to_string(),
                encoding: "utf-8",
            });
        }
        Err(_) => {}
    }

    let (decoded, _, had_errors) = WINDOWS_1252.decode(bytes);
    if had_errors {
        return None;
    }
    let text = decoded.into_owned();
    text_like_str(&text).then_some(DecodedText {
        text,
        encoding: "windows-1252",
    })
}

fn decode_utf_bom(bytes: &[u8]) -> Option<DecodedText> {
    if bytes.starts_with(&[0xEF, 0xBB, 0xBF]) {
        let (decoded, _, had_errors) = UTF_8.decode(&bytes[3..]);
        return (!had_errors).then_some(DecodedText {
            text: decoded.into_owned(),
            encoding: "utf-8",
        });
    }
    if bytes.starts_with(&[0xFF, 0xFE]) {
        let (decoded, _, had_errors) = UTF_16LE.decode(&bytes[2..]);
        return (!had_errors).then_some(DecodedText {
            text: decoded.into_owned(),
            encoding: "utf-16le",
        });
    }
    if bytes.starts_with(&[0xFE, 0xFF]) {
        let (decoded, _, had_errors) = UTF_16BE.decode(&bytes[2..]);
        return (!had_errors).then_some(DecodedText {
            text: decoded.into_owned(),
            encoding: "utf-16be",
        });
    }
    None
}

fn has_utf16_bom(bytes: &[u8]) -> bool {
    bytes.starts_with(&[0xFF, 0xFE]) || bytes.starts_with(&[0xFE, 0xFF])
}

fn strip_utf8_bom(text: &str) -> &str {
    text.strip_prefix('\u{feff}').unwrap_or(text)
}

fn text_like(decoded: &DecodedText) -> bool {
    text_like_str(&decoded.text)
}

fn text_like_str(text: &str) -> bool {
    if text.is_empty() {
        return true;
    }

    let mut total = 0_usize;
    let mut suspicious = 0_usize;
    for ch in text.chars() {
        total += 1;
        if ch.is_control() && !matches!(ch, '\t' | '\n' | '\r' | '\u{000c}') {
            suspicious += 1;
        }
    }

    suspicious * 100 <= total * 5
}
```

Run:

```bash
cargo test -p ask-server worker::tests::text_decode
```

Expected: decoder unit tests pass.

## Task 2: Use Decoder In Bounded Prefix Reads

**Files:**
- Modify: `crates/ask-server/src/worker/ingest.rs`
- Modify: `crates/ask-server/src/worker/mod.rs`

- [ ] **Step 1: Write failing prefix-reader tests**

Add worker tests:

```rust
#[test]
fn bounded_content_prefix_decodes_windows_1252_text() {
    let db = TempDb::new();
    let path = db.dir.join("latin.txt");
    std::fs::write(&path, [b'c', b'a', b'f', 0xE9]).unwrap();

    let prefix = ingest::read_content_prefix_with_budget(&path, 1024).unwrap();

    assert_eq!(prefix.content.as_deref(), Some("café"));
    assert_eq!(prefix.content_encoding, Some("windows-1252"));
    assert!(!prefix.content_utf8);
}

#[test]
fn bounded_content_prefix_decodes_utf16le_bom_text() {
    let db = TempDb::new();
    let path = db.dir.join("utf16.txt");
    std::fs::write(&path, [0xFF, 0xFE, b'h', 0, b'i', 0]).unwrap();

    let prefix = ingest::read_content_prefix_with_budget(&path, 1024).unwrap();

    assert_eq!(prefix.content.as_deref(), Some("hi"));
    assert_eq!(prefix.content_encoding, Some("utf-16le"));
    assert!(!prefix.content_utf8);
}
```

Run:

```bash
cargo test -p ask-server worker::tests::bounded_content_prefix_decodes
```

Expected: fail because `ContentReadPlan` has no `content_encoding` and currently rejects these
inputs.

- [ ] **Step 2: Extend `ContentReadPlan`**

Change `ContentReadPlan` in `ingest.rs`:

```rust
pub(super) struct ContentReadPlan {
    pub(super) content: Option<String>,
    pub(super) content_utf8: bool,
    pub(super) content_encoding: Option<&'static str>,
    pub(super) content_truncated: bool,
    pub(super) content_bytes_indexed: usize,
    pub(super) content_byte_budget: usize,
}
```

Set `content_encoding: None` in `ContentReadPlan::filename_only()` and all binary fallback returns.

- [ ] **Step 3: Replace UTF-8-only decoding**

In `read_content_prefix_with_budget`, replace `decode_bounded_utf8_prefix` with a helper that calls
`text_decode::decode_supported_text(&bytes, read_truncated)`, floors the decoded `String` to
`content_byte_budget`, and returns `content_encoding`.

Use this shape:

```rust
fn decode_bounded_text_prefix(
    bytes: &[u8],
    content_byte_budget: usize,
    read_truncated: bool,
) -> Option<(String, &'static str)> {
    let decoded = super::text_decode::decode_supported_text(bytes, read_truncated)?;
    let end = floor_char_boundary(&decoded.text, decoded.text.len().min(content_byte_budget));
    Some((decoded.text[..end].to_string(), decoded.encoding))
}
```

Then populate:

```rust
let Some((content, content_encoding)) =
    decode_bounded_text_prefix(&bytes, content_byte_budget, read_truncated)
else {
    return Ok(ContentReadPlan {
        content: None,
        content_utf8: false,
        content_encoding: None,
        content_truncated: read_truncated,
        content_bytes_indexed: 0,
        content_byte_budget,
    });
};

let content_utf8 = content_encoding == "utf-8";
```

Run:

```bash
cargo test -p ask-server worker::tests::bounded_content_prefix_decodes
```

Expected: prefix-reader tests pass.

## Task 3: Preserve Metadata Accuracy

**Files:**
- Modify: `crates/ask-server/src/worker/ingest.rs`
- Modify: `crates/ask-server/src/worker/mod.rs`

- [ ] **Step 1: Write failing metadata test**

Update `bounded_content_planner_records_truncation_metadata` so it calls:

```rust
let planned = ingest::plan_pending_embeddings_for_content(
    std::path::Path::new("bounded.txt"),
    Some(content),
    false,
    Some("windows-1252"),
    true,
    content.len(),
    1024,
    &model,
);
```

Then assert:

```rust
assert_eq!(metadata["content_utf8"], false);
assert_eq!(metadata["content_decoded"], true);
assert_eq!(metadata["content_encoding"], "windows-1252");
```

Run:

```bash
cargo test -p ask-server worker::tests::bounded_content_planner_records_truncation_metadata
```

Expected: fail because the planner signature and metadata do not yet include encoding.

- [ ] **Step 2: Update planner metadata**

Change `plan_pending_embeddings_for_read_plan` to pass:

```rust
content_plan.content_utf8,
content_plan.content_encoding,
```

Change `plan_pending_embeddings_for_content` signature to:

```rust
pub(super) fn plan_pending_embeddings_for_content(
    path: &Path,
    content: Option<&str>,
    content_utf8: bool,
    content_encoding: Option<&'static str>,
    content_truncated: bool,
    content_bytes_indexed: usize,
    content_byte_budget: usize,
    model: &EmbeddingModel,
) -> PlannedEmbeddings
```

For `Some(content)`, call metadata with `content_decoded: true`. For `None`, call metadata with
`content_decoded: false` and `content_encoding: None`.

Extend `metadata_json`:

```rust
fn metadata_json(
    strategy: &str,
    chunk_count: usize,
    content_utf8: bool,
    content_decoded: bool,
    content_encoding: Option<&'static str>,
    content_truncated: bool,
    content_bytes_indexed: usize,
    content_byte_budget: usize,
) -> String
```

Emit:

```rust
serde_json::json!({
    "strategy": strategy,
    "planned_chunk_count": chunk_count,
    "content_utf8": content_utf8,
    "content_decoded": content_decoded,
    "content_encoding": content_encoding,
    "content_truncated": content_truncated,
    "content_bytes_indexed": content_bytes_indexed,
    "content_byte_budget": content_byte_budget,
})
```

Run:

```bash
cargo test -p ask-server worker::tests::bounded_content_planner_records_truncation_metadata
```

Expected: metadata test passes.

## Task 4: Integration Coverage For Non-UTF-8 Ingest And Embed

**Files:**
- Modify: `crates/ask-server/tests/integration.rs`

- [ ] **Step 1: Add Windows-1252 ingest regression**

Add near existing ingest content tests:

```rust
#[tokio::test]
async fn ingest_windows_1252_file_gets_content_embeddings() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model(&db.pool(), now, "windows-1252-ingest");

    let dir = db.create_dir("windows_1252");
    std::fs::write(dir.join("legacy.txt"), [b'c', b'a', b'f', 0xE9]).unwrap();
    let payload = ingest_payload(&dir);

    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);
    dispatch_job(&db.pool(), &entry, model_id, test_embedding_client()).unwrap();

    let conn = db.pool().get().unwrap();
    let content_emb: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM document_embeddings WHERE chunk_type = 'content'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let metadata_json: String = conn
        .query_row("SELECT metadata_json FROM documents", [], |row| row.get(0))
        .unwrap();
    let metadata: Value = serde_json::from_str(&metadata_json).unwrap();

    assert!(content_emb > 0);
    assert_eq!(metadata["content_decoded"], true);
    assert_eq!(metadata["content_utf8"], false);
    assert_eq!(metadata["content_encoding"], "windows-1252");
}
```

Run:

```bash
cargo test -p ask-server --test integration ingest_windows_1252_file_gets_content_embeddings
```

Expected: pass after Tasks 1-3.

- [ ] **Step 2: Add embed-time matching regression**

Add:

```rust
#[tokio::test]
async fn embed_document_windows_1252_file_uses_decoded_content_chunk() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model_with_dimensions(&db.pool(), now, "windows-1252-embed", 4);
    let client = Arc::new(DeterministicEmbeddingClient::new());

    let path = db.dir.join("legacy-embed.txt");
    std::fs::write(&path, [b'c', b'a', b'f', 0xE9]).unwrap();
    let doc_id = insert_document(&db.pool(), now, &path);
    let vectors = client.embed(
        &ask_core::models::EmbeddingModel {
            id: model_id,
            name: "windows-1252-embed".to_string(),
            dimensions: 4,
            chunk_size: 512,
            chunk_overlap: 0,
            created_at: now,
        },
        &["café".to_string()],
    ).unwrap();

    let conn = db.pool().get().unwrap();
    insert_embedding_row(&conn, doc_id, model_id, ChunkType::Content, 0..5, EmbedState::Pending, now);
    enqueue_embed_document_job(&conn, doc_id, model_id, now);
    drop(conn);

    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);
    dispatch_job(&db.pool(), &entry, model_id, client).unwrap();

    let conn = db.pool().get().unwrap();
    let stored_embedding: Vec<u8> = conn
        .query_row(
            "SELECT embedding FROM document_embeddings
             WHERE document_id = ?1 AND model_id = ?2 AND chunk_type = 'content'",
            [doc_id, model_id],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(stored_embedding, serialize_embedding(&vectors[0]));
}
```

Run:

```bash
cargo test -p ask-server --test integration embed_document_windows_1252_file_uses_decoded_content_chunk
```

Expected: pass. This confirms ingest and embed agree on decoded chunk slicing.

## Task 5: Binary Fallback And Verification

**Files:**
- Verify: `crates/ask-server/src/worker/text_decode.rs`
- Verify: `crates/ask-server/src/worker/ingest.rs`
- Verify: `crates/ask-server/src/worker/embed_document.rs`
- Verify: `crates/ask-server/tests/integration.rs`

- [ ] **Step 1: Confirm binary fallback still works**

Run:

```bash
cargo test -p ask-server --test integration ingest_non_utf8_file_only_gets_filename_embedding
```

Expected: pass. If the old test name becomes misleading, rename it to
`ingest_binary_file_only_gets_filename_embedding` and keep the same assertions.

- [ ] **Step 2: Run full verification**

Run:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test
```

Expected: all commands exit with status 0.

## Risks And Notes

- Chunk offsets for decoded non-UTF-8 content are offsets into the decoded Rust `String`, not raw
  file byte offsets. This matches the no-schema-change constraint and keeps ingest/embed behavior
  consistent, but `include_location` responses for non-UTF-8 files are decoded-text offsets.
- Windows-1252 is a permissive single-byte encoding, so the text-likeness checks are important.
- UTF-16 without BOM remains filename-only in this issue to avoid unsafe guessing.
- If users need explicit encoding overrides or richer detection, add that as a separate issue.

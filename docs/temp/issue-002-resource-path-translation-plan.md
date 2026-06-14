# Resource Path Translation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Return caller-facing paths for files under either configured container root
(`resource_dir` or `data_dir`) while keeping canonical absolute paths in SQLite.

**Architecture:** Add one small ordered response-boundary path translator in the HTTP layer.
Repository and worker code continue to store and query canonical absolute paths. The `/search`
handler translates the `filepath` field after vector search returns, using canonical `resource_dir`
and `data_dir` roots stored on `AppState`.

**Tech Stack:** Rust 2024, Axum handlers, existing `AppState`, `std::path`, integration tests in
`crates/ask-server/tests/integration.rs`.

---

## Findings

- Issue file: `docs/issues/002-resource-path-translation.md`.
- Stored paths are canonical absolute paths in `documents.filepath`.
- `AppState::new` currently canonicalizes only the configured resource root and exposes it through
  `AppState::resource_root()`.
- `data_dir` exists in `Config` and `RuntimeConfig`, but `AppState` currently does not keep a
  canonical data root.
- Startup currently calls `AppState::new(pool.clone(), &config.resource_dir)?` and then
  `.with_runtime_config(RuntimeConfig::from_config(&config))`.
- Future memory files may live near the SQLite DB under `data_dir`, so response translation must
  include both roots.
- `/search` is the only current document-facing API that returns `filepath` values.
- `/documents/stale` accepts document IDs and does not return document paths.
- Internal lookups such as `repository::find_document_by_path`,
  `repository::search_documents_by_embedding`, and worker replan logic must keep using canonical
  absolute paths.

## Design Decision

Translate only at response boundaries.

For a stored path inside the configured resource root, return `resources/<relative-path>`. For a
stored path inside the configured data root, return `data/<relative-path>`. For a stored path
outside both configured roots, return the stored path unchanged. This meets the issue scope without
adding a multi-root registry, schema migration, or host-path mapping surface.

Use an ordered root list and check longer roots first so overlapping roots behave predictably.

Examples:

```text
resource root: /resources
stored path:   /resources/crates/ask-server/Cargo.toml
response path: resources/crates/ask-server/Cargo.toml

data root:     /data
stored path:   /data/memory/daily.md
response path: data/memory/daily.md

resource root: /resources
data root:     /data
stored path:   /tmp/outside.txt
response path: /tmp/outside.txt
```

On Windows, convert relative response paths to forward-slash form so API callers see stable paths.
Non-matching absolute paths should remain in platform display form.

## File Structure

- Modify `crates/ask-server/src/http.rs`
  - Add `data_root: PathBuf` and an ordered `PathTranslator` to `AppState`.
  - Add `AppState::new_with_data_dir(pool, resource_dir, data_dir)` and keep
    `AppState::new(pool, resource_dir)` as a test/backward-compatible wrapper.
  - Add `AppState::response_filepath(&self, stored_filepath: &str) -> String`.
  - Add unit tests for resource-root matches, data-root matches, overlapping roots, and
    non-matching paths.
- Modify `crates/ask-server/src/bin/ask-server.rs`
  - Construct `AppState` with both configured roots.
- Modify `crates/ask-server/src/http/search.rs`
  - Translate `SearchDocumentResult.filepath` in the handler after `search_documents` returns.
  - Keep `search_documents` and repository calls path-storage agnostic.
- Modify `crates/ask-server/tests/integration.rs`
  - Update existing search path expectations to expect `resources/...` paths.
  - Add a regression test that a stored path under `data_dir` returns `data/...`.
  - Add a regression test that an outside stored path remains unchanged.

## Task 1: Add HTTP Path Translator

**Files:**
- Modify: `crates/ask-server/src/http.rs`

- [ ] **Step 1: Write failing unit tests**

Add tests inside `#[cfg(test)] mod tests` in `crates/ask-server/src/http.rs`:

```rust
#[test]
fn response_filepath_returns_resource_relative_path_for_matching_root() {
    let db = TempHttpDb::new();
    let resource_root = db.dir.join("resources");
    let data_root = db.dir.join("data");
    std::fs::create_dir_all(resource_root.join("nested")).unwrap();
    std::fs::create_dir_all(&data_root).unwrap();
    let state = AppState::new_with_data_dir(db.pool.clone(), &resource_root, &data_root).unwrap();
    let stored = resource_root
        .join("nested")
        .join("notes.md")
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .into_owned();

    assert_eq!(state.response_filepath(&stored), "resources/nested/notes.md");
}

#[test]
fn response_filepath_returns_data_relative_path_for_matching_root() {
    let db = TempHttpDb::new();
    let resource_root = db.dir.join("resources");
    let data_root = db.dir.join("data");
    std::fs::create_dir_all(&resource_root).unwrap();
    std::fs::create_dir_all(data_root.join("memory")).unwrap();
    let state = AppState::new_with_data_dir(db.pool.clone(), &resource_root, &data_root).unwrap();
    let stored = data_root
        .join("memory")
        .join("daily.md")
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .into_owned();

    assert_eq!(state.response_filepath(&stored), "data/memory/daily.md");
}

#[test]
fn response_filepath_prefers_longest_matching_root() {
    let db = TempHttpDb::new();
    let data_root = db.dir.join("data");
    let resource_root = data_root.join("resources");
    std::fs::create_dir_all(resource_root.join("nested")).unwrap();
    let state = AppState::new_with_data_dir(db.pool.clone(), &resource_root, &data_root).unwrap();
    let stored = resource_root
        .join("nested")
        .join("notes.md")
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .into_owned();

    assert_eq!(state.response_filepath(&stored), "resources/nested/notes.md");
}

#[test]
fn response_filepath_leaves_path_outside_configured_roots_unchanged() {
    let db = TempHttpDb::new();
    let resource_root = db.dir.join("resources");
    let data_root = db.dir.join("data");
    let outside_root = db.dir.join("outside");
    std::fs::create_dir_all(&resource_root).unwrap();
    std::fs::create_dir_all(&data_root).unwrap();
    std::fs::create_dir_all(&outside_root).unwrap();
    let outside = outside_root.join("notes.md");
    std::fs::write(&outside, "outside").unwrap();
    let state = AppState::new_with_data_dir(db.pool.clone(), &resource_root, &data_root).unwrap();
    let stored = outside.canonicalize().unwrap().to_string_lossy().into_owned();

    assert_eq!(state.response_filepath(&stored), stored);
}
```

If `TempHttpDb` does not exist, add this focused helper in the same test module:

```rust
struct TempHttpDb {
    dir: PathBuf,
    pool: DbPool,
}

impl TempHttpDb {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!(
            "ask-http-path-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("ask.sqlite3");
        let pool = create_pool(&db_path.to_string_lossy()).unwrap();
        Self { dir, pool }
    }
}

impl Drop for TempHttpDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
```

Run:

```bash
cargo test -p ask-server http::tests::response_filepath
```

Expected: fail because `AppState::new_with_data_dir` and `AppState::response_filepath` do not
exist.

- [ ] **Step 2: Implement the translator**

Add these private types near `AppState` in `crates/ask-server/src/http.rs`:

```rust
#[derive(Clone)]
struct PathTranslator {
    roots: Vec<ResponsePathRoot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResponsePathRoot {
    label: &'static str,
    root: PathBuf,
}
```

Add `data_root` and `path_translator` to `AppState`:

```rust
#[derive(Clone)]
pub struct AppState {
    pool: DbPool,
    resource_root: PathBuf,
    data_root: PathBuf,
    path_translator: PathTranslator,
    embedding_client: SharedEmbeddingClient,
    runtime_config: RuntimeConfig,
}
```

Add a new constructor and keep `new` as a compatibility wrapper:

```rust
pub fn new(pool: DbPool, resource_dir: impl AsRef<Path>) -> std::io::Result<Self> {
    let resource_dir = resource_dir.as_ref();
    Self::new_with_data_dir(pool, resource_dir, resource_dir)
}

pub fn new_with_data_dir(
    pool: DbPool,
    resource_dir: impl AsRef<Path>,
    data_dir: impl AsRef<Path>,
) -> std::io::Result<Self> {
    let resource_root = std::fs::canonicalize(resource_dir)?;
    let data_root = std::fs::canonicalize(data_dir)?;
    let path_translator = PathTranslator::new([
        ResponsePathRoot {
            label: "resources",
            root: resource_root.clone(),
        },
        ResponsePathRoot {
            label: "data",
            root: data_root.clone(),
        },
    ]);

    Ok(Self {
        pool,
        resource_root: resource_root.clone(),
        data_root,
        path_translator,
        embedding_client: Arc::new(DeterministicEmbeddingClient::new()),
        runtime_config: RuntimeConfig {
            data_dir: DEFAULT_DATA_DIR.to_string(),
            database_pool_size: DEFAULT_DATABASE_POOL_SIZE,
            resource_dir: resource_root.display().to_string(),
            embedding_mode: "tei".to_string(),
            embedding_base_url: DEFAULT_TEI_BASE_URL.to_string(),
            embedding_max_batch_size: DEFAULT_EMBEDDING_MAX_BATCH_SIZE,
            embedding_worker_count: DEFAULT_WORKER_COUNT,
        },
    })
}
```

Add a data-root accessor:

```rust
#[must_use]
pub fn data_root(&self) -> &Path {
    &self.data_root
}
```

Add this method to `impl AppState`:

```rust
#[must_use]
pub(crate) fn response_filepath(&self, stored_filepath: &str) -> String {
    self.path_translator.response_filepath(stored_filepath)
}
```

Add the translator implementation near `load_active_model`:

```rust
impl PathTranslator {
    fn new<const N: usize>(roots: [ResponsePathRoot; N]) -> Self {
        let mut roots = roots.into_iter().collect::<Vec<_>>();
        roots.sort_by(|left, right| {
            right
                .root
                .components()
                .count()
                .cmp(&left.root.components().count())
        });
        Self { roots }
    }

    fn response_filepath(&self, stored_filepath: &str) -> String {
        let stored_path = Path::new(stored_filepath);
        for root in &self.roots {
            let Ok(relative) = stored_path.strip_prefix(&root.root) else {
                continue;
            };
            if relative.as_os_str().is_empty() {
                return root.label.to_string();
            }
            return format!(
                "{}/{}",
                root.label,
                normalize_relative_response_path(relative)
            );
        }

        stored_filepath.to_string()
    }
}

fn normalize_relative_response_path(path: &Path) -> String {
    path.components()
        .filter_map(|component| match component {
            std::path::Component::Normal(value) => Some(value.to_string_lossy()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}
```

Run:

```bash
cargo test -p ask-server http::tests::response_filepath
```

Expected: both tests pass.

## Task 2: Wire Startup With Both Roots

**Files:**
- Modify: `crates/ask-server/src/bin/ask-server.rs`

- [ ] **Step 1: Update startup construction**

Change startup from:

```rust
let app_state = http::AppState::new(pool.clone(), &config.resource_dir)?
    .with_runtime_config(runtime_config)
    .with_embedding_client(embedding_client.clone());
```

to:

```rust
let app_state =
    http::AppState::new_with_data_dir(pool.clone(), &config.resource_dir, &config.data_dir)?
        .with_runtime_config(runtime_config)
        .with_embedding_client(embedding_client.clone());
```

Run:

```bash
cargo test -p ask-server --bin ask-server
```

Expected: compile and tests pass. If there are no bin tests, Cargo should still compile the binary
test target successfully.

## Task 3: Translate Search Responses

**Files:**
- Modify: `crates/ask-server/src/http/search.rs`
- Modify: `crates/ask-server/tests/integration.rs`

- [ ] **Step 1: Write failing integration coverage**

Update `search_returns_unique_documents_with_match_score_only_by_default`:

```rust
assert_eq!(results[0]["filepath"], "resources/search-a.txt");
assert_eq!(results[1]["filepath"], "resources/search-b.txt");
```

Add a data-root regression test in the `/search` section:

```rust
#[tokio::test]
async fn search_returns_data_relative_path_for_document_under_data_root() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model_with_dimensions(&db.pool(), now, "search-data-path", 4);
    let client = Arc::new(DeterministicEmbeddingClient::new());

    let data_dir = db.dir.join("data");
    std::fs::create_dir_all(data_dir.join("memory")).unwrap();
    let data_path = data_dir.join("memory").join("daily.md");
    std::fs::write(&data_path, "daily memory").unwrap();
    let state = http::AppState::new_with_data_dir(db.pool().pool().clone(), &db.dir, &data_dir)
        .unwrap()
        .with_embedding_client(client.clone());
    let doc_id = insert_document(&state, now, &data_path);

    let conn = state.pool().get().unwrap();
    let model = repository::find_model_by_id(&conn, model_id).unwrap().unwrap();
    vector_index::ensure_active_search_model(&conn, &model, now).unwrap();
    drop(conn);

    let vectors = client.embed(&model, &["daily-query".to_string()]).unwrap();
    let mut conn = state.pool().get().unwrap();
    repository::replace_embeddings_for_document_model(
        &mut conn,
        doc_id,
        model_id,
        &[ask_core::models::EmbeddedChunk {
            chunk_type: ask_core::types::ChunkType::Filename,
            chunk_start: 0,
            chunk_end: 0,
            embedding: serialize_embedding(&vectors[0]),
        }],
        now,
    )
    .unwrap();
    drop(conn);

    let response = http::router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/search")
                .header("content-type", "application/json")
                .body(json_body(r#"{"query":"daily-query","limit":1}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
    assert_eq!(body.as_array().unwrap()[0]["filepath"], "data/memory/daily.md");
}
```

Add an outside-root regression test in the `/search` section:

```rust
#[tokio::test]
async fn search_leaves_paths_outside_configured_roots_unchanged() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model_with_dimensions(&db.pool(), now, "search-outside-path", 4);
    let client = Arc::new(DeterministicEmbeddingClient::new());

    let outside_dir = unique_temp_dir();
    std::fs::create_dir_all(&outside_dir).unwrap();
    let outside_path = outside_dir.join("outside-search.txt");
    std::fs::write(&outside_path, "outside").unwrap();
    let outside_display = outside_path.canonicalize().unwrap().display().to_string();
    let doc_id = insert_document(&db.pool(), now, &outside_path);

    let conn = db.pool().get().unwrap();
    let model = repository::find_model_by_id(&conn, model_id).unwrap().unwrap();
    vector_index::ensure_active_search_model(&conn, &model, now).unwrap();
    drop(conn);

    let vectors = client.embed(&model, &["outside-query".to_string()]).unwrap();
    let mut conn = db.pool().get().unwrap();
    repository::replace_embeddings_for_document_model(
        &mut conn,
        doc_id,
        model_id,
        &[ask_core::models::EmbeddedChunk {
            chunk_type: ask_core::types::ChunkType::Filename,
            chunk_start: 0,
            chunk_end: 0,
            embedding: serialize_embedding(&vectors[0]),
        }],
        now,
    )
    .unwrap();
    drop(conn);

    let response = db
        .router_with_embedding_client(client)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/search")
                .header("content-type", "application/json")
                .body(json_body(r#"{"query":"outside-query","limit":1}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
    assert_eq!(body.as_array().unwrap()[0]["filepath"], outside_display);

    let _ = std::fs::remove_dir_all(outside_dir);
}
```

Run:

```bash
cargo test -p ask-server --test integration search_returns_unique_documents_with_match_score_only_by_default
```

Expected: fail because `/search` still returns canonical absolute paths.

- [ ] **Step 2: Apply translation in the handler**

In `search`, keep the `spawn_blocking` query unchanged, then map successful responses:

```rust
match outcome {
    Ok(mut response) => {
        for result in &mut response {
            result.filepath = state.response_filepath(&result.filepath);
        }
        Ok(Json(response))
    }
    Err(SearchFailure::BadGateway(message)) => Err(error_response(
        StatusCode::BAD_GATEWAY,
        "bad_gateway",
        message,
    )),
    Err(SearchFailure::Internal(message)) => Err(error_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal_error",
        message,
    )),
}
```

Run:

```bash
cargo test -p ask-server --test integration search_returns_unique_documents_with_match_score_only_by_default
cargo test -p ask-server --test integration search_returns_data_relative_path_for_document_under_data_root
cargo test -p ask-server --test integration search_leaves_paths_outside_configured_roots_unchanged
```

Expected: all three tests pass.

## Task 4: Regression Sweep

**Files:**
- Verify: `crates/ask-server/src/http.rs`
- Verify: `crates/ask-server/src/http/search.rs`
- Verify: `crates/ask-server/tests/integration.rs`

- [ ] **Step 1: Confirm internal storage remains canonical**

Run:

```bash
cargo test -p ask-server --test integration ingest_folder_inserts_documents_and_pending_embeddings
```

Expected: pass. This confirms ingest still writes documents through the existing canonical path
pathway.

- [ ] **Step 2: Run full verification**

Run:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test
```

Expected: all commands exit with status 0.

## Risks And Notes

- This does not add a host-path mapping feature. If Docker mounts `/host/project:/resources`, the
  response becomes `relative/path.ext`, not `/host/project/relative/path.ext`.
- The `filepath` JSON field name stays unchanged for API compatibility, even when the value is
  resource-relative.
- If more document-facing routes are added later, use `AppState::response_filepath` at those
  response boundaries.

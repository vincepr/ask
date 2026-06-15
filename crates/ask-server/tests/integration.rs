use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use ask_core::migrations;
use ask_core::models::{
    DEFAULT_FILE_PATTERN, EmbedDocumentPayload, EmbeddingIdentity, IngestFolderPayload,
};
use ask_core::repository;
use ask_core::types::{ChunkType, DocCategory, EmbedState, JobType};
use ask_server::config::{
    DEFAULT_DATA_DIR, DEFAULT_DATABASE_POOL_SIZE, DEFAULT_EMBEDDING_MAX_BATCH_SIZE,
    DEFAULT_TEI_BASE_URL, DEFAULT_WORKER_COUNT,
};
use ask_server::embeddings::{DeterministicEmbeddingClient, EmbeddingClient};
use ask_server::startup::{StartupSummaryKind, reconcile_embedding_startup};
use ask_server::vector_index;
use ask_server::worker::{backfill_pending_for_model, dispatch_job};
use ask_server::{create_pool, http};
use axum::body::{Body, Bytes, to_bytes};
use axum::http::{Request, StatusCode};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tower::ServiceExt;

static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);
static WORKING_DIRECTORY_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn current_time() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

#[cfg(unix)]
fn create_file_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn create_file_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::windows::fs::symlink_file(target, link)
}

fn register_model(pool: &http::AppState, now: i64, name: &str) -> i64 {
    register_model_with_dimensions(pool, now, name, 768)
}

fn register_model_with_dimensions(
    pool: &http::AppState,
    now: i64,
    name: &str,
    dimensions: i64,
) -> i64 {
    let conn = pool.pool().get().unwrap();
    let model = ask_core::models::EmbeddingModel {
        id: 0,
        name: name.to_string(),
        dimensions,
        chunk_size: 512,
        chunk_overlap: 0,
        created_at: now,
    };
    repository::insert_model(&conn, &model).unwrap()
}

#[test]
fn embedding_identity_changes_create_distinct_model_rows() {
    let db = TempDb::new();
    let conn = db.pool().pool().get().unwrap();
    let now = current_time();

    let first = ask_core::models::EmbeddingModel {
        id: 0,
        name: "shared-model".to_string(),
        dimensions: 1024,
        chunk_size: 512,
        chunk_overlap: 0,
        created_at: now,
    };
    let second = ask_core::models::EmbeddingModel {
        id: 0,
        name: "shared-model".to_string(),
        dimensions: 1024,
        chunk_size: 256,
        chunk_overlap: 0,
        created_at: now + 1,
    };

    repository::insert_model(&conn, &first).unwrap();
    repository::insert_model(&conn, &second).unwrap();

    let found = repository::find_model_by_identity(
        &conn,
        &EmbeddingIdentity {
            name: "shared-model".to_string(),
            dimensions: 1024,
            chunk_size: 256,
            chunk_overlap: 0,
        },
    )
    .unwrap()
    .expect("identity-specific model row must be found");

    assert_eq!(found.chunk_size, 256);
}

fn insert_document(pool: &http::AppState, now: i64, path: &std::path::Path) -> i64 {
    let canonical_path = path.canonicalize().unwrap();
    let metadata = std::fs::metadata(&canonical_path).unwrap();
    let file_modified_at = metadata
        .modified()
        .unwrap()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let file_type = canonical_path
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or("")
        .to_string();
    let raw_bytes = std::fs::read(&canonical_path).unwrap();
    let document = ask_core::models::Document {
        id: 0,
        filepath: canonical_path.to_string_lossy().into_owned(),
        file_type,
        doc_category: DocCategory::Resource,
        file_modified_at,
        file_size: metadata.len() as i64,
        file_hash: hash_bytes(&raw_bytes),
        metadata_json: "{}".to_string(),
        updated_at: now,
    };
    let mut conn = pool.pool().get().unwrap();
    repository::upsert_document(&mut conn, &document).unwrap().0
}

struct TempDb {
    dir: PathBuf,
    pool: Option<http::AppState>,
}

impl TempDb {
    fn new() -> Self {
        let dir = unique_temp_dir();
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("ask.sqlite3");
        let pool = create_pool(&db_path.to_string_lossy()).unwrap();
        let mut conn = pool.get().unwrap();
        migrations::apply_pending_migrations(&mut conn).unwrap();
        let state = http::AppState::new(pool, &dir).unwrap();
        Self {
            dir,
            pool: Some(state),
        }
    }

    fn pool(&self) -> http::AppState {
        self.pool.clone().unwrap()
    }

    fn router(&self) -> axum::Router {
        http::router(self.pool())
    }

    fn router_with_embedding_client(&self, client: Arc<dyn EmbeddingClient>) -> axum::Router {
        http::router(self.pool().with_embedding_client(client))
    }

    fn create_dir(&self, name: &str) -> PathBuf {
        let p = self.dir.join(name);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn create_file(&self, name: &str) -> PathBuf {
        let p = self.dir.join(name);
        std::fs::write(&p, b"content").unwrap();
        p
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        drop(self.pool.take());
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn unique_temp_dir() -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    let counter = TEMP_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("ask-integration-{unique}-{counter}"))
}

fn json_body(text: &str) -> Body {
    Body::from(Bytes::from(text.to_owned()))
}

fn display_path(path: &std::path::Path) -> String {
    path.display()
        .to_string()
        .replace('\\', "/")
        .trim_end_matches('/')
        .to_string()
}

fn ingest_payload(root_path: &std::path::Path) -> String {
    ingest_payload_with_pattern(root_path, DEFAULT_FILE_PATTERN)
}

fn ingest_payload_with_pattern(root_path: &std::path::Path, file_pattern: &str) -> String {
    serde_json::to_string(&IngestFolderPayload {
        root_path: root_path.to_string_lossy().into_owned(),
        file_pattern: file_pattern.to_string(),
    })
    .unwrap()
}

fn count_jobs_by_type(conn: &rusqlite::Connection, job_type: JobType) -> i64 {
    conn.query_row(
        "SELECT COUNT(*) FROM job_queue WHERE job_type = ?1",
        [job_type],
        |row| row.get(0),
    )
    .unwrap()
}

fn run_git(dir: &std::path::Path, args: &[&str]) {
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git command failed: git -C {} {}\nstderr: {}",
        dir.display(),
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn init_git_repo(dir: &std::path::Path) {
    std::fs::create_dir_all(dir).unwrap();
    run_git(dir, &["init", "-q"]);
}

fn stage_git_paths(dir: &std::path::Path, paths: &[&std::path::Path]) {
    let mut command = Command::new("git");
    command.arg("-C").arg(dir).arg("add").arg("--");
    for path in paths {
        command.arg(path);
    }
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "git add failed in {}: {}",
        dir.display(),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn delete_jobs_by_type(conn: &rusqlite::Connection, job_type: JobType) {
    conn.execute("DELETE FROM job_queue WHERE job_type = ?1", [job_type])
        .unwrap();
}

fn test_embedding_client() -> Arc<DeterministicEmbeddingClient> {
    Arc::new(DeterministicEmbeddingClient::new())
}

fn serialize_embedding(vector: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(std::mem::size_of_val(vector));
    for value in vector {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

fn hash_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut hash = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut hash, "{byte:02x}").unwrap();
    }
    hash
}

async fn body_text(response: axum::response::Response) -> String {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

fn embedding_identity(name: &str, dimensions: i64) -> EmbeddingIdentity {
    EmbeddingIdentity {
        name: name.to_string(),
        dimensions,
        chunk_size: 512,
        chunk_overlap: 0,
    }
}

fn insert_embedding_row(
    conn: &rusqlite::Connection,
    document_id: i64,
    model_id: i64,
    chunk_type: ChunkType,
    chunk_range: std::ops::Range<i64>,
    state: EmbedState,
    now: i64,
) {
    conn.execute(
        "INSERT INTO document_embeddings
            (document_id, model_id, chunk_type, chunk_start, chunk_end, state, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        rusqlite::params![
            document_id,
            model_id,
            chunk_type,
            chunk_range.start,
            chunk_range.end,
            state,
            now
        ],
    )
    .unwrap();
}

fn enqueue_embed_document_job(
    conn: &rusqlite::Connection,
    document_id: i64,
    model_id: i64,
    now: i64,
) {
    repository::enqueue_job(
        conn,
        &JobType::EmbedDocument,
        &serde_json::to_string(&EmbedDocumentPayload {
            document_id,
            model_id,
        })
        .unwrap(),
        now,
    )
    .unwrap();
}

fn queued_embed_jobs(conn: &rusqlite::Connection) -> Vec<EmbedDocumentPayload> {
    conn.prepare(
        "SELECT payload
         FROM job_queue
         WHERE job_type = ?1
         ORDER BY payload ASC",
    )
    .unwrap()
    .query_map([JobType::EmbedDocument], |row| {
        let payload: String = row.get(0)?;
        Ok(serde_json::from_str(&payload).expect("embed payload must decode"))
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

fn stored_file_hash(conn: &rusqlite::Connection, filepath: &std::path::Path) -> String {
    conn.query_row(
        "SELECT file_hash FROM documents WHERE filepath = ?1",
        [filepath
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .into_owned()],
        |row| row.get(0),
    )
    .unwrap()
}

// ---------------------------------------------------------------------------
// /health
// ---------------------------------------------------------------------------

#[tokio::test]
async fn health_returns_200() {
    let db = TempDb::new();
    let res = db
        .router()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_text(res).await, r#"{"status":"healthy"}"#);
}

#[tokio::test]
async fn embedding_stats_reports_document_level_counts_and_estimates() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model(&db.pool(), now, "progress-model");
    let conn = db.pool().pool().get().unwrap();
    conn.execute(
        "INSERT INTO embedding_search_state (singleton_id, active_model_id, dimensions, updated_at)
         VALUES (1, ?1, ?2, ?3)",
        rusqlite::params![model_id, 768_i64, now],
    )
    .unwrap();

    let doc_1 = insert_document(&db.pool(), now, &db.create_file("progress-1.md"));
    let txt_path = db.dir.join("progress-2.txt");
    std::fs::write(&txt_path, b"content").unwrap();
    let doc_2 = insert_document(&db.pool(), now, &txt_path);
    let doc_3 = insert_document(&db.pool(), now, &db.create_file("progress-3.md"));
    let doc_4 = insert_document(&db.pool(), now, &db.create_file("progress-4.md"));
    let _doc_5 = insert_document(&db.pool(), now, &db.create_file("progress-5.md"));

    insert_embedding_row(
        &conn,
        doc_1,
        model_id,
        ChunkType::Filename,
        0..0,
        EmbedState::Embedded,
        now - 60,
    );
    insert_embedding_row(
        &conn,
        doc_2,
        model_id,
        ChunkType::Filename,
        0..0,
        EmbedState::Embedded,
        now - 600,
    );
    insert_embedding_row(
        &conn,
        doc_3,
        model_id,
        ChunkType::Filename,
        0..0,
        EmbedState::Pending,
        now - 120,
    );
    insert_embedding_row(
        &conn,
        doc_4,
        model_id,
        ChunkType::Filename,
        0..0,
        EmbedState::Stale,
        now - 180,
    );
    enqueue_embed_document_job(&conn, doc_3, model_id, now - 120);
    conn.execute(
        "UPDATE job_queue
         SET claimed_at = ?1
         WHERE job_type = ?2
           AND payload = ?3",
        rusqlite::params![
            now - 120,
            JobType::EmbedDocument,
            serde_json::to_string(&EmbedDocumentPayload {
                document_id: doc_3,
                model_id,
            })
            .unwrap()
        ],
    )
    .unwrap();

    let response = db
        .router()
        .oneshot(
            Request::builder()
                .uri("/embedding/stats")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
    assert_eq!(body["model"]["name"], "progress-model");
    assert_eq!(body["model"]["id"], model_id);
    assert_eq!(body["model"]["dimensions"], 768);
    assert_eq!(body["model"]["chunk_size"], 512);
    assert_eq!(body["model"]["chunk_overlap"], 0);
    assert_eq!(body["model"]["created_at"], now);
    assert_eq!(body["total_documents"], 5);
    assert_eq!(body["embedded_documents"], 2);
    assert_eq!(body["failed_locked_documents"], 1);
    assert_eq!(body["remaining_documents"], 2);
    assert_eq!(body["documents_embedded_last_five_minutes"], 1);
    assert_eq!(body["estimated_documents_per_hour"], 12.0);
    assert_eq!(body["document_embeddings_total"], 4);
    assert_eq!(body["document_embeddings_embedded"], 2);
    assert_eq!(body["document_embeddings_pending"], 1);
    assert_eq!(body["document_embeddings_stale"], 1);
    assert_eq!(body["config"]["data_dir"], DEFAULT_DATA_DIR);
    assert_eq!(
        body["config"]["resource_dir"],
        db.dir.canonicalize().unwrap().display().to_string()
    );
    assert_eq!(body["config"]["embedding_mode"], "tei");
    assert_eq!(body["config"]["embedding_base_url"], DEFAULT_TEI_BASE_URL);
    assert_eq!(
        body["config"]["embedding_max_batch_size"],
        DEFAULT_EMBEDDING_MAX_BATCH_SIZE
    );
    assert_eq!(
        body["config"]["embedding_worker_count"],
        DEFAULT_WORKER_COUNT
    );
    assert_eq!(
        body["config"]["database_pool_size"],
        DEFAULT_DATABASE_POOL_SIZE
    );
    assert!(body["config"]["embedding_auth_token"].is_null());
    let file_type_counts = body["documents_by_file_type"].as_array().unwrap();
    assert_eq!(file_type_counts.len(), 2);
    assert_eq!(file_type_counts[0]["file_type"], "md");
    assert_eq!(file_type_counts[0]["document_count"], 4);
    assert_eq!(file_type_counts[1]["file_type"], "txt");
    assert_eq!(file_type_counts[1]["document_count"], 1);
    assert!((body["progress_percent"].as_f64().unwrap() - 40.0).abs() < 0.000_001);
    assert!((body["estimated_hours_remaining"].as_f64().unwrap() - (2.0 / 12.0)).abs() < 0.000_001);
}

// ---------------------------------------------------------------------------
// POST /ingest — path validation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ingest_nonexistent_path_returns_404() {
    let db = TempDb::new();
    let res = db
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ingest")
                .header("content-type", "application/json")
                .body(json_body(
                    r#"{"root_path":"/definitely-does-not-exist-12345"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    let body: Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["error"]["code"], "not_found");
}

#[tokio::test]
async fn ingest_file_path_returns_400() {
    let db = TempDb::new();
    let file = db.create_file("not_a_dir");
    let payload = ingest_payload(&file);

    let res = db
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ingest")
                .header("content-type", "application/json")
                .body(json_body(&payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body: Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["error"]["code"], "bad_request");
}

#[tokio::test]
async fn ingest_valid_dir_returns_200() {
    let db = TempDb::new();
    let dir = db.create_dir("my_resources");
    let payload = ingest_payload(&dir);

    let res = db
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ingest")
                .header("content-type", "application/json")
                .body(json_body(&payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    let body: Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["status"], "queued");
    assert_eq!(body["job_type"], "ingest_folder");
}

#[tokio::test]
async fn ingest_git_valid_dir_returns_200() {
    let db = TempDb::new();
    let dir = db.create_dir("git_resources");
    let payload = ingest_payload(&dir);

    let res = db
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ingest/git")
                .header("content-type", "application/json")
                .body(json_body(&payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    let body: Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["status"], "queued");
    assert_eq!(body["job_type"], "ingest_folder_git");
}

#[tokio::test]
async fn ingest_path_outside_resource_root_returns_403() {
    let db = TempDb::new();
    let outside_dir = unique_temp_dir();
    std::fs::create_dir_all(&outside_dir).unwrap();
    let payload = ingest_payload(&outside_dir);

    let res = db
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ingest")
                .header("content-type", "application/json")
                .body(json_body(&payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::FORBIDDEN);
    let body: Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["error"]["code"], "forbidden");

    let conn = db.pool().pool().get().unwrap();
    let queued_jobs: i64 = conn
        .query_row("SELECT COUNT(*) FROM job_queue", [], |row| row.get(0))
        .unwrap();
    assert_eq!(queued_jobs, 0);

    std::fs::remove_dir_all(outside_dir).unwrap();
}

#[tokio::test]
async fn ingest_without_file_pattern_queues_default_pattern() {
    let db = TempDb::new();
    let dir = db.create_dir("default_pattern");
    let payload = ingest_payload(&dir);

    let res = db
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ingest")
                .header("content-type", "application/json")
                .body(json_body(&payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);

    let conn = db.pool().get().unwrap();
    let queued_payload: String = conn
        .query_row("SELECT payload FROM job_queue", [], |row| row.get(0))
        .unwrap();
    let queued_payload: IngestFolderPayload = serde_json::from_str(&queued_payload).unwrap();

    assert_eq!(queued_payload.file_pattern, DEFAULT_FILE_PATTERN);
    assert_eq!(
        queued_payload.root_path,
        dir.canonicalize().unwrap().display().to_string()
    );
}

#[tokio::test]
async fn ingest_with_custom_file_pattern_queues_resolved_pattern() {
    let db = TempDb::new();
    let dir = db.create_dir("custom_pattern");
    let payload = ingest_payload_with_pattern(&dir, r"(?i)^src/.+\.rs$");

    let res = db
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ingest")
                .header("content-type", "application/json")
                .body(json_body(&payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);

    let conn = db.pool().get().unwrap();
    let queued_payload: String = conn
        .query_row("SELECT payload FROM job_queue", [], |row| row.get(0))
        .unwrap();
    let queued_payload: IngestFolderPayload = serde_json::from_str(&queued_payload).unwrap();

    assert_eq!(queued_payload.file_pattern, r"(?i)^src/.+\.rs$");
}

#[tokio::test]
async fn ingest_invalid_file_pattern_returns_400() {
    let db = TempDb::new();
    let dir = db.create_dir("invalid_pattern");
    let payload = ingest_payload_with_pattern(&dir, "[");

    let res = db
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ingest")
                .header("content-type", "application/json")
                .body(json_body(&payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body: Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["error"]["code"], "bad_request");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("invalid file_pattern regex")
    );
}

// ---------------------------------------------------------------------------
// POST /ingest — duplicate rejection (UPSERT WHERE fix)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ingest_duplicate_path_returns_409() {
    let db = TempDb::new();
    let dir = db.create_dir("docs");
    let payload = ingest_payload(&dir);

    let res1 = db
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ingest")
                .header("content-type", "application/json")
                .body(json_body(&payload))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res1.status(), StatusCode::OK);

    let res2 = db
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ingest")
                .header("content-type", "application/json")
                .body(json_body(&payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res2.status(), StatusCode::CONFLICT);
    let body: Value = serde_json::from_str(&body_text(res2).await).unwrap();
    assert_eq!(body["error"]["code"], "conflict");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("already queued or in progress")
    );
}

#[tokio::test]
async fn ingest_git_duplicate_path_returns_409() {
    let db = TempDb::new();
    let dir = db.create_dir("git_docs");
    let payload = ingest_payload(&dir);

    let res1 = db
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ingest/git")
                .header("content-type", "application/json")
                .body(json_body(&payload))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res1.status(), StatusCode::OK);

    let res2 = db
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ingest/git")
                .header("content-type", "application/json")
                .body(json_body(&payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res2.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn ingest_and_ingest_git_same_payload_queue_independently() {
    let db = TempDb::new();
    let dir = db.create_dir("split_strategy");
    let payload = ingest_payload(&dir);

    let ingest_res = db
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ingest")
                .header("content-type", "application/json")
                .body(json_body(&payload))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ingest_res.status(), StatusCode::OK);

    let git_res = db
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ingest/git")
                .header("content-type", "application/json")
                .body(json_body(&payload))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(git_res.status(), StatusCode::OK);

    let conn = db.pool().get().unwrap();
    assert_eq!(count_jobs_by_type(&conn, JobType::IngestFolder), 1);
    assert_eq!(count_jobs_by_type(&conn, JobType::IngestFolderGit), 1);
}

#[tokio::test]
async fn ingest_canonicalized_duplicate_path_returns_409() {
    let db = TempDb::new();
    let dir = db.create_dir("canonical_docs");
    let canonical_payload = ingest_payload(&dir);
    let aliased_payload = ingest_payload(&dir.join("."));

    let res1 = db
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ingest")
                .header("content-type", "application/json")
                .body(json_body(&canonical_payload))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res1.status(), StatusCode::OK);

    let res2 = db
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ingest")
                .header("content-type", "application/json")
                .body(json_body(&aliased_payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res2.status(), StatusCode::CONFLICT);
    let body: Value = serde_json::from_str(&body_text(res2).await).unwrap();
    assert_eq!(body["error"]["code"], "conflict");
}

#[tokio::test]
async fn ingest_different_dirs_both_succeed() {
    let db = TempDb::new();
    let dir_a = db.create_dir("a");
    let dir_b = db.create_dir("b");

    let payload_a = ingest_payload(&dir_a);
    let payload_b = ingest_payload(&dir_b);

    let res1 = db
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ingest")
                .header("content-type", "application/json")
                .body(json_body(&payload_a))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res1.status(), StatusCode::OK);

    let res2 = db
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ingest")
                .header("content-type", "application/json")
                .body(json_body(&payload_b))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res2.status(), StatusCode::OK);
}

// ---------------------------------------------------------------------------
// dispatch_job — documents are actually inserted
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ingest_folder_inserts_documents_and_pending_embeddings() {
    let db = TempDb::new();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    // Register a model (same as main.rs does at startup).
    let model = {
        let conn = db.pool().get().unwrap();
        let m = ask_core::models::EmbeddingModel {
            id: 0,
            name: "test-model".to_string(),
            dimensions: 768,
            chunk_size: 10,
            chunk_overlap: 2,
            created_at: now,
        };
        let model_id = repository::insert_model(&conn, &m).unwrap();
        ask_core::models::EmbeddingModel { id: model_id, ..m }
    };

    // Create a directory with text files and a binary file.
    let ingest_dir = db.create_dir("ingest_me");
    let txt_path = ingest_dir.join("hello.txt");
    let rs_path = ingest_dir.join("main.rs");
    let bin_path = ingest_dir.join("data.bin");
    std::fs::write(&txt_path, "Hello, World!").unwrap();
    std::fs::write(&rs_path, "fn main() { println!(\"hi\"); }").unwrap();
    // Binary file: non-UTF8 bytes.
    std::fs::write(&bin_path, [0x00, 0xFF, 0xFE, 0x7F]).unwrap();

    // Enqueue an IngestFolder job.
    let payload_json = ingest_payload(&ingest_dir);
    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload_json, now).unwrap();

    // Claim the job.
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1)
        .unwrap()
        .expect("job should be claimable");
    drop(conn);

    // Process it.
    let pool = db.pool();
    dispatch_job(&pool, &entry, model.id, test_embedding_client()).unwrap();

    // Verify: documents were inserted.
    let conn = db.pool().get().unwrap();
    let doc_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM documents", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        doc_count, 3,
        "all three files should be inserted as documents"
    );

    // Verify: each document has a filename embedding (pending).
    let pending_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM document_embeddings WHERE state = 'pending'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    // 3 files × 1 filename embedding each = 3 minimum.
    assert!(
        pending_count >= 3,
        "expected at least 3 pending embeddings, got {pending_count}"
    );

    // Verify: text files also have content chunk embeddings.
    let content_embeddings: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM document_embeddings WHERE chunk_type = 'content'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    // hello.txt (13 bytes, chunk_size=10, overlap=2, step=8) → 2 chunks
    // main.rs (28 bytes, same params) → 4 chunks
    // data.bin (not valid UTF-8) → 0 chunks
    // Total: 6 content chunks
    assert_eq!(
        content_embeddings, 6,
        "expected 6 content chunks (hello.txt:2 + main.rs:4)"
    );

    assert_eq!(count_jobs_by_type(&conn, JobType::IngestFolder), 0);
    assert_eq!(count_jobs_by_type(&conn, JobType::EmbedDocument), 3);
}

#[tokio::test]
async fn ingest_folder_recursively_inserts_nested_documents() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model(&db.pool(), now, "recursive");

    let ingest_dir = db.create_dir("recursive");
    let nested_dir = ingest_dir.join("src").join("deeper");
    std::fs::create_dir_all(&nested_dir).unwrap();
    let root_file = ingest_dir.join("root.txt");
    let nested_file = nested_dir.join("nested.txt");
    std::fs::write(&root_file, "root content").unwrap();
    std::fs::write(&nested_file, "nested content").unwrap();

    let payload_json = ingest_payload(&ingest_dir);
    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload_json, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);

    dispatch_job(&db.pool(), &entry, model_id, test_embedding_client()).unwrap();

    let conn = db.pool().get().unwrap();
    let doc_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM documents", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        doc_count, 2,
        "root and nested files should both be ingested"
    );

    for path in [&root_file, &nested_file] {
        let canonical_path = path.canonicalize().unwrap().to_string_lossy().into_owned();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM documents WHERE filepath = ?1",
                [canonical_path],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "document should exist for {}", path.display());
    }
}

#[tokio::test]
async fn ingest_folder_filters_files_by_normalized_relative_path() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model(&db.pool(), now, "path_filter");

    let ingest_dir = db.create_dir("path_filter");
    let src_dir = ingest_dir.join("src");
    let docs_dir = ingest_dir.join("docs");
    std::fs::create_dir_all(&src_dir).unwrap();
    std::fs::create_dir_all(&docs_dir).unwrap();

    let keep_rs = src_dir.join("main.rs");
    let skip_md = src_dir.join("notes.md");
    let skip_txt = docs_dir.join("guide.txt");
    std::fs::write(&keep_rs, "fn main() {}\n").unwrap();
    std::fs::write(&skip_md, "# notes\n").unwrap();
    std::fs::write(&skip_txt, "guide\n").unwrap();

    let payload_json = ingest_payload_with_pattern(&ingest_dir, r"^src/.+\.rs$");
    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload_json, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);

    dispatch_job(&db.pool(), &entry, model_id, test_embedding_client()).unwrap();

    let conn = db.pool().get().unwrap();
    let ingested_paths: Vec<String> = {
        let mut stmt = conn
            .prepare("SELECT filepath FROM documents ORDER BY filepath")
            .unwrap();
        stmt.query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<String>, _>>()
            .unwrap()
    };

    assert_eq!(
        ingested_paths,
        vec![keep_rs.canonicalize().unwrap().display().to_string()]
    );
}

#[tokio::test]
async fn ingest_folder_ignore_files_and_regex_filter_compose() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model(&db.pool(), now, "ignore_compose");

    let ingest_dir = db.create_dir("ignore_compose");
    let included_dir = ingest_dir.join("docs");
    std::fs::create_dir_all(&included_dir).unwrap();
    std::fs::write(ingest_dir.join(".gitignore"), "ignored/\n*.tmp\n").unwrap();
    std::fs::create_dir_all(ingest_dir.join("ignored")).unwrap();

    let keep_file = included_dir.join("keep.txt");
    let ignored_dir_file = ingest_dir.join("ignored").join("skip.txt");
    let ignored_pattern_file = included_dir.join("skip.tmp");
    let regex_excluded_file = ingest_dir.join("root.txt");
    std::fs::write(&keep_file, "keep\n").unwrap();
    std::fs::write(&ignored_dir_file, "skip\n").unwrap();
    std::fs::write(&ignored_pattern_file, "skip\n").unwrap();
    std::fs::write(&regex_excluded_file, "skip\n").unwrap();

    let payload_json = ingest_payload_with_pattern(&ingest_dir, r"^docs/.+\.txt$");
    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload_json, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);

    dispatch_job(&db.pool(), &entry, model_id, test_embedding_client()).unwrap();

    let conn = db.pool().get().unwrap();
    let ingested_paths: Vec<String> = {
        let mut stmt = conn
            .prepare("SELECT filepath FROM documents ORDER BY filepath")
            .unwrap();
        stmt.query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<String>, _>>()
            .unwrap()
    };

    assert_eq!(
        ingested_paths,
        vec![keep_file.canonicalize().unwrap().display().to_string()]
    );
}

#[tokio::test]
async fn ingest_folder_git_only_ingests_tracked_files() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model(&db.pool(), now, "git_tracked");

    let ingest_dir = db.create_dir("git_tracked");
    init_git_repo(&ingest_dir);

    let tracked_file = ingest_dir.join("src").join("tracked.rs");
    let untracked_file = ingest_dir.join("notes.txt");
    std::fs::create_dir_all(tracked_file.parent().unwrap()).unwrap();
    std::fs::write(&tracked_file, "fn tracked() {}\n").unwrap();
    std::fs::write(&untracked_file, "local notes\n").unwrap();
    stage_git_paths(&ingest_dir, &[tracked_file.as_path()]);

    let payload_json = ingest_payload(&ingest_dir);
    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolderGit, &payload_json, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);

    dispatch_job(&db.pool(), &entry, model_id, test_embedding_client()).unwrap();

    let conn = db.pool().get().unwrap();
    let ingested_paths: Vec<String> = {
        let mut stmt = conn
            .prepare("SELECT filepath FROM documents ORDER BY filepath")
            .unwrap();
        stmt.query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<String>, _>>()
            .unwrap()
    };

    assert_eq!(
        ingested_paths,
        vec![tracked_file.canonicalize().unwrap().display().to_string()]
    );
}

#[tokio::test]
async fn ingest_folder_git_uses_enclosing_repo_for_subdirectory_roots() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model(&db.pool(), now, "git_subdir");

    let repo_root = db.create_dir("git_subdir_repo");
    init_git_repo(&repo_root);

    let request_root = repo_root.join("services").join("pricing");
    let tracked_inside = request_root.join("tracked.rs");
    let tracked_outside = repo_root.join("README.md");
    let untracked_inside = request_root.join("notes.txt");
    std::fs::create_dir_all(&request_root).unwrap();
    std::fs::write(&tracked_inside, "fn tracked() {}\n").unwrap();
    std::fs::write(&tracked_outside, "root readme\n").unwrap();
    std::fs::write(&untracked_inside, "draft\n").unwrap();
    stage_git_paths(
        &repo_root,
        &[tracked_inside.as_path(), tracked_outside.as_path()],
    );

    let payload_json = ingest_payload(&request_root);
    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolderGit, &payload_json, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);

    dispatch_job(&db.pool(), &entry, model_id, test_embedding_client()).unwrap();

    let conn = db.pool().get().unwrap();
    let ingested_paths: Vec<String> = {
        let mut stmt = conn
            .prepare("SELECT filepath FROM documents ORDER BY filepath")
            .unwrap();
        stmt.query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<String>, _>>()
            .unwrap()
    };

    assert_eq!(
        ingested_paths,
        vec![tracked_inside.canonicalize().unwrap().display().to_string()]
    );
}

#[tokio::test]
async fn ingest_folder_git_discovers_nested_repos_and_falls_back_for_plain_dirs() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model(&db.pool(), now, "git_nested");

    let root = db.create_dir("company_name");
    let repo_one = root.join("library_name").join("domain").join("prices");
    let repo_two = root
        .join("libaray_name")
        .join("non-production")
        .join("somedeploymentrepo");
    let plain_dir = root.join("notes");
    init_git_repo(&repo_one);
    init_git_repo(&repo_two);
    std::fs::create_dir_all(&plain_dir).unwrap();

    let repo_one_file = repo_one.join("src").join("main.rs");
    let repo_two_file = repo_two.join("deploy.yaml");
    let plain_file = plain_dir.join("todo.txt");
    std::fs::create_dir_all(repo_one_file.parent().unwrap()).unwrap();
    std::fs::write(&repo_one_file, "fn main() {}\n").unwrap();
    std::fs::write(&repo_two_file, "service: deploy\n").unwrap();
    std::fs::write(&plain_file, "plain fallback\n").unwrap();
    stage_git_paths(&repo_one, &[repo_one_file.as_path()]);
    stage_git_paths(&repo_two, &[repo_two_file.as_path()]);

    let payload_json = ingest_payload(&root);
    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolderGit, &payload_json, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);

    dispatch_job(&db.pool(), &entry, model_id, test_embedding_client()).unwrap();

    let conn = db.pool().get().unwrap();
    let ingested_paths: Vec<String> = {
        let mut stmt = conn
            .prepare("SELECT filepath FROM documents ORDER BY filepath")
            .unwrap();
        stmt.query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<String>, _>>()
            .unwrap()
    };

    assert_eq!(
        ingested_paths,
        vec![
            repo_two_file.canonicalize().unwrap().display().to_string(),
            repo_one_file.canonicalize().unwrap().display().to_string(),
            plain_file.canonicalize().unwrap().display().to_string(),
        ]
    );
}

#[tokio::test]
async fn ingest_folder_git_git_command_failure_keeps_job_claimed() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model(&db.pool(), now, "git_failure");

    let broken_repo = db.create_dir("broken_git_repo");
    let fake_git_dir = broken_repo.join(".git");
    std::fs::create_dir_all(&fake_git_dir).unwrap();
    std::fs::write(fake_git_dir.join("HEAD"), "ref: refs/heads/main\n").unwrap();
    std::fs::write(broken_repo.join("tracked.txt"), "content\n").unwrap();

    let payload_json = ingest_payload(&broken_repo);
    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolderGit, &payload_json, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);

    let err = dispatch_job(&db.pool(), &entry, model_id, test_embedding_client()).unwrap_err();
    let err_text = format!("{err:#}");
    assert!(
        err_text.contains("git ls-files failed"),
        "unexpected error: {err:#}"
    );

    let conn = db.pool().get().unwrap();
    let claimed_at: Option<i64> = conn
        .query_row(
            "SELECT claimed_at FROM job_queue WHERE id = ?1",
            [entry.id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(claimed_at, Some(now + 1));

    let document_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM documents", [], |row| row.get(0))
        .unwrap();
    assert_eq!(document_count, 0);
}

#[tokio::test]
async fn ingest_folder_git_skips_submodule_directories() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model(&db.pool(), now, "git_submodule");

    let root = db.create_dir("git_submodule");
    let plain_file = root.join("README.md");
    let submodule_dir = root.join("vendor").join("child");
    std::fs::create_dir_all(&submodule_dir).unwrap();
    std::fs::create_dir_all(root.join(".git").join("modules").join("child")).unwrap();
    std::fs::write(&plain_file, "hello\n").unwrap();
    std::fs::write(
        submodule_dir.join(".git"),
        "gitdir: ../../.git/modules/child\n",
    )
    .unwrap();
    std::fs::write(submodule_dir.join("secret.txt"), "should stay skipped\n").unwrap();

    let payload_json = ingest_payload(&root);
    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolderGit, &payload_json, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);

    dispatch_job(&db.pool(), &entry, model_id, test_embedding_client()).unwrap();

    let conn = db.pool().get().unwrap();
    let ingested_paths: Vec<String> = {
        let mut stmt = conn
            .prepare("SELECT filepath FROM documents ORDER BY filepath")
            .unwrap();
        stmt.query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<String>, _>>()
            .unwrap()
    };

    assert_eq!(
        ingested_paths,
        vec![plain_file.canonicalize().unwrap().display().to_string()]
    );
}

#[tokio::test]
async fn ingest_folder_git_applies_builtin_extension_denylist() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model(&db.pool(), now, "git_denylist");

    let ingest_dir = db.create_dir("git_denylist");
    init_git_repo(&ingest_dir);

    let keep_file = ingest_dir.join("notes.txt");
    let skip_file = ingest_dir.join("image.png");
    std::fs::write(&keep_file, "hello\n").unwrap();
    std::fs::write(&skip_file, [0x89, b'P', b'N', b'G']).unwrap();
    stage_git_paths(&ingest_dir, &[keep_file.as_path(), skip_file.as_path()]);

    let payload_json = ingest_payload(&ingest_dir);
    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolderGit, &payload_json, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);

    dispatch_job(&db.pool(), &entry, model_id, test_embedding_client()).unwrap();

    let conn = db.pool().get().unwrap();
    let ingested_paths: Vec<String> = {
        let mut stmt = conn
            .prepare("SELECT filepath FROM documents ORDER BY filepath")
            .unwrap();
        stmt.query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<String>, _>>()
            .unwrap()
    };

    assert_eq!(
        ingested_paths,
        vec![keep_file.canonicalize().unwrap().display().to_string()]
    );
}

#[tokio::test]
async fn ingest_folder_skips_unchanged_files() {
    let db = TempDb::new();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    // Register a model.
    {
        let conn = db.pool().get().unwrap();
        let m = ask_core::models::EmbeddingModel {
            id: 0,
            name: "test-model".to_string(),
            dimensions: 768,
            chunk_size: 512,
            chunk_overlap: 0,
            created_at: now,
        };
        repository::insert_model(&conn, &m).unwrap();
    }

    let ingest_dir = db.create_dir("stable");
    let file_path = ingest_dir.join("stable.txt");
    std::fs::write(&file_path, "content").unwrap();

    // First ingest.
    let payload_json = ingest_payload(&ingest_dir);
    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload_json, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);

    let model_id = 1;
    let pool = db.pool();
    dispatch_job(&pool, &entry, model_id, test_embedding_client()).unwrap();

    // Doc count after first ingest.
    let conn = db.pool().get().unwrap();
    let doc_count_1: i64 = conn
        .query_row("SELECT COUNT(*) FROM documents", [], |r| r.get(0))
        .unwrap();
    assert_eq!(doc_count_1, 1);
    delete_jobs_by_type(&conn, JobType::EmbedDocument);

    // Second ingest — same files, unchanged.
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload_json, now + 10).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry2 = repository::claim_job(&mut conn, now + 11).unwrap().unwrap();
    drop(conn);
    dispatch_job(&pool, &entry2, model_id, test_embedding_client()).unwrap();

    // Doc count should NOT have increased.
    let conn = db.pool().get().unwrap();
    let doc_count_2: i64 = conn
        .query_row("SELECT COUNT(*) FROM documents", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        doc_count_2, 1,
        "unchanged files should not create duplicate documents"
    );
    assert_eq!(
        count_jobs_by_type(&conn, JobType::EmbedDocument),
        0,
        "unchanged hashes should not requeue embeddings"
    );
    let file_hash = stored_file_hash(&conn, &file_path);
    assert!(!file_hash.is_empty(), "ingest should store a file hash");
}

#[tokio::test]
async fn ingest_folder_updates_existing_document_when_file_changes() {
    let db = TempDb::new();
    let now = current_time();

    {
        let conn = db.pool().get().unwrap();
        let model = ask_core::models::EmbeddingModel {
            id: 0,
            name: "test-model".to_string(),
            dimensions: 768,
            chunk_size: 512,
            chunk_overlap: 0,
            created_at: now,
        };
        repository::insert_model(&conn, &model).unwrap();
    }

    let ingest_dir = db.create_dir("changed");
    let file_path = ingest_dir.join("changed.txt");
    std::fs::write(&file_path, "alpha").unwrap();

    let payload_json = ingest_payload(&ingest_dir);
    let pool = db.pool();

    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload_json, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);
    dispatch_job(&pool, &entry, 1, test_embedding_client()).unwrap();

    let conn = db.pool().get().unwrap();
    let first_doc_id: i64 = conn
        .query_row("SELECT id FROM documents", [], |row| row.get(0))
        .unwrap();
    conn.execute(
        "UPDATE document_embeddings
         SET state = 'embedded', embedding = X'01'
         WHERE document_id = ?1",
        [first_doc_id],
    )
    .unwrap();
    delete_jobs_by_type(&conn, JobType::EmbedDocument);
    drop(conn);

    std::thread::sleep(std::time::Duration::from_secs(1));
    std::fs::write(&file_path, "alpha beta gamma").unwrap();

    let later = current_time();
    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload_json, later).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, later + 1)
        .unwrap()
        .unwrap();
    drop(conn);
    dispatch_job(&pool, &entry, 1, test_embedding_client()).unwrap();

    let conn = db.pool().get().unwrap();
    let row_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM documents", [], |row| row.get(0))
        .unwrap();
    assert_eq!(row_count, 1, "changed files should update the existing row");

    let (doc_id, file_size, updated_at): (i64, i64, i64) = conn
        .query_row(
            "SELECT id, file_size, updated_at FROM documents WHERE filepath = ?1",
            [file_path
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .into_owned()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(doc_id, first_doc_id, "the same document row must be reused");
    assert_eq!(file_size, 16, "the updated file size must be stored");
    assert!(
        updated_at >= later,
        "updated_at should move forward on change"
    );

    let pending_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM document_embeddings
             WHERE document_id = ?1 AND state = 'pending'",
            [doc_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        pending_count, 2,
        "changed files should queue fresh embeddings"
    );
}

#[tokio::test]
async fn ingest_folder_replans_when_same_size_file_hash_changes() {
    let db = TempDb::new();
    let now = current_time();

    let conn = db.pool().get().unwrap();
    let model = ask_core::models::EmbeddingModel {
        id: 0,
        name: "hash-change".to_string(),
        dimensions: 768,
        chunk_size: 3,
        chunk_overlap: 0,
        created_at: now,
    };
    let model_id = repository::insert_model(&conn, &model).unwrap();
    drop(conn);

    let ingest_dir = db.create_dir("same-size-hash");
    let file_path = ingest_dir.join("changed.txt");
    std::fs::write(&file_path, "alpha").unwrap();
    let payload_json = ingest_payload(&ingest_dir);

    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload_json, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);
    dispatch_job(&db.pool(), &entry, model_id, test_embedding_client()).unwrap();

    let conn = db.pool().get().unwrap();
    let first_hash = stored_file_hash(&conn, &file_path);
    let document_id: i64 = conn
        .query_row("SELECT id FROM documents", [], |row| row.get(0))
        .unwrap();
    conn.execute(
        "UPDATE document_embeddings
         SET state = 'embedded', embedding = zeroblob(3072)
         WHERE document_id = ?1",
        [document_id],
    )
    .unwrap();
    delete_jobs_by_type(&conn, JobType::EmbedDocument);
    drop(conn);

    std::fs::write(&file_path, "bravo").unwrap();
    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload_json, now + 2).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 3).unwrap().unwrap();
    drop(conn);
    dispatch_job(&db.pool(), &entry, model_id, test_embedding_client()).unwrap();

    let conn = db.pool().get().unwrap();
    let second_hash = stored_file_hash(&conn, &file_path);
    assert_ne!(
        first_hash, second_hash,
        "same-size content change must update hash"
    );

    let states: Vec<String> = conn
        .prepare(
            "SELECT state
             FROM document_embeddings
             WHERE document_id = ?1
             ORDER BY chunk_type, chunk_start",
        )
        .unwrap()
        .query_map([document_id], |row| row.get(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        states.iter().all(|state| state == "pending"),
        "hash changes should replace old embedded rows with pending rows"
    );
    assert_eq!(count_jobs_by_type(&conn, JobType::EmbedDocument), 1);
}

#[tokio::test]
async fn ingest_folder_path_variants_reuse_same_document_row() {
    let db = TempDb::new();
    let now = current_time();

    {
        let conn = db.pool().get().unwrap();
        let model = ask_core::models::EmbeddingModel {
            id: 0,
            name: "test-model".to_string(),
            dimensions: 768,
            chunk_size: 512,
            chunk_overlap: 0,
            created_at: now,
        };
        repository::insert_model(&conn, &model).unwrap();
    }

    let ingest_dir = db.create_dir("variant");
    let file_path = ingest_dir.join("variant.txt");
    std::fs::write(&file_path, "content").unwrap();

    let first_payload = ingest_payload(&ingest_dir);
    let second_root = ingest_dir.join("..").join("variant");
    let second_payload = ingest_payload(&second_root);
    let pool = db.pool();

    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &first_payload, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);
    dispatch_job(&pool, &entry, 1, test_embedding_client()).unwrap();

    let later = current_time();
    let conn = db.pool().get().unwrap();
    delete_jobs_by_type(&conn, JobType::EmbedDocument);
    repository::enqueue_job(&conn, &JobType::IngestFolder, &second_payload, later).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, later + 1)
        .unwrap()
        .unwrap();
    drop(conn);
    dispatch_job(&pool, &entry, 1, test_embedding_client()).unwrap();

    let conn = db.pool().get().unwrap();
    let row_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM documents", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        row_count, 1,
        "path variants should map to one canonical document"
    );
}

#[tokio::test]
async fn ingest_folder_empty_dir_completes_job() {
    let db = TempDb::new();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    // Register a model.
    {
        let conn = db.pool().get().unwrap();
        let m = ask_core::models::EmbeddingModel {
            id: 0,
            name: "test-model".to_string(),
            dimensions: 768,
            chunk_size: 512,
            chunk_overlap: 0,
            created_at: now,
        };
        repository::insert_model(&conn, &m).unwrap();
    }

    let empty_dir = db.create_dir("empty");
    let payload_json = ingest_payload(&empty_dir);
    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload_json, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);

    let pool = db.pool();
    dispatch_job(&pool, &entry, 1, test_embedding_client()).unwrap();

    // No documents, no errors, job completed.
    let conn = db.pool().get().unwrap();
    let doc_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM documents", [], |r| r.get(0))
        .unwrap();
    assert_eq!(doc_count, 0);
    let job_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM job_queue", [], |r| r.get(0))
        .unwrap();
    assert_eq!(job_count, 0);
}

#[tokio::test]
async fn ingest_folder_nonexistent_path_completes_job() {
    let db = TempDb::new();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    let payload_json = r#"{"root_path":"/tmp/ask-nonexistent-12345-unlikely","file_pattern":".*"}"#;
    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, payload_json, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);

    let pool = db.pool();
    dispatch_job(&pool, &entry, 1, test_embedding_client()).unwrap();

    let conn = db.pool().get().unwrap();
    let job_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM job_queue", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        job_count, 0,
        "nonexistent path should still complete the job"
    );
}

// ---------------------------------------------------------------------------
// dispatch_job - worker regression coverage
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dispatch_job_missing_model_returns_error_and_keeps_job_claimed() {
    let db = TempDb::new();
    let now = current_time();
    let dir = db.create_dir("missing_model");
    std::fs::write(dir.join("a.txt"), "content").unwrap();
    let payload = ingest_payload(&dir);

    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);

    let pool = db.pool();
    let err = dispatch_job(&pool, &entry, 999, test_embedding_client()).unwrap_err();
    let err_text = format!("{err:#}");
    assert!(
        err_text.contains("embedding model 999 not found"),
        "unexpected error: {err:#}"
    );

    let conn = db.pool().get().unwrap();
    let job_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM job_queue", [], |r| r.get(0))
        .unwrap();
    assert_eq!(job_count, 1, "failed jobs should stay queued until stale");

    let claimed_at: Option<i64> = conn
        .query_row(
            "SELECT claimed_at FROM job_queue WHERE id = ?1",
            [entry.id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(claimed_at, Some(now + 1));
}

#[tokio::test]
async fn multiple_ingest_jobs_sequentially_all_complete() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model(&db.pool(), now, "multi");

    let dir1 = db.create_dir("set_a");
    let dir2 = db.create_dir("set_b");
    std::fs::write(dir1.join("a.txt"), "a").unwrap();
    std::fs::write(dir1.join("b.txt"), "b").unwrap();
    std::fs::write(dir2.join("c.txt"), "c").unwrap();

    let payload1 = ingest_payload(&dir1);
    let payload2 = ingest_payload(&dir2);

    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload1, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry1 = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);
    dispatch_job(&db.pool(), &entry1, model_id, test_embedding_client()).unwrap();

    let conn = db.pool().get().unwrap();
    delete_jobs_by_type(&conn, JobType::EmbedDocument);
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload2, now + 10).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry2 = repository::claim_job(&mut conn, now + 11).unwrap().unwrap();
    drop(conn);
    dispatch_job(&db.pool(), &entry2, model_id, test_embedding_client()).unwrap();

    let conn = db.pool().get().unwrap();
    let doc_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM documents", [], |r| r.get(0))
        .unwrap();
    assert_eq!(doc_count, 3, "all files from both dirs ingested");
    assert_eq!(count_jobs_by_type(&conn, JobType::IngestFolder), 0);
    assert_eq!(count_jobs_by_type(&conn, JobType::EmbedDocument), 3);
}

#[tokio::test]
async fn ingest_files_with_special_characters_in_names() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model(&db.pool(), now, "special");

    let dir = db.create_dir("special_chars");
    let names = [
        "file with spaces.txt",
        "file(with)parens.txt",
        "resume-dash.txt",
        "a-b+c-d.txt",
    ];

    for name in &names {
        std::fs::write(dir.join(name), b"content").unwrap();
    }

    let payload = ingest_payload(&dir);
    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);
    dispatch_job(&db.pool(), &entry, model_id, test_embedding_client()).unwrap();

    let conn = db.pool().get().unwrap();
    let doc_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM documents", [], |r| r.get(0))
        .unwrap();
    assert_eq!(doc_count, names.len() as i64);

    for name in &names {
        let abs_path = dir
            .join(name)
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .to_string();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM documents WHERE filepath = ?1",
                [&abs_path],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "document for '{name}' should exist");
    }
}

#[tokio::test]
async fn ingest_mixed_file_types() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model(&db.pool(), now, "mixed");

    let dir = db.create_dir("mixed");
    std::fs::write(dir.join("readme.txt"), "hello").unwrap();
    std::fs::write(dir.join("empty.md"), "").unwrap();
    std::fs::write(dir.join("data.bin"), [0x00, 0xFF, 0xFE]).unwrap();
    std::fs::write(dir.join("Makefile"), "all:").unwrap();

    let target = dir.join("readme.txt");
    let link = dir.join("link_to_readme.txt");
    let symlink_created = create_file_symlink(&target, &link).is_ok();

    let payload = ingest_payload(&dir);
    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);
    dispatch_job(&db.pool(), &entry, model_id, test_embedding_client()).unwrap();

    let conn = db.pool().get().unwrap();
    let doc_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM documents", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        doc_count, 4,
        "symlinks should not be traversed as ingest candidates"
    );

    if symlink_created {
        let symlink_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM documents WHERE filepath = ?1",
                [link.to_string_lossy().into_owned()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            symlink_count, 0,
            "symlink paths should not be stored as documents"
        );
    }

    let content_emb: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM document_embeddings WHERE chunk_type = 'content'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        content_emb, 2,
        "canonicalized duplicates should not queue duplicate content chunks"
    );

    let empty_doc_id: i64 = conn
        .query_row(
            "SELECT id FROM documents WHERE filepath LIKE '%empty.md'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let empty_emb: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM document_embeddings WHERE document_id = ?1",
            [empty_doc_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(empty_emb, 1, "empty file gets filename embedding only");
}

#[tokio::test]
async fn ingest_large_file_produces_many_chunks() {
    let db = TempDb::new();
    let now = current_time();

    let conn = db.pool().get().unwrap();
    let model = ask_core::models::EmbeddingModel {
        id: 0,
        name: "small-chunks".to_string(),
        dimensions: 768,
        chunk_size: 10,
        chunk_overlap: 2,
        created_at: now,
    };
    let model_id = repository::insert_model(&conn, &model).unwrap();
    drop(conn);

    let dir = db.create_dir("large");
    let content: String = (0..100)
        .map(|i| char::from(b'a' + (i % 26) as u8))
        .collect();
    assert_eq!(content.len(), 100);
    std::fs::write(dir.join("big.txt"), &content).unwrap();

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
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(content_emb, 13);
}

#[tokio::test]
async fn ingest_large_file_limits_content_chunks_but_hashes_full_file() {
    let db = TempDb::new();
    let now = current_time();

    let conn = db.pool().get().unwrap();
    let model = ask_core::models::EmbeddingModel {
        id: 0,
        name: "bounded-large".to_string(),
        dimensions: 768,
        chunk_size: 512,
        chunk_overlap: 0,
        created_at: now,
    };
    let model_id = repository::insert_model(&conn, &model).unwrap();
    drop(conn);

    let dir = db.create_dir("bounded_large");
    let path = dir.join("big.txt");
    let content = "a".repeat((1024 * 1024) + 4096);
    std::fs::write(&path, &content).unwrap();

    let payload = ingest_payload(&dir);
    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);
    dispatch_job(&db.pool(), &entry, model_id, test_embedding_client()).unwrap();

    let conn = db.pool().get().unwrap();
    let (file_size, file_hash, metadata_json): (i64, String, String) = conn
        .query_row(
            "SELECT file_size, file_hash, metadata_json FROM documents WHERE filepath = ?1",
            [path.canonicalize().unwrap().to_string_lossy().into_owned()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    let max_content_end: i64 = conn
        .query_row(
            "SELECT MAX(chunk_end) FROM document_embeddings WHERE chunk_type = 'content'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let metadata: Value = serde_json::from_str(&metadata_json).unwrap();

    assert_eq!(file_size, content.len() as i64);
    assert_eq!(file_hash, hash_bytes(content.as_bytes()));
    assert!(max_content_end <= 1024 * 1024);
    assert_eq!(metadata["content_utf8"], true);
    assert_eq!(metadata["content_truncated"], true);
    assert_eq!(metadata["content_bytes_indexed"], 1024 * 1024);
    assert_eq!(metadata["content_byte_budget"], 1024 * 1024);
}

#[tokio::test]
async fn embed_document_job_replaces_rows_for_exact_document_model_pair() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model(&db.pool(), now, "embed-success");
    let other_model_id = register_model(&db.pool(), now + 1, "embed-other");

    let file_path = db.create_file("embedded.txt");
    std::fs::write(&file_path, "abcdefghij").unwrap();
    let document_id = insert_document(&db.pool(), now, &file_path);

    let conn = db.pool().get().unwrap();
    conn.execute_batch(
        "INSERT INTO document_embeddings
            (document_id, model_id, chunk_type, chunk_start, chunk_end, state, embedding, created_at)
         VALUES
            (1, 1, 'filename', 0, 0, 'pending', NULL, 100),
            (1, 1, 'content', 0, 5, 'stale', NULL, 100),
            (1, 2, 'filename', 0, 0, 'embedded', X'01', 100)",
    )
    .unwrap();

    let payload = serde_json::to_string(&EmbedDocumentPayload {
        document_id,
        model_id,
    })
    .unwrap();
    repository::enqueue_job(&conn, &JobType::EmbedDocument, &payload, now + 2).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 3).unwrap().unwrap();
    drop(conn);

    dispatch_job(&db.pool(), &entry, 999, test_embedding_client()).unwrap();

    let conn = db.pool().get().unwrap();
    let rows: Vec<(String, i64, i64, String, i64, i64)> = conn
        .prepare(
            "SELECT chunk_type, chunk_start, chunk_end, state, length(embedding), created_at
             FROM document_embeddings
             WHERE document_id = ?1 AND model_id = ?2
             ORDER BY chunk_type, chunk_start",
        )
        .unwrap()
        .query_map([document_id, model_id], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, "content");
    assert_eq!(rows[0].1, 0);
    assert_eq!(rows[0].2, 5);
    assert_eq!(rows[0].3, "embedded");
    assert_eq!(rows[0].4, 768 * 4);
    assert!(rows[0].5 >= now);
    assert_eq!(rows[1].0, "filename");
    assert_eq!(rows[1].3, "embedded");
    assert_eq!(rows[1].4, 768 * 4);

    let other_model_embedding: Vec<u8> = conn
        .query_row(
            "SELECT embedding FROM document_embeddings WHERE document_id = ?1 AND model_id = ?2",
            [document_id, other_model_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(other_model_embedding, vec![1]);

    assert_eq!(count_jobs_by_type(&conn, JobType::EmbedDocument), 0);
}

#[tokio::test]
async fn embed_document_provider_failure_keeps_job_claimed_and_rows_unchanged() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model(&db.pool(), now, "embed-failure");

    let file_path = db.create_file("fail.txt");
    std::fs::write(&file_path, "please fail-provider now").unwrap();
    let document_id = insert_document(&db.pool(), now, &file_path);

    let conn = db.pool().get().unwrap();
    conn.execute(
        "INSERT INTO document_embeddings
            (document_id, model_id, chunk_type, chunk_start, chunk_end, state, embedding, created_at)
         VALUES
            (?1, ?2, 'filename', 0, 0, 'pending', NULL, ?3),
            (?1, ?2, 'content', 0, 24, 'pending', NULL, ?3)",
        rusqlite::params![document_id, model_id, now],
    )
    .unwrap();

    let payload = serde_json::to_string(&EmbedDocumentPayload {
        document_id,
        model_id,
    })
    .unwrap();
    repository::enqueue_job(&conn, &JobType::EmbedDocument, &payload, now + 1).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 2).unwrap().unwrap();
    drop(conn);

    let err = dispatch_job(
        &db.pool(),
        &entry,
        1,
        Arc::new(DeterministicEmbeddingClient::fail_on_input("fail-provider")),
    )
    .unwrap_err();
    assert!(format!("{err:#}").contains("failed to embed document"));

    let conn = db.pool().get().unwrap();
    let pair_rows: Vec<(String, Option<Vec<u8>>)> = conn
        .prepare(
            "SELECT state, embedding FROM document_embeddings
             WHERE document_id = ?1 AND model_id = ?2
             ORDER BY chunk_type, chunk_start",
        )
        .unwrap()
        .query_map([document_id, model_id], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        pair_rows,
        vec![("pending".to_string(), None), ("pending".to_string(), None)]
    );

    let claimed_at: Option<i64> = conn
        .query_row(
            "SELECT claimed_at FROM job_queue WHERE id = ?1",
            [entry.id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(claimed_at, Some(now + 2));
}

#[tokio::test]
async fn embed_document_job_uses_stored_absolute_path_independent_of_working_directory() {
    let _working_directory_guard = WORKING_DIRECTORY_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let original_cwd = std::env::current_dir().unwrap();

    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model(&db.pool(), now, "embed-cwd");
    let other_dir = db.create_dir("other-cwd");

    let file_path = db.create_file("cwd-independent.txt");
    std::fs::write(&file_path, "abcdefghij").unwrap();
    let document_id = insert_document(&db.pool(), now, &file_path);

    let conn = db.pool().get().unwrap();
    conn.execute(
        "INSERT INTO document_embeddings
            (document_id, model_id, chunk_type, chunk_start, chunk_end, state, created_at)
         VALUES
            (?1, ?2, ?3, 0, 0, ?4, ?5),
            (?1, ?2, ?6, 0, 10, ?4, ?5)",
        rusqlite::params![
            document_id,
            model_id,
            ChunkType::Filename,
            EmbedState::Pending,
            now,
            ChunkType::Content,
        ],
    )
    .unwrap();

    let payload = serde_json::to_string(&EmbedDocumentPayload {
        document_id,
        model_id,
    })
    .unwrap();
    repository::enqueue_job(&conn, &JobType::EmbedDocument, &payload, now + 1).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 2).unwrap().unwrap();
    drop(conn);

    std::env::set_current_dir(&other_dir).unwrap();
    let dispatch_result = dispatch_job(&db.pool(), &entry, model_id, test_embedding_client());
    std::env::set_current_dir(&original_cwd).unwrap();

    dispatch_result.unwrap();

    let conn = db.pool().get().unwrap();
    let content_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM document_embeddings
             WHERE document_id = ?1 AND model_id = ?2 AND chunk_type = 'content' AND state = 'embedded'",
            rusqlite::params![document_id, model_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(content_count, 1);
}

#[tokio::test]
async fn embed_document_missing_file_deletes_document_cleanly() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model(&db.pool(), now, "embed-missing-file");

    let file_path = db.create_file("gone.txt");
    std::fs::write(&file_path, "abcdefghij").unwrap();
    let document_id = insert_document(&db.pool(), now, &file_path);
    let stored_path = file_path
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    std::fs::remove_file(&file_path).unwrap();

    let conn = db.pool().get().unwrap();
    conn.execute(
        "INSERT INTO document_embeddings
            (document_id, model_id, chunk_type, chunk_start, chunk_end, state, created_at)
         VALUES (?1, ?2, ?3, 0, 0, ?4, ?5)",
        rusqlite::params![
            document_id,
            model_id,
            ChunkType::Filename,
            EmbedState::Pending,
            now
        ],
    )
    .unwrap();

    let payload = serde_json::to_string(&EmbedDocumentPayload {
        document_id,
        model_id,
    })
    .unwrap();
    repository::enqueue_job(&conn, &JobType::EmbedDocument, &payload, now + 1).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 2).unwrap().unwrap();
    drop(conn);

    dispatch_job(&db.pool(), &entry, model_id, test_embedding_client()).unwrap();

    let conn = db.pool().get().unwrap();
    let queued_jobs: i64 = conn
        .query_row("SELECT COUNT(*) FROM job_queue", [], |row| row.get(0))
        .unwrap();
    assert_eq!(queued_jobs, 0);

    let document_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM documents WHERE id = ?1 OR filepath = ?2",
            rusqlite::params![document_id, stored_path],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(document_count, 0);

    let embedding_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM document_embeddings WHERE document_id = ?1",
            [document_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(embedding_count, 0);
}

#[tokio::test]
async fn embed_document_hash_mismatch_replans_pending_rows_and_requeues() {
    let db = TempDb::new();
    let now = current_time();

    let conn = db.pool().get().unwrap();
    let model = ask_core::models::EmbeddingModel {
        id: 0,
        name: "embed-hash-mismatch".to_string(),
        dimensions: 768,
        chunk_size: 5,
        chunk_overlap: 0,
        created_at: now,
    };
    let model_id = repository::insert_model(&conn, &model).unwrap();
    drop(conn);

    let dir = db.create_dir("embed-hash-mismatch");
    let file_path = dir.join("doc.txt");
    std::fs::write(&file_path, "abcdefghij").unwrap();
    let payload = ingest_payload(&dir);

    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let ingest_entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);
    dispatch_job(&db.pool(), &ingest_entry, model_id, test_embedding_client()).unwrap();

    let conn = db.pool().get().unwrap();
    let old_hash = stored_file_hash(&conn, &file_path);
    let mut conn = db.pool().get().unwrap();
    let embed_entry = repository::claim_job(&mut conn, now + 2).unwrap().unwrap();
    drop(conn);

    std::fs::write(&file_path, "klmnopqrst").unwrap();
    dispatch_job(&db.pool(), &embed_entry, model_id, test_embedding_client()).unwrap();

    let conn = db.pool().get().unwrap();
    let new_hash = stored_file_hash(&conn, &file_path);
    assert_ne!(old_hash, new_hash);

    let rows: Vec<(String, i64, i64)> = conn
        .prepare(
            "SELECT state, chunk_start, chunk_end
             FROM document_embeddings
             WHERE model_id = ?1
             ORDER BY chunk_type, chunk_start",
        )
        .unwrap()
        .query_map([model_id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        rows,
        vec![
            ("pending".to_string(), 0, 5),
            ("pending".to_string(), 5, 10),
            ("pending".to_string(), 0, 0),
        ]
    );
    assert_eq!(
        queued_embed_jobs(&conn),
        vec![EmbedDocumentPayload {
            document_id: 1,
            model_id,
        }]
    );
}

#[tokio::test]
async fn embed_document_hash_match_uses_stored_offsets() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model(&db.pool(), now, "embed-stored-offsets");

    let dir = db.create_dir("embed-stored-offsets");
    let file_path = dir.join("doc.txt");
    std::fs::write(&file_path, "abcdefghij").unwrap();
    let payload = ingest_payload(&dir);

    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let ingest_entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);
    dispatch_job(&db.pool(), &ingest_entry, model_id, test_embedding_client()).unwrap();

    let conn = db.pool().get().unwrap();
    delete_jobs_by_type(&conn, JobType::EmbedDocument);
    conn.execute("DELETE FROM document_embeddings", []).unwrap();
    conn.execute(
        "INSERT INTO document_embeddings
            (document_id, model_id, chunk_type, chunk_start, chunk_end, state, created_at)
         VALUES
            (1, ?1, 'filename', 0, 0, 'pending', ?2),
            (1, ?1, 'content', 2, 4, 'pending', ?2)",
        rusqlite::params![model_id, now + 2],
    )
    .unwrap();
    enqueue_embed_document_job(&conn, 1, model_id, now + 3);
    let mut conn = db.pool().get().unwrap();
    let embed_entry = repository::claim_job(&mut conn, now + 4).unwrap().unwrap();
    drop(conn);

    dispatch_job(&db.pool(), &embed_entry, model_id, test_embedding_client()).unwrap();

    let conn = db.pool().get().unwrap();
    let rows: Vec<(String, i64, i64)> = conn
        .prepare(
            "SELECT chunk_type, chunk_start, chunk_end
             FROM document_embeddings
             WHERE document_id = 1 AND model_id = ?1
             ORDER BY chunk_type, chunk_start",
        )
        .unwrap()
        .query_map([model_id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(
        rows,
        vec![
            ("content".to_string(), 2, 4),
            ("filename".to_string(), 0, 0),
        ]
    );
}

#[tokio::test]
async fn embed_document_malformed_payload_keeps_job_claimed() {
    let db = TempDb::new();
    let now = current_time();

    let conn = db.pool().get().unwrap();
    repository::enqueue_job(
        &conn,
        &JobType::EmbedDocument,
        r#"{"document_id":"bad"}"#,
        now,
    )
    .unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);

    let err = dispatch_job(&db.pool(), &entry, 1, test_embedding_client()).unwrap_err();
    assert!(format!("{err:#}").contains("failed to decode payload"));

    let conn = db.pool().get().unwrap();
    let claimed_at: Option<i64> = conn
        .query_row(
            "SELECT claimed_at FROM job_queue WHERE id = ?1",
            [entry.id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(claimed_at, Some(now + 1));
}

#[tokio::test]
async fn ingest_non_utf8_file_only_gets_filename_embedding() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model(&db.pool(), now, "nonutf8");

    let dir = db.create_dir("nonutf8");
    std::fs::write(dir.join("data.bin"), [0xFF, 0xFE, 0x80, 0x00]).unwrap();
    let payload = ingest_payload(&dir);

    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);
    dispatch_job(&db.pool(), &entry, model_id, test_embedding_client()).unwrap();

    let conn = db.pool().get().unwrap();
    let filename_emb: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM document_embeddings WHERE chunk_type = 'filename'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(filename_emb, 1);

    let content_emb: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM document_embeddings WHERE chunk_type = 'content'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(content_emb, 0);

    let (file_hash, metadata_json): (String, String) = conn
        .query_row(
            "SELECT file_hash, metadata_json FROM documents",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(
        !file_hash.is_empty(),
        "non-UTF-8 files still store raw byte hashes"
    );
    let metadata: Value = serde_json::from_str(&metadata_json).unwrap();
    assert_eq!(metadata["content_utf8"], false);
    assert_eq!(metadata["content_truncated"], false);
    assert_eq!(metadata["content_bytes_indexed"], 0);
    assert_eq!(metadata["content_byte_budget"], 1024 * 1024);
}

#[tokio::test]
async fn new_model_backfill_queues_filename_and_content_embeddings_for_existing_documents() {
    let db = TempDb::new();
    let now = current_time();
    let dir = db.create_dir("existing_docs");

    let text_path = dir.join("notes.txt");
    let binary_path = dir.join("blob.bin");
    std::fs::write(&text_path, "hello world").unwrap();
    std::fs::write(&binary_path, [0xFF, 0xFE, 0x80, 0x00]).unwrap();

    insert_document(&db.pool(), now, &text_path);
    insert_document(&db.pool(), now, &binary_path);

    let conn = db.pool().get().unwrap();
    let model = ask_core::models::EmbeddingModel {
        id: 0,
        name: "backfill".to_string(),
        dimensions: 768,
        chunk_size: 10,
        chunk_overlap: 0,
        created_at: now,
    };
    let model = ask_core::models::EmbeddingModel {
        id: repository::insert_model(&conn, &model).unwrap(),
        ..model
    };

    let seeded = backfill_pending_for_model(&conn, &model, now + 1).unwrap();
    assert_eq!(seeded, 2, "both existing documents should be backfilled");

    let queued_jobs: Vec<EmbedDocumentPayload> = conn
        .prepare("SELECT payload FROM job_queue WHERE job_type = ?1 ORDER BY payload")
        .unwrap()
        .query_map([JobType::EmbedDocument], |row| {
            let payload: String = row.get(0)?;
            Ok(serde_json::from_str(&payload).expect("embed payload must decode"))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        queued_jobs,
        vec![
            EmbedDocumentPayload {
                document_id: 1,
                model_id: model.id,
            },
            EmbedDocumentPayload {
                document_id: 2,
                model_id: model.id,
            },
        ]
    );

    let filename_emb: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM document_embeddings WHERE model_id = ?1 AND chunk_type = 'filename'",
            [model.id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(filename_emb, 2);

    let content_emb: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM document_embeddings WHERE model_id = ?1 AND chunk_type = 'content'",
            [model.id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        content_emb, 2,
        "text content should be backfilled with the normal chunk plan"
    );
}

#[test]
fn startup_recovery_seeds_jobs_for_existing_pending_rows() {
    let db = TempDb::new();
    let now = current_time();
    let doc_a = insert_document(&db.pool(), now, &db.create_file("pending-a.txt"));
    let doc_b = insert_document(&db.pool(), now, &db.create_file("pending-b.txt"));
    let model_id = register_model(&db.pool(), now, "startup-pending");
    let conn = db.pool().get().unwrap();

    insert_embedding_row(
        &conn,
        doc_a,
        model_id,
        ChunkType::Filename,
        0..0,
        EmbedState::Pending,
        now,
    );
    insert_embedding_row(
        &conn,
        doc_a,
        model_id,
        ChunkType::Content,
        0..7,
        EmbedState::Pending,
        now,
    );
    insert_embedding_row(
        &conn,
        doc_b,
        model_id,
        ChunkType::Filename,
        0..0,
        EmbedState::Pending,
        now,
    );

    let startup =
        reconcile_embedding_startup(&conn, embedding_identity("startup-pending", 768), now)
            .unwrap();

    assert_eq!(startup.backfilled_documents, 0);
    assert_eq!(startup.seeded_jobs, 2);
    assert_eq!(
        queued_embed_jobs(&conn),
        vec![
            EmbedDocumentPayload {
                document_id: doc_a,
                model_id,
            },
            EmbedDocumentPayload {
                document_id: doc_b,
                model_id,
            },
        ]
    );
}

#[test]
fn startup_recovery_seeds_jobs_for_existing_stale_rows() {
    let db = TempDb::new();
    let now = current_time();
    let doc_id = insert_document(&db.pool(), now, &db.create_file("stale.txt"));
    let model_id = register_model(&db.pool(), now, "startup-stale");
    let conn = db.pool().get().unwrap();

    insert_embedding_row(
        &conn,
        doc_id,
        model_id,
        ChunkType::Filename,
        0..0,
        EmbedState::Stale,
        now,
    );

    let startup =
        reconcile_embedding_startup(&conn, embedding_identity("startup-stale", 768), now).unwrap();

    assert_eq!(startup.backfilled_documents, 0);
    assert_eq!(startup.seeded_jobs, 1);
    assert_eq!(
        queued_embed_jobs(&conn),
        vec![EmbedDocumentPayload {
            document_id: doc_id,
            model_id,
        }]
    );
}

#[test]
fn startup_recovery_does_not_duplicate_existing_queued_or_claimed_jobs() {
    let db = TempDb::new();
    let now = current_time();
    let queued_doc = insert_document(&db.pool(), now, &db.create_file("queued.txt"));
    let claimed_doc = insert_document(&db.pool(), now, &db.create_file("claimed.txt"));
    let model_id = register_model(&db.pool(), now, "startup-dedup");
    let mut conn = db.pool().get().unwrap();

    insert_embedding_row(
        &conn,
        queued_doc,
        model_id,
        ChunkType::Filename,
        0..0,
        EmbedState::Pending,
        now,
    );
    insert_embedding_row(
        &conn,
        claimed_doc,
        model_id,
        ChunkType::Filename,
        0..0,
        EmbedState::Stale,
        now,
    );

    enqueue_embed_document_job(&conn, queued_doc, model_id, now);
    enqueue_embed_document_job(&conn, claimed_doc, model_id, now);
    let claimed = repository::claim_job(&mut conn, now + 1).unwrap();
    assert!(claimed.is_some(), "one queued job should become claimed");

    let startup =
        reconcile_embedding_startup(&conn, embedding_identity("startup-dedup", 768), now + 2)
            .unwrap();

    assert_eq!(startup.backfilled_documents, 0);
    assert_eq!(startup.seeded_jobs, 0);
    assert_eq!(
        queued_embed_jobs(&conn),
        vec![
            EmbedDocumentPayload {
                document_id: queued_doc,
                model_id,
            },
            EmbedDocumentPayload {
                document_id: claimed_doc,
                model_id,
            },
        ]
    );

    let claimed_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM job_queue WHERE job_type = ?1 AND claimed_at IS NOT NULL",
            [JobType::EmbedDocument],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(claimed_count, 1);
}

#[test]
fn startup_recovery_is_idempotent_across_repeated_boots() {
    let db = TempDb::new();
    let now = current_time();
    let doc_id = insert_document(&db.pool(), now, &db.create_file("repeat.txt"));
    let model_id = register_model(&db.pool(), now, "startup-repeat");
    let conn = db.pool().get().unwrap();

    insert_embedding_row(
        &conn,
        doc_id,
        model_id,
        ChunkType::Filename,
        0..0,
        EmbedState::Pending,
        now,
    );

    let first =
        reconcile_embedding_startup(&conn, embedding_identity("startup-repeat", 768), now).unwrap();
    let first_jobs = count_jobs_by_type(&conn, JobType::EmbedDocument);
    let second =
        reconcile_embedding_startup(&conn, embedding_identity("startup-repeat", 768), now + 1)
            .unwrap();
    let second_jobs = count_jobs_by_type(&conn, JobType::EmbedDocument);

    assert_eq!(first.seeded_jobs, 1);
    assert_eq!(second.seeded_jobs, 0);
    assert_eq!(first_jobs, 1);
    assert_eq!(second_jobs, 1);
}

#[test]
fn startup_recovery_does_not_seed_jobs_for_embedded_only_rows() {
    let db = TempDb::new();
    let now = current_time();
    let doc_id = insert_document(&db.pool(), now, &db.create_file("embedded.txt"));
    let model_id = register_model(&db.pool(), now, "startup-embedded");
    let conn = db.pool().get().unwrap();

    conn.execute(
        "INSERT INTO document_embeddings
            (document_id, model_id, chunk_type, chunk_start, chunk_end, state, embedding, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        rusqlite::params![
            doc_id,
            model_id,
            ChunkType::Filename,
            0,
            0,
            EmbedState::Embedded,
            serialize_embedding(&[1.0_f32; 768]),
            now
        ],
    )
    .unwrap();

    let startup =
        reconcile_embedding_startup(&conn, embedding_identity("startup-embedded", 768), now)
            .unwrap();

    assert_eq!(startup.backfilled_documents, 0);
    assert_eq!(startup.seeded_jobs, 0);
    assert_eq!(startup.document_count, 1);
    assert_eq!(startup.recoverable_pairs, 0);
    assert_eq!(startup.summary_kind, StartupSummaryKind::Idle);
    assert_eq!(count_jobs_by_type(&conn, JobType::EmbedDocument), 0);
}

#[test]
fn startup_recovery_reports_empty_database_state() {
    let db = TempDb::new();
    let now = current_time();
    let conn = db.pool().get().unwrap();

    let startup =
        reconcile_embedding_startup(&conn, embedding_identity("startup-empty", 768), now).unwrap();

    assert_eq!(startup.backfilled_documents, 0);
    assert_eq!(startup.seeded_jobs, 0);
    assert_eq!(startup.document_count, 0);
    assert_eq!(startup.recoverable_pairs, 0);
    assert_eq!(startup.summary_kind, StartupSummaryKind::Empty);
}

#[test]
fn startup_recovery_reports_recoverable_work_in_summary_state() {
    let db = TempDb::new();
    let now = current_time();
    let doc_id = insert_document(&db.pool(), now, &db.create_file("summary-recoverable.txt"));
    let model_id = register_model(&db.pool(), now, "startup-summary-recoverable");
    let conn = db.pool().get().unwrap();

    insert_embedding_row(
        &conn,
        doc_id,
        model_id,
        ChunkType::Filename,
        0..0,
        EmbedState::Pending,
        now,
    );

    let startup = reconcile_embedding_startup(
        &conn,
        embedding_identity("startup-summary-recoverable", 768),
        now,
    )
    .unwrap();

    assert_eq!(startup.document_count, 1);
    assert_eq!(startup.recoverable_pairs, 1);
    assert_eq!(startup.summary_kind, StartupSummaryKind::Recovered);
}

#[test]
fn startup_recovery_reports_idle_corpus_state() {
    let db = TempDb::new();
    let now = current_time();
    let doc_id = insert_document(&db.pool(), now, &db.create_file("summary-idle.txt"));
    let model_id = register_model(&db.pool(), now, "startup-summary-idle");
    let conn = db.pool().get().unwrap();

    conn.execute(
        "INSERT INTO document_embeddings
            (document_id, model_id, chunk_type, chunk_start, chunk_end, state, embedding, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        rusqlite::params![
            doc_id,
            model_id,
            ChunkType::Filename,
            0,
            0,
            EmbedState::Embedded,
            serialize_embedding(&[1.0_f32; 768]),
            now
        ],
    )
    .unwrap();

    let startup =
        reconcile_embedding_startup(&conn, embedding_identity("startup-summary-idle", 768), now)
            .unwrap();

    assert_eq!(startup.document_count, 1);
    assert_eq!(startup.recoverable_pairs, 0);
    assert_eq!(startup.summary_kind, StartupSummaryKind::Idle);
}

// ---------------------------------------------------------------------------
// Claim + complete via the repository layer (full SQLite round-trip)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_lifecycle_claim_and_complete() {
    let db = TempDb::new();
    let conn = db.pool().get().unwrap();

    use ask_core::repository;
    use ask_core::types::JobType;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    let payload = r#"{"root_path":"/tmp","file_pattern":".*"}"#;
    repository::enqueue_job(&conn, &JobType::IngestFolder, payload, now).unwrap();

    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1)
        .unwrap()
        .expect("job should be claimable");

    assert_eq!(entry.job_type, JobType::IngestFolder);
    assert_eq!(entry.payload, payload);
    assert_eq!(entry.claimed_at, Some(now + 1));

    repository::complete_job(&conn, entry.id).unwrap();

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM job_queue", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0);
}

// ---------------------------------------------------------------------------
// POST /documents/stale
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mark_stale_batch_updates_embeddings() {
    let db = TempDb::new();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    let conn = db.pool().get().unwrap();
    repository::insert_model(
        &conn,
        &ask_core::models::EmbeddingModel {
            id: 0,
            name: "test".to_string(),
            dimensions: 768,
            chunk_size: 512,
            chunk_overlap: 0,
            created_at: now,
        },
    )
    .unwrap();
    conn.execute_batch(
        "INSERT INTO documents
            (filepath, file_type, doc_category, file_modified_at, file_size,
             file_hash, metadata_json, updated_at)
         VALUES ('/a.txt', 'txt', 'resource', 100, 10, 'hash-a', '{}', 100),
                ('/b.txt', 'txt', 'resource', 101, 20, 'hash-b', '{}', 101);
         INSERT INTO document_embeddings (document_id, model_id, chunk_type, chunk_start, chunk_end, state, created_at)
         VALUES (1, 1, 'filename', 0, 0, 'embedded', 100),
                (1, 1, 'content', 0, 5, 'embedded', 100),
                (2, 1, 'filename', 0, 0, 'embedded', 101);",
    )
    .unwrap();
    drop(conn);

    // Request only doc 1 — its two embeddings should become stale;
    // doc 2's single embedding should remain embedded.
    let res = db
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/documents/stale")
                .header("content-type", "application/json")
                .body(json_body(r#"{"document_ids":[1]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    let body: Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["status"], "ok");

    let conn = db.pool().get().unwrap();
    let stale_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM document_embeddings WHERE state = 'stale'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(stale_count, 2, "doc 1 has 2 stale embeddings");

    let embedded_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM document_embeddings WHERE state = 'embedded'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(embedded_count, 1, "doc 2 still has 1 embedded embedding");
    assert_eq!(count_jobs_by_type(&conn, JobType::EmbedDocument), 1);
}

#[tokio::test]
async fn mark_stale_empty_list_returns_400() {
    let db = TempDb::new();

    let res = db
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/documents/stale")
                .header("content-type", "application/json")
                .body(json_body(r#"{"document_ids":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body: Value = serde_json::from_str(&body_text(res).await).unwrap();
    assert_eq!(body["error"]["code"], "bad_request");
}

#[tokio::test]
async fn mark_stale_nonexistent_ids_is_noop() {
    let db = TempDb::new();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    let conn = db.pool().get().unwrap();
    repository::insert_model(
        &conn,
        &ask_core::models::EmbeddingModel {
            id: 0,
            name: "test".to_string(),
            dimensions: 768,
            chunk_size: 512,
            chunk_overlap: 0,
            created_at: now,
        },
    )
    .unwrap();
    conn.execute_batch(
        "INSERT INTO documents
            (filepath, file_type, doc_category, file_modified_at, file_size,
             file_hash, metadata_json, updated_at)
         VALUES ('/keep.txt', 'txt', 'resource', 100, 10, 'hash-keep', '{}', 100);
         INSERT INTO document_embeddings (document_id, model_id, chunk_type, chunk_start, chunk_end, state, created_at)
         VALUES (1, 1, 'filename', 0, 0, 'embedded', 100);",
    )
    .unwrap();
    drop(conn);

    let res = db
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/documents/stale")
                .header("content-type", "application/json")
                .body(json_body(r#"{"document_ids":[999]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);

    // No rows matched, so the existing embedding stays 'embedded'.
    let conn = db.pool().get().unwrap();
    let embedded_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM document_embeddings WHERE state = 'embedded'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(embedded_count, 1, "existing embedding remains 'embedded'");
    let stale_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM document_embeddings WHERE state = 'stale'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(stale_count, 0, "no rows were marked stale");
    assert_eq!(count_jobs_by_type(&conn, JobType::EmbedDocument), 0);
}

#[tokio::test]
async fn mark_stale_affects_all_models_and_preserves_rows() {
    let db = TempDb::new();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    let conn = db.pool().get().unwrap();
    repository::insert_model(
        &conn,
        &ask_core::models::EmbeddingModel {
            id: 0,
            name: "m1".to_string(),
            dimensions: 768,
            chunk_size: 512,
            chunk_overlap: 0,
            created_at: now,
        },
    )
    .unwrap();
    repository::insert_model(
        &conn,
        &ask_core::models::EmbeddingModel {
            id: 0,
            name: "m2".to_string(),
            dimensions: 384,
            chunk_size: 256,
            chunk_overlap: 0,
            created_at: now,
        },
    )
    .unwrap();

    conn.execute_batch(
        "INSERT INTO documents
            (filepath, file_type, doc_category, file_modified_at, file_size,
             file_hash, metadata_json, updated_at)
         VALUES ('/doc1.txt', 'txt', 'resource', 100, 10, 'hash-doc-1', '{}', 100),
                ('/doc2.txt', 'txt', 'resource', 200, 20, 'hash-doc-2', '{}', 200);

         INSERT INTO document_embeddings (document_id, model_id, chunk_type, chunk_start, chunk_end, state, created_at)
         VALUES
            -- doc 1 / model 1: TWO embedded rows (will become stale)
            (1, 1, 'filename', 0,  0,  'embedded', 100),
            (1, 1, 'content',  0,  5,  'embedded', 100),
            -- doc 1 / model 2: ONE embedded row (will become stale)
            (1, 2, 'filename', 0,  0,  'embedded', 100),
            -- doc 1 / model 1: ONE pending row (should stay 'pending')
            (1, 1, 'content',  10, 15, 'pending',   100),
            -- doc 1 / model 2: ONE already-stale row (should stay 'stale')
            (1, 2, 'content',  0,  10, 'stale',     100),
            -- doc 2 / model 1: ONE embedded row (different doc, untouched)
            (2, 1, 'filename', 0,  0,  'embedded', 200);",
    )
    .unwrap();
    drop(conn);

    // Stale-mark only doc 1.
    let res = db
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/documents/stale")
                .header("content-type", "application/json")
                .body(json_body(r#"{"document_ids":[1]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let conn = db.pool().get().unwrap();

    // Total row count unchanged (no deletes).
    let total: i64 = conn
        .query_row("SELECT COUNT(*) FROM document_embeddings", [], |r| r.get(0))
        .unwrap();
    assert_eq!(total, 6, "all rows still exist, none were deleted");

    // Stale count: 3 embedded->stale + 1 already stale = 4.
    let stale: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM document_embeddings WHERE state = 'stale'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(stale, 4, "3 embedded->stale + 1 already stale = 4");

    // Pending row was left alone.
    let pending: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM document_embeddings WHERE state = 'pending'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(pending, 1, "pending row was left alone");

    // Doc 2's embeddings are untouched.
    let doc2_embedded: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM document_embeddings WHERE document_id = 2 AND state = 'embedded'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(doc2_embedded, 1, "doc 2's embedding is untouched");

    // Doc 1 has no remaining 'embedded' rows.
    let doc1_embedded: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM document_embeddings WHERE document_id = 1 AND state = 'embedded'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(doc1_embedded, 0, "doc 1 has no remaining 'embedded' rows");
    assert_eq!(count_jobs_by_type(&conn, JobType::EmbedDocument), 2);
}

// ---------------------------------------------------------------------------
// sqlite-vec search
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sqlite_vec_backfills_existing_embedded_rows_and_searches_without_rust_scan() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model_with_dimensions(&db.pool(), now, "vec-backfill", 2);
    let path_a = db.create_file("vec-a.txt");
    let path_b = db.create_file("vec-b.txt");
    let doc_a = insert_document(&db.pool(), now, &path_a);
    let doc_b = insert_document(&db.pool(), now, &path_b);

    let conn = db.pool().get().unwrap();
    conn.execute(
        "INSERT INTO document_embeddings
            (document_id, model_id, chunk_type, chunk_start, chunk_end, state, embedding, created_at)
         VALUES (?1, ?2, 'filename', 0, 0, 'embedded', ?3, ?4)",
        rusqlite::params![doc_a, model_id, serialize_embedding(&[1.0_f32, 0.0_f32]), now],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO document_embeddings
            (document_id, model_id, chunk_type, chunk_start, chunk_end, state, embedding, created_at)
         VALUES (?1, ?2, 'filename', 0, 0, 'embedded', ?3, ?4)",
        rusqlite::params![doc_b, model_id, serialize_embedding(&[0.0_f32, 1.0_f32]), now],
    )
    .unwrap();

    let model = repository::find_model_by_id(&conn, model_id)
        .unwrap()
        .unwrap();
    let backfilled = vector_index::ensure_active_search_model(&conn, &model, now).unwrap();
    assert_eq!(backfilled, 2);

    let hits = repository::search_documents_by_embedding(&conn, &model, &[1.0, 0.0], 2).unwrap();

    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].document_id, doc_a);
    assert_eq!(
        hits[0].filepath,
        path_a.canonicalize().unwrap().display().to_string()
    );
    assert_eq!(hits[0].distance, 0.0);
    assert_eq!(hits[1].document_id, doc_b);
    assert!(hits[1].distance > hits[0].distance);
}

#[tokio::test]
async fn sqlite_vec_search_updates_when_embeddings_are_replaced() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model_with_dimensions(&db.pool(), now, "vec-update", 2);
    let path = db.create_file("vec-update.txt");
    let doc_id = insert_document(&db.pool(), now, &path);

    let mut conn = db.pool().get().unwrap();
    let model = repository::find_model_by_id(&conn, model_id)
        .unwrap()
        .unwrap();
    vector_index::ensure_active_search_model(&conn, &model, now).unwrap();

    repository::replace_embeddings_for_document_model(
        &mut conn,
        doc_id,
        model_id,
        &[ask_core::models::EmbeddedChunk {
            chunk_type: ask_core::types::ChunkType::Filename,
            chunk_start: 0,
            chunk_end: 0,
            embedding: serialize_embedding(&[1.0, 0.0]),
        }],
        now,
    )
    .unwrap();

    let initial_hits =
        repository::search_documents_by_embedding(&conn, &model, &[1.0, 0.0], 1).unwrap();
    assert_eq!(initial_hits[0].document_id, doc_id);
    let initial_embedding_id = initial_hits[0].embedding_id;

    repository::replace_embeddings_for_document_model(
        &mut conn,
        doc_id,
        model_id,
        &[ask_core::models::EmbeddedChunk {
            chunk_type: ask_core::types::ChunkType::Filename,
            chunk_start: 0,
            chunk_end: 0,
            embedding: serialize_embedding(&[0.0, 1.0]),
        }],
        now + 1,
    )
    .unwrap();

    let old_query_hits =
        repository::search_documents_by_embedding(&conn, &model, &[1.0, 0.0], 1).unwrap();
    let new_query_hits =
        repository::search_documents_by_embedding(&conn, &model, &[0.0, 1.0], 1).unwrap();

    assert_eq!(old_query_hits[0].document_id, doc_id);
    assert!(old_query_hits[0].distance > 0.0);
    assert_eq!(new_query_hits[0].document_id, doc_id);
    assert_eq!(new_query_hits[0].distance, 0.0);
    assert_ne!(new_query_hits[0].embedding_id, initial_embedding_id);
}

#[tokio::test]
async fn sqlite_vec_search_removes_rows_when_documents_become_stale() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model_with_dimensions(&db.pool(), now, "vec-stale", 2);
    let path = db.create_file("vec-stale.txt");
    let doc_id = insert_document(&db.pool(), now, &path);

    let mut conn = db.pool().get().unwrap();
    let model = repository::find_model_by_id(&conn, model_id)
        .unwrap()
        .unwrap();
    vector_index::ensure_active_search_model(&conn, &model, now).unwrap();
    repository::replace_embeddings_for_document_model(
        &mut conn,
        doc_id,
        model_id,
        &[ask_core::models::EmbeddedChunk {
            chunk_type: ask_core::types::ChunkType::Filename,
            chunk_start: 0,
            chunk_end: 0,
            embedding: serialize_embedding(&[1.0, 0.0]),
        }],
        now,
    )
    .unwrap();
    drop(conn);

    let response = db
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/documents/stale")
                .header("content-type", "application/json")
                .body(json_body(&format!(r#"{{"document_ids":[{doc_id}]}}"#)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let conn = db.pool().get().unwrap();
    let hits = repository::search_documents_by_embedding(&conn, &model, &[1.0, 0.0], 5).unwrap();
    assert!(hits.is_empty());

    let state: String = conn
        .query_row(
            "SELECT state FROM document_embeddings WHERE document_id = ?1 AND model_id = ?2",
            [doc_id, model_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "stale");
}

// ---------------------------------------------------------------------------
// POST /search
// ---------------------------------------------------------------------------

#[tokio::test]
async fn search_returns_unique_documents_with_match_score_only_by_default() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model_with_dimensions(&db.pool(), now, "search-model", 4);
    let client = Arc::new(DeterministicEmbeddingClient::new());

    let doc_a_path = db.create_file("search-a.txt");
    let doc_b_path = db.create_file("search-b.txt");
    let doc_a = insert_document(&db.pool(), now, &doc_a_path);
    let doc_b = insert_document(&db.pool(), now, &doc_b_path);

    let conn = db.pool().get().unwrap();
    let model = repository::find_model_by_id(&conn, model_id)
        .unwrap()
        .unwrap();
    vector_index::ensure_active_search_model(&conn, &model, now).unwrap();
    drop(conn);

    let a_inputs = vec![
        "query-doc-a".to_string(),
        "query-doc-a-near-duplicate".to_string(),
    ];
    let b_inputs = vec!["query-doc-b".to_string()];
    let a_vectors = client.embed(&model, &a_inputs).unwrap();
    let b_vectors = client.embed(&model, &b_inputs).unwrap();

    let mut conn = db.pool().get().unwrap();
    repository::replace_embeddings_for_document_model(
        &mut conn,
        doc_a,
        model_id,
        &[
            ask_core::models::EmbeddedChunk {
                chunk_type: ask_core::types::ChunkType::Content,
                chunk_start: 0,
                chunk_end: 8,
                embedding: serialize_embedding(&a_vectors[0]),
            },
            ask_core::models::EmbeddedChunk {
                chunk_type: ask_core::types::ChunkType::Content,
                chunk_start: 8,
                chunk_end: 16,
                embedding: serialize_embedding(&a_vectors[1]),
            },
        ],
        now,
    )
    .unwrap();
    repository::replace_embeddings_for_document_model(
        &mut conn,
        doc_b,
        model_id,
        &[ask_core::models::EmbeddedChunk {
            chunk_type: ask_core::types::ChunkType::Content,
            chunk_start: 0,
            chunk_end: 12,
            embedding: serialize_embedding(&b_vectors[0]),
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
                .body(json_body(r#"{"query":"query-doc-a","limit":2}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
    let results = body.as_array().unwrap();
    assert_eq!(results.len(), 2, "dedupe should still return 2 documents");

    assert_eq!(
        results[0]["filepath"],
        format!("{}/search-a.txt", display_path(&db.dir))
    );
    assert_eq!(
        results[1]["filepath"],
        format!("{}/search-b.txt", display_path(&db.dir))
    );

    assert!(
        results[0]["match_score"].as_f64().unwrap() >= results[1]["match_score"].as_f64().unwrap()
    );
    assert!(results[0].get("byte_start").is_none());
    assert!(results[0].get("byte_end").is_none());
}

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
    let model = repository::find_model_by_id(&conn, model_id)
        .unwrap()
        .unwrap();
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
    assert_eq!(
        body.as_array().unwrap()[0]["filepath"],
        format!("{}/memory/daily.md", display_path(&data_dir))
    );
}

#[tokio::test]
async fn search_uses_configured_display_root_for_resource_paths() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model_with_dimensions(&db.pool(), now, "search-display-path", 4);
    let client = Arc::new(DeterministicEmbeddingClient::new());

    let resource_dir = db.dir.join("resources");
    let data_dir = db.dir.join("data");
    std::fs::create_dir_all(&resource_dir).unwrap();
    std::fs::create_dir_all(&data_dir).unwrap();
    let resource_path = resource_dir.join("notes.md");
    std::fs::write(&resource_path, "resource notes").unwrap();
    let state = http::AppState::new_with_display_dirs(
        db.pool().pool().clone(),
        &resource_dir,
        &data_dir,
        ".",
        ".data",
    )
    .unwrap()
    .with_embedding_client(client.clone());
    let doc_id = insert_document(&state, now, &resource_path);

    let conn = state.pool().get().unwrap();
    let model = repository::find_model_by_id(&conn, model_id)
        .unwrap()
        .unwrap();
    vector_index::ensure_active_search_model(&conn, &model, now).unwrap();
    drop(conn);

    let vectors = client
        .embed(&model, &["display-query".to_string()])
        .unwrap();
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
                .body(json_body(r#"{"query":"display-query","limit":1}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
    assert_eq!(body.as_array().unwrap()[0]["filepath"], "./notes.md");
}

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
    let model = repository::find_model_by_id(&conn, model_id)
        .unwrap()
        .unwrap();
    vector_index::ensure_active_search_model(&conn, &model, now).unwrap();
    drop(conn);

    let vectors = client
        .embed(&model, &["outside-query".to_string()])
        .unwrap();
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

#[tokio::test]
async fn search_include_location_uses_next_byte_offset_for_filename_hit() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model_with_dimensions(&db.pool(), now, "search-location", 4);
    let client = Arc::new(DeterministicEmbeddingClient::new());

    let doc_path = db.create_file("search-location.txt");
    let doc_id = insert_document(&db.pool(), now, &doc_path);

    let conn = db.pool().get().unwrap();
    let model = repository::find_model_by_id(&conn, model_id)
        .unwrap()
        .unwrap();
    vector_index::ensure_active_search_model(&conn, &model, now).unwrap();
    drop(conn);

    let vectors = client
        .embed(
            &model,
            &[
                "search-location.txt".to_string(),
                "search-location-far".to_string(),
            ],
        )
        .unwrap();
    let mut conn = db.pool().get().unwrap();
    repository::replace_embeddings_for_document_model(
        &mut conn,
        doc_id,
        model_id,
        &[
            ask_core::models::EmbeddedChunk {
                chunk_type: ask_core::types::ChunkType::Filename,
                chunk_start: 0,
                chunk_end: 0,
                embedding: serialize_embedding(&vectors[0]),
            },
            ask_core::models::EmbeddedChunk {
                chunk_type: ask_core::types::ChunkType::Content,
                chunk_start: 10,
                chunk_end: 25,
                embedding: serialize_embedding(&vectors[1]),
            },
            ask_core::models::EmbeddedChunk {
                chunk_type: ask_core::types::ChunkType::Content,
                chunk_start: 100,
                chunk_end: 120,
                embedding: serialize_embedding(&vectors[0]),
            },
        ],
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
                .body(json_body(
                    r#"{"query":"search-location.txt","limit":1,"include_location":true}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
    let result = &body.as_array().unwrap()[0];
    assert!(result["match_score"].as_f64().unwrap() > 0.99);
    assert_eq!(result["byte_start"], 100);
    assert_eq!(result["byte_end"], 120);
}

#[tokio::test]
async fn search_include_location_falls_back_to_filename_offsets_when_no_content_exists() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model_with_dimensions(&db.pool(), now, "search-location-fallback", 4);
    let client = Arc::new(DeterministicEmbeddingClient::new());

    let doc_path = db.create_file("search-location-fallback.txt");
    let doc_id = insert_document(&db.pool(), now, &doc_path);

    let conn = db.pool().get().unwrap();
    let model = repository::find_model_by_id(&conn, model_id)
        .unwrap()
        .unwrap();
    vector_index::ensure_active_search_model(&conn, &model, now).unwrap();
    drop(conn);

    let vectors = client
        .embed(&model, &["search-location-fallback.txt".to_string()])
        .unwrap();
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
                .body(json_body(
                    r#"{"query":"search-location-fallback.txt","limit":1,"include_location":true}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
    let result = &body.as_array().unwrap()[0];
    assert!(result["match_score"].as_f64().unwrap() > 0.99);
    assert_eq!(result["byte_start"], 0);
    assert_eq!(result["byte_end"], 0);
}

#[tokio::test]
async fn search_empty_query_returns_400() {
    let db = TempDb::new();

    let response = db
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/search")
                .header("content-type", "application/json")
                .body(json_body(r#"{"query":"   "}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
    assert_eq!(body["error"]["code"], "bad_request");
}

#[tokio::test]
async fn search_limit_zero_returns_400() {
    let db = TempDb::new();

    let response = db
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/search")
                .header("content-type", "application/json")
                .body(json_body(r#"{"query":"anything","limit":0}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
    assert_eq!(body["error"]["code"], "bad_request");
}

#[tokio::test]
async fn search_limit_above_max_returns_400() {
    let db = TempDb::new();

    let response = db
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/search")
                .header("content-type", "application/json")
                .body(json_body(r#"{"query":"anything","limit":101}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
    assert_eq!(body["error"]["code"], "bad_request");
}

#[tokio::test]
async fn search_provider_failure_returns_502() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model_with_dimensions(&db.pool(), now, "search-provider-fail", 4);
    let client = Arc::new(DeterministicEmbeddingClient::fail_on_input("fail-search"));

    let conn = db.pool().get().unwrap();
    let model = repository::find_model_by_id(&conn, model_id)
        .unwrap()
        .unwrap();
    vector_index::ensure_active_search_model(&conn, &model, now).unwrap();
    drop(conn);

    let response = db
        .router_with_embedding_client(client)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/search")
                .header("content-type", "application/json")
                .body(json_body(r#"{"query":"please fail-search now","limit":2}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
    assert_eq!(body["error"]["code"], "bad_gateway");
}

#[tokio::test]
async fn search_without_active_model_state_returns_500() {
    let db = TempDb::new();

    let response = db
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/search")
                .header("content-type", "application/json")
                .body(json_body(r#"{"query":"anything","limit":2}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
    assert_eq!(body["error"]["code"], "internal_error");
}

#[tokio::test]
async fn search_empty_index_returns_200_with_empty_results() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model_with_dimensions(&db.pool(), now, "search-empty", 4);
    let client = Arc::new(DeterministicEmbeddingClient::new());

    let conn = db.pool().get().unwrap();
    let model = repository::find_model_by_id(&conn, model_id)
        .unwrap()
        .unwrap();
    vector_index::ensure_active_search_model(&conn, &model, now).unwrap();
    drop(conn);

    let response = db
        .router_with_embedding_client(client)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/search")
                .header("content-type", "application/json")
                .body(json_body(r#"{"query":"nothing-here","limit":5}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
    assert_eq!(body, serde_json::json!([]));
}

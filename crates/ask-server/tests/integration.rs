use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use ask_core::repository;
use ask_core::types::JobType;
use ask_server::worker::process_ingest_folder;
use ask_server::{create_pool, http, migrations};
use axum::body::{Body, Bytes, to_bytes};
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

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
        Self {
            dir,
            pool: Some(pool),
        }
    }

    fn pool(&self) -> http::AppState {
        self.pool.clone().unwrap()
    }

    fn router(&self) -> axum::Router {
        http::router(self.pool())
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
    std::env::temp_dir().join(format!("ask-integration-{unique}"))
}

fn json_body(text: &str) -> Body {
    Body::from(Bytes::from(text.to_owned()))
}

async fn body_text(response: axum::response::Response) -> String {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
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
    let payload = format!(r#"{{"root_path":"{}"}}"#, file.display());

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
    let payload = format!(r#"{{"root_path":"{}"}}"#, dir.display());

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

// ---------------------------------------------------------------------------
// POST /ingest — duplicate rejection (UPSERT WHERE fix)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ingest_duplicate_path_returns_409() {
    let db = TempDb::new();
    let dir = db.create_dir("docs");
    let payload = format!(r#"{{"root_path":"{}"}}"#, dir.display());

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
async fn ingest_different_dirs_both_succeed() {
    let db = TempDb::new();
    let dir_a = db.create_dir("a");
    let dir_b = db.create_dir("b");

    let payload_a = format!(r#"{{"root_path":"{}"}}"#, dir_a.display());
    let payload_b = format!(r#"{{"root_path":"{}"}}"#, dir_b.display());

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
// process_ingest_folder — documents are actually inserted
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
    let payload_json = format!(r#"{{"root_path":"{}"}}"#, ingest_dir.display());
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
    process_ingest_folder(&pool, entry.id, &entry.payload, model.id).unwrap();

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

    // Verify: the job was completed (removed from queue).
    let job_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM job_queue", [], |r| r.get(0))
        .unwrap();
    assert_eq!(job_count, 0, "job should be removed from queue");
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
    let payload_json = format!(r#"{{"root_path":"{}"}}"#, ingest_dir.display());
    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload_json, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);

    let model_id = 1;
    let pool = db.pool();
    process_ingest_folder(&pool, entry.id, &entry.payload, model_id).unwrap();

    // Doc count after first ingest.
    let conn = db.pool().get().unwrap();
    let doc_count_1: i64 = conn
        .query_row("SELECT COUNT(*) FROM documents", [], |r| r.get(0))
        .unwrap();
    assert_eq!(doc_count_1, 1);

    // Second ingest — same files, unchanged.
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload_json, now + 10).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry2 = repository::claim_job(&mut conn, now + 11).unwrap().unwrap();
    drop(conn);
    process_ingest_folder(&pool, entry2.id, &entry2.payload, model_id).unwrap();

    // Doc count should NOT have increased.
    let conn = db.pool().get().unwrap();
    let doc_count_2: i64 = conn
        .query_row("SELECT COUNT(*) FROM documents", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        doc_count_2, 1,
        "unchanged files should not create duplicate documents"
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
    let payload_json = format!(r#"{{"root_path":"{}"}}"#, empty_dir.display());
    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload_json, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);

    let pool = db.pool();
    process_ingest_folder(&pool, entry.id, &entry.payload, 1).unwrap();

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

    let payload_json = r#"{"root_path":"/tmp/ask-nonexistent-12345-unlikely"}"#;
    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, payload_json, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);

    let pool = db.pool();
    process_ingest_folder(&pool, entry.id, &entry.payload, 1).unwrap();

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

    let payload = r#"{"root_path":"/tmp"}"#;
    repository::enqueue_job(&conn, &JobType::IngestFolder, payload, now).unwrap();

    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1)
        .unwrap()
        .expect("job should be claimable");

    assert_eq!(entry.job_type, JobType::IngestFolder);
    assert_eq!(entry.payload, payload);
    assert_eq!(entry.heartbeat, Some(now + 1));

    repository::complete_job(&conn, entry.id).unwrap();

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM job_queue", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0);
}

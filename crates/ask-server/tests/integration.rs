use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use ask_core::repository;
use ask_core::types::DocCategory;
use ask_core::types::JobType;
use ask_server::worker::{backfill_pending_for_model, dispatch_job};
use ask_server::{create_pool, http, migrations};
use axum::body::{Body, Bytes, to_bytes};
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn current_time() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn register_model(pool: &http::AppState, now: i64, name: &str) -> i64 {
    let conn = pool.get().unwrap();
    let model = ask_core::models::EmbeddingModel {
        id: 0,
        name: name.to_string(),
        dimensions: 768,
        chunk_size: 512,
        chunk_overlap: 0,
        created_at: now,
    };
    repository::insert_model(&conn, &model).unwrap()
}

fn insert_document(pool: &http::AppState, now: i64, path: &std::path::Path) -> i64 {
    let metadata = std::fs::metadata(path).unwrap();
    let file_modified_at = metadata
        .modified()
        .unwrap()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let file_type = path
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or("")
        .to_string();
    let document = ask_core::models::Document {
        id: 0,
        filepath: path.to_string_lossy().into_owned(),
        file_type,
        doc_category: DocCategory::Resource,
        file_modified_at,
        file_size: metadata.len() as i64,
        updated_at: now,
    };
    let conn = pool.get().unwrap();
    repository::insert_document(&conn, &document).unwrap()
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
    let counter = TEMP_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("ask-integration-{unique}-{counter}"))
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
    dispatch_job(&pool, &entry, model.id).unwrap();

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
    dispatch_job(&pool, &entry, model_id).unwrap();

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
    dispatch_job(&pool, &entry2, model_id).unwrap();

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
    dispatch_job(&pool, &entry, 1).unwrap();

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
    dispatch_job(&pool, &entry, 1).unwrap();

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
    let payload = format!(r#"{{"root_path":"{}"}}"#, dir.display());

    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);

    let pool = db.pool();
    let err = dispatch_job(&pool, &entry, 999).unwrap_err();
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

    let payload1 = format!(r#"{{"root_path":"{}"}}"#, dir1.display());
    let payload2 = format!(r#"{{"root_path":"{}"}}"#, dir2.display());

    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload1, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry1 = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);
    dispatch_job(&db.pool(), &entry1, model_id).unwrap();

    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload2, now + 10).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry2 = repository::claim_job(&mut conn, now + 11).unwrap().unwrap();
    drop(conn);
    dispatch_job(&db.pool(), &entry2, model_id).unwrap();

    let conn = db.pool().get().unwrap();
    let doc_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM documents", [], |r| r.get(0))
        .unwrap();
    assert_eq!(doc_count, 3, "all files from both dirs ingested");
    let job_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM job_queue", [], |r| r.get(0))
        .unwrap();
    assert_eq!(job_count, 0, "both jobs completed");
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
        "a-b+c*d?.txt",
    ];

    for name in &names {
        std::fs::write(dir.join(name), b"content").unwrap();
    }

    let payload = format!(r#"{{"root_path":"{}"}}"#, dir.display());
    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);
    dispatch_job(&db.pool(), &entry, model_id).unwrap();

    let conn = db.pool().get().unwrap();
    let doc_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM documents", [], |r| r.get(0))
        .unwrap();
    assert_eq!(doc_count, names.len() as i64);

    for name in &names {
        let abs_path = dir.join(name).to_string_lossy().to_string();
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
    std::os::unix::fs::symlink(&target, &link).unwrap();

    let payload = format!(r#"{{"root_path":"{}"}}"#, dir.display());
    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);
    dispatch_job(&db.pool(), &entry, model_id).unwrap();

    let conn = db.pool().get().unwrap();
    let doc_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM documents", [], |r| r.get(0))
        .unwrap();
    assert!(
        doc_count >= 5,
        "all expected top-level filesystem entries should be ingested"
    );

    let content_emb: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM document_embeddings WHERE chunk_type = 'content'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(content_emb, 3, "valid UTF-8 non-empty files are chunked");

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

    let payload = format!(r#"{{"root_path":"{}"}}"#, dir.display());
    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);
    dispatch_job(&db.pool(), &entry, model_id).unwrap();

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
async fn ingest_non_utf8_file_only_gets_filename_embedding() {
    let db = TempDb::new();
    let now = current_time();
    let model_id = register_model(&db.pool(), now, "nonutf8");

    let dir = db.create_dir("nonutf8");
    std::fs::write(dir.join("data.bin"), [0xFF, 0xFE, 0x80, 0x00]).unwrap();
    let payload = format!(r#"{{"root_path":"{}"}}"#, dir.display());

    let conn = db.pool().get().unwrap();
    repository::enqueue_job(&conn, &JobType::IngestFolder, &payload, now).unwrap();
    let mut conn = db.pool().get().unwrap();
    let entry = repository::claim_job(&mut conn, now + 1).unwrap().unwrap();
    drop(conn);
    dispatch_job(&db.pool(), &entry, model_id).unwrap();

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
        "INSERT INTO documents (filepath, file_type, doc_category, file_modified_at, file_size, updated_at)
         VALUES ('/a.txt', 'txt', 'resource', 100, 10, 100),
                ('/b.txt', 'txt', 'resource', 101, 20, 101);
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
        "INSERT INTO documents (filepath, file_type, doc_category, file_modified_at, file_size, updated_at)
         VALUES ('/keep.txt', 'txt', 'resource', 100, 10, 100);
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
        "INSERT INTO documents (filepath, file_type, doc_category, file_modified_at, file_size, updated_at)
         VALUES ('/doc1.txt', 'txt', 'resource', 100, 10, 100),
                ('/doc2.txt', 'txt', 'resource', 200, 20, 200);

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
}

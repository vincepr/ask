use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

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
        Self { dir, pool: Some(pool) }
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

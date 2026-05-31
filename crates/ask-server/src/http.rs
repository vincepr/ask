use std::path::Path;

use ask_core::models::IngestFolderPayload;
use ask_core::{repository, types::JobType};
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::DbPool;
use crate::ingest;

pub type AppState = DbPool;

/// Returns the HTTP router with all routes registered.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ingest", post(ingest))
        .route("/documents/stale", post(mark_stale))
        .with_state(state)
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "healthy" }))
}

/// Discriminated outcome of the ingest validation + enqueue step, produced
/// inside a single `spawn_blocking` call so that no async-runtime thread is
/// ever blocked by a filesystem syscall or a `pool.get()` wait.
enum IngestOutcome {
    Queued,
    NotFound(String),
    NotADirectory(String),
    InvalidPattern(String),
    Conflict(String),
}

#[derive(Deserialize)]
struct IngestRequest {
    root_path: String,
    file_pattern: Option<String>,
}

async fn ingest(
    State(pool): State<AppState>,
    Json(body): Json<IngestRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let result = tokio::task::spawn_blocking(move || {
        let root_path = body.root_path;
        let file_pattern = match ingest::resolve_file_pattern(body.file_pattern.as_deref()) {
            Ok(file_pattern) => file_pattern,
            Err(err) => return IngestOutcome::InvalidPattern(err.to_string()),
        };
        let canonical_root = match std::fs::canonicalize(Path::new(&root_path)) {
            Ok(path) => path,
            Err(_) => {
                return IngestOutcome::NotFound(root_path);
            }
        };

        if !canonical_root.is_dir() {
            return IngestOutcome::NotADirectory(root_path);
        }

        let canonical_root = canonical_root.to_string_lossy().into_owned();
        let payload = IngestFolderPayload {
            root_path: canonical_root,
            file_pattern,
        };

        let payload_json =
            serde_json::to_string(&payload).expect("IngestFolderPayload is always serializable");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time before epoch")
            .as_secs() as i64;

        let conn = match pool.get() {
            Ok(c) => c,
            Err(e) => return IngestOutcome::Conflict(format!("database error: {e}")),
        };

        match ask_core::repository::enqueue_job(&conn, &JobType::IngestFolder, &payload_json, now) {
            Ok(()) => IngestOutcome::Queued,
            Err(e) => IngestOutcome::Conflict(e.to_string()),
        }
    })
    .await
    .map_err(|e| {
        error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            format!("request panicked: {e}"),
        )
    })?;

    match result {
        IngestOutcome::Queued => Ok(Json(
            json!({ "status": "queued", "job_type": "ingest_folder" }),
        )),
        IngestOutcome::NotFound(p) => Err(error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("path does not exist: {p}"),
        )),
        IngestOutcome::NotADirectory(p) => Err(error_response(
            StatusCode::BAD_REQUEST,
            "bad_request",
            format!("path is not a directory: {p}"),
        )),
        IngestOutcome::InvalidPattern(msg) => Err(error_response(
            StatusCode::BAD_REQUEST,
            "bad_request",
            format!("invalid file_pattern regex: {msg}"),
        )),
        IngestOutcome::Conflict(msg) => Err(error_response(StatusCode::CONFLICT, "conflict", msg)),
    }
}

#[derive(Deserialize)]
struct MarkStalePayload {
    document_ids: Vec<i64>,
}

async fn mark_stale(
    State(pool): State<AppState>,
    Json(body): Json<MarkStalePayload>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    if body.document_ids.is_empty() {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "document_ids must not be empty".to_string(),
        ));
    }

    let doc_ids = body.document_ids;

    tokio::task::spawn_blocking(move || {
        let conn = pool.get().map_err(|e| format!("database error: {e}"))?;
        repository::mark_documents_stale(&conn, &doc_ids).map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| {
        error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            format!("request panicked: {e}"),
        )
    })?
    .map_err(|e| error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", e))?;

    Ok(Json(json!({ "status": "ok" })))
}

fn error_response(status: StatusCode, code: &str, message: String) -> (StatusCode, Json<Value>) {
    (
        status,
        Json(json!({ "error": { "code": code, "message": message } })),
    )
}

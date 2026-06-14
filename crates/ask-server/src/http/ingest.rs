use std::path::Path;

use ask_core::models::IngestFolderPayload;
use ask_core::types::JobType;
use axum::{Json, extract::State, http::StatusCode};
use serde::Deserialize;
use serde_json::{Value, json};
use utoipa::ToSchema;

use crate::ingest;

use super::{AppState, error_response};

enum IngestOutcome {
    Queued(JobType),
    NotFound(String),
    NotADirectory(String),
    OutsideAllowedRoot(String),
    InvalidPattern(String),
    Conflict(String),
}

#[derive(Debug, Deserialize, ToSchema)]
pub(super) struct IngestRequest {
    #[schema(example = "/resources")]
    root_path: String,
    file_pattern: Option<String>,
}

#[utoipa::path(
    post,
    path = "/ingest",
    request_body = IngestRequest
)]
pub(crate) async fn ingest(
    State(state): State<AppState>,
    Json(body): Json<IngestRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    queue_ingest_request(state, body, JobType::IngestFolder).await
}

#[utoipa::path(
    post,
    path = "/ingest/git",
    request_body = IngestRequest
)]
pub(crate) async fn ingest_git(
    State(state): State<AppState>,
    Json(body): Json<IngestRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    queue_ingest_request(state, body, JobType::IngestFolderGit).await
}

async fn queue_ingest_request(
    state: AppState,
    body: IngestRequest,
    job_type: JobType,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let pool = state.pool().clone();
    let resource_root = state.resource_root().to_path_buf();

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

        if !canonical_root.starts_with(&resource_root) {
            return IngestOutcome::OutsideAllowedRoot(root_path);
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

        match ask_core::repository::enqueue_job(&conn, &job_type, &payload_json, now) {
            Ok(()) => IngestOutcome::Queued(job_type),
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
        IngestOutcome::Queued(job_type) => Ok(Json(
            json!({ "status": "queued", "job_type": job_type.as_str() }),
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
        IngestOutcome::OutsideAllowedRoot(p) => Err(error_response(
            StatusCode::FORBIDDEN,
            "forbidden",
            format!("path is outside the configured resource root: {p}"),
        )),
        IngestOutcome::InvalidPattern(msg) => Err(error_response(
            StatusCode::BAD_REQUEST,
            "bad_request",
            format!("invalid file_pattern regex: {msg}"),
        )),
        IngestOutcome::Conflict(msg) => Err(error_response(StatusCode::CONFLICT, "conflict", msg)),
    }
}

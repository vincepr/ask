use std::path::Path;

use ask_core::models::IngestFolderPayload;
use ask_core::types::JobType;
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use serde_json::{Value, json};

use crate::DbPool;

pub type AppState = DbPool;

/// Returns the HTTP router with all routes registered.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ingest", post(ingest))
        .with_state(state)
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "healthy" }))
}

async fn ingest(
    State(pool): State<AppState>,
    Json(body): Json<IngestFolderPayload>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let root_path = body.root_path.clone();

    let path_exists = tokio::task::spawn_blocking(move || {
        let p = Path::new(&root_path);
        (p.exists(), p.is_dir())
    })
    .await
    .map_err(|e| {
        error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            format!("path check panicked: {e}"),
        )
    })?;

    if !path_exists.0 {
        return Err(error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("path does not exist: {}", body.root_path),
        ));
    }

    if !path_exists.1 {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "bad_request",
            format!("path is not a directory: {}", body.root_path),
        ));
    }

    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<Value> {
        let payload_json = serde_json::to_string(&body)?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time before epoch")
            .as_secs() as i64;
        let conn = pool.get()?;
        ask_core::repository::enqueue_job(&conn, &JobType::IngestFolder, &payload_json, now)
            .map(|_| json!({ "status": "queued", "job_type": "ingest_folder" }))
    })
    .await
    .map_err(|e| {
        error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            format!("enqueue panicked: {e}"),
        )
    })?;

    let inner =
        result.map_err(|e| error_response(StatusCode::CONFLICT, "conflict", e.to_string()))?;

    Ok(Json(inner))
}

fn error_response(status: StatusCode, code: &str, message: String) -> (StatusCode, Json<Value>) {
    (
        status,
        Json(json!({ "error": { "code": code, "message": message } })),
    )
}

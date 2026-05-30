use std::sync::{Arc, Mutex};

use ask_core::models::IngestFolderPayload;
use ask_core::types::JobType;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
};
use serde_json::json;

/// Shared application state held by every request handler.
pub type AppState = Arc<Mutex<rusqlite::Connection>>;

/// Returns the HTTP router with all routes registered.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ingest/{*filepath}", post(ingest))
        .with_state(state)
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({ "status": "healthy" }))
}

/// Enqueue an IngestFolder job for the given filepath.
async fn ingest(
    State(db): State<AppState>,
    Path(filepath): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    if filepath.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "filepath must not be empty" })),
        ));
    }

    let payload = IngestFolderPayload {
        root_path: filepath,
    };
    let payload_json =
        serde_json::to_string(&payload).expect("IngestFolderPayload is always serializable");

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before epoch")
        .as_secs() as i64;

    let conn = db.lock().expect("db lock poisoned");
    match ask_core::repository::enqueue_job(&conn, &JobType::IngestFolder, &payload_json, now) {
        Ok(()) => Ok(Json(
            json!({ "status": "queued", "job_type": "ingest_folder" }),
        )),
        Err(e) => Err((
            StatusCode::CONFLICT,
            Json(json!({ "error": e.to_string() })),
        )),
    }
}

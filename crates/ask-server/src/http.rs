use std::path::{Path, PathBuf};

use ask_core::models::IngestFolderPayload;
use ask_core::{repository, types::JobType};
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::Html,
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

use crate::DbPool;
use crate::ingest;

/// Shared HTTP application state.
#[derive(Clone)]
pub struct AppState {
    pool: DbPool,
    resource_root: PathBuf,
}

impl AppState {
    /// Creates HTTP state with a canonicalized ingest sandbox root.
    ///
    /// # Arguments
    ///
    /// * `pool` - Shared SQLite connection pool.
    /// * `resource_dir` - Configured filesystem root allowed for ingest requests.
    ///
    /// # Returns
    ///
    /// Canonicalized application state.
    ///
    /// # Errors
    ///
    /// Returns an error if `resource_dir` cannot be canonicalized.
    pub fn new(pool: DbPool, resource_dir: impl AsRef<Path>) -> std::io::Result<Self> {
        Ok(Self {
            pool,
            resource_root: std::fs::canonicalize(resource_dir)?,
        })
    }

    /// Returns the shared SQLite connection pool.
    #[must_use]
    pub fn pool(&self) -> &DbPool {
        &self.pool
    }
}

impl std::ops::Deref for AppState {
    type Target = DbPool;

    fn deref(&self) -> &Self::Target {
        &self.pool
    }
}

/// Returns the HTTP router with all routes registered.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(api_reference))
        .route("/health", get(health))
        .route("/api.html", get(api_reference))
        .route("/ingest", post(ingest))
        .route("/documents/stale", post(mark_stale))
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .with_state(state)
}

#[derive(OpenApi)]
#[openapi(
    paths(health, ingest, mark_stale),
    components(schemas(IngestRequest, MarkStalePayload))
)]
struct ApiDoc;

#[utoipa::path(
    get,
    path = "/health"
)]
async fn health() -> Json<Value> {
    Json(json!({ "status": "healthy" }))
}

async fn api_reference() -> Html<&'static str> {
    Html(include_str!("../static/api.html"))
}

/// Discriminated outcome of the ingest validation + enqueue step, produced
/// inside a single `spawn_blocking` call so that no async-runtime thread is
/// ever blocked by a filesystem syscall or a `pool.get()` wait.
enum IngestOutcome {
    Queued,
    NotFound(String),
    NotADirectory(String),
    OutsideAllowedRoot(String),
    InvalidPattern(String),
    Conflict(String),
}

#[derive(Debug, Deserialize, ToSchema)]
struct IngestRequest {
    root_path: String,
    file_pattern: Option<String>,
}

#[utoipa::path(
    post,
    path = "/ingest",
    request_body = IngestRequest
)]
async fn ingest(
    State(state): State<AppState>,
    Json(body): Json<IngestRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let pool = state.pool().clone();
    let resource_root = state.resource_root.clone();

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

#[derive(Debug, Deserialize, ToSchema)]
struct MarkStalePayload {
    document_ids: Vec<i64>,
}

#[utoipa::path(
    post,
    path = "/documents/stale",
    request_body = MarkStalePayload
)]
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

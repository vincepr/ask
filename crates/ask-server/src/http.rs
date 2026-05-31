use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, anyhow};
use ask_core::models::IngestFolderPayload;
use ask_core::models::{DocumentSearchResult, EmbeddingModel};
use ask_core::{repository, types::JobType};
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::Html,
    routing::{get, post},
};
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

use crate::DbPool;
use crate::embeddings::{DeterministicEmbeddingClient, EmbeddingClient, SharedEmbeddingClient};
use crate::ingest;

/// Shared HTTP application state.
#[derive(Clone)]
pub struct AppState {
    pool: DbPool,
    resource_root: PathBuf,
    embedding_client: SharedEmbeddingClient,
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
            embedding_client: Arc::new(DeterministicEmbeddingClient::new()),
        })
    }

    /// Returns a copy of this state with a custom query embedding client.
    #[must_use]
    pub fn with_embedding_client(mut self, embedding_client: SharedEmbeddingClient) -> Self {
        self.embedding_client = embedding_client;
        self
    }

    /// Returns the shared SQLite connection pool.
    #[must_use]
    pub fn pool(&self) -> &DbPool {
        &self.pool
    }

    /// Returns the shared embedding client used by `/search`.
    #[must_use]
    pub fn embedding_client(&self) -> SharedEmbeddingClient {
        self.embedding_client.clone()
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
        .route("/search", post(search))
        .route("/ingest", post(ingest))
        .route("/ingest/git", post(ingest_git))
        .route("/documents/stale", post(mark_stale))
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .with_state(state)
}

#[derive(OpenApi)]
#[openapi(
    paths(health, search, ingest, ingest_git, mark_stale),
    components(schemas(IngestRequest, MarkStalePayload, SearchRequest, SearchDocumentResult))
)]
struct ApiDoc;

const DEFAULT_SEARCH_LIMIT: usize = 10;
const MAX_SEARCH_LIMIT: usize = 100;
const SEARCH_RAW_LIMIT_MULTIPLIER: usize = 4;
const SEARCH_RAW_LIMIT_CAP: usize = 400;

#[utoipa::path(get, path = "/health")]
async fn health() -> Json<Value> {
    Json(json!({ "status": "healthy" }))
}

async fn api_reference() -> Html<&'static str> {
    Html(include_str!("../static/api.html"))
}

#[derive(Debug, Deserialize, ToSchema)]
struct SearchRequest {
    query: String,
    limit: Option<usize>,
    include_location: Option<bool>,
}

#[derive(Debug, Serialize, ToSchema)]
struct SearchDocumentResult {
    filepath: String,
    match_score: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    byte_start: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    byte_end: Option<i64>,
}

#[utoipa::path(
    post,
    path = "/search",
    request_body = SearchRequest,
    responses(
        (status = 200, description = "Search results", body = [SearchDocumentResult]),
        (status = 400, description = "Invalid input"),
        (status = 500, description = "Internal server error"),
        (status = 502, description = "Embedding provider failure")
    )
)]
async fn search(
    State(state): State<AppState>,
    Json(body): Json<SearchRequest>,
) -> Result<Json<Vec<SearchDocumentResult>>, (StatusCode, Json<Value>)> {
    let query = body.query.trim().to_string();
    if query.is_empty() {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "query must not be empty".to_string(),
        ));
    }

    let limit = body.limit.unwrap_or(DEFAULT_SEARCH_LIMIT);
    if limit == 0 {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "limit must be greater than 0".to_string(),
        ));
    }
    if limit > MAX_SEARCH_LIMIT {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "bad_request",
            format!("limit must be less than or equal to {MAX_SEARCH_LIMIT}"),
        ));
    }

    let include_location = body.include_location.unwrap_or(false);
    let pool = state.pool().clone();
    let embedding_client = state.embedding_client();

    let outcome = tokio::task::spawn_blocking(move || {
        search_documents(
            &pool,
            embedding_client.as_ref(),
            query,
            limit,
            include_location,
        )
    })
    .await
    .map_err(|err| {
        error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            format!("request panicked: {err}"),
        )
    })?;

    match outcome {
        Ok(response) => Ok(Json(response)),
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
}

/// Discriminated outcome of the ingest validation + enqueue step, produced
/// inside a single `spawn_blocking` call so that no async-runtime thread is
/// ever blocked by a filesystem syscall or a `pool.get()` wait.
enum IngestOutcome {
    Queued(JobType),
    NotFound(String),
    NotADirectory(String),
    OutsideAllowedRoot(String),
    InvalidPattern(String),
    Conflict(String),
}

#[derive(Debug, Deserialize, ToSchema)]
struct IngestRequest {
    #[schema(example = "/resources")]
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
    queue_ingest_request(state, body, JobType::IngestFolder).await
}

#[utoipa::path(
    post,
    path = "/ingest/git",
    request_body = IngestRequest
)]
async fn ingest_git(
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
        let mut conn = pool.get().map_err(|e| format!("database error: {e}"))?;
        repository::mark_documents_stale(&mut conn, &doc_ids).map_err(|e| e.to_string())
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

#[derive(Debug)]
enum SearchFailure {
    BadGateway(String),
    Internal(String),
}

fn search_documents(
    pool: &DbPool,
    embedding_client: &dyn EmbeddingClient,
    query: String,
    limit: usize,
    include_location: bool,
) -> Result<Vec<SearchDocumentResult>, SearchFailure> {
    let model = {
        let conn = pool
            .get()
            .map_err(|err| SearchFailure::Internal(format!("database error: {err}")))?;
        load_active_model(&conn).map_err(|err| {
            SearchFailure::Internal(format!("failed to load active model: {err:#}"))
        })?
    };

    let vectors = embedding_client
        .embed(&model, std::slice::from_ref(&query))
        .map_err(|err| SearchFailure::BadGateway(format!("failed to embed query: {err:#}")))?;
    let query_embedding = vectors.first().ok_or_else(|| {
        SearchFailure::BadGateway("embedding provider returned no vectors".into())
    })?;

    let conn = pool
        .get()
        .map_err(|err| SearchFailure::Internal(format!("database error: {err}")))?;
    let raw_limit = expanded_raw_limit(limit);
    let hits = repository::search_documents_by_embedding(&conn, &model, query_embedding, raw_limit)
        .map_err(|err| SearchFailure::Internal(format!("failed to run vector search: {err:#}")))?;
    Ok(collapse_to_documents(hits, limit, include_location))
}

fn load_active_model(conn: &rusqlite::Connection) -> anyhow::Result<EmbeddingModel> {
    let active_model_id = conn
        .query_row(
            "SELECT active_model_id
             FROM embedding_search_state
             WHERE singleton_id = 1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .context("active embedding_search_state row is missing")?
        .ok_or_else(|| anyhow!("active embedding_search_state row is missing"))?;

    repository::find_model_by_id(conn, active_model_id)?
        .ok_or_else(|| anyhow!("active model {active_model_id} does not exist"))
}

fn collapse_to_documents(
    hits: Vec<DocumentSearchResult>,
    limit: usize,
    include_location: bool,
) -> Vec<SearchDocumentResult> {
    let mut seen_documents = HashSet::with_capacity(limit);
    let mut results = Vec::with_capacity(limit);

    for hit in hits {
        if !seen_documents.insert(hit.document_id) {
            continue;
        }

        let (byte_start, byte_end) = if include_location {
            (Some(hit.chunk_start), Some(hit.chunk_end))
        } else {
            (None, None)
        };

        results.push(SearchDocumentResult {
            filepath: hit.filepath,
            match_score: distance_to_score(hit.distance),
            byte_start,
            byte_end,
        });

        if results.len() == limit {
            break;
        }
    }

    results
}

fn distance_to_score(distance: f64) -> f64 {
    ((2.0 - distance) / 2.0).clamp(0.0, 1.0)
}

fn expanded_raw_limit(limit: usize) -> usize {
    limit
        .saturating_mul(SEARCH_RAW_LIMIT_MULTIPLIER)
        .min(SEARCH_RAW_LIMIT_CAP)
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use ask_core::migrations;
    use ask_core::models::EmbeddingModel;
    use ask_core::repository;

    use super::*;
    use crate::create_pool;
    use crate::embeddings::EmbeddingClient;
    use crate::vector_index;

    struct BlockingEmbeddingClient {
        entered_tx: Mutex<Option<mpsc::Sender<()>>>,
        release_rx: Mutex<mpsc::Receiver<()>>,
    }

    impl BlockingEmbeddingClient {
        fn new(entered_tx: mpsc::Sender<()>, release_rx: mpsc::Receiver<()>) -> Self {
            Self {
                entered_tx: Mutex::new(Some(entered_tx)),
                release_rx: Mutex::new(release_rx),
            }
        }
    }

    impl EmbeddingClient for BlockingEmbeddingClient {
        fn embed(
            &self,
            model: &EmbeddingModel,
            inputs: &[String],
        ) -> anyhow::Result<Vec<Vec<f32>>> {
            if let Some(tx) = self.entered_tx.lock().unwrap().take() {
                tx.send(()).unwrap();
            }
            self.release_rx.lock().unwrap().recv().unwrap();
            Ok(inputs
                .iter()
                .map(|_| vec![0.0_f32; model.dimensions as usize])
                .collect())
        }
    }

    #[test]
    fn search_releases_pool_connection_before_embedding_call() {
        let temp_dir = std::env::temp_dir().join(format!(
            "ask-http-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let db_path = temp_dir.join("ask.sqlite3");
        let pool = create_pool(&db_path.to_string_lossy()).unwrap();
        let mut conn = pool.get().unwrap();
        migrations::apply_pending_migrations(&mut conn).unwrap();

        let now = 100_i64;
        let model = EmbeddingModel {
            id: 0,
            name: "search-release".to_string(),
            dimensions: 1,
            chunk_size: 16,
            chunk_overlap: 0,
            created_at: now,
        };
        let model_id = repository::insert_model(&conn, &model).unwrap();
        conn.execute(
            "INSERT INTO embedding_search_state (singleton_id, active_model_id, dimensions, updated_at)
             VALUES (1, ?1, ?2, ?3)",
            rusqlite::params![model_id, 1_i64, now],
        )
        .unwrap();
        let model = EmbeddingModel {
            id: model_id,
            ..model
        };
        vector_index::ensure_active_search_model(&conn, &model, now).unwrap();
        drop(conn);

        let held_connections = vec![
            pool.get().unwrap(),
            pool.get().unwrap(),
            pool.get().unwrap(),
        ];

        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let client = Arc::new(BlockingEmbeddingClient::new(entered_tx, release_rx));
        let pool_for_thread = pool.clone();
        let client_for_thread = client.clone();
        let handle = std::thread::spawn(move || {
            search_documents(
                &pool_for_thread,
                client_for_thread.as_ref(),
                "query".to_string(),
                10,
                false,
            )
        });

        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("search should reach embedding call");

        let extra_conn = pool
            .get_timeout(Duration::from_millis(200))
            .expect("search should release its DB slot before embedding waits");
        drop(extra_conn);

        release_tx.send(()).unwrap();
        let results = handle.join().unwrap().unwrap();
        assert!(results.is_empty());
        drop(held_connections);
        let _ = std::fs::remove_dir_all(temp_dir);
    }
}

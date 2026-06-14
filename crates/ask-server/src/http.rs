mod documents;
mod health;
mod ingest;
mod progress;
mod search;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, anyhow};
use ask_core::models::EmbeddingModel;
use ask_core::repository;
use axum::{
    Json, Router,
    http::StatusCode,
    response::Html,
    routing::{get, post},
};
use rusqlite::OptionalExtension;
use serde_json::{Value, json};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use self::documents::MarkStalePayload;
use self::ingest::IngestRequest;
use self::progress::EmbeddingStatsResponse;
use self::search::{SearchDocumentResult, SearchRequest};
use crate::DbPool;
use crate::config::{
    Config, DEFAULT_DATA_DIR, DEFAULT_DATABASE_POOL_SIZE, DEFAULT_EMBEDDING_MAX_BATCH_SIZE,
    DEFAULT_TEI_BASE_URL, DEFAULT_WORKER_COUNT,
};
use crate::embeddings::{DeterministicEmbeddingClient, SharedEmbeddingClient};

/// Non-secret runtime configuration surfaced to the frontend and diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    /// Filesystem path to the persistent data directory.
    pub data_dir: String,
    /// Maximum number of SQLite connections in the shared pool.
    pub database_pool_size: usize,
    /// Filesystem path to the ingest/resource root directory.
    pub resource_dir: String,
    /// Embedding provider mode label.
    pub embedding_mode: String,
    /// Embedding provider base URL.
    pub embedding_base_url: String,
    /// Maximum embedding batch size for outbound provider requests.
    pub embedding_max_batch_size: usize,
    /// Number of passive embedding workers.
    pub embedding_worker_count: usize,
}

impl RuntimeConfig {
    /// Builds a runtime summary from the loaded server configuration.
    #[must_use]
    pub fn from_config(config: &Config) -> Self {
        Self {
            data_dir: config.data_dir.clone(),
            database_pool_size: config.database_pool_size,
            resource_dir: config.resource_dir.clone(),
            embedding_mode: config.embedding_provider.mode_name().to_string(),
            embedding_base_url: config.embedding_provider.base_url().to_string(),
            embedding_max_batch_size: config.embedding_max_batch_size,
            embedding_worker_count: config.embedding_worker_count,
        }
    }
}

/// Shared HTTP application state.
#[derive(Clone)]
pub struct AppState {
    pool: DbPool,
    resource_root: PathBuf,
    embedding_client: SharedEmbeddingClient,
    runtime_config: RuntimeConfig,
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
        let resource_root = std::fs::canonicalize(resource_dir)?;
        Ok(Self {
            pool,
            resource_root: resource_root.clone(),
            embedding_client: Arc::new(DeterministicEmbeddingClient::new()),
            runtime_config: RuntimeConfig {
                data_dir: DEFAULT_DATA_DIR.to_string(),
                database_pool_size: DEFAULT_DATABASE_POOL_SIZE,
                resource_dir: resource_root.display().to_string(),
                embedding_mode: "tei".to_string(),
                embedding_base_url: DEFAULT_TEI_BASE_URL.to_string(),
                embedding_max_batch_size: DEFAULT_EMBEDDING_MAX_BATCH_SIZE,
                embedding_worker_count: DEFAULT_WORKER_COUNT,
            },
        })
    }

    /// Returns a copy of this state with a custom query embedding client.
    #[must_use]
    pub fn with_embedding_client(mut self, embedding_client: SharedEmbeddingClient) -> Self {
        self.embedding_client = embedding_client;
        self
    }

    /// Returns a copy of this state with a custom runtime configuration summary.
    #[must_use]
    pub fn with_runtime_config(mut self, runtime_config: RuntimeConfig) -> Self {
        self.runtime_config = runtime_config;
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

    /// Returns the canonicalized ingest sandbox root.
    #[must_use]
    pub fn resource_root(&self) -> &Path {
        &self.resource_root
    }

    /// Returns the non-secret runtime configuration summary.
    #[must_use]
    pub fn runtime_config(&self) -> &RuntimeConfig {
        &self.runtime_config
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
        .route("/health", get(health::health))
        .route("/embedding/stats", get(progress::embedding_stats))
        .route("/api.html", get(api_reference))
        .route("/search", post(search::search))
        .route("/ingest", post(ingest::ingest))
        .route("/ingest/git", post(ingest::ingest_git))
        .route("/documents/stale", post(documents::mark_stale))
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .with_state(state)
}

#[derive(OpenApi)]
#[openapi(
    paths(
        health::health,
        progress::embedding_stats,
        search::search,
        ingest::ingest,
        ingest::ingest_git,
        documents::mark_stale
    ),
    components(schemas(
        EmbeddingStatsResponse,
        IngestRequest,
        MarkStalePayload,
        SearchRequest,
        SearchDocumentResult
    ))
)]
struct ApiDoc;

async fn api_reference() -> Html<&'static str> {
    Html(include_str!("../static/api.html"))
}

pub(super) fn error_response(
    status: StatusCode,
    code: &str,
    message: String,
) -> (StatusCode, Json<Value>) {
    (
        status,
        Json(json!({ "error": { "code": code, "message": message } })),
    )
}

pub(super) fn load_active_model(conn: &rusqlite::Connection) -> anyhow::Result<EmbeddingModel> {
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
        ) -> std::result::Result<Vec<Vec<f32>>, crate::embeddings::EmbeddingError> {
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
            search::search_documents(
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

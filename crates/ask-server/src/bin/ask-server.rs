use anyhow::Result;
use ask_core::migrations;
use ask_core::{WORKSPACE_NAME, workspace_members};
use ask_server::embeddings::HttpEmbeddingClient;
use ask_server::startup::StartupSummaryKind;
use ask_server::{config, create_pool_with_max_size, http, startup, vector_index, worker};
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .try_init()
        .ok();
    let member_count = workspace_members().len();
    let config = config::load()?;
    let bind_address = config.bind_address();
    let sqlite_path = config.sqlite_path();
    let pool = create_pool_with_max_size(&sqlite_path, config.database_pool_size)?;

    // Run migrations on a dedicated connection.
    {
        let mut conn = pool.get()?;
        let applied_count = migrations::apply_pending_migrations(&mut conn)?;
        info!(applied_count, "applied pending migrations");
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;

    let startup_state = {
        let conn = pool.get()?;
        startup::reconcile_embedding_startup(&conn, config.embedding_identity(), now)?
    };
    let model = startup_state.model.clone();
    info!(
        model = %model.name,
        backfilled_documents = startup_state.backfilled_documents,
        seeded_jobs = startup_state.seeded_jobs,
        "reconciled embedding startup state"
    );
    match startup_state.summary_kind {
        StartupSummaryKind::Empty => {
            info!(
                document_count = startup_state.document_count,
                recoverable_pairs = startup_state.recoverable_pairs,
                seeded_jobs = startup_state.seeded_jobs,
                next_action = "POST /ingest",
                "startup summary: no documents ingested yet"
            );
        }
        StartupSummaryKind::Recovered => {
            info!(
                document_count = startup_state.document_count,
                recoverable_pairs = startup_state.recoverable_pairs,
                seeded_jobs = startup_state.seeded_jobs,
                "startup summary: recoverable embedding work is pending"
            );
        }
        StartupSummaryKind::Idle => {
            info!(
                document_count = startup_state.document_count,
                recoverable_pairs = startup_state.recoverable_pairs,
                seeded_jobs = startup_state.seeded_jobs,
                "startup summary: corpus is currently idle"
            );
        }
    }

    {
        let conn = pool.get()?;
        let backfilled = vector_index::ensure_active_search_model(&conn, &model, now)?;
        info!(model = %model.name, backfilled, "ensured sqlite-vec search index");
    }

    let listener = tokio::net::TcpListener::bind(&bind_address).await?;
    let embedding_client = Arc::new(HttpEmbeddingClient::new(
        config.embedding_provider.clone(),
        config.embedding_max_batch_size,
    )?);

    info!(
        workspace = WORKSPACE_NAME,
        member_count,
        sqlite_path,
        resource_dir = %config.resource_dir,
        model = %model.name,
        model_dimensions = model.dimensions,
        model_chunk_size = model.chunk_size,
        model_chunk_overlap = model.chunk_overlap,
        embedding_mode = config.embedding_provider.mode_name(),
        embedding_base_url = config.embedding_provider.base_url(),
        embedding_worker_count = config.embedding_worker_count,
        database_pool_size = config.database_pool_size,
        bind_address,
        "starting ask-server"
    );

    let runtime_config = http::RuntimeConfig::from_config(&config);
    let app_state =
        http::AppState::new_with_data_dir(pool.clone(), &config.resource_dir, &config.data_dir)?
            .with_runtime_config(runtime_config)
            .with_embedding_client(embedding_client.clone());

    worker::spawn(
        pool.clone(),
        model.id,
        embedding_client,
        config.embedding_worker_count,
    );
    axum::serve(listener, http::router(app_state)).await?;

    Ok(())
}

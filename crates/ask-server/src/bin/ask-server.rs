use anyhow::Result;
use ask_core::migrations;
use ask_core::models::EmbeddingModel;
use ask_core::repository;
use ask_core::{WORKSPACE_NAME, workspace_members};
use ask_server::embeddings::HttpEmbeddingClient;
use ask_server::{config, create_pool, http, worker};
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
    let pool = create_pool(&sqlite_path)?;

    // Run migrations on a dedicated connection.
    {
        let mut conn = pool.get()?;
        let applied_count = migrations::apply_pending_migrations(&mut conn)?;
        info!(applied_count, "applied pending migrations");
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;

    let model = {
        let conn = pool.get()?;
        let identity = config.embedding_identity();
        match repository::find_model_by_identity(&conn, &identity)? {
            Some(m) => m,
            None => {
                let m = EmbeddingModel {
                    id: 0,
                    name: identity.name,
                    dimensions: identity.dimensions,
                    chunk_size: identity.chunk_size,
                    chunk_overlap: identity.chunk_overlap,
                    created_at: now,
                };
                let model = EmbeddingModel {
                    id: repository::insert_model(&conn, &m)?,
                    ..m
                };
                let seeded = worker::backfill_pending_for_model(&conn, &model, now)?;
                info!(model = %model.name, seeded, "registered new embedding model");
                model
            }
        }
    };

    {
        let conn = pool.get()?;
        let backfilled = ask_server::vector_index::ensure_active_search_model(&conn, &model, now)?;
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
        bind_address,
        "starting ask-server"
    );

    let app_state = http::AppState::new(pool.clone(), &config.resource_dir)?
        .with_embedding_client(embedding_client.clone());

    worker::spawn(pool.clone(), model.id, embedding_client);
    axum::serve(listener, http::router(app_state)).await?;

    Ok(())
}

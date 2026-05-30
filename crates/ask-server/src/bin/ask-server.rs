use anyhow::Result;
use ask_core::models::EmbeddingModel;
use ask_core::repository;
use ask_core::{WORKSPACE_NAME, workspace_members};
use ask_server::{config, create_pool, http, migrations, worker};
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
        match repository::find_model_by_name(&conn, &config.embedding_model)? {
            Some(m) => m,
            None => {
                let m = EmbeddingModel {
                    id: 0,
                    name: config.embedding_model.clone(),
                    dimensions: config.embedding_dimensions,
                    chunk_size: config.embedding_chunk_size,
                    chunk_overlap: config.embedding_chunk_overlap,
                    created_at: now,
                };
                let model_id = repository::insert_model(&conn, &m)?;
                let seeded = repository::seed_pending_for_all_docs(&conn, model_id, now)?;
                info!(model = %m.name, seeded, "registered new embedding model");
                EmbeddingModel { id: model_id, ..m }
            }
        }
    };

    let listener = tokio::net::TcpListener::bind(&bind_address).await?;

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

    worker::spawn(pool.clone(), model.id);
    axum::serve(listener, http::router(pool)).await?;

    Ok(())
}

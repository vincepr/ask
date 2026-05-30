use anyhow::Result;
use ask_core::repository;
use ask_core::types::EmbeddingModel;
use ask_core::{WORKSPACE_NAME, workspace_members};
use ask_server::{config, http, migrations, open_database};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let member_count = workspace_members().len();
    let config = config::load()?;
    let bind_address = config.bind_address();
    let sqlite_path = config.sqlite_path();
    let mut connection = open_database(&sqlite_path)?;
    let applied_count = migrations::apply_pending_migrations(&mut connection)?;

    // Auto-register the embedding model if it doesn't exist yet.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;

    let model = match repository::find_model_by_name(&connection, &config.embedding_model)? {
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
            let model_id = repository::insert_model(&connection, &m)?;
            let seeded = repository::seed_pending_for_all_docs(&connection, model_id, now)?;
            println!(
                "Registered new model '{}' with {seeded} pending documents.",
                m.name
            );
            EmbeddingModel { id: model_id, ..m }
        }
    };

    let listener = tokio::net::TcpListener::bind(&bind_address).await?;

    println!("Starting {WORKSPACE_NAME} server workspace with {member_count} member crates.");
    println!("Using SQLite database at {sqlite_path}.");
    println!("Resource directory: {}.", config.resource_dir);
    println!(
        "Embedding model '{}' ({} dimensions, chunk size {}, overlap {}).",
        model.name, model.dimensions, model.chunk_size, model.chunk_overlap
    );
    println!(
        "Embedding provider mode '{}' at {}.",
        config.embedding_provider.mode_name(),
        config.embedding_provider.base_url()
    );
    println!("Applied {applied_count} pending migrations.");
    println!("Listening on http://{bind_address}.");

    axum::serve(listener, http::router()).await?;

    Ok(())
}

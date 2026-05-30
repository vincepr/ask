use anyhow::Result;
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
    let listener = tokio::net::TcpListener::bind(&bind_address).await?;

    println!("Starting {WORKSPACE_NAME} server workspace with {member_count} member crates.");
    println!("Using SQLite database at {sqlite_path}.");
    println!("Resource directory: {}.", config.resource_dir);
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

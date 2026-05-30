use anyhow::Result;
use ask_core::{WORKSPACE_NAME, workspace_members};
use ask_server::{config, http, migrations, open_database};

#[tokio::main]
async fn main() -> Result<()> {
    let member_count = workspace_members().len();
    let config = config::load()?;
    let bind_address = config.bind_address();
    let mut connection = open_database(&config.sqlite_path)?;
    let applied_count = migrations::apply_pending_migrations(&mut connection)?;
    let listener = tokio::net::TcpListener::bind(&bind_address).await?;

    println!("Starting {WORKSPACE_NAME} server workspace with {member_count} member crates.");
    println!("Using SQLite database at {}.", config.sqlite_path);
    println!("Applied {applied_count} pending migrations.");
    println!("Listening on http://{bind_address}.");

    axum::serve(listener, http::router()).await?;

    Ok(())
}

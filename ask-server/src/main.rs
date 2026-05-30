mod migrations;

use std::path::Path;

use anyhow::{Context, Result};
use ask_core::{WORKSPACE_NAME, workspace_members};
use rusqlite::Connection;

const SQLITE_PATH_ENV: &str = "ASK_SERVER_SQLITE_PATH";
const DEFAULT_SQLITE_PATH: &str = "data/ask.sqlite3";

fn main() -> Result<()> {
    let member_count = workspace_members().len();
    let database_path = database_path();
    let mut connection = open_database(&database_path)?;
    let applied_count = migrations::apply_pending_migrations(&mut connection)?;

    println!("Starting {WORKSPACE_NAME} server workspace with {member_count} member crates.");
    println!("Using SQLite database at {database_path}.");
    println!("Applied {applied_count} pending migrations.");

    Ok(())
}

fn database_path() -> String {
    std::env::var(SQLITE_PATH_ENV).unwrap_or_else(|_| String::from(DEFAULT_SQLITE_PATH))
}

fn open_database(database_path: &str) -> Result<Connection> {
    let path = Path::new(database_path);

    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create parent directory for database path {}",
                database_path
            )
        })?;
    }

    Connection::open(path)
        .with_context(|| format!("failed to open SQLite database at {}", database_path))
}

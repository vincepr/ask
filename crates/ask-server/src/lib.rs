pub mod config;
pub mod http;
pub mod migrations;
pub mod worker;

use std::path::Path;

use anyhow::{Context, Result};

/// Opens the SQLite database and creates parent directories when needed.
///
/// # Errors
///
/// Returns an error if the parent directory cannot be created or if the SQLite
/// database cannot be opened.
pub fn open_database(database_path: &str) -> Result<rusqlite::Connection> {
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

    rusqlite::Connection::open(path)
        .with_context(|| format!("failed to open SQLite database at {}", database_path))
}

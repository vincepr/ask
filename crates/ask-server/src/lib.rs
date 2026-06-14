pub mod config;
pub mod embeddings;
pub mod http;
mod ingest;
pub mod startup;
pub mod vector_index;
pub mod worker;

use std::path::Path;

use anyhow::{Context, Result};
use r2d2::ManageConnection;

use crate::config::DEFAULT_DATABASE_POOL_SIZE;

/// Opens the SQLite database and creates parent directories when needed.
///
/// # Errors
///
/// Returns an error if the parent directory cannot be created or if the SQLite
/// database cannot be opened.
pub fn open_database(database_path: &str) -> Result<rusqlite::Connection> {
    vector_index::register_sqlite_vec()?;
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

/// Shared database connection pool for the entire server.
pub type DbPool = r2d2::Pool<SqliteConnectionManager>;

/// Manages SQLite connections for the `r2d2` pool.
pub struct SqliteConnectionManager {
    database_path: String,
}

impl ManageConnection for SqliteConnectionManager {
    type Connection = rusqlite::Connection;
    type Error = rusqlite::Error;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let conn = rusqlite::Connection::open(&self.database_path)?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA busy_timeout=5000;
             PRAGMA foreign_keys=ON;",
        )?;
        Ok(conn)
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.execute_batch("SELECT 1")
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

/// Creates a connection pool for the given database file path.
///
/// Parent directories are created automatically. Each new connection has
/// WAL mode, a 5-second busy timeout, and foreign keys enabled.
///
/// # Errors
///
/// Returns an error if the parent directory cannot be created or the pool
/// cannot be built.
pub fn create_pool(database_path: &str) -> Result<DbPool> {
    create_pool_with_max_size(database_path, DEFAULT_DATABASE_POOL_SIZE)
}

/// Creates a connection pool with an explicit maximum connection count.
///
/// # Arguments
///
/// * `database_path` - SQLite database file path.
/// * `max_size` - Maximum number of SQLite connections kept by the shared pool.
///
/// # Returns
///
/// A ready-to-use connection pool.
///
/// # Errors
///
/// Returns an error if `max_size` is zero, cannot fit the pool builder type, the
/// parent directory cannot be created, or the pool cannot be built.
pub fn create_pool_with_max_size(database_path: &str, max_size: usize) -> Result<DbPool> {
    anyhow::ensure!(max_size > 0, "database pool size must be greater than 0");
    let max_size = u32::try_from(max_size).context("database pool size must fit into u32")?;
    vector_index::register_sqlite_vec()?;
    let path = Path::new(database_path);

    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent).with_context(|| {
            format!("failed to create parent directory for database at {database_path}")
        })?;
    }

    let manager = SqliteConnectionManager {
        database_path: database_path.to_string(),
    };

    let pool = r2d2::Pool::builder()
        .max_size(max_size)
        .build(manager)
        .context("failed to create database connection pool")?;

    // Verify the pool works by grabbing an initial connection.
    pool.get()
        .context("failed to acquire initial database connection")?;

    Ok(pool)
}

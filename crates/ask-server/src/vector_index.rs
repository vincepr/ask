use std::sync::OnceLock;

use anyhow::{Context, Result, anyhow, ensure};
use ask_core::models::EmbeddingModel;
use rusqlite::{Connection, OptionalExtension, params};

const DOCUMENT_EMBEDDING_VEC_TABLE: &str = "document_embedding_vec";
type SqliteAutoExtension = unsafe extern "C" fn(
    *mut rusqlite::ffi::sqlite3,
    *mut *mut std::os::raw::c_char,
    *const rusqlite::ffi::sqlite3_api_routines,
) -> i32;

/// Registers the sqlite-vec extension process-wide for future SQLite connections.
///
/// # Errors
///
/// Returns an error if SQLite rejects the auto-extension registration.
pub fn register_sqlite_vec() -> Result<()> {
    static REGISTRATION: OnceLock<std::result::Result<(), String>> = OnceLock::new();

    let result = REGISTRATION.get_or_init(|| unsafe {
        // sqlite3_auto_extension stores this initializer globally for every
        // future SQLite connection opened in this process.
        let code = rusqlite::ffi::sqlite3_auto_extension(Some(std::mem::transmute::<
            *const (),
            SqliteAutoExtension,
        >(
            sqlite_vec::sqlite3_vec_init as *const (),
        )));

        if code == rusqlite::ffi::SQLITE_OK {
            Ok(())
        } else {
            Err(format!(
                "sqlite3_auto_extension(sqlite-vec) returned {code}"
            ))
        }
    });

    match result {
        Ok(()) => Ok(()),
        Err(message) => Err(anyhow!(message.clone())),
    }
}

/// Ensures the active vector-search table matches `model`, creating or
/// rebuilding it as needed and backfilling currently embedded rows.
///
/// # Arguments
///
/// * `conn` - Open SQLite connection.
/// * `model` - Model that should own the single active vec table.
/// * `now` - Unix timestamp stored in `embedding_search_state.updated_at`.
///
/// # Returns
///
/// The number of embedded rows backfilled into the vec table when a rebuild was
/// required. Returns `0` when the current table already matches the model.
///
/// # Errors
///
/// Returns an error if the vec table cannot be created, the configured model
/// dimensions are invalid, or backfill fails.
pub fn ensure_active_search_model(
    conn: &Connection,
    model: &EmbeddingModel,
    now: i64,
) -> Result<usize> {
    let needs_rebuild = active_index_requires_rebuild(conn, model)?;
    if !needs_rebuild {
        return Ok(0);
    }

    let dimensions =
        usize::try_from(model.dimensions).context("embedding dimensions must fit into usize")?;
    ensure!(dimensions > 0, "embedding dimensions must be positive");

    conn.execute_batch(&format!(
        "DROP TABLE IF EXISTS {DOCUMENT_EMBEDDING_VEC_TABLE};
         CREATE VIRTUAL TABLE {DOCUMENT_EMBEDDING_VEC_TABLE}
         USING vec0(embedding float[{dimensions}]);"
    ))
    .context("failed to create sqlite-vec search table")?;

    conn.execute(
        "INSERT INTO embedding_search_state (singleton_id, active_model_id, dimensions, updated_at)
         VALUES (1, ?1, ?2, ?3)
         ON CONFLICT(singleton_id) DO UPDATE SET
             active_model_id = excluded.active_model_id,
             dimensions = excluded.dimensions,
             updated_at = excluded.updated_at",
        params![model.id, model.dimensions, now],
    )
    .context("failed to store active vector search model state")?;

    backfill_active_search_model(conn, model)
}

fn active_index_requires_rebuild(conn: &Connection, model: &EmbeddingModel) -> Result<bool> {
    let state = conn
        .query_row(
            "SELECT active_model_id, dimensions
             FROM embedding_search_state
             WHERE singleton_id = 1",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .context("failed to load vector search state")?;

    let table_exists = sqlite_table_exists(conn, DOCUMENT_EMBEDDING_VEC_TABLE)?;
    Ok(!table_exists || state != Some((model.id, model.dimensions)))
}

fn backfill_active_search_model(conn: &Connection, model: &EmbeddingModel) -> Result<usize> {
    let expected_bytes = usize::try_from(model.dimensions)
        .context("embedding dimensions must fit into usize")?
        * std::mem::size_of::<f32>();

    let invalid_rows: i64 = conn
        .query_row(
            "SELECT COUNT(*)
             FROM document_embeddings
             WHERE model_id = ?1
               AND state = ?2
               AND embedding IS NOT NULL
               AND length(embedding) != ?3",
            params![
                model.id,
                ask_core::types::EmbedState::Embedded,
                expected_bytes as i64
            ],
            |row| row.get(0),
        )
        .context("failed to validate embedded row dimensions before vec backfill")?;
    ensure!(
        invalid_rows == 0,
        "cannot backfill vector search index: {invalid_rows} embedded rows for model {} have invalid dimensions",
        model.name
    );

    conn.execute(
        "INSERT OR REPLACE INTO document_embedding_vec(rowid, embedding)
         SELECT id, embedding
         FROM document_embeddings
         WHERE model_id = ?1
           AND state = ?2
           AND embedding IS NOT NULL",
        params![model.id, ask_core::types::EmbedState::Embedded],
    )
    .context("failed to backfill sqlite-vec rows from embedded document rows")?;

    let inserted: i64 = conn
        .query_row(
            "SELECT COUNT(*)
             FROM document_embeddings
             WHERE model_id = ?1
               AND state = ?2
               AND embedding IS NOT NULL",
            params![model.id, ask_core::types::EmbedState::Embedded],
            |row| row.get(0),
        )
        .context("failed to count backfilled sqlite-vec rows")?;

    usize::try_from(inserted).context("backfilled row count must fit into usize")
}

fn sqlite_table_exists(conn: &Connection, table_name: &str) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name = ?1)",
        [table_name],
        |row| row.get(0),
    )
    .with_context(|| format!("failed to query sqlite_master for {table_name}"))
}

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use crate::models::{Document, EmbeddingModel, JobQueueEntry};
use crate::types::*;

/// A job with a claim older than this (in seconds) is considered dead and can
/// be reclaimed or replaced by a fresh enqueue.
const STALE_JOB_AGE_SECS: i64 = 86400;

// ---------------------------------------------------------------------------
// Document
// ---------------------------------------------------------------------------

fn load_document_by_path(conn: &Connection, filepath: &str) -> Result<Option<Document>> {
    conn.query_row(
        "SELECT id, filepath, file_type, doc_category, file_modified_at, file_size, updated_at
         FROM documents
         WHERE filepath = ?1
         ORDER BY updated_at DESC, id DESC
         LIMIT 1",
        params![filepath],
        |row| {
            Ok(Document {
                id: row.get(0)?,
                filepath: row.get(1)?,
                file_type: row.get(2)?,
                doc_category: row.get(3)?,
                file_modified_at: row.get(4)?,
                file_size: row.get(5)?,
                updated_at: row.get(6)?,
            })
        },
    )
    .optional()
    .context("failed to query document by path")
}

/// Insert a new document row or update the latest row for the same filepath.
///
/// # Arguments
///
/// * `conn` - Open SQLite connection.
/// * `doc` - Canonical document snapshot to persist.
///
/// # Returns
///
/// A tuple of `(document_id, changed)`. `changed` is `true` when a row was
/// inserted or its tracked metadata changed, and `false` when the stored row
/// already matched the provided document metadata.
///
/// # Errors
///
/// Returns an error if the lookup, insert, update, or stale-marking query
/// fails.
pub fn upsert_document(conn: &mut Connection, doc: &Document) -> Result<(i64, bool)> {
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .context("failed to start upsert_document transaction")?;

    let outcome = upsert_document_in_tx(&tx, doc)?;

    tx.commit().context("failed to commit upsert_document")?;
    Ok(outcome)
}

fn upsert_document_in_tx(conn: &Connection, doc: &Document) -> Result<(i64, bool)> {
    let existing = load_document_by_path(conn, &doc.filepath)?;

    let outcome = match existing {
        Some(existing)
            if existing.file_type == doc.file_type
                && existing.doc_category == doc.doc_category
                && existing.file_modified_at == doc.file_modified_at
                && existing.file_size == doc.file_size =>
        {
            (existing.id, false)
        }
        Some(existing) => {
            conn.execute(
                "UPDATE documents
                 SET file_type = ?1,
                     doc_category = ?2,
                     file_modified_at = ?3,
                     file_size = ?4,
                     updated_at = ?5
                 WHERE id = ?6",
                params![
                    doc.file_type,
                    doc.doc_category,
                    doc.file_modified_at,
                    doc.file_size,
                    doc.updated_at,
                    existing.id,
                ],
            )
            .context("failed to update document row")?;
            mark_documents_stale(conn, &[existing.id])?;
            (existing.id, true)
        }
        None => {
            conn.execute(
                "INSERT INTO documents (filepath, file_type, doc_category, file_modified_at, file_size, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    doc.filepath,
                    doc.file_type,
                    doc.doc_category,
                    doc.file_modified_at,
                    doc.file_size,
                    doc.updated_at,
                ],
            )
            .context("failed to insert document row")?;
            (conn.last_insert_rowid(), true)
        }
    };

    Ok(outcome)
}

/// Upsert one document and replace its pending embedding work in one transaction.
///
/// # Arguments
///
/// * `conn` - Open SQLite connection.
/// * `doc` - Canonical document snapshot to persist.
/// * `model_id` - Embedding model identifier whose pending work should be refreshed.
/// * `chunks` - Pending chunks to queue for the document and model.
/// * `now` - Unix timestamp stored on any queued embedding rows.
///
/// # Returns
///
/// A tuple of `(document_id, changed)`. `changed` is `true` when the document row
/// was inserted or updated and its pending embedding rows were refreshed.
///
/// # Errors
///
/// Returns an error if the transaction, document write, pending-row delete,
/// pending-row insert, or commit fails.
pub fn upsert_document_and_replace_pending_embeddings(
    conn: &mut Connection,
    doc: &Document,
    model_id: i64,
    chunks: &[(ChunkType, i64, i64)],
    now: i64,
) -> Result<(i64, bool)> {
    upsert_document_and_replace_pending_embeddings_with_hook(
        conn,
        doc,
        model_id,
        chunks,
        now,
        || Ok(()),
    )
}

fn upsert_document_and_replace_pending_embeddings_with_hook<F>(
    conn: &mut Connection,
    doc: &Document,
    model_id: i64,
    chunks: &[(ChunkType, i64, i64)],
    now: i64,
    before_queue: F,
) -> Result<(i64, bool)>
where
    F: FnOnce() -> Result<()>,
{
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .context("failed to start transactional document ingest")?;

    let (doc_id, changed) = upsert_document_in_tx(&tx, doc)?;

    if !changed {
        tx.commit()
            .context("failed to commit transactional document ingest")?;
        return Ok((doc_id, false));
    }

    before_queue()?;
    delete_pending_embeddings_for_model(&tx, doc_id, model_id)?;
    insert_pending_embeddings(&tx, doc_id, model_id, chunks, now)?;

    tx.commit()
        .context("failed to commit transactional document ingest")?;
    Ok((doc_id, true))
}

/// Find a document by its filepath.
pub fn find_document_by_path(conn: &Connection, filepath: &str) -> Result<Option<Document>> {
    load_document_by_path(conn, filepath)
}

/// Mark all embeddings for the given documents as stale across every model.
///
/// Only `'embedded'` rows are affected — `'pending'` and already-`'stale'` rows are
/// left as-is. Document IDs that do not exist in the `documents` table are silently
/// filtered out by the subquery.
pub fn mark_documents_stale(conn: &Connection, doc_ids: &[i64]) -> Result<()> {
    if doc_ids.is_empty() {
        return Ok(());
    }

    let placeholders: Vec<String> = (0..doc_ids.len())
        .map(|index| format!("?{}", index + 3))
        .collect();
    let sql = format!(
        "UPDATE document_embeddings SET state = ?1 \
         WHERE document_id IN (SELECT id FROM documents WHERE id IN ({})) \
         AND state = ?2",
        placeholders.join(", ")
    );

    let mut stmt = conn
        .prepare(&sql)
        .context("failed to prepare mark_documents_stale")?;

    let stale = EmbedState::Stale;
    let embedded = EmbedState::Embedded;
    let mut param_refs: Vec<&dyn rusqlite::types::ToSql> = Vec::with_capacity(doc_ids.len() + 2);
    param_refs.push(&stale);
    param_refs.push(&embedded);
    param_refs.extend(doc_ids.iter().map(|id| id as &dyn rusqlite::types::ToSql));

    stmt.execute(param_refs.as_slice())
        .context("failed to mark embeddings as stale")?;

    Ok(())
}

/// List all documents.
pub fn list_documents(conn: &Connection) -> Result<Vec<Document>> {
    let mut stmt = conn
        .prepare("SELECT id, filepath, file_type, doc_category, file_modified_at, file_size, updated_at FROM documents ORDER BY filepath")
        .context("failed to prepare list_documents")?;

    let rows = stmt
        .query_map([], |row| {
            Ok(Document {
                id: row.get(0)?,
                filepath: row.get(1)?,
                file_type: row.get(2)?,
                doc_category: row.get(3)?,
                file_modified_at: row.get(4)?,
                file_size: row.get(5)?,
                updated_at: row.get(6)?,
            })
        })
        .context("failed to query documents")?;

    rows.collect::<rusqlite::Result<Vec<_>>>()
        .context("failed to collect documents")
}

// ---------------------------------------------------------------------------
// EmbeddingModel
// ---------------------------------------------------------------------------

/// Find a model by its id.
pub fn find_model_by_id(conn: &Connection, id: i64) -> Result<Option<EmbeddingModel>> {
    let mut stmt = conn
        .prepare("SELECT id, name, dimensions, chunk_size, chunk_overlap, created_at FROM embedding_models WHERE id = ?1")
        .context("failed to prepare find_model_by_id")?;

    let mut rows = stmt
        .query_map(params![id], |row| {
            Ok(EmbeddingModel {
                id: row.get(0)?,
                name: row.get(1)?,
                dimensions: row.get(2)?,
                chunk_size: row.get(3)?,
                chunk_overlap: row.get(4)?,
                created_at: row.get(5)?,
            })
        })
        .context("failed to query model by id")?;

    match rows.next() {
        Some(Ok(model)) => Ok(Some(model)),
        Some(Err(e)) => Err(e.into()),
        None => Ok(None),
    }
}

/// Find a model by its name.
pub fn find_model_by_name(conn: &Connection, name: &str) -> Result<Option<EmbeddingModel>> {
    let mut stmt = conn
        .prepare("SELECT id, name, dimensions, chunk_size, chunk_overlap, created_at FROM embedding_models WHERE name = ?1")
        .context("failed to prepare find_model_by_name")?;

    let mut rows = stmt
        .query_map(params![name], |row| {
            Ok(EmbeddingModel {
                id: row.get(0)?,
                name: row.get(1)?,
                dimensions: row.get(2)?,
                chunk_size: row.get(3)?,
                chunk_overlap: row.get(4)?,
                created_at: row.get(5)?,
            })
        })
        .context("failed to query model by name")?;

    match rows.next() {
        Some(Ok(model)) => Ok(Some(model)),
        Some(Err(e)) => Err(e.into()),
        None => Ok(None),
    }
}

/// Insert a new model row. Returns the generated id.
pub fn insert_model(conn: &Connection, model: &EmbeddingModel) -> Result<i64> {
    conn.execute(
        "INSERT INTO embedding_models (name, dimensions, chunk_size, chunk_overlap, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            model.name,
            model.dimensions,
            model.chunk_size,
            model.chunk_overlap,
            model.created_at,
        ],
    )
    .context("failed to insert model row")?;
    Ok(conn.last_insert_rowid())
}

// ---------------------------------------------------------------------------
// DocumentEmbedding
// ---------------------------------------------------------------------------

/// Insert a batch of pending embedding rows for a document + model.
pub fn insert_pending_embeddings(
    conn: &Connection,
    doc_id: i64,
    model_id: i64,
    chunks: &[(ChunkType, i64, i64)],
    now: i64,
) -> Result<()> {
    let mut stmt = conn
        .prepare(
            "INSERT INTO document_embeddings
                (document_id, model_id, chunk_type, chunk_start, chunk_end, state, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(document_id, model_id, chunk_type, chunk_start) DO UPDATE SET
                 chunk_end = excluded.chunk_end,
                 state = excluded.state,
                 embedding = NULL,
                 created_at = excluded.created_at",
        )
        .context("failed to prepare insert_pending_embeddings")?;

    for (chunk_type, start, end) in chunks {
        stmt.execute(params![
            doc_id,
            model_id,
            chunk_type,
            start,
            end,
            EmbedState::Pending,
            now
        ])
        .with_context(|| {
            format!("failed to insert embedding ({doc_id}, {model_id}, {chunk_type})")
        })?;
    }

    Ok(())
}

/// Delete pending embeddings for one document/model pair.
///
/// # Arguments
///
/// * `conn` - Open SQLite connection.
/// * `doc_id` - Persisted document identifier.
/// * `model_id` - Embedding model identifier.
///
/// # Returns
///
/// `Ok(())` when all pending rows for the pair are removed.
///
/// # Errors
///
/// Returns an error if the delete query fails.
pub fn delete_pending_embeddings_for_model(
    conn: &Connection,
    doc_id: i64,
    model_id: i64,
) -> Result<()> {
    conn.execute(
        "DELETE FROM document_embeddings
         WHERE document_id = ?1 AND model_id = ?2 AND state = ?3",
        params![doc_id, model_id, EmbedState::Pending],
    )
    .context("failed to delete pending embeddings")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ChunkType, DocCategory};

    fn setup_documents_db() -> Connection {
        let conn = Connection::open_in_memory().expect("in-memory database must open");
        conn.execute_batch(
            "CREATE TABLE documents (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                filepath         TEXT NOT NULL,
                file_type        TEXT NOT NULL,
                doc_category     TEXT NOT NULL,
                file_modified_at INTEGER NOT NULL,
                file_size        INTEGER NOT NULL,
                updated_at       INTEGER NOT NULL
            );
            CREATE TABLE document_embeddings (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id     INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
                model_id        INTEGER NOT NULL,
                chunk_type      TEXT    NOT NULL,
                chunk_start     INTEGER NOT NULL,
                chunk_end       INTEGER NOT NULL,
                state           TEXT    NOT NULL,
                embedding       BLOB,
                created_at      INTEGER NOT NULL
            );
            CREATE UNIQUE INDEX idx_embeddings_unique
                ON document_embeddings (document_id, model_id, chunk_type, chunk_start);",
        )
        .expect("document schema must be created");
        conn
    }

    #[test]
    fn transactional_document_ingest_rolls_back_when_queue_step_fails() {
        let mut conn = setup_documents_db();
        let doc = Document {
            id: 0,
            filepath: "/tmp/a.txt".to_string(),
            file_type: "txt".to_string(),
            doc_category: DocCategory::Resource,
            file_modified_at: 100,
            file_size: 10,
            updated_at: 100,
        };

        let err = upsert_document_and_replace_pending_embeddings_with_hook(
            &mut conn,
            &doc,
            1,
            &[(ChunkType::Filename, 0, 0)],
            100,
            || anyhow::bail!("synthetic queue failure"),
        )
        .expect_err("synthetic queue failure must abort the transaction");

        assert!(
            err.to_string().contains("synthetic queue failure"),
            "unexpected error: {err:#}"
        );

        let document_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM documents", [], |row| row.get(0))
            .expect("document count query must succeed");
        let embedding_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM document_embeddings", [], |row| {
                row.get(0)
            })
            .expect("embedding count query must succeed");

        assert_eq!(document_count, 0);
        assert_eq!(embedding_count, 0);
    }
}

// ---------------------------------------------------------------------------
// JobQueue
// ---------------------------------------------------------------------------

/// Enqueue a job, or reset it if the existing row is stale.
///
/// If a row with the same `(job_type, payload)` already exists and its claim
/// timestamp is older than `STALE_JOB_AGE_SECS`, the job becomes unclaimed
/// again and is treated as freshly queued. Otherwise returns an error.
pub fn enqueue_job(conn: &Connection, job_type: &JobType, payload: &str, now: i64) -> Result<()> {
    let stale_cutoff = now - STALE_JOB_AGE_SECS;

    let affected = conn
        .execute(
            "INSERT INTO job_queue (job_type, payload, claimed_at, created_at)
             VALUES (?1, ?2, NULL, ?3)
             ON CONFLICT(job_type, payload) DO UPDATE SET
                 claimed_at = NULL,
                 created_at = EXCLUDED.created_at
              WHERE job_queue.claimed_at < ?4",
            params![job_type, payload, now, stale_cutoff],
        )
        .context("failed to enqueue job")?;

    if affected == 0 {
        anyhow::bail!(
            "a job with type '{t}' and payload '{p}' is already queued or in progress",
            t = job_type.as_str(),
            p = payload
        );
    }

    Ok(())
}

/// Atomically claim the next pending or stale job.
///
/// Uses an `IMMEDIATE` transaction so that only one worker ever claims any
/// given row. Returns `None` when no jobs are waiting.
pub fn claim_job(conn: &mut Connection, now: i64) -> Result<Option<JobQueueEntry>> {
    let stale_cutoff = now - STALE_JOB_AGE_SECS;
    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .context("failed to begin claim transaction")?;

    // SQLite needs the surrounding IMMEDIATE transaction here; PostgreSQL would require FOR UPDATE SKIP LOCKED.
    let entry = tx
        .query_row(
            "WITH picked AS (
                 SELECT id
                 FROM job_queue
                 WHERE claimed_at IS NULL OR claimed_at < ?1
                 ORDER BY id ASC
                 LIMIT 1
             )
            UPDATE job_queue
            SET claimed_at = ?2
            WHERE id IN (SELECT id FROM picked)
            RETURNING id, job_type, payload, claimed_at, created_at",
            params![stale_cutoff, now],
            |row| {
                Ok(JobQueueEntry {
                    id: row.get(0)?,
                    job_type: row.get(1)?,
                    payload: row.get(2)?,
                    claimed_at: row.get(3)?,
                    created_at: row.get(4)?,
                })
            },
        )
        .optional()
        .context("failed to claim and read next job")?;

    tx.commit().context("failed to commit claim transaction")?;
    Ok(entry)
}

/// Remove a completed job from the queue.
pub fn complete_job(conn: &Connection, job_id: i64) -> Result<()> {
    conn.execute("DELETE FROM job_queue WHERE id = ?1", params![job_id])
        .context("failed to complete job")?;
    Ok(())
}

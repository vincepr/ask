use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};

use crate::models::{Document, EmbeddingModel, JobQueueEntry};
use crate::types::*;

/// A job with a claim older than this (in seconds) is considered dead and can
/// be reclaimed or replaced by a fresh enqueue.
const STALE_JOB_AGE_SECS: i64 = 86400;

// ---------------------------------------------------------------------------
// Document
// ---------------------------------------------------------------------------

/// Insert a new document row. Returns the generated id.
pub fn insert_document(conn: &Connection, doc: &Document) -> Result<i64> {
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
    Ok(conn.last_insert_rowid())
}

/// Find a document by its filepath.
pub fn find_document_by_path(conn: &Connection, filepath: &str) -> Result<Option<Document>> {
    let mut stmt = conn
        .prepare("SELECT id, filepath, file_type, doc_category, file_modified_at, file_size, updated_at FROM documents WHERE filepath = ?1")
        .context("failed to prepare find_document_by_path")?;

    let mut rows = stmt
        .query_map(params![filepath], |row| {
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
        .context("failed to query document by path")?;

    match rows.next() {
        Some(Ok(doc)) => Ok(Some(doc)),
        Some(Err(e)) => Err(e.into()),
        None => Ok(None),
    }
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
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
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

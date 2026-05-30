use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};

use crate::models::{Document, DocumentEmbedding, EmbeddingModel, JobQueueEntry};
use crate::types::*;

/// A job with a heartbeat older than this (in seconds) is considered dead and
/// can be replaced by a fresh enqueue.
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
            doc.doc_category.as_str(),
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
                doc_category: DocCategory::try_from_str(&row.get::<_, String>(3)?)
                    .unwrap_or(DocCategory::Resource),
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

/// Update the file metadata and mark the document's embeddings for the given
/// model as stale.
pub fn mark_document_changed(
    conn: &mut Connection,
    doc_id: i64,
    file_modified_at: i64,
    file_size: i64,
    model_id: i64,
    now: i64,
) -> Result<()> {
    let tx = conn
        .transaction()
        .context("failed to start transaction for mark_document_changed")?;

    tx.execute(
        "UPDATE documents SET file_modified_at = ?1, file_size = ?2, updated_at = ?3 WHERE id = ?4",
        params![file_modified_at, file_size, now, doc_id],
    )
    .context("failed to update document metadata")?;

    tx.execute(
        "UPDATE document_embeddings SET state = 'stale' WHERE document_id = ?1 AND model_id = ?2 AND state = 'embedded'",
        params![doc_id, model_id],
    )
    .context("failed to mark embeddings as stale")?;

    tx.commit()
        .context("failed to commit mark_document_changed")
}

/// Delete a document and its cascaded embeddings.
pub fn delete_document(conn: &Connection, doc_id: i64) -> Result<()> {
    conn.execute("DELETE FROM documents WHERE id = ?1", params![doc_id])
        .context("failed to delete document")?;
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
                doc_category: DocCategory::try_from_str(&row.get::<_, String>(3)?)
                    .unwrap_or(DocCategory::Resource),
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
             VALUES (?1, ?2, ?3, ?4, ?5, 'pending', ?6)",
        )
        .context("failed to prepare insert_pending_embeddings")?;

    for (chunk_type, start, end) in chunks {
        stmt.execute(params![
            doc_id,
            model_id,
            chunk_type.as_str(),
            start,
            end,
            now
        ])
        .with_context(|| {
            format!("failed to insert embedding ({doc_id}, {model_id}, {chunk_type})")
        })?;
    }

    Ok(())
}

/// Return all pending embeddings for a given model, ordered by document id.
pub fn pending_embeddings_for_model(
    conn: &Connection,
    model_id: i64,
) -> Result<Vec<DocumentEmbedding>> {
    let mut stmt = conn
        .prepare(
            "SELECT id, document_id, model_id, chunk_type, chunk_start, chunk_end, state, embedding, created_at
             FROM document_embeddings
             WHERE model_id = ?1 AND (state = 'pending' OR state = 'stale')
             ORDER BY document_id",
        )
        .context("failed to prepare pending_embeddings_for_model")?;

    let rows = stmt
        .query_map(params![model_id], |row| {
            Ok(DocumentEmbedding {
                id: row.get(0)?,
                document_id: row.get(1)?,
                model_id: row.get(2)?,
                chunk_type: ChunkType::try_from_str(&row.get::<_, String>(3)?)
                    .unwrap_or(ChunkType::Content),
                chunk_start: row.get(4)?,
                chunk_end: row.get(5)?,
                state: EmbedState::try_from_str(&row.get::<_, String>(6)?)
                    .unwrap_or(EmbedState::Pending),
                embedding: row.get(7)?,
                created_at: row.get(8)?,
            })
        })
        .context("failed to query pending embeddings")?;

    rows.collect::<rusqlite::Result<Vec<_>>>()
        .context("failed to collect pending embeddings")
}

/// Mark a single embedding row as embedded.
pub fn mark_embedded(conn: &Connection, emb_id: i64, embedding: &[u8], now: i64) -> Result<()> {
    conn.execute(
        "UPDATE document_embeddings SET state = 'embedded', embedding = ?1, created_at = ?2 WHERE id = ?3",
        params![embedding, now, emb_id],
    )
    .context("failed to mark embedding as embedded")?;
    Ok(())
}

/// Delete all embedding rows for a document + model (used during re-embed).
pub fn delete_embeddings_for_doc_model(
    conn: &Connection,
    doc_id: i64,
    model_id: i64,
) -> Result<()> {
    conn.execute(
        "DELETE FROM document_embeddings WHERE document_id = ?1 AND model_id = ?2",
        params![doc_id, model_id],
    )
    .context("failed to delete embeddings for doc+model")?;
    Ok(())
}

/// Count pending or stale embeddings for a model (to check if work remains).
pub fn count_pending_for_model(conn: &Connection, model_id: i64) -> Result<i64> {
    conn.query_row(
        "SELECT COUNT(*) FROM document_embeddings WHERE model_id = ?1 AND (state = 'pending' OR state = 'stale')",
        params![model_id],
        |row| row.get(0),
    )
    .context("failed to count pending embeddings")
}

// ---------------------------------------------------------------------------
// JobQueue
// ---------------------------------------------------------------------------

/// Enqueue a job, or re-enqueue it if the existing one is stale.
///
/// If a row with the same `(job_type, payload)` already exists **and** its
/// heartbeat is older than `STALE_JOB_AGE_SECS`, the job is reset (heartbeat
/// cleared, timestamps refreshed). Otherwise returns an error — the job is
/// still active and cannot be duplicated.
pub fn enqueue_job(conn: &Connection, job_type: &JobType, payload: &str, now: i64) -> Result<()> {
    let stale_cutoff = now - STALE_JOB_AGE_SECS;

    let affected = conn
        .execute(
            "INSERT INTO job_queue (job_type, payload, heartbeat, created_at, updated_at)
             VALUES (?1, ?2, NULL, ?3, ?3)
             ON CONFLICT(job_type, payload) DO UPDATE SET
                 heartbeat = NULL,
                 payload   = EXCLUDED.payload,
                 created_at = EXCLUDED.created_at,
                 updated_at = EXCLUDED.updated_at
             WHERE job_queue.heartbeat IS NULL
                OR job_queue.heartbeat < ?4",
            params![job_type.as_str(), payload, now, stale_cutoff],
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

/// Atomically claim the oldest pending job by writing a heartbeat timestamp.
///
/// Uses an `IMMEDIATE` transaction so that only one worker ever claims any
/// given row. Returns `None` when no jobs are waiting.
pub fn claim_job(conn: &mut Connection, now: i64) -> Result<Option<JobQueueEntry>> {
    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .context("failed to begin claim transaction")?;

    let job_id: Option<i64> = tx
        .query_row(
            "SELECT id FROM job_queue WHERE heartbeat IS NULL ORDER BY created_at ASC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .context("failed to query for next job")?;

    let job_id = match job_id {
        Some(id) => id,
        None => return Ok(None),
    };

    let updated = tx
        .execute(
            "UPDATE job_queue SET heartbeat = ?1, updated_at = ?1 WHERE id = ?2 AND heartbeat IS NULL",
            params![now, job_id],
        )
        .context("failed to claim job")?;

    if updated == 0 {
        return Ok(None);
    }

    let entry = tx
        .query_row(
            "SELECT id, job_type, payload, heartbeat, created_at, updated_at FROM job_queue WHERE id = ?1",
            params![job_id],
            |row| {
                let job_type_str: String = row.get(1)?;
                let job_type = JobType::try_from_str(&job_type_str).ok_or_else(|| {
                    rusqlite::Error::InvalidParameterName(format!("unknown job_type: {job_type_str}"))
                })?;

                Ok(JobQueueEntry {
                    id: row.get(0)?,
                    job_type,
                    payload: row.get(2)?,
                    heartbeat: row.get(3)?,
                    created_at: row.get(4)?,
                    updated_at: row.get(5)?,
                })
            },
        )
        .context("failed to read claimed job")?;

    tx.commit().context("failed to commit claim transaction")?;
    Ok(Some(entry))
}

/// Refresh the heartbeat for an in-progress job so other workers know it is
/// still alive.
pub fn update_heartbeat(conn: &Connection, job_id: i64, now: i64) -> Result<()> {
    conn.execute(
        "UPDATE job_queue SET heartbeat = ?1, updated_at = ?1 WHERE id = ?2",
        params![now, job_id],
    )
    .context("failed to update job heartbeat")?;
    Ok(())
}

/// Remove a completed job from the queue.
pub fn complete_job(conn: &Connection, job_id: i64) -> Result<()> {
    conn.execute("DELETE FROM job_queue WHERE id = ?1", params![job_id])
        .context("failed to complete job")?;
    Ok(())
}

/// Insert pending embedding rows for every existing document under a new model.
pub fn seed_pending_for_all_docs(conn: &Connection, model_id: i64, now: i64) -> Result<usize> {
    let docs = list_documents(conn)?;
    let mut count = 0;

    for doc in &docs {
        insert_pending_embeddings(conn, doc.id, model_id, &[(ChunkType::Filename, 0, 0)], now)?;
        count += 1;
    }

    Ok(count)
}

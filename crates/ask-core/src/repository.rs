use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use crate::models::{
    Document, DocumentFilepathSearchResult, DocumentSearchResult, EmbedDocumentPayload,
    EmbeddedChunk, EmbeddingIdentity, EmbeddingModel, JobQueueEntry,
};
use crate::types::*;

/// A job with a claim older than this (in seconds) is considered dead and can
/// be reclaimed or replaced by a fresh enqueue.
const STALE_JOB_AGE_SECS: i64 = 86400;
const DOCUMENT_EMBEDDING_VEC_TABLE: &str = "document_embedding_vec";
const DOCUMENT_FILEPATH_SEARCH_TABLE: &str = "document_filepath_search";
const DOCUMENT_FILEPATH_SEARCH_FTS_TABLE: &str = "document_filepath_search_fts";

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
            sync_document_filepath_search_row(conn, existing.id, &doc.filepath)?;
            mark_documents_stale_in_queue_time(conn, &[existing.id], doc.updated_at)?;
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
            let doc_id = conn.last_insert_rowid();
            sync_document_filepath_search_row(conn, doc_id, &doc.filepath)?;
            (doc_id, true)
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
    seed_embed_jobs(&tx, now)?;

    tx.commit()
        .context("failed to commit transactional document ingest")?;
    Ok((doc_id, true))
}

/// Find a document by its filepath.
pub fn find_document_by_path(conn: &Connection, filepath: &str) -> Result<Option<Document>> {
    load_document_by_path(conn, filepath)
}

/// Find a document by its id.
///
/// # Arguments
///
/// * `conn` - Open SQLite connection.
/// * `id` - Persisted document identifier.
///
/// # Returns
///
/// The matching document when present.
///
/// # Errors
///
/// Returns an error if the lookup query fails.
pub fn find_document_by_id(conn: &Connection, id: i64) -> Result<Option<Document>> {
    conn.query_row(
        "SELECT id, filepath, file_type, doc_category, file_modified_at, file_size, updated_at
         FROM documents
         WHERE id = ?1",
        params![id],
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
    .context("failed to query document by id")
}

/// Mark all embeddings for the given documents as stale across every model.
///
/// Only `'embedded'` rows are affected — `'pending'` and already-`'stale'` rows are
/// left as-is. Document IDs that do not exist in the `documents` table are silently
/// filtered out by the subquery.
pub fn mark_documents_stale(conn: &Connection, doc_ids: &[i64]) -> Result<()> {
    mark_documents_stale_in_queue_time(conn, doc_ids, current_unix_timestamp())
}

fn mark_documents_stale_in_queue_time(conn: &Connection, doc_ids: &[i64], now: i64) -> Result<()> {
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

    delete_vec_rows_for_documents(conn, doc_ids)?;

    seed_embed_jobs(conn, now)?;

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
        .query_map(params![id], map_embedding_model)
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
        .query_map(params![name], map_embedding_model)
        .context("failed to query model by name")?;

    match rows.next() {
        Some(Ok(model)) => Ok(Some(model)),
        Some(Err(e)) => Err(e.into()),
        None => Ok(None),
    }
}

/// Find a model by its full embedding identity.
///
/// # Arguments
///
/// * `conn` - Open SQLite connection.
/// * `identity` - Immutable embedding identity to match exactly.
///
/// # Returns
///
/// The matching persisted model row when present.
///
/// # Errors
///
/// Returns an error if the lookup query fails.
pub fn find_model_by_identity(
    conn: &Connection,
    identity: &EmbeddingIdentity,
) -> Result<Option<EmbeddingModel>> {
    let mut stmt = conn
        .prepare(
            "SELECT id, name, dimensions, chunk_size, chunk_overlap, created_at
             FROM embedding_models
             WHERE name = ?1
               AND dimensions = ?2
               AND chunk_size = ?3
               AND chunk_overlap = ?4",
        )
        .context("failed to prepare find_model_by_identity")?;

    let mut rows = stmt
        .query_map(
            params![
                &identity.name,
                identity.dimensions,
                identity.chunk_size,
                identity.chunk_overlap,
            ],
            map_embedding_model,
        )
        .context("failed to query model by identity")?;

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

fn map_embedding_model(row: &rusqlite::Row<'_>) -> rusqlite::Result<EmbeddingModel> {
    Ok(EmbeddingModel {
        id: row.get(0)?,
        name: row.get(1)?,
        dimensions: row.get(2)?,
        chunk_size: row.get(3)?,
        chunk_overlap: row.get(4)?,
        created_at: row.get(5)?,
    })
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

    delete_vec_rows_for_non_embedded_document_model(conn, doc_id, model_id)?;

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

/// Replace every embedding row for one document/model pair in one transaction.
///
/// # Arguments
///
/// * `conn` - Open SQLite connection.
/// * `doc_id` - Persisted document identifier.
/// * `model_id` - Embedding model identifier.
/// * `chunks` - Fully embedded rows to store for the pair.
/// * `now` - Unix timestamp stored on the replacement rows.
///
/// # Returns
///
/// `Ok(())` when the pair has been replaced atomically.
///
/// # Errors
///
/// Returns an error if the transaction cannot start, the old rows cannot be
/// deleted, a new row cannot be inserted, or the transaction cannot commit.
pub fn replace_embeddings_for_document_model(
    conn: &mut Connection,
    doc_id: i64,
    model_id: i64,
    chunks: &[EmbeddedChunk],
    now: i64,
) -> Result<()> {
    let expected_embedding_bytes = load_expected_embedding_bytes(conn, model_id)?;
    validate_chunk_embedding_lengths(chunks, expected_embedding_bytes)?;

    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .context("failed to start replace_embeddings_for_document_model transaction")?;

    delete_vec_rows_for_document_model(&tx, doc_id, model_id)?;

    tx.execute(
        "DELETE FROM document_embeddings WHERE document_id = ?1 AND model_id = ?2",
        params![doc_id, model_id],
    )
    .context("failed to delete old embeddings for document/model pair")?;

    let mut stmt = tx
        .prepare(
            "INSERT INTO document_embeddings
                (document_id, model_id, chunk_type, chunk_start, chunk_end, state, embedding, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )
        .context("failed to prepare replacement embedding insert")?;

    for chunk in chunks {
        stmt.execute(params![
            doc_id,
            model_id,
            chunk.chunk_type,
            chunk.chunk_start,
            chunk.chunk_end,
            EmbedState::Embedded,
            &chunk.embedding,
            now,
        ])
        .with_context(|| {
            format!(
                "failed to insert embedded chunk ({doc_id}, {model_id}, {}, {})",
                chunk.chunk_type, chunk.chunk_start
            )
        })?;
    }

    drop(stmt);
    sync_vec_rows_for_document_model(&tx, doc_id, model_id)?;
    tx.commit()
        .context("failed to commit replace_embeddings_for_document_model")?;
    Ok(())
}

/// Search the active SQLite vec index for one embedding model.
///
/// # Arguments
///
/// * `conn` - Open SQLite connection.
/// * `model` - Embedding model whose vec index should be queried.
/// * `query_embedding` - Query vector with exactly `model.dimensions` float values.
/// * `limit` - Maximum number of hits to return.
///
/// # Returns
///
/// Search hits ordered by ascending distance.
///
/// # Errors
///
/// Returns an error if the query vector length does not match the model, the
/// active vec index is missing or configured for a different model, or the SQL
/// query fails.
pub fn search_documents_by_embedding(
    conn: &Connection,
    model: &EmbeddingModel,
    query_embedding: &[f32],
    limit: usize,
) -> Result<Vec<DocumentSearchResult>> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    validate_query_embedding_dimensions(query_embedding, model)?;
    ensure_active_vec_model(conn, model)?;

    let query_blob = serialize_embedding(query_embedding);
    let mut stmt = conn
        .prepare(
            "WITH nearest AS (
                 SELECT rowid, distance
                 FROM document_embedding_vec
                 WHERE embedding MATCH ?1
                 ORDER BY distance ASC
                 LIMIT ?2
             )
             SELECT d.id, d.filepath, d.file_type, d.doc_category, d.file_modified_at,
                    d.file_size, d.updated_at, de.id, de.model_id, de.chunk_type,
                    de.chunk_start, de.chunk_end, nearest.distance
             FROM nearest
             JOIN document_embeddings de ON de.id = nearest.rowid
             JOIN documents d ON d.id = de.document_id
             WHERE de.model_id = ?3 AND de.state = ?4
             ORDER BY nearest.distance ASC, de.id ASC",
        )
        .context("failed to prepare search_documents_by_embedding query")?;

    let rows = stmt
        .query_map(
            params![query_blob, limit as i64, model.id, EmbedState::Embedded],
            |row| {
                Ok(DocumentSearchResult {
                    document_id: row.get(0)?,
                    filepath: row.get(1)?,
                    file_type: row.get(2)?,
                    doc_category: row.get(3)?,
                    file_modified_at: row.get(4)?,
                    file_size: row.get(5)?,
                    document_updated_at: row.get(6)?,
                    embedding_id: row.get(7)?,
                    model_id: row.get(8)?,
                    chunk_type: row.get(9)?,
                    chunk_start: row.get(10)?,
                    chunk_end: row.get(11)?,
                    distance: row.get(12)?,
                })
            },
        )
        .context("failed to query vector search results")?;

    rows.collect::<rusqlite::Result<Vec<_>>>()
        .context("failed to collect vector search results")
}

/// Search document filepaths using the SQLite FTS5 trigram index when possible.
///
/// # Arguments
///
/// * `conn` - Open SQLite connection.
/// * `query` - Raw filepath search query.
/// * `limit` - Maximum number of document hits to return.
///
/// # Returns
///
/// Matching documents ordered from best to worst filepath match.
///
/// # Errors
///
/// Returns an error if the SQLite query fails.
pub fn search_documents_by_filepath_fuzzy(
    conn: &Connection,
    query: &str,
    limit: usize,
) -> Result<Vec<DocumentFilepathSearchResult>> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    let normalized_query = normalize_filepath_search_value(query);
    if normalized_query.is_empty() {
        return Ok(Vec::new());
    }

    if normalized_query.chars().count() < 3 {
        return search_documents_by_filepath_like(conn, &normalized_query, limit);
    }

    let match_query = fts5_quote(&normalized_query);
    let candidate_limit = expanded_filepath_candidate_limit(limit);

    // Motivation: keep the SQL structure close to quick Postgres trigram
    // search ideas so this can be ported later if the repo ever switches
    // away from SQLite.
    // https://rdegges.com/2013/easy-fuzzy-text-searching-with-postgresql/
    let mut stmt = conn
        .prepare(&format!(
            "WITH ranked AS (
                 SELECT rowid AS document_id,
                        bm25({fts_table}, 1.0, 5.0) AS fts_score
                 FROM {fts_table}
                 WHERE {fts_table} MATCH ?1
                 ORDER BY fts_score ASC
                 LIMIT ?2
             )
             SELECT d.id,
                    d.filepath,
                    CASE
                        WHEN s.normalized_basename = ?3 OR s.normalized_filepath = ?3 THEN 1.0
                        WHEN s.normalized_basename LIKE '%' || ?3 || '%' THEN 0.9
                        WHEN s.normalized_filepath LIKE '%' || ?3 || '%' THEN 0.75
                        WHEN ranked.fts_score < 0.0 THEN 0.5
                        ELSE 0.5 / (1.0 + ranked.fts_score)
                    END AS match_score
             FROM ranked
             JOIN {search_table} s ON s.document_id = ranked.document_id
             JOIN documents d ON d.id = ranked.document_id
             ORDER BY
                 CASE
                     WHEN s.normalized_basename = ?3 OR s.normalized_filepath = ?3 THEN 0
                     WHEN s.normalized_basename LIKE '%' || ?3 || '%' THEN 1
                     WHEN s.normalized_filepath LIKE '%' || ?3 || '%' THEN 2
                     ELSE 3
                 END ASC,
                 CASE
                     WHEN s.normalized_basename = ?3 OR s.normalized_basename LIKE '%' || ?3 || '%'
                         THEN length(s.normalized_basename)
                     ELSE length(s.normalized_filepath)
                 END ASC,
                 ranked.fts_score ASC,
                 length(s.normalized_filepath) ASC,
                 d.id ASC
             LIMIT ?4",
            fts_table = DOCUMENT_FILEPATH_SEARCH_FTS_TABLE,
            search_table = DOCUMENT_FILEPATH_SEARCH_TABLE,
        ))
        .context("failed to prepare search_documents_by_filepath_fuzzy query")?;

    let rows = stmt
        .query_map(
            params![
                match_query,
                candidate_limit as i64,
                normalized_query,
                limit as i64
            ],
            |row| {
                Ok(DocumentFilepathSearchResult {
                    document_id: row.get(0)?,
                    filepath: row.get(1)?,
                    match_score: row.get(2)?,
                })
            },
        )
        .context("failed to query filepath fuzzy search results")?;

    rows.collect::<rusqlite::Result<Vec<_>>>()
        .context("failed to collect filepath fuzzy search results")
}

/// Enqueue one `embed_document` job for each distinct queued document/model pair.
///
/// Rows in either `pending` or `stale` state are considered work that still needs
/// an embedding pass. Existing queue uniqueness on `(job_type, payload)` is used to
/// deduplicate repeated seeding calls.
///
/// # Arguments
///
/// * `conn` - Open SQLite connection.
/// * `now` - Unix timestamp stored on newly queued jobs.
///
/// # Returns
///
/// The number of jobs newly enqueued or refreshed from a stale claim.
///
/// # Errors
///
/// Returns an error if the distinct pair query fails, payload serialization fails,
/// or queue insertion fails for reasons other than a duplicate active job.
pub fn seed_embed_jobs(conn: &Connection, now: i64) -> Result<usize> {
    let mut stmt = conn
        .prepare(
            "SELECT DISTINCT document_id, model_id
             FROM document_embeddings
             WHERE state IN (?1, ?2)
             ORDER BY document_id ASC, model_id ASC",
        )
        .context("failed to prepare seed_embed_jobs query")?;

    let pairs = stmt
        .query_map(params![EmbedState::Pending, EmbedState::Stale], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
        })
        .context("failed to query distinct embedding jobs to seed")?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("failed to collect distinct embedding jobs to seed")?;

    let mut seeded = 0usize;

    for (document_id, model_id) in pairs {
        let payload = serde_json::to_string(&EmbedDocumentPayload {
            document_id,
            model_id,
        })
        .expect("EmbedDocumentPayload is always serializable");

        match enqueue_job(conn, &JobType::EmbedDocument, &payload, now) {
            Ok(()) => seeded += 1,
            Err(err) if is_job_already_queued_error(&err) => {}
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "failed to enqueue embed_document job for document {} and model {}",
                        document_id, model_id
                    )
                });
            }
        }
    }

    Ok(seeded)
}

fn is_job_already_queued_error(err: &anyhow::Error) -> bool {
    err.to_string().contains("already queued or in progress")
}

fn search_documents_by_filepath_like(
    conn: &Connection,
    normalized_query: &str,
    limit: usize,
) -> Result<Vec<DocumentFilepathSearchResult>> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT d.id,
                    d.filepath,
                    CASE
                        WHEN s.normalized_basename = ?1 OR s.normalized_filepath = ?1 THEN 1.0
                        WHEN s.normalized_basename LIKE '%' || ?1 || '%' THEN 0.9
                        ELSE 0.75
                    END AS match_score
             FROM {search_table} s
             JOIN documents d ON d.id = s.document_id
             WHERE s.normalized_basename LIKE '%' || ?1 || '%'
                OR s.normalized_filepath LIKE '%' || ?1 || '%'
             ORDER BY
                 CASE
                     WHEN s.normalized_basename = ?1 OR s.normalized_filepath = ?1 THEN 0
                     WHEN s.normalized_basename LIKE '%' || ?1 || '%' THEN 1
                     ELSE 2
                 END ASC,
                 CASE
                     WHEN s.normalized_basename = ?1 OR s.normalized_basename LIKE '%' || ?1 || '%'
                         THEN length(s.normalized_basename)
                     ELSE length(s.normalized_filepath)
                 END ASC,
                 length(s.normalized_filepath) ASC,
                 d.id ASC
             LIMIT ?2",
            search_table = DOCUMENT_FILEPATH_SEARCH_TABLE,
        ))
        .context("failed to prepare short-query filepath search")?;

    let rows = stmt
        .query_map(params![normalized_query, limit as i64], |row| {
            Ok(DocumentFilepathSearchResult {
                document_id: row.get(0)?,
                filepath: row.get(1)?,
                match_score: row.get(2)?,
            })
        })
        .context("failed to query short-query filepath search results")?;

    rows.collect::<rusqlite::Result<Vec<_>>>()
        .context("failed to collect short-query filepath search results")
}

fn normalize_filepath_search_value(value: &str) -> String {
    value.trim().replace('\\', "/").to_lowercase()
}

fn basename_from_normalized_filepath(normalized_filepath: &str) -> String {
    normalized_filepath
        .rsplit('/')
        .find(|segment| !segment.is_empty())
        .unwrap_or(normalized_filepath)
        .to_string()
}

fn fts5_quote(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn expanded_filepath_candidate_limit(limit: usize) -> usize {
    limit.saturating_mul(8).min(200).max(limit)
}

fn sync_document_filepath_search_row(conn: &Connection, doc_id: i64, filepath: &str) -> Result<()> {
    let normalized_filepath = normalize_filepath_search_value(filepath);
    let normalized_basename = basename_from_normalized_filepath(&normalized_filepath);

    conn.execute(
        &format!(
            "INSERT INTO {search_table} (document_id, normalized_filepath, normalized_basename)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(document_id) DO UPDATE SET
                 normalized_filepath = excluded.normalized_filepath,
                 normalized_basename = excluded.normalized_basename",
            search_table = DOCUMENT_FILEPATH_SEARCH_TABLE,
        ),
        params![doc_id, normalized_filepath, normalized_basename],
    )
    .with_context(|| format!("failed to sync filepath search row for document {doc_id}"))?;

    conn.execute(
        &format!(
            "INSERT OR REPLACE INTO {fts_table}(rowid, normalized_filepath, normalized_basename)
             VALUES (?1, ?2, ?3)",
            fts_table = DOCUMENT_FILEPATH_SEARCH_FTS_TABLE,
        ),
        params![doc_id, normalized_filepath, normalized_basename],
    )
    .with_context(|| format!("failed to sync filepath search fts row for document {doc_id}"))?;

    Ok(())
}

fn validate_query_embedding_dimensions(
    query_embedding: &[f32],
    model: &EmbeddingModel,
) -> Result<()> {
    let expected_dimensions =
        usize::try_from(model.dimensions).context("embedding dimensions must fit into usize")?;
    anyhow::ensure!(
        query_embedding.len() == expected_dimensions,
        "query embedding length {} does not match model {} dimensions {}",
        query_embedding.len(),
        model.name,
        model.dimensions
    );
    Ok(())
}

fn ensure_active_vec_model(conn: &Connection, model: &EmbeddingModel) -> Result<()> {
    let (active_model_id, dimensions): (i64, i64) = conn
        .query_row(
            "SELECT active_model_id, dimensions
             FROM embedding_search_state
             WHERE singleton_id = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .context("failed to load active vector search model")?;

    anyhow::ensure!(
        active_model_id == model.id,
        "vector search index is configured for model id {} but search requested {}",
        active_model_id,
        model.id
    );
    anyhow::ensure!(
        dimensions == model.dimensions,
        "vector search index dimensions {} do not match model {} dimensions {}",
        dimensions,
        model.name,
        model.dimensions
    );
    anyhow::ensure!(
        sqlite_table_exists(conn, DOCUMENT_EMBEDDING_VEC_TABLE)?,
        "vector search table {} is missing",
        DOCUMENT_EMBEDDING_VEC_TABLE
    );
    Ok(())
}

fn load_expected_embedding_bytes(conn: &Connection, model_id: i64) -> Result<usize> {
    let dimensions: i64 = conn
        .query_row(
            "SELECT dimensions FROM embedding_models WHERE id = ?1",
            [model_id],
            |row| row.get(0),
        )
        .with_context(|| format!("failed to load dimensions for model {}", model_id))?;
    let dimensions =
        usize::try_from(dimensions).context("embedding dimensions must fit into usize")?;
    Ok(dimensions * std::mem::size_of::<f32>())
}

fn validate_chunk_embedding_lengths(
    chunks: &[EmbeddedChunk],
    expected_embedding_bytes: usize,
) -> Result<()> {
    for chunk in chunks {
        anyhow::ensure!(
            chunk.embedding.len() == expected_embedding_bytes,
            "embedded chunk ({}, {}) stored {} bytes but expected {}",
            chunk.chunk_type,
            chunk.chunk_start,
            chunk.embedding.len(),
            expected_embedding_bytes
        );
    }
    Ok(())
}

fn serialize_embedding(vector: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(std::mem::size_of_val(vector));
    for value in vector {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

fn sqlite_table_exists(conn: &Connection, table_name: &str) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?1)",
        [table_name],
        |row| row.get(0),
    )
    .with_context(|| format!("failed to query sqlite_master for table {table_name}"))
}

fn delete_vec_rows_for_documents(conn: &Connection, doc_ids: &[i64]) -> Result<()> {
    if doc_ids.is_empty() || !sqlite_table_exists(conn, DOCUMENT_EMBEDDING_VEC_TABLE)? {
        return Ok(());
    }

    let placeholders: Vec<String> = (0..doc_ids.len())
        .map(|index| format!("?{}", index + 1))
        .collect();
    let sql = format!(
        "DELETE FROM {DOCUMENT_EMBEDDING_VEC_TABLE}
         WHERE rowid IN (
             SELECT id
             FROM document_embeddings
             WHERE document_id IN ({})
         )",
        placeholders.join(", ")
    );

    let mut stmt = conn
        .prepare(&sql)
        .context("failed to prepare vec delete for documents")?;
    let param_refs: Vec<&dyn rusqlite::types::ToSql> = doc_ids
        .iter()
        .map(|id| id as &dyn rusqlite::types::ToSql)
        .collect();
    stmt.execute(param_refs.as_slice())
        .context("failed to delete vec rows for documents")?;
    Ok(())
}

fn delete_vec_rows_for_document_model(conn: &Connection, doc_id: i64, model_id: i64) -> Result<()> {
    if !sqlite_table_exists(conn, DOCUMENT_EMBEDDING_VEC_TABLE)? {
        return Ok(());
    }

    conn.execute(
        "DELETE FROM document_embedding_vec
         WHERE rowid IN (
             SELECT id FROM document_embeddings WHERE document_id = ?1 AND model_id = ?2
         )",
        params![doc_id, model_id],
    )
    .context("failed to delete vec rows for document/model pair")?;
    Ok(())
}

fn sync_vec_rows_for_document_model(conn: &Connection, doc_id: i64, model_id: i64) -> Result<()> {
    if !sqlite_table_exists(conn, DOCUMENT_EMBEDDING_VEC_TABLE)? {
        return Ok(());
    }

    conn.execute(
        "INSERT OR REPLACE INTO document_embedding_vec(rowid, embedding)
         SELECT id, embedding
         FROM document_embeddings
         WHERE document_id = ?1
           AND model_id = ?2
           AND state = ?3
           AND embedding IS NOT NULL",
        params![doc_id, model_id, EmbedState::Embedded],
    )
    .context("failed to sync vec rows for document/model pair")?;
    Ok(())
}

fn delete_vec_rows_for_non_embedded_document_model(
    conn: &Connection,
    doc_id: i64,
    model_id: i64,
) -> Result<()> {
    if !sqlite_table_exists(conn, DOCUMENT_EMBEDDING_VEC_TABLE)? {
        return Ok(());
    }

    conn.execute(
        "DELETE FROM document_embedding_vec
         WHERE rowid IN (
             SELECT id
             FROM document_embeddings
             WHERE document_id = ?1
               AND model_id = ?2
               AND state != ?3
         )",
        params![doc_id, model_id, EmbedState::Embedded],
    )
    .context("failed to delete vec rows for non-embedded document/model rows")?;
    Ok(())
}

fn current_unix_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before epoch")
        .as_secs() as i64
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations;
    use crate::types::{ChunkType, DocCategory};

    fn setup_documents_db() -> Connection {
        let mut conn = Connection::open_in_memory().expect("in-memory database must open");
        migrations::apply_pending_migrations(&mut conn)
            .expect("core migrations must initialize repository test schema");
        conn
    }

    #[test]
    fn transactional_document_ingest_rolls_back_when_queue_step_fails() {
        let mut conn = setup_documents_db();
        conn.execute(
            "INSERT INTO embedding_models (id, name, dimensions, chunk_size, chunk_overlap, created_at)
             VALUES (1, 'test', 1, 16, 0, 100)",
            [],
        )
        .expect("embedding model insert must succeed");
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

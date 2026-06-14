use std::collections::HashSet;

use anyhow::{Context, anyhow};
use ask_core::models::{EmbedDocumentPayload, EmbeddingModel};
use ask_core::types::{EmbedState, JobType};
use axum::{Json, extract::State, http::StatusCode};
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

use super::{AppState, error_response, load_active_model};

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct EmbeddingStatsResponse {
    model: ActiveEmbeddingModelResponse,
    total_documents: i64,
    embedded_documents: i64,
    failed_locked_documents: i64,
    remaining_documents: i64,
    progress_percent: f64,
    documents_embedded_last_five_minutes: i64,
    estimated_documents_per_hour: f64,
    documents_by_file_type: Vec<DocumentFileTypeCount>,
    document_embeddings_total: i64,
    document_embeddings_embedded: i64,
    document_embeddings_pending: i64,
    document_embeddings_stale: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    estimated_hours_remaining: Option<f64>,
}

#[derive(Debug)]
struct EmbeddingStatsSnapshot {
    total_documents: i64,
    embedded_documents: i64,
    failed_locked_documents: i64,
    remaining_documents: i64,
    documents_embedded_last_five_minutes: i64,
    documents_by_file_type: Vec<DocumentFileTypeCount>,
    document_embeddings_total: i64,
    document_embeddings_embedded: i64,
    document_embeddings_pending: i64,
    document_embeddings_stale: i64,
}

#[derive(Debug, Serialize, ToSchema)]
struct ActiveEmbeddingModelResponse {
    id: i64,
    name: String,
    dimensions: i64,
    chunk_size: i64,
    chunk_overlap: i64,
    created_at: i64,
}

#[derive(Debug, Serialize, ToSchema)]
struct DocumentFileTypeCount {
    file_type: String,
    document_count: i64,
}

#[utoipa::path(
    get,
    path = "/embedding/stats",
    responses(
        (status = 200, description = "Embedding stats for the active model", body = EmbeddingStatsResponse),
        (status = 500, description = "Internal server error")
    )
)]
pub(crate) async fn embedding_stats(
    State(state): State<AppState>,
) -> Result<Json<EmbeddingStatsResponse>, (StatusCode, Json<Value>)> {
    let pool = state.pool().clone();

    let response = tokio::task::spawn_blocking(move || {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time before epoch")
            .as_secs() as i64;
        let conn = pool.get().map_err(|err| anyhow!("database error: {err}"))?;
        let model = load_active_model(&conn)?;
        let snapshot = load_embedding_stats_snapshot(&conn, &model, now)?;
        Ok::<_, anyhow::Error>(build_embedding_stats_response(&model, snapshot))
    })
    .await
    .map_err(|err| {
        error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            format!("request panicked: {err}"),
        )
    })?
    .map_err(|err| {
        error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            format!("failed to load embedding stats: {err:#}"),
        )
    })?;

    Ok(Json(response))
}

fn load_embedding_stats_snapshot(
    conn: &rusqlite::Connection,
    model: &EmbeddingModel,
    now: i64,
) -> anyhow::Result<EmbeddingStatsSnapshot> {
    const FAILED_LOCKOUT_SECS: i64 = 24 * 60 * 60;
    const RECENT_RATE_WINDOW_SECS: i64 = 5 * 60;

    let stale_cutoff = now - FAILED_LOCKOUT_SECS;
    let recent_window_start = now - RECENT_RATE_WINDOW_SECS;
    let total_documents = conn
        .query_row("SELECT COUNT(*) FROM documents", [], |row| {
            row.get::<_, i64>(0)
        })
        .context("failed to count documents")?;
    let recoverable_documents = load_distinct_document_ids_for_states(
        conn,
        model.id,
        &[EmbedState::Pending, EmbedState::Stale],
    )?;
    let embedded_documents =
        load_distinct_document_ids_for_states(conn, model.id, &[EmbedState::Embedded])?;
    let claimed_documents = load_claimed_embed_document_ids(conn, model.id, stale_cutoff)?;
    let recent_embedded_documents =
        load_recent_embedded_document_ids(conn, model.id, recent_window_start)?;
    let documents_by_file_type = load_document_file_type_counts(conn)?;
    let (
        document_embeddings_total,
        document_embeddings_embedded,
        document_embeddings_pending,
        document_embeddings_stale,
    ) = load_document_embedding_counts(conn, model.id)?;

    let failed_locked_documents = recoverable_documents
        .intersection(&claimed_documents)
        .count() as i64;
    let embedded_documents = embedded_documents
        .difference(&recoverable_documents)
        .count() as i64;
    let documents_embedded_last_five_minutes = recent_embedded_documents
        .difference(&recoverable_documents)
        .count() as i64;
    let remaining_documents = total_documents
        .saturating_sub(embedded_documents)
        .saturating_sub(failed_locked_documents);

    Ok(EmbeddingStatsSnapshot {
        total_documents,
        embedded_documents,
        failed_locked_documents,
        remaining_documents,
        documents_embedded_last_five_minutes,
        documents_by_file_type,
        document_embeddings_total,
        document_embeddings_embedded,
        document_embeddings_pending,
        document_embeddings_stale,
    })
}

fn load_distinct_document_ids_for_states(
    conn: &rusqlite::Connection,
    model_id: i64,
    states: &[EmbedState],
) -> anyhow::Result<HashSet<i64>> {
    let mut document_ids = HashSet::new();
    let mut stmt = conn
        .prepare(
            "SELECT DISTINCT document_id
             FROM document_embeddings
             WHERE model_id = ?1
               AND state = ?2",
        )
        .context("failed to prepare embedding document-state query")?;

    for state in states {
        let rows = stmt
            .query_map(rusqlite::params![model_id, state], |row| {
                row.get::<_, i64>(0)
            })
            .with_context(|| format!("failed to query embedding documents in state {state}"))?;
        for document_id in rows {
            document_ids.insert(document_id.context("failed to read embedding document id")?);
        }
    }

    Ok(document_ids)
}

fn load_claimed_embed_document_ids(
    conn: &rusqlite::Connection,
    model_id: i64,
    claimed_after: i64,
) -> anyhow::Result<HashSet<i64>> {
    let mut stmt = conn
        .prepare(
            "SELECT payload
             FROM job_queue
             WHERE job_type = ?1
               AND claimed_at IS NOT NULL
               AND claimed_at >= ?2",
        )
        .context("failed to prepare claimed embed-document job query")?;
    let rows = stmt
        .query_map(
            rusqlite::params![JobType::EmbedDocument, claimed_after],
            |row| row.get::<_, String>(0),
        )
        .context("failed to query claimed embed-document jobs")?;

    let mut document_ids = HashSet::new();
    for payload in rows {
        let payload = payload.context("failed to read claimed job payload")?;
        let payload: EmbedDocumentPayload = serde_json::from_str(&payload)
            .context("failed to decode claimed embed-document payload")?;
        if payload.model_id == model_id {
            document_ids.insert(payload.document_id);
        }
    }

    Ok(document_ids)
}

fn load_recent_embedded_document_ids(
    conn: &rusqlite::Connection,
    model_id: i64,
    created_after: i64,
) -> anyhow::Result<HashSet<i64>> {
    let mut stmt = conn
        .prepare(
            "SELECT DISTINCT document_id
             FROM document_embeddings
             WHERE model_id = ?1
               AND state = ?2
               AND created_at >= ?3",
        )
        .context("failed to prepare recent embedded-document query")?;
    let rows = stmt
        .query_map(
            rusqlite::params![model_id, EmbedState::Embedded, created_after],
            |row| row.get::<_, i64>(0),
        )
        .context("failed to query recent embedded documents")?;

    let mut document_ids = HashSet::new();
    for document_id in rows {
        document_ids.insert(document_id.context("failed to read recent embedded document id")?);
    }

    Ok(document_ids)
}

fn load_document_file_type_counts(
    conn: &rusqlite::Connection,
) -> anyhow::Result<Vec<DocumentFileTypeCount>> {
    let mut stmt = conn
        .prepare(
            "SELECT file_type, COUNT(*)
             FROM documents
             GROUP BY file_type
             ORDER BY file_type ASC",
        )
        .context("failed to prepare document file-type count query")?;
    let rows = stmt
        .query_map([], |row| {
            Ok(DocumentFileTypeCount {
                file_type: row.get::<_, String>(0)?,
                document_count: row.get::<_, i64>(1)?,
            })
        })
        .context("failed to query document file-type counts")?;

    rows.collect::<Result<Vec<_>, _>>()
        .context("failed to collect document file-type counts")
}

fn load_document_embedding_counts(
    conn: &rusqlite::Connection,
    model_id: i64,
) -> anyhow::Result<(i64, i64, i64, i64)> {
    conn.query_row(
        "SELECT
             COUNT(*) AS total,
             SUM(CASE WHEN state = ?2 THEN 1 ELSE 0 END) AS embedded,
             SUM(CASE WHEN state = ?3 THEN 1 ELSE 0 END) AS pending,
             SUM(CASE WHEN state = ?4 THEN 1 ELSE 0 END) AS stale
         FROM document_embeddings
         WHERE model_id = ?1",
        rusqlite::params![
            model_id,
            EmbedState::Embedded,
            EmbedState::Pending,
            EmbedState::Stale
        ],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        },
    )
    .context("failed to query document embedding counts")
}

fn build_embedding_stats_response(
    model: &EmbeddingModel,
    snapshot: EmbeddingStatsSnapshot,
) -> EmbeddingStatsResponse {
    let progress_percent = if snapshot.total_documents == 0 {
        0.0
    } else {
        (snapshot.embedded_documents as f64 / snapshot.total_documents as f64) * 100.0
    };
    let estimated_documents_per_hour = snapshot.documents_embedded_last_five_minutes as f64 * 12.0;
    let estimated_hours_remaining = if estimated_documents_per_hour > 0.0 {
        Some(snapshot.remaining_documents as f64 / estimated_documents_per_hour)
    } else {
        None
    };

    EmbeddingStatsResponse {
        model: ActiveEmbeddingModelResponse {
            id: model.id,
            name: model.name.clone(),
            dimensions: model.dimensions,
            chunk_size: model.chunk_size,
            chunk_overlap: model.chunk_overlap,
            created_at: model.created_at,
        },
        total_documents: snapshot.total_documents,
        embedded_documents: snapshot.embedded_documents,
        failed_locked_documents: snapshot.failed_locked_documents,
        remaining_documents: snapshot.remaining_documents,
        progress_percent,
        documents_embedded_last_five_minutes: snapshot.documents_embedded_last_five_minutes,
        estimated_documents_per_hour,
        documents_by_file_type: snapshot.documents_by_file_type,
        document_embeddings_total: snapshot.document_embeddings_total,
        document_embeddings_embedded: snapshot.document_embeddings_embedded,
        document_embeddings_pending: snapshot.document_embeddings_pending,
        document_embeddings_stale: snapshot.document_embeddings_stale,
        estimated_hours_remaining,
    }
}

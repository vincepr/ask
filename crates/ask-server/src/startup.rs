use anyhow::Result;
use ask_core::models::{EmbeddingIdentity, EmbeddingModel};
use ask_core::repository;
use ask_core::types::EmbedState;
use rusqlite::params;

use crate::worker;

/// High-level startup state after lightweight reconciliation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupSummaryKind {
    /// No documents exist yet, so the next action is manual ingest.
    Empty,
    /// Existing pending or stale work was observed during startup.
    Recovered,
    /// Documents exist and there is no recoverable embedding work pending.
    Idle,
}

/// Summary of embedding-related startup reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupEmbeddingState {
    /// Resolved persisted model row for the configured embedding identity.
    pub model: EmbeddingModel,
    /// Number of existing documents backfilled because the model row was newly created.
    pub backfilled_documents: usize,
    /// Number of `embed_document` jobs seeded during the unconditional startup pass.
    pub seeded_jobs: usize,
    /// Number of persisted documents visible at startup.
    pub document_count: usize,
    /// Number of distinct `(document_id, model_id)` pairs in `pending` or `stale` state.
    pub recoverable_pairs: usize,
    /// High-level startup summary classification used for logging.
    pub summary_kind: StartupSummaryKind,
}

/// Resolve the configured model row and reconcile pending embedding work.
///
/// When the model identity does not yet exist, this inserts it and backfills
/// pending rows for every existing document. Regardless of whether the model was
/// new or already present, this always performs one `seed_embed_jobs()` pass so
/// orphaned `pending` or `stale` rows are re-queued before workers start.
///
/// # Arguments
///
/// * `conn` - Open SQLite connection used for startup reconciliation.
/// * `identity` - Configured embedding identity for this process.
/// * `now` - Unix timestamp used for newly inserted rows and queued jobs.
///
/// # Returns
///
/// The resolved model plus counts for new-model backfill and unconditional job
/// seeding.
///
/// # Errors
///
/// Returns an error if model lookup or insert fails, pending backfill fails, or
/// startup job seeding fails.
pub fn reconcile_embedding_startup(
    conn: &rusqlite::Connection,
    identity: EmbeddingIdentity,
    now: i64,
) -> Result<StartupEmbeddingState> {
    let mut backfilled_documents = 0;

    let model = match repository::find_model_by_identity(conn, &identity)? {
        Some(model) => model,
        None => {
            let new_model = EmbeddingModel {
                id: 0,
                name: identity.name,
                dimensions: identity.dimensions,
                chunk_size: identity.chunk_size,
                chunk_overlap: identity.chunk_overlap,
                created_at: now,
            };
            let model = EmbeddingModel {
                id: repository::insert_model(conn, &new_model)?,
                ..new_model
            };
            backfilled_documents = worker::backfill_pending_for_model(conn, &model, now)?;
            model
        }
    };

    let seeded_jobs = repository::seed_embed_jobs(conn, now)?;
    let document_count = load_document_count(conn)?;
    let recoverable_pairs = load_recoverable_pair_count(conn)?;
    let summary_kind = classify_summary(document_count, recoverable_pairs);

    Ok(StartupEmbeddingState {
        model,
        backfilled_documents,
        seeded_jobs,
        document_count,
        recoverable_pairs,
        summary_kind,
    })
}

fn load_document_count(conn: &rusqlite::Connection) -> Result<usize> {
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM documents", [], |row| row.get(0))?;
    Ok(usize::try_from(count).expect("document count must fit into usize"))
}

fn load_recoverable_pair_count(conn: &rusqlite::Connection) -> Result<usize> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM (
             SELECT DISTINCT document_id, model_id
             FROM document_embeddings
             WHERE state IN (?1, ?2)
         )",
        params![EmbedState::Pending, EmbedState::Stale],
        |row| row.get(0),
    )?;
    Ok(usize::try_from(count).expect("recoverable pair count must fit into usize"))
}

fn classify_summary(document_count: usize, recoverable_pairs: usize) -> StartupSummaryKind {
    if document_count == 0 {
        StartupSummaryKind::Empty
    } else if recoverable_pairs > 0 {
        StartupSummaryKind::Recovered
    } else {
        StartupSummaryKind::Idle
    }
}

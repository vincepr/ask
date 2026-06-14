use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use ask_core::models::{EmbeddingModel, JobQueueEntry};
use ask_core::repository;
use ask_core::types::JobType;
use tracing::{error, info};

use crate::DbPool;
use crate::embeddings::{EmbeddingClient, EmbeddingError, SharedEmbeddingClient};

mod chunking;
mod embed_document;
mod ingest;

use embed_document::EmbedDocumentHandler;
use ingest::{IngestFolderGitHandler, IngestFolderHandler};

const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Spawns a background task that polls for unclaimed or stale jobs.
pub fn spawn(
    pool: DbPool,
    model_id: i64,
    embedding_client: SharedEmbeddingClient,
    worker_count: usize,
) {
    if worker_count == 0 {
        info!("background embedding workers disabled");
        return;
    }

    for worker_id in 0..worker_count {
        let pool = pool.clone();
        let embedding_client = embedding_client.clone();

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(POLL_INTERVAL).await;

                if let Err(err) = tick(pool.clone(), model_id, embedding_client.clone()).await {
                    error!(
                        worker_id,
                        error = %format!("{err:#}"),
                        "worker tick failed"
                    );
                }
            }
        });
    }
}

/// Dispatch a claimed job to the appropriate handler.
///
/// Successful handlers remove the queue row. Failed handlers leave the claim in
/// place so the row becomes claimable again only after the stale timeout.
pub fn dispatch_job(
    pool: &DbPool,
    entry: &JobQueueEntry,
    model_id: i64,
    embedding_client: Arc<dyn EmbeddingClient>,
) -> Result<()> {
    dispatch_job_with_resolver(pool, entry, model_id, embedding_client, resolve_handler)
}

/// Queue pending embeddings for every existing document under a new model.
///
/// This reuses the same filename/content chunking plan as normal ingest so a
/// newly registered model sees the full existing corpus.
///
/// # Errors
///
/// Returns an error if existing documents cannot be listed or embedding rows
/// cannot be queued for a document.
pub fn backfill_pending_for_model(
    conn: &rusqlite::Connection,
    model: &EmbeddingModel,
    now: i64,
) -> Result<usize> {
    let docs = repository::list_documents(conn)?;
    let mut count = 0;

    for doc in docs {
        ingest::queue_pending_embeddings_for_document(
            conn,
            std::path::Path::new(&doc.filepath),
            doc.id,
            model,
            now,
        )
        .with_context(|| {
            format!(
                "failed to backfill pending embeddings for document {} at {}",
                doc.id, doc.filepath
            )
        })?;
        count += 1;
    }

    repository::seed_embed_jobs(conn, now)?;

    Ok(count)
}

async fn tick(pool: DbPool, model_id: i64, embedding_client: SharedEmbeddingClient) -> Result<()> {
    tokio::task::spawn_blocking(move || -> Result<()> {
        let now = unix_now();
        let entry = match claim_pending_job(&pool, now)? {
            Some(entry) => entry,
            None => return Ok(()),
        };

        info!(job_id = entry.id, job_type = %entry.job_type, "claimed job");
        dispatch_job_with_resolver(&pool, &entry, model_id, embedding_client, resolve_handler)
    })
    .await
    .context("worker tick panicked")??;

    Ok(())
}

fn claim_pending_job(pool: &DbPool, now: i64) -> Result<Option<JobQueueEntry>> {
    let mut conn = pool.get()?;
    repository::claim_job(&mut conn, now)
}

struct JobContext<'a> {
    pool: &'a DbPool,
    entry: &'a JobQueueEntry,
    ingest_model_id: i64,
    embedding_client: SharedEmbeddingClient,
}

trait JobHandler: Send + Sync {
    fn job_type(&self) -> JobType;

    /// Process the claimed job.
    ///
    /// Called from within `spawn_blocking`, so blocking I/O is fine.
    fn process(&self, ctx: JobContext<'_>) -> Result<()>;
}

fn dispatch_job_with_resolver<R>(
    pool: &DbPool,
    entry: &JobQueueEntry,
    model_id: i64,
    embedding_client: SharedEmbeddingClient,
    resolve_handler: R,
) -> Result<()>
where
    R: Fn(JobType) -> Box<dyn JobHandler>,
{
    let handler = resolve_handler(entry.job_type);
    if handler.job_type() != entry.job_type {
        return Err(anyhow!(
            "job handler registry mismatch for job {}: claimed {}, resolved {}",
            entry.id,
            entry.job_type,
            handler.job_type()
        ));
    }

    let result = handler.process(JobContext {
        pool,
        entry,
        ingest_model_id: model_id,
        embedding_client,
    });

    match result {
        Ok(()) => {
            complete_job(pool, entry.id)?;
            info!(job_id = entry.id, job_type = %entry.job_type, "job completed");
            Ok(())
        }
        Err(err) => {
            if let Some(retry_after_secs) = embedding_retry_after_secs(&err) {
                defer_job_retry(pool, entry.id, unix_now(), retry_after_secs)?;
                error!(
                    job_id = entry.id,
                    job_type = %entry.job_type,
                    retry_after_secs,
                    error = %format!("{err:#}"),
                    "job failed transiently; deferred retry"
                );
                return Err(err.context(format!("job {} ({})", entry.id, entry.job_type)));
            }

            error!(
                job_id = entry.id,
                job_type = %entry.job_type,
                error = %format!("{err:#}"),
                "job failed; leaving claim in place until stale"
            );
            Err(err.context(format!("job {} ({})", entry.id, entry.job_type)))
        }
    }
}

fn resolve_handler(job_type: JobType) -> Box<dyn JobHandler> {
    match job_type {
        JobType::EmbedDocument => Box::new(EmbedDocumentHandler),
        JobType::IngestFolder => Box::new(IngestFolderHandler),
        JobType::IngestFolderGit => Box::new(IngestFolderGitHandler),
    }
}

fn complete_job(pool: &DbPool, job_id: i64) -> Result<()> {
    let conn = pool
        .get()
        .with_context(|| format!("failed to acquire connection to complete job {job_id}"))?;
    repository::complete_job(&conn, job_id)
        .with_context(|| format!("failed to complete job {job_id}"))?;
    Ok(())
}

fn defer_job_retry(pool: &DbPool, job_id: i64, now: i64, retry_after_secs: u64) -> Result<()> {
    let conn = pool
        .get()
        .with_context(|| format!("failed to acquire connection to defer job {job_id}"))?;
    repository::defer_job_for_retry(&conn, job_id, now, retry_after_secs)
        .with_context(|| format!("failed to defer job {job_id}"))?;
    Ok(())
}

fn embedding_retry_after_secs(err: &anyhow::Error) -> Option<u64> {
    err.chain().find_map(|cause| {
        cause
            .downcast_ref::<EmbeddingError>()
            .and_then(EmbeddingError::retry_after_secs)
    })
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before epoch")
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use anyhow::{Result, anyhow};
    use ask_core::migrations;
    use ask_core::models::JobQueueEntry;
    use ask_core::repository;
    use ask_core::types::JobType;

    use super::*;
    use crate::create_pool;
    use crate::embeddings::{
        DeterministicEmbeddingClient, EmbeddingError, TRANSIENT_RETRY_DELAY_SECS,
    };

    static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDb {
        dir: PathBuf,
        pool: Option<DbPool>,
    }

    impl TempDb {
        fn new() -> Self {
            let dir = unique_temp_dir();
            std::fs::create_dir_all(&dir).unwrap();
            let db_path = dir.join("ask.sqlite3");
            let pool = create_pool(&db_path.to_string_lossy()).unwrap();
            let mut conn = pool.get().unwrap();
            migrations::apply_pending_migrations(&mut conn).unwrap();

            Self {
                dir,
                pool: Some(pool),
            }
        }

        fn pool(&self) -> DbPool {
            self.pool.clone().unwrap()
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            drop(self.pool.take());
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    struct FailingHandler;

    struct TransientEmbedFailingHandler;

    impl JobHandler for FailingHandler {
        fn job_type(&self) -> JobType {
            JobType::IngestFolder
        }

        fn process(&self, _ctx: JobContext<'_>) -> Result<()> {
            Err(anyhow!("synthetic handler failure"))
        }
    }

    impl JobHandler for TransientEmbedFailingHandler {
        fn job_type(&self) -> JobType {
            JobType::EmbedDocument
        }

        fn process(&self, _ctx: JobContext<'_>) -> Result<()> {
            Err(anyhow::Error::new(EmbeddingError::retryable(
                anyhow!("synthetic upstream overload"),
                TRANSIENT_RETRY_DELAY_SECS,
            )))
        }
    }

    fn current_time() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    fn unique_temp_dir() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos();
        let counter = TEMP_DIR_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);

        std::env::temp_dir().join(format!("ask-worker-test-{unique}-{counter}"))
    }

    fn enqueue_and_claim_job(
        db: &TempDb,
        payload: &str,
        queued_at: i64,
        claimed_at: i64,
    ) -> JobQueueEntry {
        enqueue_and_claim_job_with_type(db, JobType::IngestFolder, payload, queued_at, claimed_at)
    }

    fn enqueue_and_claim_job_with_type(
        db: &TempDb,
        job_type: JobType,
        payload: &str,
        queued_at: i64,
        claimed_at: i64,
    ) -> JobQueueEntry {
        let conn = db.pool().get().unwrap();
        repository::enqueue_job(&conn, &job_type, payload, queued_at).unwrap();

        let mut conn = db.pool().get().unwrap();
        repository::claim_job(&mut conn, claimed_at)
            .unwrap()
            .expect("job should be claimable")
    }

    fn job_queue_count(pool: &DbPool) -> i64 {
        let conn = pool.get().unwrap();
        conn.query_row("SELECT COUNT(*) FROM job_queue", [], |row| row.get(0))
            .unwrap()
    }

    #[test]
    fn claim_pending_job_releases_pool_slot_before_dispatch() {
        let db = TempDb::new();
        let payload = r#"{"root_path":"/tmp"}"#;
        let queued_at = 10;
        let claimed_at = 100;
        let conn = db.pool().get().unwrap();
        repository::enqueue_job(&conn, &JobType::IngestFolder, payload, queued_at).unwrap();
        drop(conn);

        let pool = db.pool();
        let held_connections = vec![
            pool.get().unwrap(),
            pool.get().unwrap(),
            pool.get().unwrap(),
        ];

        let claimed = claim_pending_job(&pool, claimed_at)
            .unwrap()
            .expect("job should be claimable");
        assert_eq!(claimed.claimed_at, Some(claimed_at));

        let extra_conn = pool
            .get_timeout(Duration::from_millis(200))
            .expect("claim helper must release its pool slot");
        drop(extra_conn);
        drop(held_connections);
    }

    #[test]
    fn dispatcher_returns_handler_failures_and_keeps_job_row() {
        let db = TempDb::new();
        let entry = enqueue_and_claim_job(&db, r#"{"root_path":"/tmp"}"#, 10, 100);
        let pool = db.pool();

        let err = dispatch_job_with_resolver(
            &pool,
            &entry,
            1,
            Arc::new(DeterministicEmbeddingClient::new()),
            |_| Box::new(FailingHandler),
        )
        .unwrap_err();
        let err_text = format!("{err:#}");

        assert!(
            err_text.contains("synthetic handler failure"),
            "unexpected error: {err:#}"
        );
        assert_eq!(job_queue_count(&pool), 1);

        let conn = pool.get().unwrap();
        let claimed_at: Option<i64> = conn
            .query_row(
                "SELECT claimed_at FROM job_queue WHERE id = ?1",
                [entry.id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(claimed_at, Some(100));
    }

    #[test]
    fn failed_job_becomes_claimable_again_after_stale_timeout() {
        let db = TempDb::new();
        let entry = enqueue_and_claim_job(&db, r#"{"root_path":"/tmp"}"#, 10, 100);
        let pool = db.pool();

        dispatch_job_with_resolver(
            &pool,
            &entry,
            1,
            Arc::new(DeterministicEmbeddingClient::new()),
            |_| Box::new(FailingHandler),
        )
        .unwrap_err();

        let mut conn = pool.get().unwrap();
        let reclaimed = repository::claim_job(&mut conn, 100 + 86_400 + 1)
            .unwrap()
            .expect("stale failed job should be claimable again");

        assert_eq!(reclaimed.id, entry.id);
        assert_eq!(reclaimed.claimed_at, Some(100 + 86_400 + 1));
    }

    #[test]
    fn transient_embed_failure_defers_retry_instead_of_waiting_for_stale_timeout() {
        let db = TempDb::new();
        let entry = enqueue_and_claim_job_with_type(
            &db,
            JobType::EmbedDocument,
            r#"{"document_id":7,"model_id":1}"#,
            10,
            100,
        );
        let pool = db.pool();

        let err = dispatch_job_with_resolver(
            &pool,
            &entry,
            1,
            Arc::new(DeterministicEmbeddingClient::new()),
            |_| Box::new(TransientEmbedFailingHandler),
        )
        .unwrap_err();
        let err_text = format!("{err:#}");

        assert!(
            err_text.contains("synthetic upstream overload"),
            "unexpected error: {err:#}"
        );

        let mut conn = pool.get().unwrap();
        let deferred_claimed_at: i64 = conn
            .query_row(
                "SELECT claimed_at FROM job_queue WHERE id = ?1",
                [entry.id],
                |row| row.get(0),
            )
            .unwrap();
        let retry_ready_at = deferred_claimed_at + 86_400;

        let too_early = repository::claim_job(&mut conn, retry_ready_at).unwrap();
        assert!(
            too_early.is_none(),
            "transient embed failure should not be re-claimed before 5 minutes"
        );

        let retried = repository::claim_job(&mut conn, retry_ready_at + 1)
            .unwrap()
            .expect("job should become claimable after 5-minute cooldown");
        assert_eq!(retried.id, entry.id);
        assert_eq!(retried.claimed_at, Some(retry_ready_at + 1));
    }

    #[test]
    fn dispatcher_surfaces_payload_decode_failures_and_keeps_job_row() {
        let db = TempDb::new();
        let entry = enqueue_and_claim_job(&db, "{not json", 10, 100);
        let pool = db.pool();

        let err = dispatch_job(
            &pool,
            &entry,
            1,
            Arc::new(DeterministicEmbeddingClient::new()),
        )
        .unwrap_err();
        let err_text = format!("{err:#}");

        assert!(
            err_text.contains("failed to decode payload"),
            "unexpected error: {err:#}"
        );
        assert_eq!(job_queue_count(&pool), 1);
    }

    #[test]
    fn fixed_utf8_chunks_empty() {
        assert!(chunking::fixed_utf8_chunks("", 10, 0).is_empty());
    }

    #[test]
    fn fixed_utf8_chunks_zero_chunk_size() {
        assert!(chunking::fixed_utf8_chunks("hello", 0, 0).is_empty());
    }

    #[test]
    fn fixed_utf8_chunks_smaller_than_chunk() {
        let chunks = chunking::fixed_utf8_chunks("hello", 100, 0);
        assert_eq!(chunks, vec![chunking::ChunkSpan { start: 0, end: 5 }]);
    }

    #[test]
    fn fixed_utf8_chunks_exact_size() {
        let chunks = chunking::fixed_utf8_chunks("12345", 5, 0);
        assert_eq!(chunks, vec![chunking::ChunkSpan { start: 0, end: 5 }]);
    }

    #[test]
    fn fixed_utf8_chunks_multiple_chunks_no_overlap() {
        let chunks = chunking::fixed_utf8_chunks("abcdefghij", 5, 0);
        assert_eq!(
            chunks,
            vec![
                chunking::ChunkSpan { start: 0, end: 5 },
                chunking::ChunkSpan { start: 5, end: 10 },
            ]
        );
    }

    #[test]
    fn fixed_utf8_chunks_with_overlap() {
        let chunks = chunking::fixed_utf8_chunks("abcdefghij", 6, 2);
        assert_eq!(
            chunks,
            vec![
                chunking::ChunkSpan { start: 0, end: 6 },
                chunking::ChunkSpan { start: 4, end: 10 },
            ]
        );
    }

    #[test]
    fn fixed_utf8_chunks_full_overlap_returns_single_chunk() {
        let chunks = chunking::fixed_utf8_chunks("hello world", 5, 5);
        assert_eq!(chunks, vec![chunking::ChunkSpan { start: 0, end: 11 }]);
    }

    #[test]
    fn fixed_utf8_chunks_overlap_greater_than_size() {
        let chunks = chunking::fixed_utf8_chunks("hello world", 5, 10);
        assert_eq!(chunks, vec![chunking::ChunkSpan { start: 0, end: 11 }]);
    }

    #[test]
    fn fixed_utf8_chunks_chunk_size_one() {
        let chunks = chunking::fixed_utf8_chunks("abc", 1, 0);
        assert_eq!(
            chunks,
            vec![
                chunking::ChunkSpan { start: 0, end: 1 },
                chunking::ChunkSpan { start: 1, end: 2 },
                chunking::ChunkSpan { start: 2, end: 3 },
            ]
        );
    }

    #[test]
    fn fixed_utf8_chunks_never_split_multibyte_scalar() {
        let chunks = chunking::fixed_utf8_chunks("éé", 3, 0);
        assert_eq!(
            chunks,
            vec![
                chunking::ChunkSpan { start: 0, end: 2 },
                chunking::ChunkSpan { start: 2, end: 4 },
            ]
        );
    }

    #[test]
    fn fixed_utf8_planner_implements_chunk_planner_trait() {
        fn plan_with_trait(
            planner: &impl chunking::ChunkPlanner,
            content: &str,
        ) -> chunking::ChunkPlan {
            planner.plan(content, 5, 0)
        }

        let planner = chunking::FixedUtf8ChunkPlanner;
        let plan = plan_with_trait(&planner, "abcdefghij");

        assert_eq!(plan.strategy, "fixed_utf8");
        assert_eq!(
            plan.spans,
            vec![
                chunking::ChunkSpan { start: 0, end: 5 },
                chunking::ChunkSpan { start: 5, end: 10 },
            ]
        );
    }

    #[test]
    fn structure_chunks_prefers_heading_breakpoint() {
        let content = "# Alpha\nfirst paragraph\n\n## Beta\nsecond paragraph\n";
        let split = content.find("## Beta").unwrap();
        let plan = chunking::structure_chunks(content, 28, 0, 16);

        assert_eq!(
            plan[0],
            chunking::ChunkSpan {
                start: 0,
                end: split
            }
        );
    }

    #[test]
    fn structure_chunks_uses_blank_line_before_list_item() {
        let content = "intro paragraph\n\n- first item\n- second item\n";
        let split = content.find("- first item").unwrap();
        let plan = chunking::structure_chunks(content, 24, 0, 12);

        assert_eq!(
            plan[0],
            chunking::ChunkSpan {
                start: 0,
                end: split
            }
        );
    }

    #[test]
    fn structure_chunks_uses_horizontal_rule_breakpoint() {
        let content = "opening paragraph\n\n---\n\nclosing paragraph\n";
        let split = content.find("---").unwrap();
        let plan = chunking::structure_chunks(content, 24, 0, 12);

        assert_eq!(
            plan[0],
            chunking::ChunkSpan {
                start: 0,
                end: split
            }
        );
    }

    #[test]
    fn structure_chunks_ignores_headings_inside_fenced_code() {
        let content = "intro\n```\n# not a heading\n```\n\n# Real Heading\nbody\n";
        let fenced_heading = content.find("# not a heading").unwrap();
        let plan = chunking::structure_chunks(content, 25, 0, 20);

        assert_ne!(plan[0].end, fenced_heading);
        assert!(plan[0].end <= content.find("# Real Heading").unwrap());
    }

    #[test]
    fn structure_chunks_falls_back_to_utf8_safe_fixed_split() {
        let chunks = chunking::structure_chunks("éé", 3, 0, 1);
        assert_eq!(
            chunks,
            vec![
                chunking::ChunkSpan { start: 0, end: 2 },
                chunking::ChunkSpan { start: 2, end: 4 },
            ]
        );
    }

    #[test]
    fn routed_planner_defaults_to_structure() {
        let plan = chunking::plan_chunks(std::path::Path::new("notes.md"), "# A\n\n# B\n", 6, 0);

        assert_eq!(plan.strategy, "structure");
    }

    #[test]
    fn malformed_payload_failure_does_not_insert_documents() {
        let db = TempDb::new();
        let entry = enqueue_and_claim_job(&db, r#"{"root_path":1}"#, 10, 100);
        let pool = db.pool();

        let err = dispatch_job(
            &pool,
            &entry,
            current_time(),
            Arc::new(DeterministicEmbeddingClient::new()),
        )
        .unwrap_err();
        let err_text = format!("{err:#}");
        assert!(
            err_text.contains("failed to decode payload"),
            "unexpected error: {err:#}"
        );

        let conn = pool.get().unwrap();
        let document_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM documents", [], |row| row.get(0))
            .unwrap();
        assert_eq!(document_count, 0);
    }

    #[test]
    fn serialize_embedding_writes_little_endian_f32_bytes() {
        let bytes = embed_document::serialize_embedding(&[1.0, -2.5]);

        assert_eq!(bytes, [0, 0, 128, 63, 0, 0, 32, 192]);
    }
}

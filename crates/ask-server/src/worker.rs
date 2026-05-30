use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use ask_core::models::{Document, IngestFolderPayload, JobQueueEntry};
use ask_core::repository;
use ask_core::types::{ChunkType, DocCategory, JobType};
use tracing::{error, info, warn};

use crate::DbPool;

const POLL_INTERVAL: Duration = Duration::from_secs(5);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

/// Spawns a background task that polls for unclaimed jobs and processes them.
pub fn spawn(pool: DbPool, model_id: i64) {
    let shutdown = ShutdownToken::default();

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(POLL_INTERVAL).await;

            if shutdown.is_shutdown() {
                info!("worker shutdown requested");
                break;
            }

            if let Err(err) = tick(pool.clone(), model_id, shutdown.clone()).await {
                error!(error = %format!("{err:#}"), "worker tick failed");
            }
        }
    });
}

/// Claim a job and process it, all on a blocking thread so the async runtime
/// is never held up by DB or filesystem calls.
async fn tick(pool: DbPool, model_id: i64, shutdown: ShutdownToken) -> Result<()> {
    tokio::task::spawn_blocking(move || -> Result<()> {
        let now = unix_now();
        let mut conn = pool.get()?;
        let entry = repository::claim_job(&mut conn, now)?;

        let entry = match entry {
            Some(entry) => entry,
            None => return Ok(()),
        };

        info!(job_id = entry.id, job_type = %entry.job_type, "claimed job");
        dispatch_job_with_resolver(
            &pool,
            &entry,
            model_id,
            &shutdown,
            HEARTBEAT_INTERVAL,
            resolve_handler,
        )
    })
    .await
    .context("worker tick panicked")??;

    Ok(())
}

#[derive(Clone, Debug, Default)]
struct ShutdownToken {
    state: Arc<AtomicBool>,
}

impl ShutdownToken {
    fn is_shutdown(&self) -> bool {
        self.state.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    fn request_shutdown(&self) {
        self.state.store(true, Ordering::Relaxed);
    }
}

struct JobContext<'a> {
    pool: &'a DbPool,
    entry: &'a JobQueueEntry,
    model_id: i64,
    shutdown: &'a ShutdownToken,
}

trait JobHandler: Send + Sync {
    fn job_type(&self) -> JobType;

    /// Process the claimed job.
    ///
    /// Called from within `spawn_blocking`, so blocking I/O is fine.
    fn process(&self, ctx: JobContext<'_>) -> Result<()>;
}

struct HeartbeatGuard {
    stop_tx: mpsc::Sender<()>,
    join_handle: thread::JoinHandle<Result<()>>,
}

impl HeartbeatGuard {
    fn start(pool: DbPool, job_id: i64, interval: Duration, shutdown: ShutdownToken) -> Self {
        let (stop_tx, stop_rx) = mpsc::channel();
        let join_handle = thread::spawn(move || -> Result<()> {
            loop {
                match stop_rx.recv_timeout(interval) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        if shutdown.is_shutdown() {
                            return Ok(());
                        }

                        let conn = pool.get().with_context(|| {
                            format!("failed to acquire connection for heartbeat on job {job_id}")
                        })?;
                        repository::update_heartbeat(&conn, job_id, unix_now()).with_context(
                            || format!("failed to update heartbeat for job {job_id}"),
                        )?;
                    }
                }
            }
        });

        Self {
            stop_tx,
            join_handle,
        }
    }

    fn stop(self) -> Result<()> {
        let _ = self.stop_tx.send(());

        self.join_handle
            .join()
            .map_err(|panic| anyhow!("heartbeat thread panicked: {panic:?}"))?
    }
}

/// Dispatch a claimed job to the appropriate handler.
///
/// The dispatcher owns the lifecycle for every job: heartbeat, handler
/// execution, and queue completion. Handler failures remain visible to the
/// caller even though current completion policy still removes the job.
pub fn dispatch_job(pool: &DbPool, entry: &JobQueueEntry, model_id: i64) -> Result<()> {
    let shutdown = ShutdownToken::default();

    dispatch_job_with_resolver(
        pool,
        entry,
        model_id,
        &shutdown,
        HEARTBEAT_INTERVAL,
        resolve_handler,
    )
}

fn dispatch_job_with_resolver<R>(
    pool: &DbPool,
    entry: &JobQueueEntry,
    model_id: i64,
    shutdown: &ShutdownToken,
    heartbeat_interval: Duration,
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

    let heartbeat_guard =
        HeartbeatGuard::start(pool.clone(), entry.id, heartbeat_interval, shutdown.clone());
    let handler_result = handler.process(JobContext {
        pool,
        entry,
        model_id,
        shutdown,
    });
    let heartbeat_result = heartbeat_guard.stop();
    let completion_result = complete_job(pool, entry.id);

    let dispatch_result =
        combine_dispatch_results(entry, handler_result, heartbeat_result, completion_result);

    match &dispatch_result {
        Ok(()) => {
            info!(job_id = entry.id, job_type = %entry.job_type, "job completed");
        }
        Err(err) => {
            error!(
                job_id = entry.id,
                job_type = %entry.job_type,
                error = %format!("{err:#}"),
                "job failed"
            );
        }
    }

    dispatch_result
}

fn resolve_handler(job_type: JobType) -> Box<dyn JobHandler> {
    match job_type {
        JobType::IngestFolder => Box::new(IngestFolderHandler),
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

fn combine_dispatch_results(
    entry: &JobQueueEntry,
    handler_result: Result<()>,
    heartbeat_result: Result<()>,
    completion_result: Result<()>,
) -> Result<()> {
    let prefix = format!("job {} ({})", entry.id, entry.job_type);

    match (handler_result, heartbeat_result, completion_result) {
        (Ok(()), Ok(()), Ok(())) => Ok(()),
        (Err(err), Ok(()), Ok(())) | (Ok(()), Err(err), Ok(())) | (Ok(()), Ok(()), Err(err)) => {
            Err(err.context(prefix))
        }
        (handler, heartbeat, completion) => {
            let mut failures = Vec::with_capacity(3);

            if let Err(err) = handler {
                failures.push(format!("handler failed: {err:#}"));
            }
            if let Err(err) = heartbeat {
                failures.push(format!("heartbeat failed: {err:#}"));
            }
            if let Err(err) = completion {
                failures.push(format!("completion failed: {err:#}"));
            }

            Err(anyhow!("{prefix} failed: {}", failures.join("; ")))
        }
    }
}

// ---------------------------------------------------------------------------
// IngestFolder
// ---------------------------------------------------------------------------

struct IngestFolderHandler;

impl JobHandler for IngestFolderHandler {
    fn job_type(&self) -> JobType {
        JobType::IngestFolder
    }

    fn process(&self, ctx: JobContext<'_>) -> Result<()> {
        let payload: IngestFolderPayload = serde_json::from_str(&ctx.entry.payload)
            .with_context(|| format!("failed to decode payload for job {}", ctx.entry.id))?;
        let root_path = Path::new(&payload.root_path);

        if !root_path.is_dir() {
            warn!(
                job_id = ctx.entry.id,
                path = %payload.root_path,
                "ingest_folder path is missing or not a directory; completing job"
            );
            return Ok(());
        }

        info!(
            job_id = ctx.entry.id,
            path = %payload.root_path,
            "processing ingest_folder job"
        );

        let model = {
            let conn = ctx.pool.get().with_context(|| {
                format!(
                    "failed to acquire connection to load model {} for job {}",
                    ctx.model_id, ctx.entry.id
                )
            })?;

            repository::find_model_by_id(&conn, ctx.model_id)?.with_context(|| {
                format!(
                    "embedding model {} not found for job {}",
                    ctx.model_id, ctx.entry.id
                )
            })?
        };

        let entries = std::fs::read_dir(root_path)
            .with_context(|| format!("failed to read ingest root {}", payload.root_path))?;

        for entry_result in entries {
            if ctx.shutdown.is_shutdown() {
                return Err(anyhow!(
                    "job {} ({}) cancelled by shutdown",
                    ctx.entry.id,
                    ctx.entry.job_type
                ));
            }

            let dir_entry = match entry_result {
                Ok(dir_entry) => dir_entry,
                Err(err) => {
                    warn!(
                        job_id = ctx.entry.id,
                        error = %err,
                        "failed to read directory entry; continuing"
                    );
                    continue;
                }
            };

            let path = dir_entry.path();
            if !path.is_file() {
                continue;
            }

            let metadata = match std::fs::metadata(&path) {
                Ok(metadata) => metadata,
                Err(err) => {
                    warn!(
                        job_id = ctx.entry.id,
                        path = ?path,
                        error = %err,
                        "failed to read file metadata; continuing"
                    );
                    continue;
                }
            };

            let now = unix_now();
            let filepath = path.to_string_lossy().into_owned();
            let file_type = path
                .extension()
                .and_then(|extension| extension.to_str())
                .unwrap_or("")
                .to_string();
            let file_modified_at = metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|duration| duration.as_secs() as i64)
                .unwrap_or(now);
            let file_size = metadata.len() as i64;

            let conn = ctx.pool.get().with_context(|| {
                format!("failed to acquire connection while ingesting {filepath}")
            })?;

            if let Some(existing) = repository::find_document_by_path(&conn, &filepath)?
                && existing.file_modified_at == file_modified_at
                && existing.file_size == file_size
            {
                continue;
            }

            let doc = Document {
                id: 0,
                filepath: filepath.clone(),
                file_type,
                doc_category: DocCategory::Resource,
                file_modified_at,
                file_size,
                updated_at: now,
            };

            let doc_id = repository::insert_document(&conn, &doc)
                .with_context(|| format!("failed to insert document for {filepath}"))?;

            repository::insert_pending_embeddings(
                &conn,
                doc_id,
                ctx.model_id,
                &[(ChunkType::Filename, 0, 0)],
                now,
            )
            .with_context(|| format!("failed to queue filename embedding for {filepath}"))?;

            match std::fs::read_to_string(&path) {
                Ok(content) if !content.is_empty() => {
                    let chunk_refs: Vec<(ChunkType, i64, i64)> = chunk_content(
                        &content,
                        model.chunk_size as usize,
                        model.chunk_overlap as usize,
                    )
                    .into_iter()
                    .map(|(start, end)| (ChunkType::Content, start as i64, end as i64))
                    .collect();

                    if !chunk_refs.is_empty() {
                        repository::insert_pending_embeddings(
                            &conn,
                            doc_id,
                            ctx.model_id,
                            &chunk_refs,
                            now,
                        )
                        .with_context(|| {
                            format!("failed to queue content embeddings for {filepath}")
                        })?;
                    }
                }
                Ok(_) => {}
                Err(err) => {
                    warn!(
                        job_id = ctx.entry.id,
                        path = %filepath,
                        error = %err,
                        "skipping content chunking for unreadable file"
                    );
                }
            }
        }

        Ok(())
    }
}

/// Split `content` into overlapping chunks by byte offset.
/// Each chunk covers at most `chunk_size` bytes; consecutive chunks overlap
/// by `overlap` bytes.
fn chunk_content(content: &str, chunk_size: usize, overlap: usize) -> Vec<(usize, usize)> {
    if content.is_empty() || chunk_size == 0 {
        return Vec::new();
    }

    let len = content.len();
    let step = chunk_size.saturating_sub(overlap);
    if step == 0 {
        return vec![(0, len)];
    }

    let mut chunks = Vec::new();
    let mut start = 0;

    while start < len {
        let end = std::cmp::min(start + chunk_size, len);
        chunks.push((start, end));
        if end >= len {
            break;
        }
        start += step;
    }

    chunks
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
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    use std::sync::{Condvar, Mutex, mpsc as std_mpsc};
    use std::time::{Instant, SystemTime, UNIX_EPOCH};

    use ask_core::repository;

    use super::*;
    use crate::{create_pool, migrations};

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

    #[derive(Clone, Default)]
    struct BlockingGate {
        state: Arc<(Mutex<bool>, Condvar)>,
    }

    impl BlockingGate {
        fn wait(&self) {
            let (lock, condvar) = &*self.state;
            let mut released = lock.lock().unwrap();

            while !*released {
                released = condvar.wait(released).unwrap();
            }
        }

        fn open(&self) {
            let (lock, condvar) = &*self.state;
            let mut released = lock.lock().unwrap();
            *released = true;
            condvar.notify_all();
        }
    }

    struct BlockingHandler {
        started_tx: std_mpsc::Sender<()>,
        gate: BlockingGate,
    }

    impl JobHandler for BlockingHandler {
        fn job_type(&self) -> JobType {
            JobType::IngestFolder
        }

        fn process(&self, _ctx: JobContext<'_>) -> Result<()> {
            self.started_tx.send(()).unwrap();
            self.gate.wait();
            Ok(())
        }
    }

    struct FailingHandler;

    impl JobHandler for FailingHandler {
        fn job_type(&self) -> JobType {
            JobType::IngestFolder
        }

        fn process(&self, _ctx: JobContext<'_>) -> Result<()> {
            Err(anyhow!("synthetic handler failure"))
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
        let conn = db.pool().get().unwrap();
        repository::enqueue_job(&conn, &JobType::IngestFolder, payload, queued_at).unwrap();

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
    fn dispatcher_heartbeat_updates_while_handler_is_blocked() {
        let db = TempDb::new();
        let entry = enqueue_and_claim_job(&db, r#"{"root_path":"/tmp"}"#, 10, 100);
        let initial_heartbeat = entry.heartbeat.expect("claimed jobs must have a heartbeat");
        let pool = db.pool();
        let shutdown = ShutdownToken::default();
        let gate = BlockingGate::default();
        let gate_for_handler = gate.clone();
        let (started_tx, started_rx) = std_mpsc::channel();
        let entry_for_thread = entry.clone();
        let pool_for_thread = pool.clone();

        let handle = thread::spawn(move || {
            dispatch_job_with_resolver(
                &pool_for_thread,
                &entry_for_thread,
                1,
                &shutdown,
                Duration::from_millis(25),
                move |_| {
                    Box::new(BlockingHandler {
                        started_tx: started_tx.clone(),
                        gate: gate_for_handler.clone(),
                    })
                },
            )
        });

        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("handler should start");

        let deadline = Instant::now() + Duration::from_secs(2);
        let mut observed_heartbeat = None;

        while Instant::now() < deadline {
            let conn = pool.get().unwrap();
            let heartbeat: Option<i64> = conn
                .query_row(
                    "SELECT heartbeat FROM job_queue WHERE id = ?1",
                    [entry.id],
                    |row| row.get(0),
                )
                .unwrap();

            if let Some(heartbeat) = heartbeat
                && heartbeat > initial_heartbeat
            {
                observed_heartbeat = Some(heartbeat);
                break;
            }

            thread::sleep(Duration::from_millis(10));
        }

        assert!(
            observed_heartbeat.is_some(),
            "expected heartbeat to advance while handler was still running"
        );

        gate.open();
        assert!(handle.join().unwrap().is_ok());
        assert_eq!(job_queue_count(&pool), 0);
    }

    #[test]
    fn dispatcher_returns_handler_failures_and_still_completes_job() {
        let db = TempDb::new();
        let entry = enqueue_and_claim_job(&db, r#"{"root_path":"/tmp"}"#, 10, 100);
        let pool = db.pool();
        let shutdown = ShutdownToken::default();

        let err =
            dispatch_job_with_resolver(&pool, &entry, 1, &shutdown, Duration::from_secs(1), |_| {
                Box::new(FailingHandler)
            })
            .unwrap_err();
        let err_text = format!("{err:#}");

        assert!(
            err_text.contains("synthetic handler failure"),
            "unexpected error: {err:#}"
        );
        assert_eq!(job_queue_count(&pool), 0);
    }

    #[test]
    fn dispatcher_surfaces_payload_decode_failures_and_completes_job() {
        let db = TempDb::new();
        let entry = enqueue_and_claim_job(&db, "{not json", 10, 100);
        let pool = db.pool();

        let err = dispatch_job(&pool, &entry, 1).unwrap_err();
        let err_text = format!("{err:#}");

        assert!(
            err_text.contains("failed to decode payload"),
            "unexpected error: {err:#}"
        );
        assert_eq!(job_queue_count(&pool), 0);
    }

    #[test]
    fn shutdown_token_reports_requested_shutdown() {
        let token = ShutdownToken::default();
        assert!(!token.is_shutdown());

        token.request_shutdown();

        assert!(token.is_shutdown());
    }

    #[test]
    fn chunk_content_empty() {
        assert!(chunk_content("", 10, 0).is_empty());
    }

    #[test]
    fn chunk_content_zero_chunk_size() {
        assert!(chunk_content("hello", 0, 0).is_empty());
    }

    #[test]
    fn chunk_content_smaller_than_chunk() {
        let chunks = chunk_content("hello", 100, 0);
        assert_eq!(chunks, vec![(0, 5)]);
    }

    #[test]
    fn chunk_content_exact_size() {
        let chunks = chunk_content("12345", 5, 0);
        assert_eq!(chunks, vec![(0, 5)]);
    }

    #[test]
    fn chunk_content_multiple_chunks_no_overlap() {
        let chunks = chunk_content("abcdefghij", 5, 0);
        assert_eq!(chunks, vec![(0, 5), (5, 10)]);
    }

    #[test]
    fn chunk_content_with_overlap() {
        let chunks = chunk_content("abcdefghij", 6, 2);
        assert_eq!(chunks, vec![(0, 6), (4, 10)]);
    }

    #[test]
    fn chunk_content_full_overlap_returns_single_chunk() {
        let chunks = chunk_content("hello world", 5, 5);
        assert_eq!(chunks, vec![(0, 11)]);
    }

    #[test]
    fn chunk_content_overlap_greater_than_size() {
        let chunks = chunk_content("hello world", 5, 10);
        assert_eq!(chunks, vec![(0, 11)]);
    }

    #[test]
    fn chunk_content_chunk_size_one() {
        let chunks = chunk_content("abc", 1, 0);
        assert_eq!(chunks, vec![(0, 1), (1, 2), (2, 3)]);
    }

    #[test]
    fn chunk_content_multibyte_utf8() {
        let chunks = chunk_content("añb", 3, 0);
        assert_eq!(chunks, vec![(0, 3), (3, 4)]);
    }

    #[test]
    fn malformed_payload_failure_does_not_insert_documents() {
        let db = TempDb::new();
        let entry = enqueue_and_claim_job(&db, r#"{"root_path":1}"#, 10, 100);
        let pool = db.pool();

        let err = dispatch_job(&pool, &entry, current_time()).unwrap_err();
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
}

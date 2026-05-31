use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use ask_core::models::{Document, EmbeddingModel, IngestFolderPayload, JobQueueEntry};
use ask_core::repository;
use ask_core::types::{ChunkType, DocCategory, JobType};
use ignore::WalkBuilder;
use tracing::{error, info, warn};

use crate::DbPool;
use crate::ingest;

const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Spawns a background task that polls for unclaimed or stale jobs.
pub fn spawn(pool: DbPool, model_id: i64) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(POLL_INTERVAL).await;

            if let Err(err) = tick(pool.clone(), model_id).await {
                error!(error = %format!("{err:#}"), "worker tick failed");
            }
        }
    });
}

/// Claim a job and process it, all on a blocking thread so the async runtime
/// is never held up by DB or filesystem calls.
async fn tick(pool: DbPool, model_id: i64) -> Result<()> {
    tokio::task::spawn_blocking(move || -> Result<()> {
        let now = unix_now();
        let mut conn = pool.get()?;
        let entry = repository::claim_job(&mut conn, now)?;

        let entry = match entry {
            Some(entry) => entry,
            None => return Ok(()),
        };

        info!(job_id = entry.id, job_type = %entry.job_type, "claimed job");
        dispatch_job_with_resolver(&pool, &entry, model_id, resolve_handler)
    })
    .await
    .context("worker tick panicked")??;

    Ok(())
}

struct JobContext<'a> {
    pool: &'a DbPool,
    entry: &'a JobQueueEntry,
    model_id: i64,
}

trait JobHandler: Send + Sync {
    fn job_type(&self) -> JobType;

    /// Process the claimed job.
    ///
    /// Called from within `spawn_blocking`, so blocking I/O is fine.
    fn process(&self, ctx: JobContext<'_>) -> Result<()>;
}

/// Dispatch a claimed job to the appropriate handler.
///
/// Successful handlers remove the queue row. Failed handlers leave the claim in
/// place so the row becomes claimable again only after the stale timeout.
pub fn dispatch_job(pool: &DbPool, entry: &JobQueueEntry, model_id: i64) -> Result<()> {
    dispatch_job_with_resolver(pool, entry, model_id, resolve_handler)
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
        queue_pending_embeddings_for_document(conn, Path::new(&doc.filepath), doc.id, model, now)
            .with_context(|| {
            format!(
                "failed to backfill pending embeddings for document {} at {}",
                doc.id, doc.filepath
            )
        })?;
        count += 1;
    }

    Ok(count)
}

fn dispatch_job_with_resolver<R>(
    pool: &DbPool,
    entry: &JobQueueEntry,
    model_id: i64,
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
        model_id,
    });

    match result {
        Ok(()) => {
            complete_job(pool, entry.id)?;
            info!(job_id = entry.id, job_type = %entry.job_type, "job completed");
            Ok(())
        }
        Err(err) => {
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
        let file_pattern = ingest::compile_file_pattern(&payload.file_pattern)
            .with_context(|| format!("failed to compile file pattern for job {}", ctx.entry.id))?;

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
            file_pattern = %payload.file_pattern,
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

        let walker = WalkBuilder::new(root_path).follow_links(false).build();

        for entry_result in walker {
            let dir_entry = match entry_result {
                Ok(dir_entry) => dir_entry,
                Err(err) => {
                    warn!(
                        job_id = ctx.entry.id,
                        error = %err,
                        "failed to walk directory entry; continuing"
                    );
                    continue;
                }
            };

            let file_type = match dir_entry.file_type() {
                Some(file_type) => file_type,
                None => {
                    warn!(
                        job_id = ctx.entry.id,
                        path = ?dir_entry.path(),
                        "failed to read directory entry type; continuing"
                    );
                    continue;
                }
            };

            if !file_type.is_file() {
                continue;
            }

            let path = dir_entry.into_path();

            let relative_path = match ingest::normalize_relative_path(root_path, &path) {
                Some(relative_path) => relative_path,
                None => {
                    warn!(
                        job_id = ctx.entry.id,
                        path = ?path,
                        "failed to normalize relative file path; continuing"
                    );
                    continue;
                }
            };

            if !file_pattern.is_match(&relative_path) {
                continue;
            }

            let canonical_path = match std::fs::canonicalize(&path) {
                Ok(path) => path,
                Err(err) => {
                    warn!(
                        job_id = ctx.entry.id,
                        path = ?path,
                        error = %err,
                        "failed to canonicalize file path; continuing"
                    );
                    continue;
                }
            };

            let metadata = match std::fs::metadata(&canonical_path) {
                Ok(metadata) => metadata,
                Err(err) => {
                    warn!(
                        job_id = ctx.entry.id,
                        path = ?canonical_path,
                        error = %err,
                        "failed to read file metadata; continuing"
                    );
                    continue;
                }
            };

            let now = unix_now();
            let filepath = canonical_path.to_string_lossy().into_owned();
            let file_type = canonical_path
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

            let mut conn = ctx.pool.get().with_context(|| {
                format!("failed to acquire connection while ingesting {filepath}")
            })?;

            let doc = Document {
                id: 0,
                filepath: filepath.clone(),
                file_type,
                doc_category: DocCategory::Resource,
                file_modified_at,
                file_size,
                updated_at: now,
            };

            let (doc_id, changed) = repository::upsert_document(&mut conn, &doc)
                .with_context(|| format!("failed to upsert document for {filepath}"))?;

            if !changed {
                continue;
            }

            repository::delete_pending_embeddings_for_model(&conn, doc_id, model.id)
                .with_context(|| format!("failed to clear pending embeddings for {filepath}"))?;

            queue_pending_embeddings_for_document(&conn, &canonical_path, doc_id, &model, now)
                .with_context(|| format!("failed to queue pending embeddings for {filepath}"))?;
        }

        Ok(())
    }
}

fn queue_pending_embeddings_for_document(
    conn: &rusqlite::Connection,
    path: &Path,
    doc_id: i64,
    model: &EmbeddingModel,
    now: i64,
) -> Result<()> {
    let filepath = path.to_string_lossy();

    repository::insert_pending_embeddings(
        conn,
        doc_id,
        model.id,
        &[(ChunkType::Filename, 0, 0)],
        now,
    )
    .with_context(|| format!("failed to queue filename embedding for {filepath}"))?;

    let content = match std::fs::read_to_string(path) {
        Ok(content) if !content.is_empty() => content,
        Ok(_) => return Ok(()),
        Err(err) => {
            warn!(
                path = %filepath,
                error = %err,
                "skipping content chunking for unreadable file"
            );
            return Ok(());
        }
    };

    let chunk_refs: Vec<(ChunkType, i64, i64)> = chunk_content(
        &content,
        model.chunk_size as usize,
        model.chunk_overlap as usize,
    )
    .into_iter()
    .map(|(start, end)| (ChunkType::Content, start as i64, end as i64))
    .collect();

    if chunk_refs.is_empty() {
        return Ok(());
    }

    repository::insert_pending_embeddings(conn, doc_id, model.id, &chunk_refs, now)
        .with_context(|| format!("failed to queue content embeddings for {filepath}"))?;

    Ok(())
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
    use std::time::{SystemTime, UNIX_EPOCH};

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
    fn dispatcher_returns_handler_failures_and_keeps_job_row() {
        let db = TempDb::new();
        let entry = enqueue_and_claim_job(&db, r#"{"root_path":"/tmp"}"#, 10, 100);
        let pool = db.pool();

        let err =
            dispatch_job_with_resolver(&pool, &entry, 1, |_| Box::new(FailingHandler)).unwrap_err();
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

        dispatch_job_with_resolver(&pool, &entry, 1, |_| Box::new(FailingHandler)).unwrap_err();

        let mut conn = pool.get().unwrap();
        let reclaimed = repository::claim_job(&mut conn, 100 + 86_400 + 1)
            .unwrap()
            .expect("stale failed job should be claimable again");

        assert_eq!(reclaimed.id, entry.id);
        assert_eq!(reclaimed.claimed_at, Some(100 + 86_400 + 1));
    }

    #[test]
    fn dispatcher_surfaces_payload_decode_failures_and_keeps_job_row() {
        let db = TempDb::new();
        let entry = enqueue_and_claim_job(&db, "{not json", 10, 100);
        let pool = db.pool();

        let err = dispatch_job(&pool, &entry, 1).unwrap_err();
        let err_text = format!("{err:#}");

        assert!(
            err_text.contains("failed to decode payload"),
            "unexpected error: {err:#}"
        );
        assert_eq!(job_queue_count(&pool), 1);
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

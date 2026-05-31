use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use ask_core::models::{
    Document, EmbedDocumentPayload, EmbeddedChunk, EmbeddingModel, IngestFolderPayload,
    JobQueueEntry,
};
use ask_core::repository;
use ask_core::types::{ChunkType, DocCategory, JobType};
use ignore::WalkBuilder;
use tracing::{error, info, warn};

use crate::DbPool;
use crate::embeddings::{EmbeddingClient, SharedEmbeddingClient};
use crate::ingest;

const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Spawns a background task that polls for unclaimed or stale jobs.
pub fn spawn(pool: DbPool, model_id: i64, embedding_client: SharedEmbeddingClient) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(POLL_INTERVAL).await;

            if let Err(err) = tick(pool.clone(), model_id, embedding_client.clone()).await {
                error!(error = %format!("{err:#}"), "worker tick failed");
            }
        }
    });
}

/// Claim a job and process it, all on a blocking thread so the async runtime
/// is never held up by DB or filesystem calls.
async fn tick(pool: DbPool, model_id: i64, embedding_client: SharedEmbeddingClient) -> Result<()> {
    tokio::task::spawn_blocking(move || -> Result<()> {
        let now = unix_now();
        let mut conn = pool.get()?;
        let entry = repository::claim_job(&mut conn, now)?;

        let entry = match entry {
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
        queue_pending_embeddings_for_document(conn, Path::new(&doc.filepath), doc.id, model, now)
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

struct EmbedDocumentHandler;

impl JobHandler for EmbedDocumentHandler {
    fn job_type(&self) -> JobType {
        JobType::EmbedDocument
    }

    fn process(&self, ctx: JobContext<'_>) -> Result<()> {
        let payload: EmbedDocumentPayload = serde_json::from_str(&ctx.entry.payload)
            .with_context(|| format!("failed to decode payload for job {}", ctx.entry.id))?;

        info!(
            job_id = ctx.entry.id,
            document_id = payload.document_id,
            model_id = payload.model_id,
            "processing embed_document job"
        );

        let (document, model) = {
            let conn = ctx.pool.get().with_context(|| {
                format!(
                    "failed to acquire connection to load document {} and model {} for job {}",
                    payload.document_id, payload.model_id, ctx.entry.id
                )
            })?;

            let document = repository::find_document_by_id(&conn, payload.document_id)?
                .with_context(|| {
                    format!(
                        "document {} not found for job {}",
                        payload.document_id, ctx.entry.id
                    )
                })?;
            let model =
                repository::find_model_by_id(&conn, payload.model_id)?.with_context(|| {
                    format!(
                        "embedding model {} not found for job {}",
                        payload.model_id, ctx.entry.id
                    )
                })?;

            (document, model)
        };

        let prepared_chunks = prepare_embedded_chunks(Path::new(&document.filepath), &model)
            .with_context(|| {
                format!(
                    "failed to prepare chunks for document {} and model {}",
                    document.id, model.id
                )
            })?;
        let inputs = prepared_chunks
            .iter()
            .map(|chunk| chunk.input.clone())
            .collect::<Vec<_>>();
        let vectors = ctx
            .embedding_client
            .embed(&model, &inputs)
            .with_context(|| {
                format!(
                    "failed to embed document {} with model {}",
                    document.id, model.id
                )
            })?;

        if vectors.len() != prepared_chunks.len() {
            return Err(anyhow!(
                "embedding client returned {} vectors for {} prepared chunks",
                vectors.len(),
                prepared_chunks.len()
            ));
        }

        let rows = prepared_chunks
            .into_iter()
            .zip(vectors)
            .map(|(chunk, vector)| EmbeddedChunk {
                chunk_type: chunk.chunk_type,
                chunk_start: chunk.chunk_start,
                chunk_end: chunk.chunk_end,
                embedding: serialize_embedding(&vector),
            })
            .collect::<Vec<_>>();

        let mut conn = ctx.pool.get().with_context(|| {
            format!(
                "failed to acquire connection to replace embeddings for document {} and model {}",
                document.id, model.id
            )
        })?;
        repository::replace_embeddings_for_document_model(
            &mut conn,
            document.id,
            model.id,
            &rows,
            unix_now(),
        )
        .with_context(|| {
            format!(
                "failed to replace embeddings for document {} and model {}",
                document.id, model.id
            )
        })?;

        Ok(())
    }
}

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
                    ctx.ingest_model_id, ctx.entry.id
                )
            })?;

            repository::find_model_by_id(&conn, ctx.ingest_model_id)?.with_context(|| {
                format!(
                    "embedding model {} not found for job {}",
                    ctx.ingest_model_id, ctx.entry.id
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

            let chunk_refs = plan_pending_embeddings_for_document(&canonical_path, &model)
                .with_context(|| format!("failed to plan pending embeddings for {filepath}"))?;

            let (_doc_id, changed) = repository::upsert_document_and_replace_pending_embeddings(
                &mut conn,
                &doc,
                model.id,
                &chunk_refs,
                now,
            )
            .with_context(|| format!("failed to ingest document for {filepath}"))?;

            if !changed {
                continue;
            }
        }

        Ok(())
    }
}

struct PreparedChunk {
    chunk_type: ChunkType,
    chunk_start: i64,
    chunk_end: i64,
    input: String,
}

fn prepare_embedded_chunks(path: &Path, model: &EmbeddingModel) -> Result<Vec<PreparedChunk>> {
    let filepath = path.to_string_lossy().into_owned();
    let filename = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| filepath.clone());
    let mut chunks = vec![PreparedChunk {
        chunk_type: ChunkType::Filename,
        chunk_start: 0,
        chunk_end: 0,
        input: filename,
    }];

    let content = match std::fs::read_to_string(path) {
        Ok(content) if !content.is_empty() => content,
        Ok(_) => return Ok(chunks),
        Err(err) => {
            if err.kind() != std::io::ErrorKind::InvalidData {
                return Err(err)
                    .with_context(|| format!("failed to read document content from {filepath}"));
            }

            warn!(
                path = %filepath,
                error = %err,
                "skipping content embedding for non-utf8 file"
            );
            return Ok(chunks);
        }
    };

    for (start, end) in chunk_content(
        &content,
        model.chunk_size as usize,
        model.chunk_overlap as usize,
    ) {
        let input = String::from_utf8_lossy(&content.as_bytes()[start..end]).into_owned();
        chunks.push(PreparedChunk {
            chunk_type: ChunkType::Content,
            chunk_start: start as i64,
            chunk_end: end as i64,
            input,
        });
    }

    Ok(chunks)
}

fn serialize_embedding(vector: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(std::mem::size_of_val(vector));
    for value in vector {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

fn queue_pending_embeddings_for_document(
    conn: &rusqlite::Connection,
    path: &Path,
    doc_id: i64,
    model: &EmbeddingModel,
    now: i64,
) -> Result<()> {
    let filepath = path.to_string_lossy();

    let chunk_refs = plan_pending_embeddings_for_document(path, model)?;

    repository::insert_pending_embeddings(conn, doc_id, model.id, &chunk_refs, now)
        .with_context(|| format!("failed to queue embeddings for {filepath}"))?;

    Ok(())
}

fn plan_pending_embeddings_for_document(
    path: &Path,
    model: &EmbeddingModel,
) -> Result<Vec<(ChunkType, i64, i64)>> {
    let filepath = path.to_string_lossy();
    let mut chunk_refs = vec![(ChunkType::Filename, 0, 0)];

    let content = match std::fs::read_to_string(path) {
        Ok(content) if !content.is_empty() => content,
        Ok(_) => return Ok(chunk_refs),
        Err(err) => {
            warn!(
                path = %filepath,
                error = %err,
                "skipping content chunking for unreadable file"
            );
            return Ok(chunk_refs);
        }
    };

    chunk_refs.extend(
        chunk_content(
            &content,
            model.chunk_size as usize,
            model.chunk_overlap as usize,
        )
        .into_iter()
        .map(|(start, end)| (ChunkType::Content, start as i64, end as i64)),
    );

    Ok(chunk_refs)
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use ask_core::migrations;
    use ask_core::repository;

    use super::*;
    use crate::create_pool;
    use crate::embeddings::DeterministicEmbeddingClient;

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
        let bytes = serialize_embedding(&[1.0, -2.5]);

        assert_eq!(bytes, [0, 0, 128, 63, 0, 0, 32, 192]);
    }
}

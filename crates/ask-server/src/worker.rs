use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use ask_core::models::{Document, IngestFolderPayload};
use ask_core::repository;
use ask_core::types::{ChunkType, DocCategory, JobType};

use crate::DbPool;

const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Spawns a background task that polls for unclaimed jobs and processes them.
pub fn spawn(pool: DbPool, model_id: i64) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(POLL_INTERVAL).await;

            if let Err(e) = tick(pool.clone(), model_id).await {
                eprintln!("worker tick failed: {e:#}");
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
            Some(e) => e,
            None => return Ok(()),
        };

        println!("claimed job {} ({})", entry.id, entry.job_type.as_str());
        dispatch_job(&pool, &entry, model_id)
    })
    .await
    .context("worker tick panicked")??;

    Ok(())
}

/// Implemented by job-type-specific handlers that process claimed jobs.
///
/// The framework handles claiming, initial heartbeat, and completion.
/// Long-running handlers should call `repository::update_heartbeat`
/// periodically via the pool to prevent the job from being considered stale.
trait JobHandler: Send {
    /// Process the job.
    ///
    /// Called from within `spawn_blocking`, so blocking I/O is fine.
    fn handle(&self, pool: &DbPool, job_id: i64, model_id: i64) -> Result<()>;
}

/// Dispatch a claimed job to the appropriate handler.
///
/// Sets the initial heartbeat, runs the handler, and always completes the
/// job (removes it from the queue) regardless of the handler's result.
pub fn dispatch_job(
    pool: &DbPool,
    entry: &ask_core::models::JobQueueEntry,
    model_id: i64,
) -> Result<()> {
    let handler: Box<dyn JobHandler> = match entry.job_type {
        JobType::IngestFolder => {
            let payload: IngestFolderPayload = serde_json::from_str(&entry.payload)?;
            Box::new(IngestFolderHandler { payload })
        }
    };

    let now = unix_now();
    let conn = pool.get()?;
    repository::update_heartbeat(&conn, entry.id, now)?;

    let result = handler.handle(pool, entry.id, model_id);

    let conn = pool.get()?;
    repository::complete_job(&conn, entry.id)?;

    result
}

// ---------------------------------------------------------------------------
// IngestFolder
// ---------------------------------------------------------------------------

struct IngestFolderHandler {
    payload: IngestFolderPayload,
}

impl JobHandler for IngestFolderHandler {
    fn handle(&self, pool: &DbPool, job_id: i64, model_id: i64) -> Result<()> {
        let root_path = Path::new(&self.payload.root_path);

        if !root_path.is_dir() {
            eprintln!(
                "ingest_folder path does not exist (skipped): {}",
                self.payload.root_path
            );
            return Ok(());
        }

        println!("processing ingest_folder: {}", self.payload.root_path);

        let model = {
            let conn = pool.get()?;
            repository::find_model_by_id(&conn, model_id)?
        };

        if let Ok(entries) = std::fs::read_dir(root_path) {
            for result in entries {
                let entry = match result {
                    Ok(e) => e,
                    Err(e) => {
                        eprintln!("  failed to read directory entry: {e}");
                        continue;
                    }
                };
                let now = unix_now();
                let path = entry.path();

                if !path.is_file() {
                    continue;
                }

                let metadata = match std::fs::metadata(&path) {
                    Ok(m) => m,
                    Err(e) => {
                        eprintln!("  failed to read metadata for {:?}: {e}", path);
                        continue;
                    }
                };

                let filepath = path.to_string_lossy().to_string();
                let file_type = path
                    .extension()
                    .and_then(|e| e.to_str())
                    .unwrap_or("")
                    .to_string();
                let file_modified_at = metadata
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(now);
                let file_size = metadata.len() as i64;

                let conn = pool.get()?;

                if let Some(existing) = repository::find_document_by_path(&conn, &filepath)? {
                    if existing.file_modified_at == file_modified_at
                        && existing.file_size == file_size
                    {
                        repository::update_heartbeat(&conn, job_id, now)?;
                        continue;
                    }
                }

                let doc = Document {
                    id: 0,
                    filepath: filepath.clone(),
                    file_type: file_type.clone(),
                    doc_category: DocCategory::Resource,
                    file_modified_at,
                    file_size,
                    updated_at: now,
                };

                let doc_id = repository::insert_document(&conn, &doc)?;

                repository::insert_pending_embeddings(
                    &conn,
                    doc_id,
                    model_id,
                    &[(ChunkType::Filename, 0, 0)],
                    now,
                )?;

                if let Some(ref m) = model {
                    if let Ok(content) = std::fs::read_to_string(&path) {
                        if !content.is_empty() {
                            let chunk_size = m.chunk_size as usize;
                            let overlap = m.chunk_overlap as usize;
                            let chunks = chunk_content(&content, chunk_size, overlap);

                            if !chunks.is_empty() {
                                let chunk_refs: Vec<(ChunkType, i64, i64)> = chunks
                                    .iter()
                                    .map(|(start, end)| {
                                        (ChunkType::Content, *start as i64, *end as i64)
                                    })
                                    .collect();
                                repository::insert_pending_embeddings(
                                    &conn,
                                    doc_id,
                                    model_id,
                                    &chunk_refs,
                                    now,
                                )?;
                            }
                        }
                    }
                }

                repository::update_heartbeat(&conn, job_id, now)?;
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
    use super::chunk_content;

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
}

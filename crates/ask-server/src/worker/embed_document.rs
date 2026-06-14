use std::path::Path;

use anyhow::{Context, Result, anyhow};
use ask_core::models::{Document, DocumentEmbedding, EmbedDocumentPayload, EmbeddedChunk};
use ask_core::repository;
use ask_core::types::{ChunkType, JobType};
use tracing::{info, warn};

use super::{JobContext, JobHandler, unix_now};

pub(super) struct EmbedDocumentHandler;

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

            let Some(document) = repository::find_document_by_id(&conn, payload.document_id)?
            else {
                repository::complete_job(&conn, ctx.entry.id).with_context(|| {
                    format!(
                        "failed to complete orphaned embed job {} for missing document {}",
                        ctx.entry.id, payload.document_id
                    )
                })?;
                return Ok(());
            };
            let model =
                repository::find_model_by_id(&conn, payload.model_id)?.with_context(|| {
                    format!(
                        "embedding model {} not found for job {}",
                        payload.model_id, ctx.entry.id
                    )
                })?;

            (document, model)
        };

        let path = Path::new(&document.filepath);
        let raw_bytes = match std::fs::read(path) {
            Ok(raw_bytes) => raw_bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let mut conn = ctx.pool.get().with_context(|| {
                    format!(
                        "failed to acquire connection to delete missing document {}",
                        document.id
                    )
                })?;
                repository::delete_document(&mut conn, document.id).with_context(|| {
                    format!("failed to delete missing document {}", document.id)
                })?;
                return Ok(());
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to read document bytes from {}", document.filepath)
                });
            }
        };

        let current_hash = super::ingest::hash_bytes(&raw_bytes);
        if current_hash != document.file_hash {
            let mut conn = ctx.pool.get().with_context(|| {
                format!(
                    "failed to acquire connection to replan document {}",
                    document.id
                )
            })?;
            repository::complete_job(&conn, ctx.entry.id)
                .with_context(|| format!("failed to remove stale embed job {}", ctx.entry.id))?;
            super::ingest::replan_document_from_bytes(
                &mut conn,
                &document,
                &model,
                &raw_bytes,
                unix_now(),
            )
            .with_context(|| {
                format!(
                    "failed to replan document {} after hash mismatch",
                    document.id
                )
            })?;
            return Ok(());
        }

        let recoverable_rows = {
            let conn = ctx.pool.get().with_context(|| {
                format!(
                    "failed to acquire connection to load pending chunks for document {} and model {}",
                    document.id, model.id
                )
            })?;
            repository::list_recoverable_embeddings_for_model(&conn, document.id, model.id)?
        };

        let prepared_chunks =
            prepare_embedded_chunks(path, &document, &recoverable_rows, &raw_bytes).with_context(
                || {
                    format!(
                        "failed to prepare chunks for document {} and model {}",
                        document.id, model.id
                    )
                },
            )?;
        let inputs = prepared_chunks
            .iter()
            .map(|chunk| chunk.input.clone())
            .collect::<Vec<_>>();
        let vectors = ctx
            .embedding_client
            .embed(&model, &inputs)
            .map_err(anyhow::Error::new)
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

struct PreparedChunk {
    chunk_type: ChunkType,
    chunk_start: i64,
    chunk_end: i64,
    input: String,
}

fn prepare_embedded_chunks(
    path: &Path,
    document: &Document,
    rows: &[DocumentEmbedding],
    raw_bytes: &[u8],
) -> Result<Vec<PreparedChunk>> {
    let filepath = path.to_string_lossy().into_owned();
    let filename = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| filepath.clone());

    let content = match std::str::from_utf8(raw_bytes) {
        Ok(content) => Some(content),
        Err(err) => {
            warn!(
                path = %filepath,
                error = %err,
                "skipping content embedding for non-utf8 file"
            );
            None
        }
    };

    let mut chunks = Vec::with_capacity(rows.len());
    for row in rows {
        let input = match row.chunk_type {
            ChunkType::Filename => filename.clone(),
            ChunkType::Content => {
                let content = content.with_context(|| {
                    format!(
                        "document {} has content rows but is not valid UTF-8",
                        document.id
                    )
                })?;
                let start =
                    usize::try_from(row.chunk_start).context("chunk_start must fit into usize")?;
                let end =
                    usize::try_from(row.chunk_end).context("chunk_end must fit into usize")?;
                anyhow::ensure!(
                    start <= end
                        && end <= content.len()
                        && content.is_char_boundary(start)
                        && content.is_char_boundary(end),
                    "stored chunk range {}..{} is invalid for document {}",
                    row.chunk_start,
                    row.chunk_end,
                    document.id
                );
                content[start..end].to_string()
            }
        };

        chunks.push(PreparedChunk {
            chunk_type: row.chunk_type,
            chunk_start: row.chunk_start,
            chunk_end: row.chunk_end,
            input,
        });
    }

    Ok(chunks)
}

pub(super) fn serialize_embedding(vector: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(std::mem::size_of_val(vector));
    for value in vector {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

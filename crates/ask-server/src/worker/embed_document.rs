use std::path::Path;

use anyhow::{Context, Result, anyhow};
use ask_core::models::{EmbedDocumentPayload, EmbeddedChunk};
use ask_core::repository;
use ask_core::types::{ChunkType, JobType};
use tracing::info;

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

struct PreparedChunk {
    chunk_type: ChunkType,
    chunk_start: i64,
    chunk_end: i64,
    input: String,
}

fn prepare_embedded_chunks(
    path: &Path,
    model: &ask_core::models::EmbeddingModel,
) -> Result<Vec<PreparedChunk>> {
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

            tracing::warn!(
                path = %filepath,
                error = %err,
                "skipping content embedding for non-utf8 file"
            );
            return Ok(chunks);
        }
    };

    for (start, end) in super::ingest::chunk_content(
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

pub(super) fn serialize_embedding(vector: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(std::mem::size_of_val(vector));
    for value in vector {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

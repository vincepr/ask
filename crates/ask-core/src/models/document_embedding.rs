use crate::types::{ChunkType, EmbedState};

/// A single embedding row — one chunk of one document for one model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentEmbedding {
    pub id: i64,
    pub document_id: i64,
    pub model_id: i64,
    pub chunk_type: ChunkType,
    pub chunk_start: i64,
    pub chunk_end: i64,
    pub state: EmbedState,
    pub embedding: Option<Vec<u8>>,
    pub created_at: i64,
}

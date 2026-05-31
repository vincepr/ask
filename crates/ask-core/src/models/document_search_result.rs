use crate::types::{ChunkType, DocCategory};

/// One vector-search hit joined back to its embedding row and document.
#[derive(Debug, Clone, PartialEq)]
pub struct DocumentSearchResult {
    pub document_id: i64,
    pub filepath: String,
    pub file_type: String,
    pub doc_category: DocCategory,
    pub file_modified_at: i64,
    pub file_size: i64,
    pub document_updated_at: i64,
    pub embedding_id: i64,
    pub model_id: i64,
    pub chunk_type: ChunkType,
    pub chunk_start: i64,
    pub chunk_end: i64,
    pub distance: f64,
}

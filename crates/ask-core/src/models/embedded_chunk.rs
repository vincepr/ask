use crate::types::ChunkType;

/// One fully materialized embedding row ready to store.
#[derive(Debug, Clone, PartialEq)]
pub struct EmbeddedChunk {
    /// Whether this row embeds the filename or a content chunk.
    pub chunk_type: ChunkType,
    /// Inclusive start byte offset of the chunk within the source text.
    pub chunk_start: i64,
    /// Exclusive end byte offset of the chunk within the source text.
    pub chunk_end: i64,
    /// Serialized embedding vector bytes.
    pub embedding: Vec<u8>,
}

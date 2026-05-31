/// The immutable identity of one embedding configuration.
///
/// This identity determines whether persisted embeddings can be reused. Changes
/// to transport details such as provider URL or auth token do not affect it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingIdentity {
    /// Exact `model` string sent to the embedding provider.
    pub name: String,
    /// Number of `f32` values returned per embedding vector.
    pub dimensions: i64,
    /// Maximum chunk size used when splitting document content.
    pub chunk_size: i64,
    /// Overlap between adjacent content chunks.
    pub chunk_overlap: i64,
}

/// An embedding model known to the system.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingModel {
    pub id: i64,
    pub name: String,
    pub dimensions: i64,
    pub chunk_size: i64,
    pub chunk_overlap: i64,
    pub created_at: i64,
}

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

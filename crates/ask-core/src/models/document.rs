use crate::types::DocCategory;

/// A tracked file on the host machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Document {
    pub id: i64,
    pub filepath: String,
    pub file_type: String,
    pub doc_category: DocCategory,
    pub file_modified_at: i64,
    pub file_size: i64,
    pub updated_at: i64,
}

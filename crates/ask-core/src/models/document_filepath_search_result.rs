/// One filepath-fuzzy-search hit joined back to its document row.
#[derive(Debug, Clone, PartialEq)]
pub struct DocumentFilepathSearchResult {
    pub document_id: i64,
    pub filepath: String,
    pub match_score: f64,
}

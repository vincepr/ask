use std::collections::HashMap;

use ask_core::models::DocumentSearchResult;
use ask_core::repository;
use ask_core::types::ChunkType;
use axum::{Json, extract::State, http::StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

use crate::DbPool;
use crate::embeddings::EmbeddingClient;

use super::{AppState, error_response, load_active_model};

const DEFAULT_SEARCH_LIMIT: usize = 10;
const MAX_SEARCH_LIMIT: usize = 100;
const SEARCH_RAW_LIMIT_MULTIPLIER: usize = 4;
const SEARCH_RAW_LIMIT_CAP: usize = 400;

#[derive(Debug, Deserialize, ToSchema)]
pub(super) struct SearchRequest {
    query: String,
    limit: Option<usize>,
    include_location: Option<bool>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct SearchDocumentResult {
    filepath: String,
    match_score: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    byte_start: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    byte_end: Option<i64>,
}

#[utoipa::path(
    post,
    path = "/search",
    request_body = SearchRequest,
    responses(
        (status = 200, description = "Search results", body = [SearchDocumentResult]),
        (status = 400, description = "Invalid input"),
        (status = 500, description = "Internal server error"),
        (status = 502, description = "Embedding provider failure")
    )
)]
pub(crate) async fn search(
    State(state): State<AppState>,
    Json(body): Json<SearchRequest>,
) -> Result<Json<Vec<SearchDocumentResult>>, (StatusCode, Json<Value>)> {
    let query = body.query.trim().to_string();
    if query.is_empty() {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "query must not be empty".to_string(),
        ));
    }

    let limit = body.limit.unwrap_or(DEFAULT_SEARCH_LIMIT);
    if limit == 0 {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "limit must be greater than 0".to_string(),
        ));
    }
    if limit > MAX_SEARCH_LIMIT {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "bad_request",
            format!("limit must be less than or equal to {MAX_SEARCH_LIMIT}"),
        ));
    }

    let include_location = body.include_location.unwrap_or(false);
    let pool = state.pool().clone();
    let embedding_client = state.embedding_client();

    let outcome = tokio::task::spawn_blocking(move || {
        search_documents(
            &pool,
            embedding_client.as_ref(),
            query,
            limit,
            include_location,
        )
    })
    .await
    .map_err(|err| {
        error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            format!("request panicked: {err}"),
        )
    })?;

    match outcome {
        Ok(response) => Ok(Json(response)),
        Err(SearchFailure::BadGateway(message)) => Err(error_response(
            StatusCode::BAD_GATEWAY,
            "bad_gateway",
            message,
        )),
        Err(SearchFailure::Internal(message)) => Err(error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            message,
        )),
    }
}

#[derive(Debug)]
pub(super) enum SearchFailure {
    BadGateway(String),
    Internal(String),
}

pub(super) fn search_documents(
    pool: &DbPool,
    embedding_client: &dyn EmbeddingClient,
    query: String,
    limit: usize,
    include_location: bool,
) -> Result<Vec<SearchDocumentResult>, SearchFailure> {
    let model = {
        let conn = pool
            .get()
            .map_err(|err| SearchFailure::Internal(format!("database error: {err}")))?;
        load_active_model(&conn).map_err(|err| {
            SearchFailure::Internal(format!("failed to load active model: {err:#}"))
        })?
    };

    let vectors = embedding_client
        .embed(&model, std::slice::from_ref(&query))
        .map_err(|err| SearchFailure::BadGateway(format!("failed to embed query: {err:#}")))?;
    let query_embedding = vectors.first().ok_or_else(|| {
        SearchFailure::BadGateway("embedding provider returned no vectors".into())
    })?;

    let conn = pool
        .get()
        .map_err(|err| SearchFailure::Internal(format!("database error: {err}")))?;
    let raw_limit = expanded_raw_limit(limit);
    let hits = repository::search_documents_by_embedding(&conn, &model, query_embedding, raw_limit)
        .map_err(|err| SearchFailure::Internal(format!("failed to run vector search: {err:#}")))?;
    Ok(collapse_to_documents(hits, limit, include_location))
}

fn collapse_to_documents(
    hits: Vec<DocumentSearchResult>,
    limit: usize,
    include_location: bool,
) -> Vec<SearchDocumentResult> {
    let mut document_indices: HashMap<i64, (usize, bool)> = HashMap::with_capacity(limit);
    let mut results: Vec<SearchDocumentResult> = Vec::with_capacity(limit);

    for hit in hits {
        if let Some((index, location_from_filename)) = document_indices.get_mut(&hit.document_id) {
            if include_location && *location_from_filename && hit.chunk_type != ChunkType::Filename
            {
                results[*index].byte_start = Some(hit.chunk_start);
                results[*index].byte_end = Some(hit.chunk_end);
                *location_from_filename = false;
            }
            continue;
        }

        if results.len() == limit {
            continue;
        }

        let byte_start = include_location.then_some(hit.chunk_start);
        let byte_end = include_location.then_some(hit.chunk_end);
        let location_from_filename = include_location && hit.chunk_type == ChunkType::Filename;
        let index = results.len();

        results.push(SearchDocumentResult {
            filepath: hit.filepath,
            match_score: distance_to_score(hit.distance),
            byte_start,
            byte_end,
        });
        document_indices.insert(hit.document_id, (index, location_from_filename));
    }

    results
}

fn distance_to_score(distance: f64) -> f64 {
    ((2.0 - distance) / 2.0).clamp(0.0, 1.0)
}

fn expanded_raw_limit(limit: usize) -> usize {
    limit
        .saturating_mul(SEARCH_RAW_LIMIT_MULTIPLIER)
        .min(SEARCH_RAW_LIMIT_CAP)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hit(
        document_id: i64,
        chunk_type: ChunkType,
        chunk_start: i64,
        chunk_end: i64,
        distance: f64,
    ) -> DocumentSearchResult {
        DocumentSearchResult {
            document_id,
            filepath: format!("doc-{document_id}.txt"),
            file_type: "txt".to_string(),
            doc_category: ask_core::types::DocCategory::Resource,
            file_modified_at: 0,
            file_size: 0,
            document_updated_at: 0,
            embedding_id: document_id,
            model_id: 1,
            chunk_type,
            chunk_start,
            chunk_end,
            distance,
        }
    }

    #[test]
    fn collapse_to_documents_preserves_score_and_backfills_location_from_later_chunk() {
        let results = collapse_to_documents(
            vec![
                hit(1, ChunkType::Filename, 0, 0, 0.0),
                hit(1, ChunkType::Content, 42, 64, 0.5),
            ],
            1,
            true,
        );

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].match_score, 1.0);
        assert_eq!(results[0].byte_start, Some(42));
        assert_eq!(results[0].byte_end, Some(64));
    }

    #[test]
    fn collapse_to_documents_keeps_filename_offsets_when_no_better_location_exists() {
        let results = collapse_to_documents(vec![hit(1, ChunkType::Filename, 0, 0, 0.0)], 1, true);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].match_score, 1.0);
        assert_eq!(results[0].byte_start, Some(0));
        assert_eq!(results[0].byte_end, Some(0));
    }
}

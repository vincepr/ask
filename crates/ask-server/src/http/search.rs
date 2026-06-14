use std::collections::HashSet;

use ask_core::models::DocumentSearchResult;
use ask_core::repository;
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
    let mut seen_documents = HashSet::with_capacity(limit);
    let mut results = Vec::with_capacity(limit);

    for hit in hits {
        if !seen_documents.insert(hit.document_id) {
            continue;
        }

        let (byte_start, byte_end) = if include_location {
            (Some(hit.chunk_start), Some(hit.chunk_end))
        } else {
            (None, None)
        };

        results.push(SearchDocumentResult {
            filepath: hit.filepath,
            match_score: distance_to_score(hit.distance),
            byte_start,
            byte_end,
        });

        if results.len() == limit {
            break;
        }
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

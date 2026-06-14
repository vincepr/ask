use ask_core::repository;
use axum::{Json, extract::State};
use serde::Deserialize;
use serde_json::{Value, json};
use utoipa::ToSchema;

use super::{AppState, error_response};

#[derive(Debug, Deserialize, ToSchema)]
pub(super) struct MarkStalePayload {
    document_ids: Vec<i64>,
}

#[utoipa::path(
    post,
    path = "/documents/stale",
    request_body = MarkStalePayload
)]
pub(crate) async fn mark_stale(
    State(pool): State<AppState>,
    Json(body): Json<MarkStalePayload>,
) -> Result<Json<Value>, (axum::http::StatusCode, Json<Value>)> {
    if body.document_ids.is_empty() {
        return Err(error_response(
            axum::http::StatusCode::BAD_REQUEST,
            "bad_request",
            "document_ids must not be empty".to_string(),
        ));
    }

    let doc_ids = body.document_ids;

    tokio::task::spawn_blocking(move || {
        let mut conn = pool.get().map_err(|e| format!("database error: {e}"))?;
        repository::mark_documents_stale(&mut conn, &doc_ids).map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| {
        error_response(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            format!("request panicked: {e}"),
        )
    })?
    .map_err(|e| {
        error_response(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            e,
        )
    })?;

    Ok(Json(json!({ "status": "ok" })))
}

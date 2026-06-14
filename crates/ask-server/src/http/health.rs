use axum::Json;
use serde_json::{Value, json};

#[utoipa::path(get, path = "/health")]
pub(crate) async fn health() -> Json<Value> {
    Json(json!({ "status": "healthy" }))
}

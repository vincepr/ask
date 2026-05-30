use axum::{Json, Router, routing::get};

/// Returns the HTTP router exposed by the server.
pub fn router() -> Router {
    Router::new().route("/health", get(health))
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "healthy" })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
struct HealthResponse {
    status: &'static str,
}

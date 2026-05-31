use std::time::{SystemTime, UNIX_EPOCH};

use ask_server::create_pool;
use ask_server::http;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use tower::ServiceExt;

fn dummy_state() -> http::AppState {
    let unique_suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("ask-server-test-{unique_suffix}"));
    std::fs::create_dir_all(&dir).expect("test dir");
    let path = dir.join("ask.sqlite3");
    let pool = create_pool(&path.to_string_lossy()).expect("test pool");
    http::AppState::new(pool, &dir).expect("test state")
}

#[tokio::test]
async fn health_endpoint_returns_healthy_status() {
    let response = http::router(dummy_state())
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .expect("router must handle request");

    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body must be readable");

    assert_eq!(body, "{\"status\":\"healthy\"}");
}

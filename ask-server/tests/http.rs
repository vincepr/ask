use ask_server::http;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use tower::ServiceExt;

#[tokio::test]
async fn health_endpoint_returns_healthy_status() {
    let response = http::router()
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

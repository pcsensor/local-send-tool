use crate::peer::PeerRegistry;
use axum::{extract::State, routing::post, Json, Router};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone)]
pub struct ServerState {
    pub registry: PeerRegistry,
    pub download_dir: PathBuf,
}

#[derive(Deserialize)]
pub struct MessagePayload {
    pub sender_name: String,
    pub text: String,
}

#[derive(Serialize)]
pub struct StandardResponse {
    pub status: String,
}

pub fn make_router(registry: PeerRegistry, download_dir: PathBuf) -> Router {
    let state = Arc::new(ServerState {
        registry,
        download_dir,
    });
    Router::new()
        .route("/api/message", post(handle_message))
        .with_state(state)
}

async fn handle_message(
    State(_state): State<Arc<ServerState>>,
    Json(payload): Json<MessagePayload>,
) -> Json<StandardResponse> {
    println!(
        "\n[收到来自 {} 的文字消息]: {}",
        payload.sender_name, payload.text
    );
    Json(StandardResponse {
        status: "success".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::PeerRegistry;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_receive_message() {
        let registry = PeerRegistry::new();
        let router = make_router(registry, PathBuf::from("./downloads"));

        let request = Request::builder()
            .method("POST")
            .uri("/api/message")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"sender_name": "test-sender", "text": "Hello, world!"}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "success");
    }
}

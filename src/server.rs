use crate::peer::PeerRegistry;
use axum::{
    extract::{DefaultBodyLimit, Multipart, State},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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
    let state = ServerState {
        registry,
        download_dir,
    };
    Router::new()
        .route(
            "/api/message",
            post(handle_message).layer(DefaultBodyLimit::max(1024 * 1024)),
        )
        .route(
            "/api/file",
            post(handle_file).layer(DefaultBodyLimit::max(100 * 1024 * 1024)),
        )
        .with_state(state)
}

async fn handle_message(
    State(_state): State<ServerState>,
    payload: Result<Json<MessagePayload>, axum::extract::rejection::JsonRejection>,
) -> impl IntoResponse {
    let Json(payload) = match payload {
        Ok(p) => p,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    println!(
        "\n[收到来自 {} 的文字消息]: {}",
        payload.sender_name, payload.text
    );
    Json(StandardResponse {
        status: "success".to_string(),
    })
    .into_response()
}

async fn handle_file(
    State(state): State<ServerState>,
    mut multipart: Multipart,
) -> impl IntoResponse {
    if let Err(e) = tokio::fs::create_dir_all(&state.download_dir).await {
        eprintln!("Failed to create download directory: {}", e);
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    let mut sender_name = String::new();
    let mut saved_file_path = None;

    while let Ok(Some(mut field)) = multipart.next_field().await {
        let name = match field.name() {
            Some(n) => n.to_string(),
            None => continue,
        };

        if name == "sender_name" {
            if let Ok(text) = field.text().await {
                sender_name = text;
            }
        } else if name == "file" {
            let fname = match field.file_name() {
                Some(fname) => fname,
                None => return StatusCode::BAD_REQUEST.into_response(),
            };

            // 防范路径穿越安全漏洞
            let basename = match std::path::Path::new(fname).file_name().and_then(|f| f.to_str()) {
                Some(b) if !b.is_empty() => b,
                _ => return StatusCode::BAD_REQUEST.into_response(),
            };

            let mut final_path = state.download_dir.join(basename);
            let path = std::path::Path::new(basename);
            let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
            let extension = path.extension().and_then(|e| e.to_str()).unwrap_or("");

            let mut file;
            let mut counter = 0;

            loop {
                let current_path = if counter == 0 {
                    final_path.clone()
                } else {
                    let new_name = if extension.is_empty() {
                        format!("{}_{}", stem, counter)
                    } else {
                        format!("{}_{}.{}", stem, counter, extension)
                    };
                    state.download_dir.join(&new_name)
                };

                match tokio::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&current_path)
                    .await
                {
                    Ok(f) => {
                        file = f;
                        final_path = current_path;
                        break;
                    }
                    Err(e) => {
                        if e.kind() == std::io::ErrorKind::AlreadyExists {
                            counter += 1;
                        } else {
                            eprintln!("Failed to create file: {}", e);
                            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                        }
                    }
                }
            }

            use tokio::io::AsyncWriteExt;
            loop {
                match field.chunk().await {
                    Ok(Some(chunk)) => {
                        if let Err(e) = file.write_all(&chunk).await {
                            eprintln!("Failed to write chunk: {}", e);
                            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        eprintln!("Multipart error while reading chunk: {}", e);
                        return StatusCode::BAD_REQUEST.into_response();
                    }
                }
            }

            if let Err(e) = file.sync_all().await {
                eprintln!("Failed to sync file: {}", e);
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }

            saved_file_path = Some(final_path);
        }
    }

    let final_path = match saved_file_path {
        Some(path) => path,
        None => return StatusCode::BAD_REQUEST.into_response(),
    };

    println!(
        "\n[成功接收文件] 来自: {}, 保存至: {}",
        sender_name,
        final_path.display()
    );

    Json(StandardResponse {
        status: "success".to_string(),
    })
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::PeerRegistry;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tempfile::tempdir;
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_receive_message() {
        let registry = PeerRegistry::new();
        let tmp_dir = tempdir().unwrap();
        let router = make_router(registry, tmp_dir.path().to_path_buf());

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

    #[tokio::test]
    async fn test_receive_message_bad_request() {
        let registry = PeerRegistry::new();
        let tmp_dir = tempdir().unwrap();
        let router = make_router(registry, tmp_dir.path().to_path_buf());

        let request = Request::builder()
            .method("POST")
            .uri("/api/message")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"sender_name": "test-sender"}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_upload_file_success() {
        let registry = PeerRegistry::new();
        let tmp_dir = tempdir().unwrap();
        let router = make_router(registry, tmp_dir.path().to_path_buf());

        let boundary = "X-MULTIPART-BOUNDARY";
        let body = format!(
            "--{boundary}\r\n\
             Content-Disposition: form-data; name=\"sender_name\"\r\n\r\n\
             test-sender\r\n\
             --{boundary}\r\n\
             Content-Disposition: form-data; name=\"file\"; filename=\"test.txt\"\r\n\
             Content-Type: text/plain\r\n\r\n\
             Hello, file!\r\n\
             --{boundary}--\r\n"
        );

        let request = Request::builder()
            .method("POST")
            .uri("/api/file")
            .header("content-type", format!("multipart/form-data; boundary={boundary}"))
            .body(Body::from(body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(json["status"], "success");

        // 检查文件是否成功保存
        let saved_path = tmp_dir.path().join("test.txt");
        assert!(saved_path.exists());
        let content = std::fs::read_to_string(saved_path).unwrap();
        assert_eq!(content, "Hello, file!");
    }

    #[tokio::test]
    async fn test_upload_file_conflict() {
        let registry = PeerRegistry::new();
        let tmp_dir = tempdir().unwrap();
        
        // 事先在下载目录创建 test.txt 和 test_1.txt
        let download_dir = tmp_dir.path();
        std::fs::write(download_dir.join("test.txt"), "old content").unwrap();
        std::fs::write(download_dir.join("test_1.txt"), "old content 1").unwrap();

        let router = make_router(registry, download_dir.to_path_buf());

        let boundary = "X-MULTIPART-BOUNDARY";
        let body = format!(
            "--{boundary}\r\n\
             Content-Disposition: form-data; name=\"sender_name\"\r\n\r\n\
             test-sender\r\n\
             --{boundary}\r\n\
             Content-Disposition: form-data; name=\"file\"; filename=\"test.txt\"\r\n\
             Content-Type: text/plain\r\n\r\n\
             New multipart data\r\n\
             --{boundary}--\r\n"
        );

        let request = Request::builder()
            .method("POST")
            .uri("/api/file")
            .header("content-type", format!("multipart/form-data; boundary={boundary}"))
            .body(Body::from(body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(json["status"], "success");

        // 应该保存为 test_2.txt，因为 test.txt 和 test_1.txt 都存在
        let expected_path = download_dir.join("test_2.txt");
        assert!(expected_path.exists());
        let content = std::fs::read_to_string(expected_path).unwrap();
        assert_eq!(content, "New multipart data");
    }
}



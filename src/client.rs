use reqwest::Client;
use reqwest::multipart::Form;
use serde::Serialize;
use std::path::Path;

#[derive(Serialize)]
struct MessagePayload<'a> {
    sender_name: &'a str,
    text: &'a str,
}

fn format_url(to_addr: &str, path: &str) -> String {
    let base = if to_addr.starts_with("http://") || to_addr.starts_with("https://") {
        to_addr.to_string()
    } else {
        format!("http://{}", to_addr)
    };
    let base = base.trim_end_matches('/');
    format!("{}{}", base, path)
}

pub async fn send_text(to_addr: &str, sender_name: &str, text: &str) -> Result<(), reqwest::Error> {
    let url = format_url(to_addr, "/api/message");
    let client = Client::new();
    let payload = MessagePayload { sender_name, text };
    
    let response = client.post(&url)
        .json(&payload)
        .send()
        .await?;
        
    response.error_for_status()?;
    Ok(())
}

pub async fn send_file(to_addr: &str, sender_name: &str, file_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let url = format_url(to_addr, "/api/file");
    let client = Client::new();
    
    let form = Form::new()
        .text("sender_name", sender_name.to_string())
        .file("file", file_path)
        .await?;
        
    let response = client.post(&url)
        .multipart(form)
        .send()
        .await?;
        
    response.error_for_status()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::PeerRegistry;
    use crate::server::make_router;
    use tokio::net::TcpListener;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_client_send_text_and_file() {
        let registry = PeerRegistry::new();
        let tmp_dir = tempdir().unwrap();
        let router = make_router(registry, tmp_dir.path().to_path_buf());

        // 启动测试服务器
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();
        let server_addr = format!("{}", local_addr);

        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        // 1. 测试发送文本
        send_text(&server_addr, "client-sender", "Hello from client!")
            .await
            .unwrap();

        // 2. 测试发送文件
        let test_file_path = tmp_dir.path().join("upload_test.txt");
        tokio::fs::write(&test_file_path, "Client file content").await.unwrap();

        send_file(&server_addr, "client-sender", &test_file_path)
            .await
            .unwrap();

        // 检查服务器端是否保存了文件
        let saved_path = tmp_dir.path().join("upload_test.txt");
        assert!(saved_path.exists());
        let content = tokio::fs::read_to_string(saved_path).await.unwrap();
        assert_eq!(content, "Client file content");
    }
}

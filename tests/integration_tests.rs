use std::time::Duration;
use tempfile::tempdir;
use tokio::time::sleep;

#[tokio::test]
async fn test_integration_flow() {
    let tmp_dir = tempdir().unwrap();
    let download_dir = tmp_dir.path().to_path_buf();

    let registry = lan_share::peer::PeerRegistry::new();

    // 启动服务端，绑定本地随机端口 127.0.0.1:0
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server_addr = format!("127.0.0.1:{}", port);

    let app = lan_share::server::make_router(registry.clone(), download_dir.clone());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // 客户端发送文字
    lan_share::client::send_text(&server_addr, "Tester", "Hello Integration")
        .await
        .unwrap();

    // 客户端发送临时文件
    let temp_file_path = download_dir.join("source_file.txt");
    tokio::fs::write(&temp_file_path, "Integrate Content")
        .await
        .unwrap();

    lan_share::client::send_file(&server_addr, "Tester", &temp_file_path)
        .await
        .unwrap();

    // 验证文件保存成功（并且内容一致），使用轮询加超时机制
    let saved_file = download_dir.join("source_file_1.txt"); // 因为源文件和目标目录在同一文件夹下，复制时应该发生重名递增

    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(2);
    let mut success = false;
    while start.elapsed() < timeout {
        if saved_file.exists() {
            if let Ok(content) = tokio::fs::read_to_string(&saved_file).await {
                if content == "Integrate Content" {
                    success = true;
                    break;
                }
            }
        }
        sleep(Duration::from_millis(10)).await;
    }

    assert!(
        success,
        "File was not saved successfully or content did not match within timeout"
    );
}

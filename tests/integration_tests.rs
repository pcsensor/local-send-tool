use std::time::Duration;
use tokio::time::sleep;
use tempfile::tempdir;

#[tokio::test]
async fn test_integration_flow() {
    let tmp_dir = tempdir().unwrap();
    let download_dir = tmp_dir.path().to_path_buf();

    let registry = lan_share::peer::PeerRegistry::new();
    
    // 启动服务端
    let app = lan_share::server::make_router(registry.clone(), download_dir.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:28080").await.unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    sleep(Duration::from_millis(200)).await;

    // 客户端发送文字
    lan_share::client::send_text("127.0.0.1:28080", "Tester", "Hello Integration").await.unwrap();

    // 客户端发送临时文件
    let temp_file_path = download_dir.join("source_file.txt");
    tokio::fs::write(&temp_file_path, "Integrate Content").await.unwrap();
    
    lan_share::client::send_file("127.0.0.1:28080", "Tester", &temp_file_path).await.unwrap();

    sleep(Duration::from_millis(500)).await;

    // 验证文件保存成功（并且内容一致）
    let saved_file = download_dir.join("source_file_1.txt"); // 因为源文件和目标目录在同一文件夹下，复制时应该发生重名递增
    assert!(saved_file.exists());
    let content = tokio::fs::read_to_string(saved_file).await.unwrap();
    assert_eq!(content, "Integrate Content");
}

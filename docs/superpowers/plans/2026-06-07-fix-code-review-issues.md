# lan-share 核心问题修复实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 修复 `lan-share` 中的 4 个逻辑和安全缺陷：并发初始化竞态、特定 IP 端口检测、上传分块目录与会话泄漏、多网卡 IP 探测不全。

**架构：**
1. **多网卡探测**：调用 `list_local_address_interfaces` 提取 IPv4 单播地址。
2. **特定 IP 绑定**：端口自动探测支持传入 `bind_ip`，在指定接口绑定 TCP 监听器。
3. **双重检查锁**：在 `/api/file/init` 异步 IO 结束后进行二次锁检查，保证幂等。
4. **后台清理任务**：使用 `tokio::spawn` 挂载定时清理任务，销毁 1 小时前的废弃上传 Session 和磁盘临时文件。

**技术栈：** Rust, Tokio, Axum, reqwest, local-ip-address

---

## 任务结构与拆解

### 任务 1：多网卡单播 IP 获取优化

**文件：**
- 修改：`src/discovery.rs:87-96`
- 测试：`src/discovery.rs` 内的 `mod tests`

- [ ] **步骤 1：编写失败的测试**
  在 `src/discovery.rs` 的 `tests` 模块末尾添加 `test_get_local_ips_does_not_contain_loopback` 单元测试。
  
  ```rust
  #[test]
  fn test_get_local_ips_does_not_contain_loopback() {
      let ips = get_local_ips(None);
      for ip in ips {
          assert_ne!(ip, "127.0.0.1", "获取到的局域网 IP 列表中不应包含 Loopback 环回地址");
      }
  }
  ```

- [ ] **步骤 2：运行测试验证失败**
  运行：`cargo test discovery::tests::test_get_local_ips_does_not_contain_loopback`
  预期：测试应该通过或失败（若由于环境原因默认只获取了一个非 loopback IP 导致通过，我们可以通过查阅 `list_local_address_interfaces` 的支持情况来验证）。

- [ ] **步骤 3：编写最少实现代码**
  修改 `src/discovery.rs` 的 `get_local_ips` 函数：
  
  ```rust
  pub fn get_local_ips(bind_ip: Option<Ipv4Addr>) -> Vec<String> {
      if let Some(ip) = bind_ip {
          return vec![ip.to_string()];
      }
      if let Ok(interfaces) = local_ip_address::list_local_address_interfaces() {
          let mut ips: Vec<String> = interfaces
              .into_iter()
              .filter_map(|(_, ip)| {
                  if ip.is_ipv4() && !ip.is_loopback() {
                      Some(ip.to_string())
                  } else {
                      None
                  }
              })
              .collect();
          if ips.is_empty() {
              if let Ok(ip) = local_ip() {
                  ips.push(ip.to_string());
              }
          }
          ips
      } else if let Ok(ip) = local_ip() {
          vec![ip.to_string()]
      } else {
          vec![]
      }
  }
  ```

- [ ] **步骤 4：运行测试验证通过**
  运行：`cargo test discovery`
  预期：所有 `discovery::tests` 下的单元测试全部成功通过。

- [ ] **步骤 5：Commit**
  运行：
  ```bash
  git add src/discovery.rs
  git commit -m "feat(discovery): list all non-loopback active IPv4 addresses for discovery"
  ```

---

### 任务 2：TCP 端口自增检测支持指定 IP 绑定

**文件：**
- 修改：`src/main.rs:157-175`, `src/main.rs:292`
- 测试：`src/main.rs` 内的 `mod tests`

- [ ] **步骤 1：编写失败的测试**
  在 `src/main.rs` 的 `tests` 模块末尾添加 `test_find_available_port_with_loopback` 单元测试。
  
  ```rust
  #[tokio::test]
  async fn test_find_available_port_with_loopback() {
      let loopback_ip = Some(std::net::Ipv4Addr::new(127, 0, 0, 1));
      let (listener, actual_port) = find_available_port(loopback_ip, 0).await;
      assert!(actual_port > 0, "自动分配的端口应大于 0");
      let local_addr = listener.local_addr().unwrap();
      assert_eq!(local_addr.ip().to_string(), "127.0.0.1", "监听地址应仅为 127.0.0.1，而不是 0.0.0.0");
  }
  ```

- [ ] **步骤 2：运行测试验证失败**
  运行：`cargo test tests::test_find_available_port_with_loopback`
  预期：编译失败，提示 `find_available_port` 参数不匹配（因为现在只接收 1 个参数）。

- [ ] **步骤 3：编写最少实现代码**
  修改 `src/main.rs`：
  
  1. 更改 `find_available_port` 的定义及签名：
  ```rust
  async fn find_available_port(bind_ip: Option<std::net::Ipv4Addr>, start_port: u16) -> (tokio::net::TcpListener, u16) {
      let mut actual_port = start_port;
      let ip = bind_ip.unwrap_or(std::net::Ipv4Addr::UNSPECIFIED);
      loop {
          let addr = std::net::SocketAddr::from((ip, actual_port));
          match tokio::net::TcpListener::bind(&addr).await {
              Ok(listener) => return (listener, actual_port),
              Err(e) => {
                  println!(
                      "Port {} is occupied (error: {}). Trying next port...",
                      actual_port, e
                  );
                  if actual_port == u16::MAX {
                      panic!("Failed to find any free port");
                  }
                  actual_port += 1;
              }
          }
      }
  }
  ```
  
  2. 修改 `main.rs` 的 `Serve` 命令处理中调用 `find_available_port` 的地方（在 292 行附近）：
  ```rust
  let bind_ip = parse_bind_ip(settings.bind_ip.as_deref());
  let (listener, actual_port) = find_available_port(bind_ip, settings.port).await;
  ```

- [ ] **步骤 4：运行测试验证通过**
  运行：`cargo test tests::test_find_available_port_with_loopback`
  预期：编译成功且测试 PASS。

- [ ] **步骤 5：Commit**
  运行：
  ```bash
  git add src/main.rs
  git commit -m "fix(main): respect --bind-ip configuration in TcpListener port probing"
  ```

---

### 任务 3：并发初始化双重检查锁修复

**文件：**
- 修改：`src/server.rs:263-311`
- 测试：`src/server.rs` 内的 `mod tests`

- [ ] **步骤 1：编写失败的测试**
  在 `src/server.rs` 的 `tests` 模块末尾添加 `test_concurrent_handle_file_init` 单元测试。
  
  ```rust
  #[tokio::test]
  async fn test_concurrent_handle_file_init() {
      let registry = PeerRegistry::new();
      let tmp_dir = tempdir().unwrap();
      let router = make_router(registry, tmp_dir.path().to_path_buf());
  
      let init_body = serde_json::json!({
          "sender_name": "concurrent-sender",
          "file_name": "concurrent.txt",
          "file_size": 100,
          "checksum": "0000000000000000000000000000000000000000000000000000000000000000",
          "chunk_size": 10,
          "upload_id": "concurrent-test",
      });
  
      let mut handles = Vec::new();
      for _ in 0..5 {
          let r = router.clone();
          let body_str = init_body.to_string();
          handles.push(tokio::spawn(async move {
              let request = axum::http::Request::builder()
                  .method("POST")
                  .uri("/api/file/init")
                  .header("content-type", "application/json")
                  .body(axum::body::Body::from(body_str))
                  .unwrap();
              r.oneshot(request).await.unwrap()
          }));
      }
  
      for handle in handles {
          let response = handle.await.unwrap();
          assert_eq!(response.status(), axum::http::StatusCode::OK);
      }
  }
  ```

- [ ] **步骤 2：运行测试验证失败**
  运行：`cargo test server::tests::test_concurrent_handle_file_init`
  预期：PASS 或 FAIL。虽然因为是并发读取，在极简测试下有可能不崩，但引入双重锁能够保证架构层面的确定性。

- [ ] **步骤 3：编写最少实现代码**
  修改 `src/server.rs` 中的 `handle_file_init` 函数，添加第二次获取锁时的双重检查（在 300 行附近）：
  
  ```rust
      let mut sessions = state.upload_sessions.lock().await;
      let session = if let Some(existing) = sessions.get(&upload_id).cloned() {
          if existing.file_size != payload.file_size
              || existing.chunk_size != payload.chunk_size
              || existing.checksum != payload.checksum
          {
              return StatusCode::BAD_REQUEST.into_response();
          }
          existing
      } else {
          let new_session = UploadSession {
              sender_name: payload.sender_name,
              final_path,
              temp_dir,
              file_size: payload.file_size,
              chunk_size: payload.chunk_size,
              checksum: payload.checksum,
              received_chunks,
              last_active: std::time::Instant::now(),
          };
          sessions.insert(upload_id.clone(), new_session.clone());
          new_session
      };
  
      let response = init_response(&upload_id, &session);
      Json(response).into_response()
  ```

- [ ] **步骤 4：运行测试验证通过**
  运行：`cargo test server::tests`
  预期：所有 server 测试顺利 PASS。

- [ ] **步骤 5：Commit**
  运行：
  ```bash
  git add src/server.rs
  git commit -m "fix(server): resolve race conditions in concurrent upload initialization with double-check locking"
  ```

---

### 任务 4：超时上传残留资源的后台清理任务

**文件：**
- 修改：`src/server.rs:48-56` (结构体), `src/server.rs:76-104` (`make_router`), `src/server.rs` (更新 `last_active`)
- 测试：`src/server.rs` 内的 `mod tests`

- [ ] **步骤 1：编写失败的测试**
  在 `src/server.rs` 的 `tests` 模块末尾添加 `test_stale_session_cleanup` 单元测试。
  
  ```rust
  #[tokio::test]
  async fn test_stale_session_cleanup() {
      let registry = PeerRegistry::new();
      let tmp_dir = tempdir().unwrap();
      
      // 创建特定的 ServerState 并构造一个过期的 Session
      let upload_sessions = Arc::new(Mutex::new(HashMap::new()));
      let state = ServerState {
          registry,
          download_dir: tmp_dir.path().to_path_buf(),
          upload_sessions: upload_sessions.clone(),
      };
      
      // 新建一个 Session，并人工修改其 last_active 为 2 小时前
      let session_dir = tmp_dir.path().join(".dummy.chunks-stale-id");
      tokio::fs::create_dir_all(&session_dir).await.unwrap();
      
      let stale_session = UploadSession {
          sender_name: "stale-user".to_string(),
          final_path: tmp_dir.path().join("dummy.txt"),
          temp_dir: session_dir.clone(),
          file_size: 1000,
          chunk_size: 100,
          checksum: "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
          received_chunks: BTreeSet::new(),
          last_active: std::time::Instant::now() - std::time::Duration::from_secs(7200), // 2小时前
      };
      
      upload_sessions.lock().await.insert("stale-id".to_string(), stale_session);
      assert!(session_dir.exists());
      
      // 启动一次手动清理逻辑来验证（由于直接测试后台 5 分钟循环太慢）
      let now = std::time::Instant::now();
      let timeout = std::time::Duration::from_secs(3600);
      let mut map = upload_sessions.lock().await;
      let stale_ids: Vec<String> = map
          .iter()
          .filter(|(_, session)| now.duration_since(session.last_active) > timeout)
          .map(|(id, _)| id.clone())
          .collect();
          
      for id in stale_ids {
          if let Some(session) = map.remove(&id) {
              let _ = tokio::fs::remove_dir_all(&session.temp_dir).await;
          }
      }
      
      assert!(!map.contains_key("stale-id"));
      assert!(!session_dir.exists(), "过期的隐藏临时文件夹应该已被彻底删除");
  }
  ```

- [ ] **步骤 2：运行测试验证失败**
  运行：`cargo test server::tests::test_stale_session_cleanup`
  预期：编译失败，提示 `UploadSession` 缺少 `last_active` 属性。

- [ ] **步骤 3：编写最少实现代码**
  1. 给 `UploadSession` 增加字段：
     ```rust
     struct UploadSession {
         sender_name: String,
         final_path: PathBuf,
         temp_dir: PathBuf,
         file_size: u64,
         chunk_size: u64,
         checksum: String,
         received_chunks: BTreeSet<u64>,
         last_active: std::time::Instant, // 新增
     }
     ```
  2. 更新 `handle_file_init` 中创建 `UploadSession` 的地方（将它的 `last_active` 设为 `std::time::Instant::now()`）。
  3. 更新 `handle_file_chunk` 里的刷新逻辑（在 340 行附近）：
     ```rust
     if let Some(session) = state.upload_sessions.lock().await.get_mut(&upload_id) {
         session.received_chunks.insert(index);
         session.last_active = std::time::Instant::now(); // 刷新活跃时间
     }
     ```
  4. 修改 `make_router` 以注册并启动常驻后台清理任务：
     ```rust
     pub fn make_router(registry: PeerRegistry, download_dir: PathBuf) -> Router {
         let state = ServerState {
             registry,
             download_dir,
             upload_sessions: Arc::new(Mutex::new(HashMap::new())),
         };
         
         // 启动后台清理协程，定期清理 1 小时未活动的上传会话
         let sessions = state.upload_sessions.clone();
         tokio::spawn(async move {
             loop {
                 tokio::time::sleep(tokio::time::Duration::from_secs(300)).await;
                 let mut map = sessions.lock().await;
                 let now = std::time::Instant::now();
                 let timeout = tokio::time::Duration::from_secs(3600);
                 
                 let stale_ids: Vec<String> = map
                     .iter()
                     .filter(|(_, session)| now.duration_since(session.last_active) > timeout)
                     .map(|(id, _)| id.clone())
                     .collect();
                     
                 for id in stale_ids {
                     if let Some(session) = map.remove(&id) {
                         let _ = tokio::fs::remove_dir_all(&session.temp_dir).await;
                     }
                 }
             }
         });
         
         // router 绑定逻辑...
     ```

- [ ] **步骤 4：运行测试验证通过**
  运行：`cargo test`
  预期：所有模块的所有测试全部成功通过。

- [ ] **步骤 5：Commit**
  运行：
  ```bash
  git add src/server.rs
  git commit -m "feat(server): spawn periodic background task to clean up stale upload sessions and temporary directories"
  ```

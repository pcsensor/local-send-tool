# lan-share 局域网文件传输助手实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标**：在 Rust 中开发一个跨平台的局域网命令行文件和文本传输工具，支持自动服务发现。

**架构**：单二进制文件，分为服务模块和客户端模块。服务模块运行 Axum HTTP 接收端和后台 UDP 组播接收/广播发现机制；客户端模块使用 Reqwest 向发现的对端节点发送 HTTP 请求。在线节点注册列表存放在共享的 `Arc<RwLock<PeerRegistry>>` 中。

**技术栈**：Rust, Tokio, Axum, Reqwest, Clap, Serde, Serde_json, Uuid, Local-ip-address.

---

## 文件结构与职责

- `Cargo.toml`: 声明项目依赖和元数据。
- `src/main.rs`: 命令行界面解析及子命令分发入口。
- `src/peer.rs`: 核心数据结构（`Peer`、`PeerRegistry`）及其序列化/反序列化逻辑。
- `src/discovery.rs`: UDP 组播网络发现逻辑（Broadcaster 和 Listener）。
- `src/server.rs`: 基于 Axum 的 HTTP 接收服务端实现。
- `src/client.rs`: 基于 Reqwest 的 HTTP 客户端发送端实现。
- `tests/integration_tests.rs`: 覆盖服务发现和文件/文本传输的端到端集成测试。

---

## 任务拆解

### 任务 1：初始化项目及依赖配置

**文件：**
- 创建：`Cargo.toml`
- 创建：`src/main.rs`

- [ ] **步骤 1：编写 Cargo.toml 依赖**
  
  ```toml
  [package]
  name = "lan-share"
  version = "0.1.0"
  edition = "2021"

  [dependencies]
  tokio = { version = "1.35", features = ["full"] }
  axum = { version = "0.7", features = ["multipart"] }
  reqwest = { version = "0.11", features = ["json", "stream", "multipart"] }
  clap = { version = "4.4", features = ["derive"] }
  serde = { version = "1.0", features = ["derive"] }
  serde_json = "1.0"
  uuid = { version = "1.6", features = ["v4", "serde"] }
  local-ip-address = "0.5"
  futures-util = "0.3"
  tower-http = { version = "0.5", features = ["fs", "cors"] }
  ```

- [ ] **步骤 2：创建空的 main.rs 文件**

  ```rust
  fn main() {
      println!("Hello, lan-share!");
  }
  ```

- [ ] **步骤 3：验证编译**

  运行：`cargo check`
  预期：SUCCESS，所有依赖成功下载并编译。

- [ ] **步骤 4：Commit**

  ```bash
  git add Cargo.toml src/main.rs
  git commit -m "chore: initialize project and configure dependencies"
  ```

---

### 任务 2：节点注册表（Peer Registry）数据结构与测试

我们需要定义节点结构、心跳 Payload 以及用于跟踪局域网内所有活跃节点的内存注册表。

**文件：**
- 创建：`src/peer.rs`

- [ ] **步骤 1：编写对 Peer 及其序列化的单元测试**

  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;

      #[test]
      fn test_peer_serialization() {
          let peer = Peer {
              uuid: "f47ac10b-58cc-4372-a567-0e02b2c3d479".to_string(),
              name: "test-node".to_string(),
              port: 8080,
              ips: vec!["192.168.1.100".to_string()],
          };
          let serialized = serde_json::to_string(&peer).unwrap();
          let deserialized: Peer = serde_json::from_str(&serialized).unwrap();
          assert_eq!(peer.uuid, deserialized.uuid);
          assert_eq!(peer.name, deserialized.name);
          assert_eq!(peer.port, deserialized.port);
          assert_eq!(peer.ips, deserialized.ips);
      }
  }
  ```

- [ ] **步骤 2：运行测试验证失败**

  运行：`cargo test --package lan-share --lib peer::tests`
  预期：FAIL，缺少 `Peer` 定义。

- [ ] **步骤 3：编写最小实现代码**

  ```rust
  use serde::{Deserialize, Serialize};
  use std::collections::HashMap;
  use std::sync::{Arc, RwLock};
  use std::time::{Duration, Instant};

  #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
  pub struct Peer {
      pub uuid: String,
      pub name: String,
      pub port: u16,
      pub ips: Vec<String>,
  }

  #[derive(Debug)]
  pub struct PeerInfo {
      pub peer: Peer,
      pub last_seen: Instant,
  }

  #[derive(Clone, Default)]
  pub struct PeerRegistry {
      peers: Arc<RwLock<HashMap<String, PeerInfo>>>,
  }

  impl PeerRegistry {
      pub fn new() -> Self {
          Self {
              peers: Arc::new(RwLock::new(HashMap::new())),
          }
      }

      pub fn register(&self, peer: Peer) {
          let mut map = self.peers.write().unwrap();
          map.insert(
              peer.uuid.clone(),
              PeerInfo {
                  peer,
                  last_seen: Instant::now(),
              },
          );
      }

      pub fn clean_stale(&self, timeout: Duration) {
          let mut map = self.peers.write().unwrap();
          let now = Instant::now();
          map.retain(|_, info| now.duration_since(info.last_seen) < timeout);
      }

      pub fn list(&self) -> Vec<Peer> {
          let map = self.peers.read().unwrap();
          map.values().map(|info| info.peer.clone()).collect()
      }

      pub fn find_by_name_or_ip(&self, target: &str) -> Option<Peer> {
          let map = self.peers.read().unwrap();
          // 尝试匹配 UUID
          if let Some(info) = map.get(target) {
              return Some(info.peer.clone());
          }
          // 尝试匹配名字或 IP
          for info in map.values() {
              if info.peer.name == target {
                  return Some(info.peer.clone());
              }
              for ip in &info.peer.ips {
                  if ip == target || format!("{}:{}", ip, info.peer.port) == target {
                      return Some(info.peer.clone());
                  }
              }
          }
          None
      }
  }
  ```

- [ ] **步骤 4：在 main.rs 中声明 mod peer 并运行测试验证通过**

  修改 `src/main.rs`:
  ```rust
  pub mod peer;
  fn main() {}
  ```
  运行：`cargo test`
  预期：PASS

- [ ] **步骤 5：Commit**

  ```bash
  git add src/peer.rs src/main.rs
  git commit -m "feat: add peer structs and registry with unit tests"
  ```

---

### 任务 3：UDP 组播服务发现实现与测试

**文件：**
- 创建：`src/discovery.rs`
- 修改：`src/main.rs`

- [ ] **步骤 1：编写 UDP 广播与收听的单元/集成测试**

  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;
      use crate::peer::PeerRegistry;
      use std::time::Duration;

      #[tokio::test]
      async fn test_multicast_discovery() {
          let registry = PeerRegistry::new();
          let peer = Peer {
              uuid: "test-uuid-123".to_string(),
              name: "tester".to_string(),
              port: 9999,
              ips: vec!["127.0.0.1".to_string()],
          };

          // 开启接收监听
          let rx_registry = registry.clone();
          let join_handle = tokio::spawn(async move {
              start_listener(rx_registry).await.unwrap();
          });

          // 延迟后发送广播
          tokio::time::sleep(Duration::from_millis(200)).await;
          broadcast_once(&peer).await.unwrap();

          // 等待接收并验证
          tokio::time::sleep(Duration::from_millis(500)).await;
          let list = registry.list();
          assert!(!list.is_empty(), "Peers list should not be empty");
          assert_eq!(list[0].uuid, "test-uuid-123");

          join_handle.abort();
      }
  }
  ```

- [ ] **步骤 2：运行测试验证失败**

  运行：`cargo test --package lan-share --lib discovery::tests`
  预期：FAIL，缺少 `start_listener` 和 `broadcast_once` 等定义。

- [ ] **步骤 3：编写最小实现代码**

  使用 `socket2` 或标准 `tokio::net::UdpSocket` 绑定组播地址。
  ```rust
  use crate::peer::{Peer, PeerRegistry};
  use local_ip_address::local_ip;
  use socket2::{Domain, Protocol, Socket, Type};
  use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket as StdUdpSocket};
  use std::time::Duration;
  use tokio::net::UdpSocket;

  const MULTICAST_ADDR: Ipv4Addr = Ipv4Addr::new(224, 0, 0, 188);
  const MULTICAST_PORT: u16 = 50001;

  pub async fn broadcast_once(peer: &Peer) -> std::io::Result<()> {
      let socket = UdpSocket::bind("0.0.0.0:0").await?;
      socket.set_broadcast(true)?;
      let payload = serde_json::to_vec(peer)?;
      let target_addr: SocketAddr = format!("{}:{}", MULTICAST_ADDR, MULTICAST_PORT).parse().unwrap();
      socket.send_to(&payload, target_addr).await?;
      Ok(())
  }

  pub async fn start_broadcaster(peer: Peer) -> std::io::Result<()> {
      loop {
          if let Err(e) = broadcast_once(&peer).await {
              eprintln!("Broadcaster error: {}", e);
          }
          tokio::time::sleep(Duration::from_secs(3)).await;
      }
  }

  fn create_multicast_socket() -> std::io::Result<StdUdpSocket> {
      let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
      socket.set_reuse_address(true)?;
      #[cfg(not(windows))]
      socket.set_reuse_port(true)?;
      
      let bind_addr: SocketAddr = format!("0.0.0.0:{}", MULTICAST_PORT).parse().unwrap();
      socket.bind(&bind_addr.into())?;
      socket.join_multicast_v4(&MULTICAST_ADDR, &Ipv4Addr::UNSPECIFIED)?;
      Ok(StdUdpSocket::from(socket))
  }

  pub async fn start_listener(registry: PeerRegistry) -> std::io::Result<()> {
      let std_socket = create_multicast_socket()?;
      std_socket.set_nonblocking(true)?;
      let socket = UdpSocket::from_std(std_socket)?;
      let mut buf = vec![0u8; 65535];

      loop {
          let (len, _addr) = socket.recv_from(&mut buf).await?;
          if let Ok(peer) = serde_json::from_slice::<Peer>(&buf[..len]) {
              registry.register(peer);
          }
      }
  }

  pub fn get_local_ips() -> Vec<String> {
      if let Ok(ip) = local_ip() {
          vec![ip.to_string()]
      } else {
          vec![]
      }
  }
  ```

- [ ] **步骤 4：在 main.rs 中声明 mod discovery 并运行测试验证通过**

  修改 `src/main.rs`:
  ```rust
  pub mod peer;
  pub mod discovery;
  fn main() {}
  ```
  运行：`cargo test`
  预期：PASS

- [ ] **步骤 5：Commit**

  ```bash
  git add src/discovery.rs src/main.rs
  git commit -m "feat: implement UDP multicast service discovery and listener"
  ```

---

### 任务 4：HTTP 接收端（Axum Server）与文字消息接口

**文件：**
- 创建：`src/server.rs`
- 修改：`src/main.rs`

- [ ] **步骤 1：编写文字消息接收端的测试**

  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;
      use crate::peer::PeerRegistry;
      use axum::http::StatusCode;
      use serde_json::json;

      #[tokio::test]
      async fn test_receive_message() {
          let registry = PeerRegistry::new();
          let router = make_router(registry, std::path::PathBuf::from("./downloads"));

          let response = axum::test_helper::TestClient::new(router)
              .post("/api/message")
              .json(&json!({
                  "sender_name": "test-sender",
                  "text": "Hello, world!"
              }))
              .send()
              .await;

          assert_eq!(response.status(), StatusCode::OK);
          let body: serde_json::Value = response.json().await;
          assert_eq!(body["status"], "success");
      }
  }
  ```
  *注意：如果 axum 测试辅助工具在 v0.7 中没有 `TestClient`，可使用 `tower::ServiceExt` 自行构造 `Request` 来测试路由，如下：*
  ```rust
  use tower::ServiceExt; // for oneshot
  use axum::body::Body;
  use axum::http::{Request, StatusCode};
  // 见步骤 3 中的测试辅助方式
  ```

- [ ] **步骤 2：运行测试验证失败**

  运行：`cargo test --package lan-share --lib server::tests`
  预期：FAIL，缺少 `make_router` 的定义。

- [ ] **步骤 3：编写最小实现代码**

  ```rust
  use crate::peer::PeerRegistry;
  use axum::{
      extract::State,
      routing::post,
      Json, Router,
  };
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
      State(state): State<Arc<ServerState>>,
      Json(payload): Json<MessagePayload>,
  ) -> Json<StandardResponse> {
      println!("\n[收到来自 {} 的文字消息]: {}", payload.sender_name, payload.text);
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
      use http_body_util::BodyExt; // 针对 axum 0.7 需要的库
      use tower::ServiceExt;

      #[tokio::test]
      async fn test_receive_message() {
          let registry = PeerRegistry::new();
          let router = make_router(registry, PathBuf::from("./downloads"));

          let request = Request::builder()
              .method("POST")
              .uri("/api/message")
              .header("content-type", "application/json")
              .body(Body::from(r#"{"sender_name": "test-sender", "text": "Hello, world!"}"#))
              .unwrap();

          let response = router.oneshot(request).await.unwrap();
          assert_eq!(response.status(), StatusCode::OK);
      }
  }
  ```

- [ ] **步骤 4：在 main.rs 中声明 mod server 并运行测试验证通过**

  修改 `src/main.rs`:
  ```rust
  pub mod peer;
  pub mod discovery;
  pub mod server;
  fn main() {}
  ```
  运行：`cargo test`
  预期：PASS。如有依赖未导入，需在 Cargo.toml 中为 `http-body-util` 增加对应版本（`http-body-util = "0.1"`）。

- [ ] **步骤 5：Commit**

  ```bash
  git add src/server.rs src/main.rs Cargo.toml
  git commit -m "feat: implement axum router and text message receiver"
  ```

---

### 任务 5：HTTP 文件接收接口（Axum Multipart）与客户端发送逻辑

**文件：**
- 修改：`src/server.rs`
- 创建：`src/client.rs`
- 修改：`src/main.rs`

- [ ] **步骤 1：编写文件上传处理逻辑的单元测试**

  ```rust
  // 在 src/server.rs 的 tests 模块中编写测试，构造 multipart 表单并上传文件
  ```

- [ ] **步骤 2：运行测试验证失败**

  运行：`cargo test --package lan-share --lib server::tests`
  预期：FAIL，接口 `/api/file` 未定义。

- [ ] **步骤 3：编写接收端的 Multipart 文件流保存逻辑以及客户端发送逻辑**

  修改 `src/server.rs` 添加 `/api/file`：
  ```rust
  use axum::extract::Multipart;
  use tokio::fs::File;
  use tokio::io::AsyncWriteExt;

  pub fn make_router(registry: PeerRegistry, download_dir: PathBuf) -> Router {
      let state = Arc::new(ServerState {
          registry,
          download_dir,
      });
      Router::new()
          .route("/api/message", post(handle_message))
          .route("/api/file", post(handle_file))
          .with_state(state)
  }

  async fn handle_file(
      State(state): State<Arc<ServerState>>,
      mut multipart: Multipart,
  ) -> Result<Json<StandardResponse>, (axum::http::StatusCode, String)> {
      tokio::fs::create_dir_all(&state.download_dir).await.map_err(|e| {
          (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to create download dir: {}", e))
      })?;

      let mut filename = "uploaded_file".to_string();
      let mut sender_name = "Unknown".to_string();
      let mut file_data = Vec::new();

      while let Some(field) = multipart.next_field().await.map_err(|e| {
          (axum::http::StatusCode::BAD_REQUEST, format!("Multipart error: {}", e))
      })? {
          let name = field.name().unwrap_or("").to_string();
          if name == "sender_name" {
              sender_name = field.text().await.unwrap_or_else(|_| "Unknown".to_string());
          } else if name == "file" {
              filename = field.file_name().unwrap_or("uploaded_file").to_string();
              let bytes = field.bytes().await.map_err(|e| {
                  (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to read bytes: {}", e))
              })?;
              file_data = bytes.to_vec();
          }
      }

      // 处理文件名冲突
      let mut file_path = state.download_dir.join(&filename);
      if file_path.exists() {
          let stem = PathBuf::from(&filename)
              .file_stem()
              .unwrap_or_default()
              .to_string_lossy()
              .into_owned();
          let ext = PathBuf::from(&filename)
              .extension()
              .map(|e| format!(".{}", e.to_string_lossy()))
              .unwrap_or_default();
          let mut counter = 1;
          while file_path.exists() {
              let new_name = format!("{}_{}{}", stem, counter, ext);
              file_path = state.download_dir.join(new_name);
              counter += 1;
          }
      }

      let mut file = File::create(&file_path).await.map_err(|e| {
          (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to create file: {}", e))
      })?;
      file.write_all(&file_data).await.map_err(|e| {
          (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to write file: {}", e))
      })?;

      println!(
          "\n[成功接收文件] 来自: {}, 保存至: {}",
          sender_name,
          file_path.display()
      );

      Ok(Json(StandardResponse {
          status: "success".to_string(),
      }))
  }
  ```

  创建 `src/client.rs` 用来向对方节点发送消息和文件：
  ```rust
  use reqwest::multipart::{Form, Part};
  use std::path::Path;

  pub async fn send_text(to_addr: &str, sender_name: &str, text: &str) -> Result<(), reqwest::Error> {
      let client = reqwest::Client::new();
      let url = format!("http://{}/api/message", to_addr);
      let payload = serde_json::json!({
          "sender_name": sender_name,
          "text": text
      });
      client.post(&url).json(&payload).send().await?.error_for_status()?;
      Ok(())
  }

  pub async fn send_file(to_addr: &str, sender_name: &str, file_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
      let client = reqwest::Client::new();
      let url = format!("http://{}/api/file", to_addr);
      let file_name = file_path
          .file_name()
          .ok_or("Invalid file name")?
          .to_string_lossy()
          .to_string();

      let file_bytes = tokio::fs::read(file_path).await?;
      let part = Part::bytes(file_bytes).file_name(file_name);
      
      let form = Form::new()
          .text("sender_name", sender_name.to_string())
          .part("file", part);

      client.post(&url).multipart(form).send().await?.error_for_status()?;
      Ok(())
  }
  ```

- [ ] **步骤 4：在 main.rs 中声明 mod client 并运行测试验证通过**

  修改 `src/main.rs`:
  ```rust
  pub mod peer;
  pub mod discovery;
  pub mod server;
  pub mod client;
  fn main() {}
  ```
  运行：`cargo test`
  预期：PASS。

- [ ] **步骤 5：Commit**

  ```bash
  git add src/server.rs src/client.rs src/main.rs
  git commit -m "feat: implement file receiving and reqwest client sending logic"
  ```

---

### 任务 6：命令行参数（Clap CLI）拼装与节点列表刷新

**文件：**
- 修改：`src/main.rs`
- 创建：`tests/integration_tests.rs`

- [ ] **步骤 1：编写端到端集成测试**
  
  ```rust
  // 创建 tests/integration_tests.rs
  // 模拟一个局域网通信，启动 serve 后从 client 发送 text 和 file 并验证结果
  ```

- [ ] **步骤 2：运行测试验证失败**

  运行：`cargo test --test integration_tests`
  预期：FAIL，因为缺少命令行功能逻辑导致测试代码中运行的守护进程不完整。

- [ ] **步骤 3：编写完整的命令行逻辑与端口防冲突绑定**

  在 `src/main.rs` 中完整拼装逻辑：
  ```rust
  pub mod peer;
  pub mod discovery;
  pub mod server;
  pub mod client;

  use clap::{Parser, Subcommand};
  use peer::{Peer, PeerRegistry};
  use std::net::TcpListener;
  use std::path::PathBuf;
  use uuid::Uuid;

  #[derive(Parser)]
  #[command(name = "lan-share")]
  #[command(about = "LAN text and file transfer assistant", long_about = None)]
  struct Cli {
      #[command(subcommand)]
      command: Commands,
  }

  #[derive(Subcommand)]
  enum Commands {
      Serve {
          #[arg(long, default_value_t = 8080)]
          port: u16,
          #[arg(long)]
          name: Option<String>,
          #[arg(long, default_value = "downloads")]
          dir: PathBuf,
      },
      Peers,
      SendText {
          #[arg(long)]
          to: String,
          #[arg(long)]
          msg: String,
      },
      SendFile {
          #[arg(long)]
          to: String,
          #[arg(long)]
          file: PathBuf,
      },
  }

  fn find_available_port(start_port: u16) -> u16 {
      let mut port = start_port;
      while port < 65535 {
          if TcpListener::bind(format!("127.0.0.1:{}", port)).is_ok() {
              return port;
          }
          port += 1;
      }
      panic!("No available ports!");
  }

  #[tokio::main]
  async fn main() -> Result<(), Box<dyn std::error::Error>> {
      let cli = Cli::parse();
      
      // 对于 client 端命令，需要读取本地已发现的在线节点缓存。由于没有常驻 daemon，
      // 我们在执行 peers/send 命令前，先收听 1.5 秒的局域网广播以获取当前在线节点。
      let registry = PeerRegistry::new();

      match cli.command {
          Commands::Serve { port, name, dir } => {
              let actual_port = find_available_port(port);
              let node_name = name.unwrap_or_else(|| {
                  hostname::get()
                      .map(|s| s.to_string_lossy().to_string())
                      .unwrap_or_else(|_| "Unknown-Node".to_string())
              });
              let uuid = Uuid::new_v4().to_string();
              let ips = discovery::get_local_ips();

              let peer = Peer {
                  uuid,
                  name: node_name.clone(),
                  port: actual_port,
                  ips,
              };

              println!("=== 启动 lan-share 接收服务 ===");
              println!("节点名称: {}", peer.name);
              println!("监听端口: {}", peer.port);
              println!("接收目录: {}", dir.display());
              println!("当前 IP 列表: {:?}", peer.ips);

              // 启动 UDP 组播广播
              let p_broadcaster = peer.clone();
              tokio::spawn(async move {
                  let _ = discovery::start_broadcaster(p_broadcaster).await;
              });

              // 启动 UDP 组播收听
              let r_listener = registry.clone();
              tokio::spawn(async move {
                  let _ = discovery::start_listener(r_listener).await;
              });

              // 启动 Axum HTTP 服务
              let app = server::make_router(registry, dir);
              let addr = std::net::SocketAddr::from(([0, 0, 0, 0], actual_port));
              let listener = tokio::net::TcpListener::bind(addr).await?;
              axum::serve(listener, app).await?;
          }
          Commands::Peers => {
              println!("正在扫描局域网节点，请稍候...");
              let r_listener = registry.clone();
              let handle = tokio::spawn(async move {
                  let _ = discovery::start_listener(r_listener).await;
              });
              tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
              handle.abort();

              registry.clean_stale(std::time::Duration::from_secs(9));
              let list = registry.list();
              if list.is_empty() {
                  println!("未发现在线节点。请确保接收端已启动 `lan-share serve`。");
              } else {
                  println!("{:<20} {:<25} {:<36}", "NAME", "ADDRESSES", "UUID");
                  for p in list {
                      let addrs: Vec<String> = p.ips.iter().map(|ip| format!("{}:{}", ip, p.port)).collect();
                      println!("{:<20} {:<25} {:<36}", p.name, addrs.join(", "), p.uuid);
                  }
              }
          }
          Commands::SendText { to, msg } => {
              // 自动扫描 1.5 秒定位目标节点名称
              let r_listener = registry.clone();
              let handle = tokio::spawn(async move {
                  let _ = discovery::start_listener(r_listener).await;
              });
              tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
              handle.abort();

              let dest = if let Some(p) = registry.find_by_name_or_ip(&to) {
                  format!("{}:{}", p.ips[0], p.port)
              } else {
                  to // 找不到则直接作为 IP:Port 处理
              };

              let sender_name = hostname::get()
                  .map(|s| s.to_string_lossy().to_string())
                  .unwrap_or_else(|_| "Unknown-Node".to_string());
              
              println!("正在发送消息至 {}...", dest);
              client::send_text(&dest, &sender_name, &msg).await?;
              println!("发送成功！");
          }
          Commands::SendFile { to, file } => {
              if !file.exists() {
                  return Err(format!("文件不存在: {}", file.display()).into());
              }

              let r_listener = registry.clone();
              let handle = tokio::spawn(async move {
                  let _ = discovery::start_listener(r_listener).await;
              });
              tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
              handle.abort();

              let dest = if let Some(p) = registry.find_by_name_or_ip(&to) {
                  format!("{}:{}", p.ips[0], p.port)
              } else {
                  to
              };

              let sender_name = hostname::get()
                  .map(|s| s.to_string_lossy().to_string())
                  .unwrap_or_else(|_| "Unknown-Node".to_string());

              println!("正在传输文件 {} 至 {}...", file.display(), dest);
              client::send_file(&dest, &sender_name, &file).await?;
              println!("传输成功！");
          }
      }
      Ok(())
  }
  ```

  并在 `tests/integration_tests.rs` 编写集成测试代码：
  ```rust
  use std::path::PathBuf;
  use std::time::Duration;
  use tokio::time::sleep;

  #[tokio::test]
  async fn test_integration_flow() {
      let download_dir = PathBuf::from("./test_downloads");
      if download_dir.exists() {
          let _ = std::fs::remove_dir_all(&download_dir);
      }

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
      let temp_file = PathBuf::from("./temp_test_file.txt");
      std::fs::write(&temp_file, "Integrate Content").unwrap();
      lan_share::client::send_file("127.0.0.1:28080", "Tester", &temp_file).await.unwrap();

      sleep(Duration::from_millis(500)).await;

      // 验证文件保存成功
      let saved_file = download_dir.join("temp_test_file.txt");
      assert!(saved_file.exists());
      let content = std::fs::read_to_string(saved_file).unwrap();
      assert_eq!(content, "Integrate Content");

      // 清理
      let _ = std::fs::remove_file(temp_file);
      let _ = std::fs::remove_dir_all(download_dir);
  }
  ```

  *由于我们要支持集成测试引用库中模块，需将 Cargo.toml 配置为支持库和二进制两个 Target，或将主要逻辑移至 `src/lib.rs`。*
  修改 `Cargo.toml`:
  ```toml
  [lib]
  name = "lan_share"
  path = "src/lib.rs"

  [[bin]]
  name = "lan-share"
  path = "src/main.rs"
  ```
  在 `src/lib.rs` 中声明公有模块：
  ```rust
  pub mod peer;
  pub mod discovery;
  pub mod server;
  pub mod client;
  ```

- [ ] **步骤 4：运行集成测试验证通过**

  运行：`cargo test`
  预期：PASS

- [ ] **步骤 5：Commit**

  ```bash
  git add .
  git commit -m "feat: complete CLI parsing, integration tests, and library decoupling"
  ```

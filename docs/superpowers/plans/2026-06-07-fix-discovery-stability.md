# 局域网服务发现稳定性与时延优化实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标**：将接收端组播心跳广播频率缩短到 1 秒/次，并在发送端实现“轮询 + 提前退出”以及“直接 IP 绕过扫描”机制，以彻底解决发送失败率高且发送命令强行等待的问题。

**技术栈**：Rust, Tokio, Clap, Reqwest.

---

## 文件结构与职责

- `src/discovery.rs`: 负责 UDP 组播发送心跳。将 `start_broadcaster` 的循环广播睡眠从 3 秒改为 1 秒。
- `src/main.rs`: 负责处理客户端的发送命令。解析目标别名时，实现检测 IP:Port 绕过扫描逻辑，并在执行扫描时改为轮询加提前退出的高效率机制。

---

## 任务拆解

### 任务 1：优化服务端心跳广播频率

**文件：**
- 修改：`src/discovery.rs:21-28`

- [ ] **步骤 1：修改 start_broadcaster 广播循环**
  
  将 `tokio::time::sleep(Duration::from_secs(3))` 修改为 `Duration::from_secs(1)`：
  ```rust
  pub async fn start_broadcaster(peer: Peer) -> std::io::Result<()> {
      loop {
          if let Err(e) = broadcast_once(&peer).await {
              eprintln!("Broadcaster error: {}", e);
          }
          tokio::time::sleep(Duration::from_secs(1)).await;
      }
  }
  ```

- [ ] **步骤 2：验证编译与现有测试**

  运行：`cargo test`
  预期：SUCCESS，所有 11 个测试顺利通过（且 `test_multicast_discovery` 应该依然通过）。

- [ ] **步骤 3：Commit**

  ```bash
  git add src/discovery.rs
  git commit -m "chore: change UDP multicast heartbeat interval from 3s to 1s"
  ```

---

### 任务 2：实现发送端轮询查找与提前退出，以及直接 IP:Port 绕过

**文件：**
- 修改：`src/main.rs`

- [ ] **步骤 1：重构 SendText 和 SendFile 中的扫描解析逻辑**

  在 `src/main.rs` 中，对 `Commands::SendText` 和 `Commands::SendFile` 分支做如下重构：
  
  1. 判断 `to` 是否是直接可解析的 `SocketAddr` 或 `IpAddr`。如果是，则不执行任何 UDP 扫描，直接使用其作为目标物理地址。
  2. 如果不是，启动 UDP 监听 100ms 后，开始每 50ms 轮询检测一次，最多等待 2.0 秒。一旦在 `registry` 中找到该名称或 IP，**立刻提前退出扫描**并获取目标地址，无需等待满时间。
  3. 修改后的逻辑：
  
  ```rust
  // 辅助函数判断是否是直接地址
  fn is_direct_address(addr: &str) -> bool {
      addr.parse::<std::net::SocketAddr>().is_ok() || addr.parse::<std::net::IpAddr>().is_ok()
  }
  ```
  
  在 `SendText` / `SendFile` 命令中：
  ```rust
  let dest_addr = if is_direct_address(&to) {
      to.clone()
  } else {
      let registry = lan_share::peer::PeerRegistry::new();
      let listener_registry = registry.clone();

      // 启动接收
      let listen_handle = tokio::spawn(async move {
          let _ = lan_share::discovery::start_listener(listener_registry).await;
      });

      println!("Scanning for target '{}' in local network (up to 2.0 seconds)...", to);
      let start_time = std::time::Instant::now();
      let timeout = std::time::Duration::from_secs(2);
      let mut resolved_peer = None;

      while start_time.elapsed() < timeout {
          if let Some(peer) = registry.find_by_name_or_ip(&to) {
              resolved_peer = Some(peer);
              break;
          }
          tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
      }
      listen_handle.abort();

      if let Some(peer) = resolved_peer {
          if let Some(ip) = peer.ips.first() {
              format!("{}:{}", ip, peer.port)
          } else {
              to.clone()
          }
      } else {
          to.clone()
      }
  };
  ```

- [ ] **步骤 2：验证集成测试**

  运行：`cargo test`
  预期：SUCCESS，所有测试（包括集成测试）依然完美通过。由于集成测试是向已开启的本地服务端发送，现在测试应该能体验到零延时立即发现并发送的性能提升。

- [ ] **步骤 3：Commit**

  ```bash
  git add src/main.rs
  git commit -m "feat: implement polling discovery with early exit and direct IP bypass"
  ```

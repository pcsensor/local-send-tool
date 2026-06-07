# `--bind-ip` 参数支持 实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 为所有子命令（`serve`、`peers`、`send-text`、`send-file`）添加可选的 `--bind-ip` 参数，解决开启 TUN 代理后无法发现局域网设备的问题。

**架构：** 在 `discovery.rs` 的所有网络函数签名中新增 `bind_ip: Option<Ipv4Addr>` 参数，使发送和接收 socket 均可绑定到指定局域网网卡。在 `main.rs` 的四个子命令中各加一个 `--bind-ip` CLI 参数，解析后透传给 discovery 函数。

**技术栈：** Rust、tokio、socket2、clap（derive 特性）

**规格文档：** `docs/superpowers/specs/2026-06-07-bind-ip-option-design.md`

---

## 文件清单

| 文件 | 操作 | 说明 |
|------|------|------|
| `src/discovery.rs` | 修改 | 所有公开函数加 `bind_ip` 参数 |
| `src/main.rs` | 修改 | 四个子命令加 `--bind-ip` CLI 参数 |

---

### 任务 1：更新 `discovery.rs` — `get_local_ips` 函数

**文件：**
- 修改：`src/discovery.rs:77-83`

- [ ] **步骤 1：编写失败的测试**

在 `src/discovery.rs` 的 `#[cfg(test)] mod tests` 块中新增：

```rust
#[test]
fn test_get_local_ips_with_bind_ip() {
    use std::net::Ipv4Addr;
    let ip = Ipv4Addr::new(192, 168, 1, 5);
    let ips = get_local_ips(Some(ip));
    assert_eq!(ips, vec!["192.168.1.5".to_string()]);
}

#[test]
fn test_get_local_ips_without_bind_ip() {
    // None 时行为与原来一致：返回非空列表（有网络时）
    let ips = get_local_ips(None);
    // 只验证不 panic，不对具体值做断言（CI 环境 IP 不固定）
    let _ = ips;
}
```

- [ ] **步骤 2：运行测试验证编译失败**

```bash
cargo test test_get_local_ips 2>&1 | head -30
```

预期：编译错误，因为 `get_local_ips` 签名尚未修改（参数数量不符）。

- [ ] **步骤 3：修改 `get_local_ips` 函数**

将 `src/discovery.rs` 中的 `get_local_ips` 替换为：

```rust
pub fn get_local_ips(bind_ip: Option<Ipv4Addr>) -> Vec<String> {
    if let Some(ip) = bind_ip {
        return vec![ip.to_string()];
    }
    if let Ok(ip) = local_ip() {
        vec![ip.to_string()]
    } else {
        vec![]
    }
}
```

同时在文件顶部确认已有 `use std::net::Ipv4Addr;`（当前已有，无需新增）。

- [ ] **步骤 4：运行测试验证通过**

```bash
cargo test test_get_local_ips 2>&1
```

预期：`test_get_local_ips_with_bind_ip` 和 `test_get_local_ips_without_bind_ip` 均 PASS。

- [ ] **步骤 5：Commit**

```bash
git add src/discovery.rs
git commit -m "feat: get_local_ips 支持 bind_ip 参数"
```

---

### 任务 2：更新 `discovery.rs` — `broadcast_once` 和 `start_broadcaster`

**文件：**
- 修改：`src/discovery.rs:11-28`

- [ ] **步骤 1：修改 `broadcast_once` 函数签名和实现**

将 `src/discovery.rs` 中的 `broadcast_once` 替换为：

```rust
pub async fn broadcast_once(peer: &Peer, bind_ip: Option<Ipv4Addr>) -> std::io::Result<()> {
    let bind_addr = bind_ip
        .map(|ip| format!("{}:0", ip))
        .unwrap_or_else(|| "0.0.0.0:0".to_string());
    let socket = UdpSocket::bind(&bind_addr).await?;
    let payload = serde_json::to_vec(peer)?;
    let target_addr: SocketAddr = format!("{}:{}", MULTICAST_ADDR, MULTICAST_PORT)
        .parse()
        .unwrap();
    socket.send_to(&payload, target_addr).await?;
    Ok(())
}
```

- [ ] **步骤 2：修改 `start_broadcaster` 函数签名和实现**

将 `src/discovery.rs` 中的 `start_broadcaster` 替换为：

```rust
pub async fn start_broadcaster(peer: Peer, bind_ip: Option<Ipv4Addr>) -> std::io::Result<()> {
    loop {
        if let Err(e) = broadcast_once(&peer, bind_ip).await {
            eprintln!("Broadcaster error: {}", e);
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
```

- [ ] **步骤 3：运行编译检查**

```bash
cargo check 2>&1
```

预期：编译错误，提示 `main.rs` 中调用 `start_broadcaster` / `broadcast_once` 的地方参数数量不对。这是预期现象，任务 4 统一修复 `main.rs`。

- [ ] **步骤 4：Commit 当前进度**

```bash
git add src/discovery.rs
git commit -m "feat: broadcast_once/start_broadcaster 支持 bind_ip 参数"
```

---

### 任务 3：更新 `discovery.rs` — `create_multicast_socket` 和 `start_listener`

**文件：**
- 修改：`src/discovery.rs:30-75`

- [ ] **步骤 1：修改 `create_multicast_socket` 函数签名和实现**

将 `src/discovery.rs` 中的 `create_multicast_socket` 替换为：

```rust
fn create_multicast_socket(bind_ip: Option<Ipv4Addr>) -> std::io::Result<StdUdpSocket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    #[cfg(not(windows))]
    socket.set_reuse_port(true)?;

    #[cfg(windows)]
    let bind_addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, MULTICAST_PORT));
    #[cfg(not(windows))]
    let bind_addr = SocketAddr::from((MULTICAST_ADDR, MULTICAST_PORT));

    socket.bind(&bind_addr.into())?;

    // 若指定了 bind_ip，在该接口加入组播组；否则由系统自动选择
    let iface = bind_ip.unwrap_or(Ipv4Addr::UNSPECIFIED);
    socket.join_multicast_v4(&MULTICAST_ADDR, &iface)?;
    Ok(StdUdpSocket::from(socket))
}
```

- [ ] **步骤 2：修改 `start_listener` 函数签名**

将 `src/discovery.rs` 中的 `start_listener` 替换为：

```rust
pub async fn start_listener(registry: PeerRegistry, bind_ip: Option<Ipv4Addr>) -> std::io::Result<()> {
    let std_socket = create_multicast_socket(bind_ip)?;
    std_socket.set_nonblocking(true)?;
    let socket = UdpSocket::from_std(std_socket)?;
    let mut buf = vec![0u8; 65535];
    let mut backoff = Duration::from_millis(10);

    loop {
        match socket.recv_from(&mut buf).await {
            Ok((len, _addr)) => {
                backoff = Duration::from_millis(10);
                if let Ok(peer) = serde_json::from_slice::<Peer>(&buf[..len]) {
                    registry.register(peer);
                }
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::ConnectionReset {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
                eprintln!(
                    "Listener recv_from error: {}. Backing off for {:?}",
                    e, backoff
                );
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, Duration::from_secs(1));
            }
        }
    }
}
```

- [ ] **步骤 3：更新 `test_multicast_discovery` 中的调用**

在 `src/discovery.rs` 的测试块中，将原来的调用更新为传入 `None`：

```rust
// 原来：
start_listener(rx_registry).await.unwrap();
// 修改为：
start_listener(rx_registry, None).await.unwrap();

// 原来：
broadcast_once(&peer).await.unwrap();
// 修改为：
broadcast_once(&peer, None).await.unwrap();
```

- [ ] **步骤 4：运行 discovery 相关测试**

```bash
cargo test --lib -- discovery 2>&1
```

预期：`test_multicast_discovery` PASS，无编译错误。（`main.rs` 的调用仍报错，下一任务修复）

- [ ] **步骤 5：Commit**

```bash
git add src/discovery.rs
git commit -m "feat: create_multicast_socket/start_listener 支持 bind_ip 参数"
```

---

### 任务 4：更新 `main.rs` — 添加 `--bind-ip` CLI 参数并修复所有调用点

**文件：**
- 修改：`src/main.rs`

- [ ] **步骤 1：在四个子命令枚举变体中添加 `bind_ip` 字段**

在 `src/main.rs` 的 `Commands` 枚举中，为每个变体添加字段（注意：Serve 中已有 `name` 字段，在其后添加）：

```rust
// Serve 变体，新增：
/// 指定局域网网卡 IP（开启 TUN 代理时使用，例如 192.168.1.5）
#[arg(long, value_name = "IP")]
bind_ip: Option<String>,

// Peers 变体，新增：
/// 指定局域网网卡 IP（开启 TUN 代理时使用，例如 192.168.1.5）
#[arg(long, value_name = "IP")]
bind_ip: Option<String>,

// SendText 变体，新增：
/// 指定局域网网卡 IP（开启 TUN 代理时使用，例如 192.168.1.5）
#[arg(long, value_name = "IP")]
bind_ip: Option<String>,

// SendFile 变体，新增：
/// 指定局域网网卡 IP（开启 TUN 代理时使用，例如 192.168.1.5）
#[arg(long, value_name = "IP")]
bind_ip: Option<String>,
```

- [ ] **步骤 2：新增公共解析辅助函数**

在 `main.rs` 的 `fn is_direct_address` 之前添加：

```rust
/// 将 --bind-ip 字符串解析为 Ipv4Addr，无效时打印错误并退出
fn parse_bind_ip(bind_ip: Option<&str>) -> Option<std::net::Ipv4Addr> {
    bind_ip.map(|s| {
        s.parse::<std::net::Ipv4Addr>().unwrap_or_else(|_| {
            eprintln!("错误：--bind-ip '{}' 不是有效的 IPv4 地址（示例：192.168.1.5）", s);
            std::process::exit(1);
        })
    })
}
```

- [ ] **步骤 3：更新 `resolve_destination` 函数签名**

将 `resolve_destination` 修改为接受 `bind_ip`：

```rust
async fn resolve_destination(to: &str, bind_ip: Option<std::net::Ipv4Addr>) -> String {
    if is_direct_address(to) {
        return fallback_address(to);
    }

    let registry = lan_share::peer::PeerRegistry::new();
    let listener_registry = registry.clone();

    let listen_handle = tokio::spawn(async move {
        let _ = lan_share::discovery::start_listener(listener_registry, bind_ip).await;
    });

    println!("Scanning for target '{}' in local network...", to);

    let mut found_peer = None;
    for _ in 0..40 {
        if let Some(peer) = registry.find_by_name_or_ip(to) {
            found_peer = Some(peer);
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
    listen_handle.abort();

    if let Some(peer) = found_peer {
        if let Some(ip) = peer.ips.first() {
            format!("{}:{}", ip, peer.port)
        } else {
            fallback_address(to)
        }
    } else {
        fallback_address(to)
    }
}
```

- [ ] **步骤 4：更新 `Commands::Serve` 处理逻辑**

在 `Commands::Serve { dir, port, name, bind_ip }` 的处理代码中，在 `node_name` 赋值后加入解析和透传：

```rust
Commands::Serve { dir, port, name, bind_ip } => {
    let (listener, actual_port) = find_available_port(port).await;

    let node_name = match name {
        Some(n) => n,
        None => hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .unwrap_or_else(|| "Unknown-Node".to_string()),
    };

    let bind_ip = parse_bind_ip(bind_ip.as_deref());

    let registry = lan_share::peer::PeerRegistry::new();

    // 1. 启动后台 UDP 心跳包接收
    let listener_registry = registry.clone();
    tokio::spawn(async move {
        if let Err(e) = lan_share::discovery::start_listener(listener_registry, bind_ip).await {
            eprintln!("Listener background task failed: {}", e);
        }
    });

    // 2. 启动后台 UDP 心跳包广播
    let peer = lan_share::peer::Peer {
        uuid: uuid::Uuid::new_v4().to_string(),
        name: node_name.clone(),
        port: actual_port,
        ips: lan_share::discovery::get_local_ips(bind_ip),
    };
    let broadcaster_peer = peer.clone();
    tokio::spawn(async move {
        if let Err(e) = lan_share::discovery::start_broadcaster(broadcaster_peer, bind_ip).await {
            eprintln!("Broadcaster background task failed: {}", e);
        }
    });

    // 3. 运行 Axum 服务端
    let app = lan_share::server::make_router(registry, dir);
    println!("Server node name: {}", node_name);
    println!("Server UUID: {}", peer.uuid);
    println!("Serving on http://{}", listener.local_addr().unwrap());

    if let Err(e) = axum::serve(listener, app).await {
        eprintln!("Server exited with error: {}", e);
    }
}
```

- [ ] **步骤 5：更新 `Commands::Peers` 处理逻辑**

```rust
Commands::Peers { bind_ip } => {
    let bind_ip = parse_bind_ip(bind_ip.as_deref());
    let registry = lan_share::peer::PeerRegistry::new();
    let listener_registry = registry.clone();

    let listen_handle = tokio::spawn(async move {
        let _ = lan_share::discovery::start_listener(listener_registry, bind_ip).await;
    });

    println!("Scanning local network for peers (listening for 1.5 seconds)...");
    tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;
    listen_handle.abort();

    let list = registry.list();
    if list.is_empty() {
        println!("No peers discovered.");
    } else {
        println!("{:<36} | {:<20} | {:<5} | IPs", "UUID", "Name", "Port");
        println!("{}", "-".repeat(80));
        for peer in list {
            println!(
                "{:<36} | {:<20} | {:<5} | {}",
                peer.uuid,
                peer.name,
                peer.port,
                peer.ips.join(", ")
            );
        }
    }
}
```

- [ ] **步骤 6：更新 `Commands::SendText` 和 `Commands::SendFile` 处理逻辑**

`SendText`：

```rust
Commands::SendText { to, name, text, bind_ip } => {
    let bind_ip = parse_bind_ip(bind_ip.as_deref());
    let dest_addr = resolve_destination(&to, bind_ip).await;

    let sender_name = match name {
        Some(n) => n,
        None => hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .unwrap_or_else(|| "Unknown-Sender".to_string()),
    };

    println!("Sending text message to {} as '{}'...", dest_addr, sender_name);
    match lan_share::client::send_text(&dest_addr, &sender_name, &text).await {
        Ok(_) => println!("Text sent successfully!"),
        Err(e) => {
            eprintln!("Failed to send text message: {}", e);
            std::process::exit(1);
        }
    }
}
```

`SendFile`：

```rust
Commands::SendFile { to, name, file, bind_ip } => {
    if !file.exists() {
        eprintln!("Error: File '{}' does not exist.", file.display());
        std::process::exit(1);
    }

    let bind_ip = parse_bind_ip(bind_ip.as_deref());
    let dest_addr = resolve_destination(&to, bind_ip).await;

    let sender_name = match name {
        Some(n) => n,
        None => hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .unwrap_or_else(|| "Unknown-Sender".to_string()),
    };

    println!("Sending file '{}' to {} as '{}'...", file.display(), dest_addr, sender_name);
    match lan_share::client::send_file(&dest_addr, &sender_name, &file).await {
        Ok(_) => println!("File sent successfully!"),
        Err(e) => {
            eprintln!("Failed to send file: {}", e);
            std::process::exit(1);
        }
    }
}
```

- [ ] **步骤 7：更新测试中的 `resolve_destination` 调用**

在 `main.rs` 的 `#[cfg(test)] mod tests` 中：

```rust
#[tokio::test]
async fn test_resolve_destination_direct() {
    assert_eq!(resolve_destination("127.0.0.1:9000", None).await, "127.0.0.1:9000");
    assert_eq!(resolve_destination("127.0.0.1", None).await, "127.0.0.1:8080");
}
```

- [ ] **步骤 8：全量编译并运行所有测试**

```bash
cargo test 2>&1
```

预期：所有测试 PASS，无编译警告（除可能的 unused import 等良性警告）。

- [ ] **步骤 9：验证 CLI 帮助信息包含 `--bind-ip`**

```bash
cargo run -- serve --help 2>&1
cargo run -- peers --help 2>&1
cargo run -- send-text --help 2>&1
cargo run -- send-file --help 2>&1
```

预期：每个子命令的帮助中均出现 `--bind-ip <IP>` 及说明文字。

- [ ] **步骤 10：Commit**

```bash
git add src/main.rs
git commit -m "feat: 所有子命令添加 --bind-ip 参数，修复 TUN 代理下无法发现局域网设备的问题"
```

---

## 自检

**规格覆盖度：**
- ✅ `broadcast_once` 绑定指定 IP → 任务 2
- ✅ `create_multicast_socket` 指定接口加入组播组 → 任务 3
- ✅ `get_local_ips` 优先返回 bind_ip → 任务 1
- ✅ 四个子命令均加 `--bind-ip` 参数 → 任务 4
- ✅ 向后兼容（参数可选，None 时行为不变）→ 贯穿所有任务

**占位符扫描：** 无 TODO / 待定，所有步骤均含完整代码。

**类型一致性：**
- `bind_ip` 在 discovery 函数中统一为 `Option<Ipv4Addr>`
- `parse_bind_ip` 返回 `Option<std::net::Ipv4Addr>`，与 discovery 函数签名匹配
- 所有调用点透传方式一致

# 修复方案设计规格说明书 (Fix Design Specification)

* **文档路径**: `docs/superpowers/specs/2026-06-07-fix-code-review-issues-design-specs.md`
* **创建时间**: 2026-06-07
* **主题**: 修复局域网共享工具 `lan-share` 审查报告中的 4 个核心问题

---

## 1. 需求与目标 (Requirements & Scope)

本次修复的目标是解决 `lan-share` 项目在安全性、并发性以及跨网卡可用性上的四个核心问题：
1. **解决 `/api/file/init` 的并发竞态条件**：确保在异步扫描磁盘分块时，并发初始化同一 `upload_id` 不会产生会话脏读或相互覆盖。
2. **修正端口自增检测与实际 TCP 监听 IP 绑定不一致**：使用户指定的 `--bind-ip` 能正确作用于 TCP 服务器端口的绑定探测，增强安全性。
3. **实现超时废弃上传会话与磁盘临时文件的定时清理**：防止因客户端异常退出导致内存和磁盘空间发生泄漏。
4. **扩展 `get_local_ips` 支持多网卡 IPv4 探测**：使得在多网卡/多子网环境下，各网段的局域网节点均能顺利连接。

---

## 2. 详细设计 (Detailed Design)

### 2.1 并发初始化双重检查锁
在 `server.rs` 中的 [handle_file_init](file:///Users/pcsensor/Projects/local-send-tool/src/server.rs#L237) 中，第二次获取锁并准备插入 `UploadSession` 时，执行双重检查锁模式：
1. **第一次检查**（在磁盘 IO 扫描之前，加锁）：若已存在 `upload_id` 且参数一致，直接返回。
2. **释放锁**：调用 `scan_received_chunks.await` 读取磁盘。
3. **第二次检查**（重新获取锁）：如果其他线程在此期间已经创建了该 `upload_id` 的会话，则放弃当前计算的结果，直接采用已存在的会话。如果已存在会话的元数据（文件大小、分块大小、校验和）与本次请求不符，返回 `BAD_REQUEST`；否则直接返回该已有会话。

### 2.2 TCP 端口自增检测支持指定 IP
在 `main.rs` 中：
* 将 `find_available_port` 的签名调整为：
  ```rust
  async fn find_available_port(bind_ip: Option<std::net::Ipv4Addr>, start_port: u16) -> (tokio::net::TcpListener, u16)
  ```
* 绑定地址由硬编码的 `0.0.0.0` 变更为：
  ```rust
  let ip = bind_ip.unwrap_or(std::net::Ipv4Addr::UNSPECIFIED);
  let addr = std::net::SocketAddr::from((ip, actual_port));
  tokio::net::TcpListener::bind(&addr)
  ```

### 2.3 上传会话的定时清理任务
在 `server.rs` 中：
* `UploadSession` 结构体中新增 `last_active: std::time::Instant` 字段。
* 在以下时机刷新 `last_active = Instant::now()`：
  - 会话初始化时（`handle_file_init`）
  - 收到并写入新的分块时（`handle_file_chunk`）
* 在 `make_router` 启动时，使用 `tokio::spawn` 启动一个常驻后台清理任务：
  - 每隔 5 分钟（`Duration::from_secs(300)`）执行一次。
  - 获取 `upload_sessions` 的 `Mutex` 锁，计算当前时间与 `last_active` 的差值。
  - 将超过 1 小时（`Duration::from_secs(3600)`）未活动的会话移除，并异步调用 `tokio::fs::remove_dir_all` 清除其在磁盘上的隐藏临时目录。

### 2.4 多网卡 IP 单播发现
在 `discovery.rs` 中：
* 利用 `local_ip_address::list_local_address_interfaces()` 遍历本机的所有网卡。
* 过滤条件：`ip.is_ipv4() && !ip.is_loopback()`。
* 若接口查询失败，则采用 `local_ip()` 作为兜底方案，确保向上兼容性。

---

## 3. 测试策略与验证 (Testing Strategy)

为了验证上述修改的正确性，需要进行以下测试：

1. **并发初始化测试**：
   - 编写单元测试，并发发起 10 个相同 `upload_id` 的 `handle_file_init` 请求，确认能够正常幂等返回，没有发生会话相互覆盖。
2. **特定 IP 绑定测试**：
   - 验证传入 `Option<Ipv4Addr>` 后，`find_available_port` 能正确监听在指定 IP（如 `127.0.0.1`）而非 `0.0.0.0`。
3. **超时清理测试**：
   - 模拟一个已过期的 `UploadSession`，并将扫描周期 and 超时时间缩短，验证后台清理协程能成功将内存会话移除并删除隐藏的分块目录。
4. **多网卡探测测试**：
   - 运行单元测试，确保 `get_local_ips(None)` 返回包含本机主要活动 IP 在内的列表。

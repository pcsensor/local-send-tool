# 设计文档：`--bind-ip` 参数支持

**日期：** 2026-06-07  
**状态：** 已批准  
**作者：** Antigravity  

---

## 背景

当设备开启 TUN 模式网络代理时，操作系统路由表会将默认路由指向虚拟 TUN 网卡。这导致：

1. **发送侧**：组播心跳包从 TUN 虚拟网卡发出，局域网其他设备收不到。
2. **接收侧**：`join_multicast_v4(..., UNSPECIFIED)` 让系统自动选网卡，TUN 模式下可能选中虚拟网卡，导致收不到局域网组播。
3. **IP 广播错误**：`get_local_ips()` 可能返回 TUN 分配的虚拟 IP（如 `198.18.x.x`），对端拿到后无法连接。

---

## 目标

为所有子命令（`serve`、`peers`、`send-text`、`send-file`）添加可选的 `--bind-ip` 参数，允许用户显式指定局域网网卡 IP，绕过 TUN 代理的路由干扰。

---

## 非目标

- 自动检测并过滤 TUN 网卡（属于方案3，本次不实现）。
- 支持 IPv6 绑定（当前组播实现仅支持 IPv4）。

---

## 设计方案：方案 B（所有子命令统一加参数）

### CLI 变更

每个子命令均新增可选参数：

```
--bind-ip <IP>    指定用于局域网发现的本机 IPv4 地址（开启 TUN 代理时使用）
```

示例用法：

```bash
lan-share serve --bind-ip 192.168.1.5
lan-share peers --bind-ip 192.168.1.5
lan-share send-file --to peer-name --bind-ip 192.168.1.5 ./file.txt
lan-share send-text --to peer-name --bind-ip 192.168.1.5 "hello"
```

### `discovery.rs` 函数签名变更汇总

| 函数 | 原签名 | 新签名 |
|------|--------|--------|
| `broadcast_once` | `(peer: &Peer)` | `(peer: &Peer, bind_ip: Option<Ipv4Addr>)` |
| `start_broadcaster` | `(peer: Peer)` | `(peer: Peer, bind_ip: Option<Ipv4Addr>)` |
| `create_multicast_socket` | `()` | `(bind_ip: Option<Ipv4Addr>)` |
| `start_listener` | `(registry: PeerRegistry)` | `(registry: PeerRegistry, bind_ip: Option<Ipv4Addr>)` |
| `get_local_ips` | `()` | `(bind_ip: Option<Ipv4Addr>)` |

### 核心逻辑修改

**broadcast_once**：绑定指定 IP 出口，让组播包从正确网卡发出
```rust
let bind_addr = bind_ip
    .map(|ip| format!("{}:0", ip))
    .unwrap_or_else(|| "0.0.0.0:0".to_string());
let socket = UdpSocket::bind(&bind_addr).await?;
```

**create_multicast_socket**：在指定接口加入组播组
```rust
let iface = bind_ip.unwrap_or(Ipv4Addr::UNSPECIFIED);
socket.join_multicast_v4(&MULTICAST_ADDR, &iface)?;
```

**get_local_ips**：若指定了 bind_ip，直接返回它作为本机 IP
```rust
if let Some(ip) = bind_ip {
    return vec![ip.to_string()];
}
```

### `main.rs` 变更

每个 Commands 枚举变体新增字段：

```rust
/// 指定局域网网卡 IP（开启 TUN 代理时使用，如 192.168.1.5）
#[arg(long, value_name = "IP")]
bind_ip: Option<String>,
```

各子命令处理时统一解析：

```rust
let bind_ip: Option<Ipv4Addr> = bind_ip
    .as_deref()
    .map(|s| s.parse().expect("--bind-ip 必须是有效的 IPv4 地址"));
```

---

## 错误处理

- 若传入值不是有效的 IPv4 地址，程序给出明确错误提示并退出。
- 参数完全可选，不传时行为与修改前完全一致（向后兼容）。

---

## 测试策略

- 现有单元测试：传入 `None` 验证向后兼容，不需改动逻辑。
- 更新 `test_multicast_discovery`：显式传入 `None` 以适配新签名。

---

## 影响范围

- `src/discovery.rs`：5 处函数签名 + 逻辑修改
- `src/main.rs`：4 个子命令各加一个参数字段，约 8 处调用点修改
- 无新增依赖
- 完全向后兼容

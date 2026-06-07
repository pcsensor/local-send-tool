# lan-share 局域网文件传输助手

`lan-share` 是一个用 Rust 编写的高性能、跨平台（macOS、Windows、Linux）局域网命令行文字与大文件传输工具。它无需任何第三方服务器中转，支持自动服务发现，专为追求极速和安全性的终端用户设计。

---

## 🌟 核心特性

- **服务自动发现**：基于 UDP 组播协议（多播地址 `224.0.0.188:50001`）实现，节点上线后自动广播，无需手动输入对方 IP。
- **高性能流式文件传输**：服务端使用 Axum 异步接收文件并以 $O(1)$ 常数内存占用逐步流式写入磁盘，支持百兆、千兆网络满速传输。
- **高安全性保障**：
  - **路径穿越防御**：严格净化接收文件名（只保留 Basename），防止恶意文件覆盖系统敏感文件。
  - **并发写安全**：采用内核级原子创建操作 `create_new`，在多用户高并发上传同名文件时，自动检测冲突并进行重名递增（例如 `file_1.txt`），无数据被覆盖风险。
- **跨平台多实例调试**：针对 macOS、Linux 以及 Windows 的底层端口复用差异进行适配，完美支持在单机上启动多个实例用于开发测试。

---

## 🚀 快速上手

### 1. 编译安装

在开始之前，请确保您的系统已安装 Rust 工具链。

```bash
# 克隆仓库并进入目录后编译
cargo build --release
```

编译产物位于 `target/release/lan-share`。

### 2. 启动服务（接收端）

在想要接收文件或消息的机器上启动服务模式：

```bash
# 启动接收服务，默认监听 8080 端口并将接收的文件保存至 ./downloads
./target/release/lan-share serve

# 您可以自定义保存目录与初始端口
./target/release/lan-share serve --dir ~/Downloads/LAN-Share --port 9000
```
*注：如果指定端口已被占用，服务会自动查找并递增绑定下一个可用端口（如 9001）。*

### 3. 在线节点扫描（发送端）

在另一台机器上，扫描当前局域网内的在线服务：

```bash
./target/release/lan-share peers
```

输出示例：
```text
Scanning local network for peers (listening for 1.5 seconds)...
UUID                                 | Name                 | Port  | IPs
--------------------------------------------------------------------------------
fb741f93-6856-4a75-a760-4f723fefccb2 | archlinux            | 8080  | 192.168.100.155
```

### 4. 发送文字消息

使用 `send-text` 子命令。其中 `--to` 目标可以传入**节点别名**（如 `archlinux`）、**UUID** 或 **IP:Port**：

```bash
./target/release/lan-share send-text --to archlinux "你好，局域网的伙伴！"
```

### 5. 发送文件

使用 `send-file` 子命令直接传输文件：

```bash
./target/release/lan-share send-file --to archlinux ~/Downloads/report.pdf
```

---

## 🛠️ 命令帮助

```text
A simple LAN file sharing tool

Usage: lan-share <COMMAND>

Commands:
  serve      Start the file sharing server
  peers      List all discovered online peers
  send-text  Send a text message to a specific peer
  send-file  Send a file to a specific peer
  help       Print this message or the help of the given subcommand(s)
```

---

## 🧪 自动化测试

项目内嵌了完善的单元测试和端到端集成测试，支持对并发重名冲突、组播包多路分发进行模拟：

```bash
cargo test
```

# lan-share 局域网文件传输助手

`lan-share` 是一个用 Rust 编写的高性能、跨平台（macOS、Windows、Linux）局域网命令行文字与大文件传输工具。它无需任何第三方服务器中转，支持自动服务发现，专为追求极速和安全性的终端用户设计。

---

## 🌟 核心特性

- **服务自动发现**：基于 UDP 组播协议（多播地址 `224.0.0.188:50001`）实现，节点上线后自动广播，无需手动输入对方 IP。
- **高性能流式文件传输**：服务端使用 Axum 异步接收文件并以 $O(1)$ 常数内存占用逐步流式写入磁盘，支持百兆、千兆网络满速传输。
- **进度、重试与完整性校验**：发送端支持进度条、速度/ETA、指数退避重试；接收端使用 SHA-256 校验并在失败时清理临时文件。
- **分片并行与断点续传**：可通过 `--chunked` 启用多连接分片上传，并使用 `--resume-upload-id` 继续未完成的分片传输。
- **高安全性保障**：
  - **配置控制面仅限本机**：配置读写接口（`/api/config`）只接受来自 localhost 的请求，局域网其他设备无法远程改写本机配置。Web 页面发送接口（`/api/web/message`、`/api/web/file`）允许本机或同源 Web 页面请求，便于通过局域网地址打开 Web UI 后发送文字和文件；接收类接口（`/api/message`、`/api/file/*`）仍对局域网开放以便正常收件。
  - **路径穿越防御**：严格净化接收文件名（只保留 Basename），防止恶意文件覆盖系统敏感文件。
  - **并发写安全**：采用内核级原子创建操作 `create_new`，在多用户高并发上传同名文件时，自动检测冲突并进行重名递增（例如 `file_1.txt`），无数据被覆盖风险。
- **跨平台多实例调试**：针对 macOS、Linux 以及 Windows 的底层端口复用差异进行适配，完美支持在单机上启动多个实例用于开发测试。

---

## 🚀 编译与安装

在开始之前，请确保您的系统已安装 Rust 工具链。

```bash
# 克隆仓库并进入目录后编译
cargo build --release
```

编译产物位于 `target/release/lan-share`。您可以将该可执行文件复制到系统的 `PATH` 路径下（如 `/usr/local/bin`）以方便全局调用。

---

## 💻 所有使用方法

### 1. 启动服务（接收端）
使用 `serve` 命令启动本地监听。

```bash
lan-share serve [FLAGS]
```

**支持参数：**
*   `--dir <DIR>`：指定接收文件的保存目录。默认值为当前目录下的 `./downloads`。
*   `-p`, `--port <PORT>`：指定绑定的 TCP 端口。默认值为 `8080`。**若端口已被占用，程序会自动递增尝试下一个可用端口**（如 8081, 8082...）。
*   `-n`, `--name <NAME>`：为本地节点设置一个局域网别名（Alias）。默认使用系统主机名。
*   `--bind-ip <IP>`：指定局域网网卡 IP（开启 TUN 网络代理，如 Clash/Sing-box 的 TUN 模式时，建议绑定实际的局域网 IP，例如 `192.168.1.5`）。

**示例：**
```bash
# 使用默认配置启动
lan-share serve

# 启动并设置别名为 "archlinux"，文件保存到 ~/Downloads/LAN-Share，从 9000 端口开始尝试绑定
lan-share serve --name archlinux --dir ~/Downloads/LAN-Share --port 9000
```

---

### 2. 扫描局域网节点
使用 `peers` 命令扫描当前局域网中运行着 `lan-share serve` 的所有活动设备。

```bash
lan-share peers [FLAGS]
```

**支持参数：**
*   `--bind-ip <IP>`：指定局域网网卡 IP（开启 TUN 网络代理时使用，例如 `192.168.1.5`）。

**示例输出：**
```text
Scanning local network for peers (listening for 1.5 seconds)...
UUID                                 | Name                 | Port  | IPs
--------------------------------------------------------------------------------
fb741f93-6856-4a75-a760-4f723fefccb2 | archlinux            | 8080  | 192.168.100.155
9ea6338f-c990-495e-83b3-74d958be324e | win11-laptop         | 8081  | 192.168.100.102
```

---

### 3. 发送文字消息
使用 `send-text` 命令向局域网节点发送简短消息。

```bash
lan-share send-text --to <TARGET> [FLAGS] <TEXT>
```

**支持参数：**
*   `--to <TARGET>`（必需）：指定接收端目标。可以是**节点别名**（如 `archlinux`）、**UUID**、**IP 地址**、**IP:Port** 或 **IPv6**。
*   `-n`, `--name <SENDER_NAME>`：指定您的发送者署名。默认使用系统主机名。
*   `--bind-ip <IP>`：指定局域网网卡 IP（开启 TUN 网络代理时使用，例如 `192.168.1.5`）。
*   `<TEXT>`（位置参数）：要发送的文字消息内容，如有空格需用引号包裹。

**示例：**
```bash
# 发送给别名为 archlinux 的节点
lan-share send-text --to archlinux "Hello from macOS"

# 发送给直连 IP 且指定发送人名称为 Alice
lan-share send-text --to 192.168.100.155:8080 --name Alice "这是文字测试"
```

---

### 4. 发送本地文件
使用 `send-file` 命令传输本地文件。

```bash
lan-share send-file --to <TARGET> [FLAGS] <FILE_PATH>
```

**支持参数：**
*   `--to <TARGET>`（必需）：指定接收端目标。可以是**节点别名**、**UUID**、**IP 地址**、**IP:Port** 或 **IPv6**。
*   `-n`, `--name <SENDER_NAME>`：指定您的发送者署名。默认使用系统主机名。
*   `--bind-ip <IP>`：指定局域网网卡 IP（开启 TUN 网络代理时使用，例如 `192.168.1.5`）。
*   `--retry <N>`：发送失败时最多重试 N 次。
*   `--compress <auto|always|never>`：是否使用 zstd 压缩文件流，默认 `auto`。
*   `--progress`：显示上传进度、速度和 ETA。
*   `--chunked`：启用分片多连接上传。
*   `--chunk-size <BYTES>`：分片大小，默认 `8388608`（8 MiB）。
*   `--chunk-concurrency <N>`：分片并发连接数，默认 `4`。
*   `--resume-upload-id <ID>`：继续指定 upload id 的未完成分片上传。
*   `--cancel-timeout <SECONDS>`：收到 Ctrl+C 后等待接收端清理的提示超时时间，默认 `10`。
*   `<FILE_PATH>`（位置参数）：本地要发送的文件路径。

**示例：**
```bash
# 发送本地 pdf 准考证文件给 archlinux 节点
lan-share send-file --to archlinux ~/Downloads/ticket.pdf

# 使用直连 IP 传输 zip 压缩包
lan-share send-file --to 192.168.100.155:8080 ./archive.zip

# 显示进度并在失败时重试 3 次
lan-share send-file --to archlinux --progress --retry 3 ./large.log

# 启用分片多连接上传；如果中断，可保留输出中的 upload id 后续续传
lan-share send-file --to archlinux --chunked --chunk-concurrency 4 ./movie.mkv
```

---

### 5. 批量发送多个文件
使用 `send-files` 命令一次发送多个文件，默认最多 3 个文件并发上传。

```bash
lan-share send-files --to <TARGET> [FLAGS] <FILE_PATH>...
```

**支持参数：**
*   `--to <TARGET>`（必需）：指定接收端目标。
*   `-n`, `--name <SENDER_NAME>`：指定您的发送者署名。
*   `--bind-ip <IP>`：指定局域网网卡 IP。
*   `--concurrency <N>`：同时发送的文件数，默认 `3`。
*   `--retry <N>`：每个文件失败时最多重试 N 次。
*   `--compress <auto|always|never>`：是否使用 zstd 压缩文件流，默认 `auto`。
*   `--progress`：显示上传进度、速度和 ETA。
*   `--chunked`：为每个文件启用分片多连接上传。
*   `--chunk-size <BYTES>`：分片大小，默认 `8388608`（8 MiB）。
*   `--chunk-concurrency <N>`：每个文件的分片并发连接数，默认 `4`。

**示例：**
```bash
lan-share send-files --to archlinux --concurrency 2 ./a.zip ./b.zip ./c.zip
```

---

### 6. 配置文件与环境变量
配置优先级为：CLI 参数 > 环境变量 > 配置文件 > 默认值。

配置文件路径固定为 `~/.config/lan-share/config.toml`，不随操作系统切换到其他标准配置目录。示例：

```toml
[defaults]
download_dir = "~/Downloads/LAN-Share"
port = 9000
name = "macbook"
bind_ip = "192.168.1.5"
retry = 3
compress = "auto"
progress = true
cancel_timeout = 10
chunked = false
chunk_size = 8388608
chunk_concurrency = 4
concurrency = 3
```

这些字段会作为对应 CLI 参数的默认值：`--dir`、`--port`、`--name`、`--bind-ip`、`--retry`、`--compress`、`--progress`、`--cancel-timeout`、`--chunked`、`--chunk-size`、`--chunk-concurrency`、`send-files --concurrency`。

路径值支持以 `~` 开头表示用户主目录，例如 `~/Downloads/LAN-Share`。该规则由 `lan-share` 自己处理，在 Windows、macOS 和 Linux 上都适用于配置文件、`LAN_SHARE_DIR`、带引号传入的 `--dir` 以及发送文件路径。

以下参数不写入全局配置：`--to`、文字内容、文件路径、文件列表、`--resume-upload-id`。它们属于单次发送任务，应该每次在命令行指定。

支持的环境变量：

```bash
LAN_SHARE_DIR=~/Downloads/LAN-Share
LAN_SHARE_PORT=9000
LAN_SHARE_NAME=macbook
LAN_SHARE_BIND_IP=192.168.1.5
LAN_SHARE_RETRY=3
LAN_SHARE_COMPRESS=auto
LAN_SHARE_PROGRESS=true
LAN_SHARE_CANCEL_TIMEOUT=10
LAN_SHARE_CHUNKED=false
LAN_SHARE_CHUNK_SIZE=8388608
LAN_SHARE_CHUNK_CONCURRENCY=4
LAN_SHARE_CONCURRENCY=3
```

---

## 💡 推荐使用方法 (最佳实践)

### 1. 设备别名动态发送 (最常推荐)
在局域网内设备 IP 经常变动的无线 Wi-Fi 环境下，建议在启动服务时为设备起一个固定的别名（例如 `--name archlinux`）。
发送时，直接使用 `--to archlinux`：
```bash
lan-share send-file --to archlinux ~/movie.mp4
```
**为什么推荐**：
*   无需记忆随时可能变化的 IP。
*   **毫秒级极速解析**：客户端底层内置了“动态轮询与提前退出”机制。一旦组播心跳捕获到该别名对应的 IP，会**立刻终止扫描并进行发送**，通常仅需 `50ms - 200ms` 的解析延迟。

---

### 2. IP:Port 直连绕过扫描 (0ms 延迟，适合自动化脚本)
在已知对方物理地址（如 `192.168.100.155:8080` 或 `[::1]:8080`）的场景下，直接传递 `IP:Port` 作为参数。
```bash
lan-share send-file --to 192.168.100.155:8080 ~/movie.mp4
```
**为什么推荐**：
*   **0ms 扫描时延**：程序检测到符合 `SocketAddr` 或 `IpAddr` 规范的直连地址时，将**完全跳过启动 UDP 监听和 2 秒扫描检测的过程**，直接建立 HTTP TCP 连接进行极速秒发。
*   适合用于内网自动化脚本（如定时日志备份、编译产物分发）。

---

### 3. 跨网卡多 IP 环境直连建议
当节点存在多块网卡（如同时开启 Wi-Fi、以太网和虚拟机 VPN 网卡）时，组播心跳会把所有可用的本地单播 IP 汇总广播给发送端。
发送端会自动提取最匹配的物理 IP。如果因特殊网络策略无法自动解析，您可以利用 `peers` 查看到的指定 IP 手动指定目标直连：
```bash
# peers 返回的 IPs 表里包含了多个地址，挑选能连通的地址直连
lan-share send-text --to 192.168.100.155:8080 "Hello"
```

---

### 4. TUN 代理（Clash / Sing-box）下的设备发现与网卡绑定
当你的设备开启了 TUN 模式的网络代理时，组播和广播包可能会被虚拟代理网卡拦截，导致无法在局域网内发现其他设备。

**解决方案**：在接收端和发送端都通过 `--bind-ip` 参数指定绑定到实际的局域网物理网卡 IP（例如 `192.168.1.5`），强制网络包通过物理网卡收发，从而绕过虚拟代理网卡。

*   **接收端（serve）绑定**：
    ```bash
    lan-share serve --bind-ip 192.168.1.5
    ```
*   **扫描端（peers）绑定**：
    ```bash
    lan-share peers --bind-ip 192.168.1.5
    ```
*   **发送端（send-text / send-file）绑定**：
    ```bash
    lan-share send-file --to archlinux --bind-ip 192.168.1.5 ~/movie.mp4
    ```

---

## 🧪 自动化测试

项目内嵌了完善的单元测试和端到端集成测试，支持对并发重名冲突、组播包多路分发进行模拟：

```bash
cargo test
```

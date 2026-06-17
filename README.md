# lan-share

`lan-share` 是一个基于 Rust 的局域网文本与文件传输工具。它通过 UDP 组播发现局域网节点，通过 HTTP 接收文本、普通文件流和分片文件上传，不依赖第三方中转服务。

项目同时提供命令行模式和内置 Web UI。命令行适合脚本、终端工作流和大文件传输；Web UI 适合在浏览器中查看节点、发送消息和拖拽文件。

## 功能概览

- 局域网自动发现：UDP 组播地址 `224.0.0.188:50001`。
- 文本发送：支持按节点名、UUID、IP、`IP:Port` 或 IPv6 目标发送。
- 文件发送：支持普通流式上传、SHA-256 校验、失败清理和重名避让。
- 分片上传：支持多连接分片上传、并发控制和按 upload id 续传。
- 批量发送：`send-files` 可并发发送多个本地文件。
- 传输选项：支持重试、zstd 压缩、进度条、取消清理等待时间。
- Web UI：提供节点列表、聊天式消息视图、文件发送、配置查看和本机配置保存。
- 网卡绑定：支持 `--bind-ip`，适用于 TUN 代理、多网卡和组播路由异常场景。

## 安装

需要 Rust 工具链。

```bash
cargo build --release
```

编译产物位于：

```text
target/release/lan-share
```

可以将它放入 `PATH`，例如：

```bash
cp target/release/lan-share /usr/local/bin/lan-share
```

开发时也可以直接使用：

```bash
cargo run -- <COMMAND>
```

## 快速开始

在接收端启动服务：

```bash
lan-share serve --name macbook --dir ~/Downloads/LAN-Share
```

在另一台设备扫描节点：

```bash
lan-share peers
```

发送文本：

```bash
lan-share send-text --to macbook "hello from another machine"
```

发送文件：

```bash
lan-share send-file --to macbook ~/Downloads/archive.zip
```

启动 Web UI：

```bash
lan-share web --name macbook --dir ~/Downloads/LAN-Share
```

命令会打印可访问地址，例如：

```text
Web 界面: http://192.168.1.5:8080
```

## 命令参考

### `serve`

启动接收服务，开放文本和文件接收接口。

```bash
lan-share serve [OPTIONS]
```

常用选项：

- `--dir <DIR>`：接收文件保存目录，默认 `./downloads`。
- `-p, --port <PORT>`：HTTP 监听端口，默认 `8080`；端口占用时自动递增。
- `-n, --name <NAME>`：当前节点名称，默认使用系统主机名。
- `--bind-ip <IP>`：绑定指定 IPv4 网卡地址。

示例：

```bash
lan-share serve --name linux-box --dir ~/Downloads/LAN-Share --port 9000
```

### `web`

启动接收服务和内置 Web UI。

```bash
lan-share web [OPTIONS]
```

选项与 `serve` 相同：

- `--dir <DIR>`
- `-p, --port <PORT>`
- `-n, --name <NAME>`
- `--bind-ip <IP>`

Web UI 支持：

- 查看在线节点。
- 按群组或单个节点发送文字。
- 上传并发送文件。
- 显示当前有效运行配置。
- 通过 loopback 本机访问时保存配置到 `~/.config/lan-share/config.toml`。

远程浏览器可以查看配置值，但配置保存接口只接受 TCP 来源为 loopback 的请求。也就是说，只有服务监听 loopback 且通过 `127.0.0.1` 或 `localhost` 打开时才能在页面内保存配置；通过局域网地址打开时配置弹窗为只读，保存按钮会被禁用。

### `peers`

扫描局域网内在线节点。

```bash
lan-share peers [OPTIONS]
```

选项：

- `--bind-ip <IP>`：从指定网卡监听组播发现包。

示例：

```bash
lan-share peers --bind-ip 192.168.1.5
```

### `send-text`

发送文本消息。

```bash
lan-share send-text --to <TARGET> [OPTIONS] <TEXT>
```

选项：

- `--to <TARGET>`：目标节点名、UUID、IP、`IP:Port` 或 IPv6。
- `-n, --name <NAME>`：发送方名称，默认使用系统主机名或配置值。
- `--bind-ip <IP>`：从指定网卡发现目标。

示例：

```bash
lan-share send-text --to macbook "build finished"
lan-share send-text --to 192.168.1.5:8080 --name ci "deploy ready"
```

### `send-file`

发送单个文件。

```bash
lan-share send-file --to <TARGET> [OPTIONS] <FILE>
```

选项：

- `--to <TARGET>`：目标节点名、UUID、IP、`IP:Port` 或 IPv6。
- `-n, --name <NAME>`：发送方名称。
- `--bind-ip <IP>`：从指定网卡发现目标。
- `--retry <N>`：失败后重试次数，默认 `0`。
- `--compress <auto|always|never>`：zstd 压缩策略，默认 `auto`。
- `--progress[=true|false]`：显示上传进度，默认 `false`。
- `--cancel-timeout <SECONDS>`：收到 Ctrl+C 后等待接收端清理的时间，默认 `10`。
- `--chunked[=true|false]`：启用分片上传，默认 `false`。
- `--chunk-size <BYTES>`：分片大小，默认 `8388608`。
- `--chunk-concurrency <N>`：分片上传并发数，默认 `4`。
- `--resume-upload-id <ID>`：继续指定 upload id 的分片上传。

示例：

```bash
lan-share send-file --to macbook ./report.pdf
lan-share send-file --to macbook --progress --retry 3 ./large.log
lan-share send-file --to macbook --chunked --chunk-concurrency 4 ./video.mkv
lan-share send-file --to 192.168.1.5:8080 --compress never ./archive.zip
```

分片上传开始时会输出 upload id。传输中断后，可使用该 id 尝试续传：

```bash
lan-share send-file --to macbook --chunked --resume-upload-id <UPLOAD_ID> ./video.mkv
```

### `send-files`

并发发送多个文件。

```bash
lan-share send-files --to <TARGET> [OPTIONS] <FILES>...
```

除不支持 `--resume-upload-id` 外，传输选项与 `send-file` 基本一致，并额外支持：

- `--concurrency <N>`：同时发送的文件数量，默认 `3`。

示例：

```bash
lan-share send-files --to macbook --concurrency 2 ./a.zip ./b.zip ./c.zip
```

## 目标解析

`--to` 支持以下形式：

- 节点名：例如 `macbook`。
- UUID：来自 `lan-share peers` 输出。
- IPv4：例如 `192.168.1.5`，未写端口时默认补 `8080`。
- IPv4 + 端口：例如 `192.168.1.5:9000`。
- IPv6：例如 `[fe80::1]:8080`。

如果目标是 IP 或 socket 地址，客户端会跳过组播扫描并直接连接。否则客户端会短暂监听组播发现结果，找到节点后选择可连通地址发送。

## Web UI 行为

Web UI 使用与 CLI 相同的接收服务和发送客户端。

- 文本发送走当前节点代发到目标节点。
- 文件发送会使用当前有效传输配置，包括 `retry`、`compress`、`chunked`、`chunk_size`、`chunk_concurrency` 和 `cancel_timeout`。
- 群组发送会向当前发现的在线节点逐个发送。
- 消息历史只保存在当前浏览器页面状态中，刷新页面后不会恢复历史。
- 配置弹窗远程可读、本机可写；保存配置后部分字段需要重启服务才会生效。

Web UI 受保护接口使用运行期 token 和同源请求信号，避免普通局域网客户端仅伪造 Fetch Metadata 头就驱动 Web 发送接口。能打开 Web UI 页面的用户仍可通过页面执行发送操作，这是 Web UI 的设计行为。

## 配置

配置优先级：

```text
CLI 参数 > 环境变量 > 配置文件 > 默认值
```

配置文件固定为：

```text
~/.config/lan-share/config.toml
```

示例：

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
chunked = true
chunk_size = 8388608
chunk_concurrency = 4
concurrency = 3
```

字段说明：

| 字段 | 作用 | 默认值 |
| --- | --- | --- |
| `download_dir` | 接收文件保存目录 | `./downloads` |
| `port` | HTTP 监听端口 | `8080` |
| `name` | 节点名和默认发送方名称 | 系统主机名 |
| `bind_ip` | 绑定的 IPv4 网卡地址 | 不指定 |
| `retry` | 文件发送失败重试次数 | `0` |
| `compress` | 文件压缩策略：`auto`、`always`、`never` | `auto` |
| `progress` | CLI 文件发送进度条 | `false` |
| `cancel_timeout` | Ctrl+C 后等待接收端清理的秒数 | `10` |
| `chunked` | 是否启用分片上传 | `false` |
| `chunk_size` | 分片大小，字节 | `8388608` |
| `chunk_concurrency` | 分片上传并发数 | `4` |
| `concurrency` | `send-files` 文件并发数 | `3` |

路径字段支持 `~`、`~/` 和 `~\` 开头，会解析为用户主目录。

环境变量：

```bash
LAN_SHARE_DIR=~/Downloads/LAN-Share
LAN_SHARE_PORT=9000
LAN_SHARE_NAME=macbook
LAN_SHARE_BIND_IP=192.168.1.5
LAN_SHARE_RETRY=3
LAN_SHARE_COMPRESS=auto
LAN_SHARE_PROGRESS=true
LAN_SHARE_CANCEL_TIMEOUT=10
LAN_SHARE_CHUNKED=true
LAN_SHARE_CHUNK_SIZE=8388608
LAN_SHARE_CHUNK_CONCURRENCY=4
LAN_SHARE_CONCURRENCY=3
```

以下内容不会写入全局配置，需要每次命令显式指定：

- `--to`
- 文本内容
- 文件路径
- 文件列表
- `--resume-upload-id`

## 安全模型

- 接收接口对局域网开放：`/api/message`、`/api/file`、`/api/file/init`、`/api/file/chunk/*`、`/api/file/complete/*`、`/api/file/cancel/*`。
- 配置接口只允许本机访问：`/api/config`。
- Web 发送接口需要 Web 页面运行期 token 和同源请求信号：`/api/runtime`、`/api/web/message`、`/api/web/file`。
- 接收文件名会被净化为 basename，避免路径穿越。
- 接收端使用 `create_new` 和重名递增策略避免覆盖已有文件。
- 文件接收支持 SHA-256 校验，校验失败会清理临时文件。

`lan-share` 面向可信局域网设计。不要把服务直接暴露到公网。

## TUN 代理和多网卡

开启 Clash、Sing-box 等 TUN 模式后，组播发现可能被虚拟网卡或路由策略影响。此时建议显式指定物理局域网 IP：

```bash
lan-share web --bind-ip 192.168.1.5
lan-share peers --bind-ip 192.168.1.5
lan-share send-file --to macbook --bind-ip 192.168.1.5 ./movie.mkv
```

如果 `peers` 能发现节点但发送失败，可以使用 `peers` 输出中的 `IP:Port` 直连：

```bash
lan-share send-text --to 192.168.1.5:9000 "hello"
```

## 排障

### 扫描不到节点

- 确认接收端正在运行 `serve` 或 `web`。
- 检查防火墙是否允许 UDP `50001` 和对应 TCP 监听端口。
- 多网卡或 TUN 代理环境下使用 `--bind-ip`。
- 确认两台设备在同一局域网或组播可达网络内。

### 能发现节点但发送失败

- 使用 `lan-share peers` 查看目标实际端口。
- 尝试使用 `IP:Port` 直连，排除名称解析和多 IP 选择问题。
- 检查接收端端口是否被防火墙拦截。
- 对大文件启用 `--retry`，必要时启用 `--chunked`。

### Web 配置无法保存

- 远程浏览器只能查看配置，不能保存配置。
- 配置保存接口要求请求来源为 loopback。请让服务监听 loopback，并通过 `http://127.0.0.1:<PORT>` 或 `http://localhost:<PORT>` 打开后保存。
- 如果 `web` 绑定到了局域网 IP，通过该局域网地址打开时仍会被视为非 loopback 访问；此时请直接编辑 `~/.config/lan-share/config.toml` 或用不指定 `--bind-ip` 的本机实例保存配置。
- 保存后，端口、绑定 IP、节点名等运行参数需要重启服务才会生效。

### 配置中的 `bind_ip` 不属于当前网卡

`web` 模式下，如果配置文件中的 `bind_ip` 已不属于当前网卡且本次没有显式传 `--bind-ip`，程序会回退到默认绑定并打印提示。需要固定网卡时，请传入当前机器实际存在的局域网 IPv4。

## 开发

运行测试：

```bash
cargo test
```

格式化：

```bash
cargo fmt
```

常用开发启动：

```bash
cargo run -- web --port 0 --name dev-node
```

`--port 0` 会让系统分配空闲端口，适合本机多实例调试。

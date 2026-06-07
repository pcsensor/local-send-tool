use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "lan-share")]
#[command(about = "A simple LAN file sharing tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the file sharing server
    Serve {
        /// Download directory for incoming files
        #[arg(long)]
        dir: Option<PathBuf>,

        /// Port to bind the HTTP server. If occupied, auto-increment to find a free port.
        #[arg(short, long)]
        port: Option<u16>,

        /// Peer name (alias) for this node
        #[arg(short, long)]
        name: Option<String>,

        /// 指定局域网网卡 IP（开启 TUN 代理时使用，例如 192.168.1.5）
        #[arg(long, value_name = "IP")]
        bind_ip: Option<String>,
    },
    /// List all discovered online peers
    Peers {
        /// 指定局域网网卡 IP（开启 TUN 代理时使用，例如 192.168.1.5）
        #[arg(long, value_name = "IP")]
        bind_ip: Option<String>,
    },
    /// Send a text message to a specific peer
    SendText {
        /// The peer name, UUID, IP, or IP:Port to send to
        #[arg(long)]
        to: String,

        /// Sender name
        #[arg(short, long)]
        name: Option<String>,

        /// The message text
        text: String,

        /// 指定局域网网卡 IP（开启 TUN 代理时使用，例如 192.168.1.5）
        #[arg(long, value_name = "IP")]
        bind_ip: Option<String>,
    },
    /// Send a file to a specific peer
    SendFile {
        /// The peer name, UUID, IP, or IP:Port to send to
        #[arg(long)]
        to: String,

        /// Sender name
        #[arg(short, long)]
        name: Option<String>,

        /// Path to the file
        file: PathBuf,

        /// 指定局域网网卡 IP（开启 TUN 代理时使用，例如 192.168.1.5）
        #[arg(long, value_name = "IP")]
        bind_ip: Option<String>,

        /// Retry failed uploads N times with exponential backoff
        #[arg(long)]
        retry: Option<usize>,

        /// Compress file payloads with zstd: auto, always, or never
        #[arg(long, value_enum)]
        compress: Option<lan_share::client::CompressionMode>,

        /// Show upload progress, speed, and ETA
        #[arg(long, num_args = 0..=1, default_missing_value = "true", require_equals = true)]
        progress: Option<bool>,

        /// Seconds to wait for graceful cancellation cleanup after Ctrl+C
        #[arg(long)]
        cancel_timeout: Option<u64>,

        /// Use chunked multi-connection upload
        #[arg(long, num_args = 0..=1, default_missing_value = "true", require_equals = true)]
        chunked: Option<bool>,

        /// Chunk size in bytes for chunked uploads
        #[arg(long)]
        chunk_size: Option<u64>,

        /// Number of concurrent chunk upload connections
        #[arg(long)]
        chunk_concurrency: Option<usize>,

        /// Resume a previous chunked upload by upload id
        #[arg(long)]
        resume_upload_id: Option<String>,
    },
    /// Send multiple files to a specific peer
    SendFiles {
        /// The peer name, UUID, IP, or IP:Port to send to
        #[arg(long)]
        to: String,

        /// Sender name
        #[arg(short, long)]
        name: Option<String>,

        /// Paths to the files
        #[arg(required = true)]
        files: Vec<PathBuf>,

        /// Number of concurrent uploads
        #[arg(long)]
        concurrency: Option<usize>,

        /// 指定局域网网卡 IP（开启 TUN 代理时使用，例如 192.168.1.5）
        #[arg(long, value_name = "IP")]
        bind_ip: Option<String>,

        /// Retry failed uploads N times with exponential backoff
        #[arg(long)]
        retry: Option<usize>,

        /// Compress file payloads with zstd: auto, always, or never
        #[arg(long, value_enum)]
        compress: Option<lan_share::client::CompressionMode>,

        /// Show upload progress, speed, and ETA
        #[arg(long, num_args = 0..=1, default_missing_value = "true", require_equals = true)]
        progress: Option<bool>,

        /// Seconds to wait for graceful cancellation cleanup after Ctrl+C
        #[arg(long)]
        cancel_timeout: Option<u64>,

        /// Use chunked multi-connection upload for each file
        #[arg(long, num_args = 0..=1, default_missing_value = "true", require_equals = true)]
        chunked: Option<bool>,

        /// Chunk size in bytes for chunked uploads
        #[arg(long)]
        chunk_size: Option<u64>,

        /// Number of concurrent chunk upload connections per file
        #[arg(long)]
        chunk_concurrency: Option<usize>,
    },
}

async fn find_available_port(start_port: u16) -> (tokio::net::TcpListener, u16) {
    let mut actual_port = start_port;
    loop {
        let addr = format!("0.0.0.0:{}", actual_port);
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

/// 将 --bind-ip 字符串解析为 Ipv4Addr，无效时打印错误并退出
fn parse_bind_ip(bind_ip: Option<&str>) -> Option<std::net::Ipv4Addr> {
    bind_ip.map(|s| {
        s.parse::<std::net::Ipv4Addr>().unwrap_or_else(|_| {
            eprintln!(
                "错误：--bind-ip '{}' 不是有效的 IPv4 地址（示例：192.168.1.5）",
                s
            );
            std::process::exit(1);
        })
    })
}

fn is_direct_address(addr: &str) -> bool {
    if addr.parse::<std::net::SocketAddr>().is_ok() || addr.parse::<std::net::IpAddr>().is_ok() {
        return true;
    }
    if addr.starts_with('[') && addr.ends_with(']') {
        let inner = &addr[1..addr.len() - 1];
        if inner.parse::<std::net::Ipv6Addr>().is_ok() {
            return true;
        }
    }
    false
}

fn fallback_address(to: &str) -> String {
    if let Ok(ip) = to.parse::<std::net::IpAddr>() {
        return match ip {
            std::net::IpAddr::V4(ip) => format!("{}:8080", ip),
            std::net::IpAddr::V6(ip) => format!("[{}]:8080", ip),
        };
    }
    if to.parse::<std::net::SocketAddr>().is_ok() {
        return to.to_string();
    }
    if to.starts_with('[') && to.ends_with(']') {
        let inner = &to[1..to.len() - 1];
        if inner.parse::<std::net::Ipv6Addr>().is_ok() {
            return format!("{}:8080", to);
        }
    }
    if !to.contains(':') {
        return format!("{}:8080", to);
    }
    to.to_string()
}

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

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let app_config = lan_share::config::AppConfig::load().unwrap_or_else(|e| {
        eprintln!("配置文件读取失败：{}", e);
        std::process::exit(1);
    });
    let env_config = lan_share::config::EnvConfig::from_env().unwrap_or_else(|e| {
        eprintln!("环境变量配置无效：{}", e);
        std::process::exit(1);
    });

    match cli.command {
        Commands::Serve {
            dir,
            port,
            name,
            bind_ip,
        } => {
            let settings = lan_share::config::resolve_serve_settings(
                lan_share::config::ConfigOverrides {
                    download_dir: dir,
                    port,
                    name,
                    bind_ip,
                    ..lan_share::config::ConfigOverrides::default()
                },
                &env_config,
                &app_config,
            );
            let (listener, actual_port) = find_available_port(settings.port).await;

            let node_name = match settings.name {
                Some(n) => n,
                None => hostname::get()
                    .ok()
                    .and_then(|h| h.into_string().ok())
                    .unwrap_or_else(|| "Unknown-Node".to_string()),
            };

            let bind_ip = parse_bind_ip(settings.bind_ip.as_deref());

            let registry = lan_share::peer::PeerRegistry::new();

            // 1. 启动后台 UDP 心跳包接收并注册在线节点
            let listener_registry = registry.clone();
            tokio::spawn(async move {
                if let Err(e) =
                    lan_share::discovery::start_listener(listener_registry, bind_ip).await
                {
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
                if let Err(e) =
                    lan_share::discovery::start_broadcaster(broadcaster_peer, bind_ip).await
                {
                    eprintln!("Broadcaster background task failed: {}", e);
                }
            });

            // 3. 运行 Axum 服务端
            let app = lan_share::server::make_router(registry, settings.download_dir);
            println!("Server node name: {}", node_name);
            println!("Server UUID: {}", peer.uuid);
            println!("Serving on http://{}", listener.local_addr().unwrap());

            if let Err(e) = axum::serve(listener, app).await {
                eprintln!("Server exited with error: {}", e);
            }
        }
        Commands::Peers { bind_ip } => {
            let settings = lan_share::config::resolve_send_settings(
                lan_share::config::ConfigOverrides {
                    bind_ip,
                    ..lan_share::config::ConfigOverrides::default()
                },
                &env_config,
                &app_config,
            );
            let bind_ip = parse_bind_ip(settings.bind_ip.as_deref());
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
        Commands::SendText {
            to,
            name,
            text,
            bind_ip,
        } => {
            let settings = lan_share::config::resolve_send_settings(
                lan_share::config::ConfigOverrides {
                    name,
                    bind_ip,
                    ..lan_share::config::ConfigOverrides::default()
                },
                &env_config,
                &app_config,
            );
            let bind_ip = parse_bind_ip(settings.bind_ip.as_deref());
            let dest_addr = resolve_destination(&to, bind_ip).await;

            let sender_name = match settings.name {
                Some(n) => n,
                None => hostname::get()
                    .ok()
                    .and_then(|h| h.into_string().ok())
                    .unwrap_or_else(|| "Unknown-Sender".to_string()),
            };

            println!(
                "Sending text message to {} as '{}'...",
                dest_addr, sender_name
            );
            match lan_share::client::send_text(&dest_addr, &sender_name, &text).await {
                Ok(_) => println!("Text sent successfully!"),
                Err(e) => {
                    eprintln!("Failed to send text message: {}", e);
                    std::process::exit(1);
                }
            }
        }
        Commands::SendFile {
            to,
            name,
            file,
            bind_ip,
            retry,
            compress,
            progress,
            cancel_timeout,
            chunked,
            chunk_size,
            chunk_concurrency,
            resume_upload_id,
        } => {
            if !file.exists() {
                eprintln!("Error: File '{}' does not exist.", file.display());
                std::process::exit(1);
            }

            let settings = lan_share::config::resolve_send_settings(
                lan_share::config::ConfigOverrides {
                    name,
                    bind_ip,
                    retry,
                    compress,
                    progress,
                    cancel_timeout,
                    chunked,
                    chunk_size,
                    chunk_concurrency,
                    ..lan_share::config::ConfigOverrides::default()
                },
                &env_config,
                &app_config,
            );
            let bind_ip = parse_bind_ip(settings.bind_ip.as_deref());
            let dest_addr = resolve_destination(&to, bind_ip).await;

            let sender_name = match settings.name {
                Some(n) => n,
                None => hostname::get()
                    .ok()
                    .and_then(|h| h.into_string().ok())
                    .unwrap_or_else(|| "Unknown-Sender".to_string()),
            };

            let options = file_send_options(FileSendCliOptions {
                retry_attempts: settings.retry_attempts,
                compression: settings.compression,
                progress: settings.progress,
                cancel_timeout_secs: settings.cancel_timeout,
                use_chunked: settings.chunked,
                chunk_size: settings.chunk_size,
                chunk_concurrency: settings.chunk_concurrency,
                resume_upload_id,
            });
            println!(
                "Sending file '{}' to {} as '{}'...",
                file.display(),
                dest_addr,
                sender_name
            );
            let cancel_timeout = options.cancel_timeout;
            tokio::select! {
                result = lan_share::client::send_file_with_options(&dest_addr, &sender_name, &file, options) => {
                    match result {
                        Ok(_) => println!("File sent successfully!"),
                        Err(e) => {
                            eprintln!("Failed to send file: {}", e);
                            std::process::exit(1);
                        }
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    eprintln!(
                        "Upload canceled. Connection closed; receiver will clean partial files within {:?}.",
                        cancel_timeout
                    );
                    std::process::exit(130);
                }
            }
        }
        Commands::SendFiles {
            to,
            name,
            files,
            concurrency,
            bind_ip,
            retry,
            compress,
            progress,
            cancel_timeout,
            chunked,
            chunk_size,
            chunk_concurrency,
        } => {
            for file in &files {
                if !file.exists() {
                    eprintln!("Error: File '{}' does not exist.", file.display());
                    std::process::exit(1);
                }
            }

            let settings = lan_share::config::resolve_send_settings(
                lan_share::config::ConfigOverrides {
                    name,
                    bind_ip,
                    retry,
                    compress,
                    progress,
                    cancel_timeout,
                    chunked,
                    chunk_size,
                    chunk_concurrency,
                    concurrency,
                    ..lan_share::config::ConfigOverrides::default()
                },
                &env_config,
                &app_config,
            );
            let bind_ip = parse_bind_ip(settings.bind_ip.as_deref());
            let dest_addr = resolve_destination(&to, bind_ip).await;

            let sender_name = match settings.name {
                Some(n) => n,
                None => hostname::get()
                    .ok()
                    .and_then(|h| h.into_string().ok())
                    .unwrap_or_else(|| "Unknown-Sender".to_string()),
            };

            let options = file_send_options(FileSendCliOptions {
                retry_attempts: settings.retry_attempts,
                compression: settings.compression,
                progress: settings.progress,
                cancel_timeout_secs: settings.cancel_timeout,
                use_chunked: settings.chunked,
                chunk_size: settings.chunk_size,
                chunk_concurrency: settings.chunk_concurrency,
                resume_upload_id: None,
            });
            let cancel_timeout = options.cancel_timeout;
            println!(
                "Sending {} files to {} as '{}' with concurrency {}...",
                files.len(),
                dest_addr,
                sender_name,
                settings.concurrency
            );
            tokio::select! {
                result = lan_share::client::send_files(&dest_addr, &sender_name, &files, settings.concurrency, options) => {
                    match result {
                        Ok(_) => println!("Files sent successfully!"),
                        Err(e) => {
                            eprintln!("Failed to send files: {}", e);
                            std::process::exit(1);
                        }
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    eprintln!(
                        "Uploads canceled. Connections closed; receiver will clean partial files within {:?}.",
                        cancel_timeout
                    );
                    std::process::exit(130);
                }
            }
        }
    }
}

struct FileSendCliOptions {
    retry_attempts: usize,
    compression: lan_share::client::CompressionMode,
    progress: bool,
    cancel_timeout_secs: u64,
    use_chunked: bool,
    chunk_size: u64,
    chunk_concurrency: usize,
    resume_upload_id: Option<String>,
}

fn file_send_options(cli: FileSendCliOptions) -> lan_share::client::FileSendOptions {
    let mut options = lan_share::client::FileSendOptions {
        retry_attempts: cli.retry_attempts,
        compression: cli.compression,
        cancel_timeout: Duration::from_secs(cli.cancel_timeout_secs),
        use_chunked: cli.use_chunked,
        chunk_size: cli.chunk_size,
        chunk_concurrency: cli.chunk_concurrency,
        resume_upload_id: cli.resume_upload_id,
        ..lan_share::client::FileSendOptions::default()
    };
    if cli.progress {
        options.progress = lan_share::client::ProgressMode::Indicatif;
    }
    options
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_direct_address() {
        assert!(is_direct_address("127.0.0.1:8080"));
        assert!(is_direct_address("[::1]:8080"));
        assert!(is_direct_address("127.0.0.1"));
        assert!(is_direct_address("::1"));
        assert!(is_direct_address("[::1]"));
        assert!(!is_direct_address("localhost"));
        assert!(!is_direct_address("archlinux"));
    }

    #[test]
    fn test_fallback_address() {
        assert_eq!(fallback_address("127.0.0.1"), "127.0.0.1:8080");
        assert_eq!(fallback_address("archlinux"), "archlinux:8080");
        assert_eq!(fallback_address("127.0.0.1:9000"), "127.0.0.1:9000");
        assert_eq!(fallback_address("example.com:9000"), "example.com:9000");
        assert_eq!(fallback_address("::1"), "[::1]:8080");
        assert_eq!(fallback_address("[::1]"), "[::1]:8080");
    }

    #[test]
    fn test_send_files_cli_parse() {
        let cli = Cli::try_parse_from([
            "lan-share",
            "send-files",
            "--to",
            "127.0.0.1:8080",
            "--concurrency",
            "2",
            "--retry",
            "3",
            "--compress",
            "always",
            "--progress",
            "--cancel-timeout",
            "12",
            "--chunked",
            "--chunk-size",
            "4096",
            "--chunk-concurrency",
            "2",
            "a.txt",
            "b.txt",
        ])
        .unwrap();

        match cli.command {
            Commands::SendFiles {
                to,
                files,
                concurrency,
                retry,
                compress,
                progress,
                cancel_timeout,
                chunked,
                chunk_size,
                chunk_concurrency,
                ..
            } => {
                assert_eq!(to, "127.0.0.1:8080");
                assert_eq!(files, vec![PathBuf::from("a.txt"), PathBuf::from("b.txt")]);
                assert_eq!(concurrency, Some(2));
                assert_eq!(retry, Some(3));
                assert_eq!(compress, Some(lan_share::client::CompressionMode::Always));
                assert_eq!(progress, Some(true));
                assert_eq!(cancel_timeout, Some(12));
                assert_eq!(chunked, Some(true));
                assert_eq!(chunk_size, Some(4096));
                assert_eq!(chunk_concurrency, Some(2));
            }
            _ => panic!("expected send-files command"),
        }
    }

    #[test]
    fn test_send_files_progress_flag_does_not_consume_file_path() {
        let cli = Cli::try_parse_from([
            "lan-share",
            "send-files",
            "--to",
            "127.0.0.1:8080",
            "--progress",
            "a.txt",
        ])
        .unwrap();

        match cli.command {
            Commands::SendFiles {
                files, progress, ..
            } => {
                assert_eq!(progress, Some(true));
                assert_eq!(files, vec![PathBuf::from("a.txt")]);
            }
            _ => panic!("expected send-files command"),
        }
    }

    #[tokio::test]
    async fn test_resolve_destination_direct() {
        assert_eq!(
            resolve_destination("127.0.0.1:9000", None).await,
            "127.0.0.1:9000"
        );
        assert_eq!(
            resolve_destination("127.0.0.1", None).await,
            "127.0.0.1:8080"
        );
    }
}

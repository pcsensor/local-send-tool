use clap::{Parser, Subcommand};
use std::path::PathBuf;

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
        #[arg(long, default_value = "./downloads")]
        dir: PathBuf,

        /// Port to bind the HTTP server. If occupied, auto-increment to find a free port.
        #[arg(short, long, default_value_t = 8080)]
        port: u16,

        /// Peer name (alias) for this node
        #[arg(short, long)]
        name: Option<String>,
    },
    /// List all discovered online peers
    Peers,
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

async fn resolve_destination(to: &str) -> String {
    if is_direct_address(to) {
        return fallback_address(to);
    }

    let registry = lan_share::peer::PeerRegistry::new();
    let listener_registry = registry.clone();

    // 启动接收
    let listen_handle = tokio::spawn(async move {
        let _ = lan_share::discovery::start_listener(listener_registry).await;
    });

    println!("Scanning for target '{}' in local network...", to);

    let mut found_peer = None;
    // 每 50ms 轮询一次，最多等待 2.0 秒（40次）
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
    match cli.command {
        Commands::Serve { dir, port, name } => {
            let (listener, actual_port) = find_available_port(port).await;

            let node_name = match name {
                Some(n) => n,
                None => hostname::get()
                    .ok()
                    .and_then(|h| h.into_string().ok())
                    .unwrap_or_else(|| "Unknown-Node".to_string()),
            };

            let registry = lan_share::peer::PeerRegistry::new();

            // 1. 启动后台 UDP 心跳包接收并注册在线节点
            let listener_registry = registry.clone();
            tokio::spawn(async move {
                if let Err(e) = lan_share::discovery::start_listener(listener_registry).await {
                    eprintln!("Listener background task failed: {}", e);
                }
            });

            // 2. 启动后台 UDP 心跳包广播
            let peer = lan_share::peer::Peer {
                uuid: uuid::Uuid::new_v4().to_string(),
                name: node_name.clone(),
                port: actual_port,
                ips: lan_share::discovery::get_local_ips(),
            };
            let broadcaster_peer = peer.clone();
            tokio::spawn(async move {
                if let Err(e) = lan_share::discovery::start_broadcaster(broadcaster_peer).await {
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
        Commands::Peers => {
            let registry = lan_share::peer::PeerRegistry::new();
            let listener_registry = registry.clone();

            // 启动 UDP 接收监听
            let listen_handle = tokio::spawn(async move {
                let _ = lan_share::discovery::start_listener(listener_registry).await;
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
        Commands::SendText { to, name, text } => {
            let dest_addr = resolve_destination(&to).await;

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
        Commands::SendFile { to, name, file } => {
            if !file.exists() {
                eprintln!("Error: File '{}' does not exist.", file.display());
                std::process::exit(1);
            }

            let dest_addr = resolve_destination(&to).await;

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
    }
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

    #[tokio::test]
    async fn test_resolve_destination_direct() {
        assert_eq!(resolve_destination("127.0.0.1:9000").await, "127.0.0.1:9000");
        assert_eq!(resolve_destination("127.0.0.1").await, "127.0.0.1:8080");
    }
}


pub mod client;
pub mod config;
pub mod discovery;
pub mod peer;
pub mod server;
pub mod tui;
pub mod web_ui;

/// Find an available TCP port starting from `start_port`.
/// Returns the bound listener and the actual port number.
pub async fn find_available_port(
    bind_ip: Option<std::net::Ipv4Addr>,
    start_port: u16,
) -> std::io::Result<(tokio::net::TcpListener, u16)> {
    let mut actual_port = start_port;
    let ip = bind_ip.unwrap_or(std::net::Ipv4Addr::UNSPECIFIED);
    loop {
        let addr = std::net::SocketAddr::from((ip, actual_port));
        match tokio::net::TcpListener::bind(&addr).await {
            Ok(listener) => {
                let port = listener
                    .local_addr()
                    .map(|a| a.port())
                    .unwrap_or(actual_port);
                return Ok((listener, port));
            }
            Err(e) => {
                if start_port == 0 || e.kind() != std::io::ErrorKind::AddrInUse {
                    return Err(e);
                }
                if actual_port == u16::MAX {
                    return Err(e);
                }
                actual_port += 1;
            }
        }
    }
}

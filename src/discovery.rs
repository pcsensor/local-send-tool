use crate::peer::{Peer, PeerRegistry};
use local_ip_address::local_ip;
use socket2::{Domain, Protocol, Socket, Type};
use std::net::{Ipv4Addr, SocketAddr, UdpSocket as StdUdpSocket};
use std::time::Duration;
use tokio::net::UdpSocket;

pub const MULTICAST_ADDR: Ipv4Addr = Ipv4Addr::new(224, 0, 0, 188);
pub const MULTICAST_PORT: u16 = 50001;

pub async fn broadcast_once(peer: &Peer) -> std::io::Result<()> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    let payload = serde_json::to_vec(peer)?;
    let target_addr: SocketAddr = format!("{}:{}", MULTICAST_ADDR, MULTICAST_PORT)
        .parse()
        .unwrap();
    socket.send_to(&payload, target_addr).await?;
    Ok(())
}

pub async fn start_broadcaster(peer: Peer) -> std::io::Result<()> {
    loop {
        if let Err(e) = broadcast_once(&peer).await {
            eprintln!("Broadcaster error: {}", e);
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

fn create_multicast_socket() -> std::io::Result<StdUdpSocket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    #[cfg(not(windows))]
    socket.set_reuse_port(true)?;

    #[cfg(windows)]
    let bind_addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, MULTICAST_PORT));
    #[cfg(not(windows))]
    let bind_addr = SocketAddr::from((MULTICAST_ADDR, MULTICAST_PORT));

    socket.bind(&bind_addr.into())?;
    socket.join_multicast_v4(&MULTICAST_ADDR, &Ipv4Addr::UNSPECIFIED)?;
    Ok(StdUdpSocket::from(socket))
}

pub async fn start_listener(registry: PeerRegistry) -> std::io::Result<()> {
    let std_socket = create_multicast_socket()?;
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

pub fn get_local_ips() -> Vec<String> {
    if let Ok(ip) = local_ip() {
        vec![ip.to_string()]
    } else {
        vec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::PeerRegistry;
    use std::time::Duration;

    #[tokio::test]
    async fn test_multicast_discovery() {
        let registry = PeerRegistry::new();
        let peer = Peer {
            uuid: "test-uuid-123".to_string(),
            name: "tester".to_string(),
            port: 9999,
            ips: vec!["127.0.0.1".to_string()],
        };

        // 开启接收监听
        let rx_registry = registry.clone();
        let join_handle = tokio::spawn(async move {
            start_listener(rx_registry).await.unwrap();
        });

        // 延迟后发送广播
        tokio::time::sleep(Duration::from_millis(200)).await;
        broadcast_once(&peer).await.unwrap();

        // 轮询判定 + 超时机制，代替 sleep(500ms)
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(2);
        let mut list = Vec::new();
        while start.elapsed() < timeout {
            list = registry.list();
            if !list.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert!(
            !list.is_empty(),
            "Peers list should not be empty after timeout"
        );
        assert_eq!(list[0].uuid, "test-uuid-123");

        join_handle.abort();
    }
}

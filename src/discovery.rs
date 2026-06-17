use crate::peer::{Peer, PeerRegistry};
use local_ip_address::local_ip;
use socket2::{Domain, Protocol, Socket, Type};
use std::net::{Ipv4Addr, SocketAddr, UdpSocket as StdUdpSocket};
use std::time::Duration;
use tokio::net::UdpSocket;

pub const MULTICAST_ADDR: Ipv4Addr = Ipv4Addr::new(224, 0, 0, 188);
pub const MULTICAST_PORT: u16 = 50001;

pub async fn broadcast_once(peer: &Peer, bind_ip: Option<Ipv4Addr>) -> std::io::Result<()> {
    let bind_addr: std::net::SocketAddr =
        std::net::SocketAddr::from((bind_ip.unwrap_or(Ipv4Addr::UNSPECIFIED), 0u16));
    let socket = UdpSocket::bind(bind_addr).await?;
    let payload = serde_json::to_vec(peer)?;
    let target_addr: SocketAddr = format!("{}:{}", MULTICAST_ADDR, MULTICAST_PORT)
        .parse()
        .unwrap();
    socket.send_to(&payload, target_addr).await?;
    Ok(())
}

pub async fn start_broadcaster(peer: Peer, bind_ip: Option<Ipv4Addr>) -> std::io::Result<()> {
    loop {
        if let Err(e) = broadcast_once(&peer, bind_ip).await {
            eprintln!("Broadcaster error: {}", e);
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn create_multicast_socket(bind_ip: Option<Ipv4Addr>) -> std::io::Result<StdUdpSocket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    #[cfg(not(windows))]
    socket.set_reuse_port(true)?;

    #[cfg(any(windows, target_os = "macos"))]
    let bind_addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, MULTICAST_PORT));
    #[cfg(not(any(windows, target_os = "macos")))]
    let bind_addr = SocketAddr::from((MULTICAST_ADDR, MULTICAST_PORT));

    socket.bind(&bind_addr.into())?;

    let iface = match bind_ip {
        Some(ip) => ip,
        None => auto_multicast_iface()?,
    };
    socket.join_multicast_v4(&MULTICAST_ADDR, &iface)?;
    Ok(StdUdpSocket::from(socket))
}

fn auto_multicast_iface() -> std::io::Result<Ipv4Addr> {
    if let Ok(std::net::IpAddr::V4(v4)) = local_ip() {
        return Ok(v4);
    }
    if let Ok(interfaces) = local_ip_address::list_afinet_netifas() {
        for (_, ip) in interfaces {
            if ip.is_ipv4() && !ip.is_loopback() {
                if let std::net::IpAddr::V4(v4) = ip {
                    return Ok(v4);
                }
            }
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AddrNotAvailable,
        "无法自动确定组播网卡 IP，请通过 --bind-ip 指定",
    ))
}

pub async fn start_listener(
    registry: PeerRegistry,
    bind_ip: Option<Ipv4Addr>,
) -> std::io::Result<()> {
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

pub fn get_local_ips(bind_ip: Option<Ipv4Addr>) -> Vec<String> {
    if let Some(ip) = bind_ip {
        return vec![ip.to_string()];
    }

    // The IP of the interface backing the default route. We advertise this
    // first so peers prefer the routable LAN address over VPN/tunnel or
    // virtual-adapter addresses (e.g. utunN 10.x), which are usually not
    // reachable from other machines.
    let primary = match local_ip() {
        Ok(std::net::IpAddr::V4(v4)) if !v4.is_loopback() && !v4.is_link_local() => {
            Some(v4.to_string())
        }
        _ => None,
    };

    let others: Vec<String> = if let Ok(interfaces) = local_ip_address::list_afinet_netifas() {
        interfaces
            .into_iter()
            .filter_map(|(_, ip)| match ip {
                // Drop loopback and link-local (169.254.0.0/16) addresses:
                // neither is reachable by remote peers.
                std::net::IpAddr::V4(v4) if !v4.is_loopback() && !v4.is_link_local() => {
                    Some(v4.to_string())
                }
                _ => None,
            })
            .collect()
    } else {
        Vec::new()
    };

    let mut ips = order_local_ips(primary, others);
    if ips.is_empty() {
        if let Ok(std::net::IpAddr::V4(v4)) = local_ip() {
            ips.push(v4.to_string());
        }
    }
    ips
}

/// Build the advertised address list: the primary (default-route) address
/// first, followed by the remaining addresses sorted and de-duplicated.
fn order_local_ips(primary: Option<String>, mut others: Vec<String>) -> Vec<String> {
    others.sort();
    others.dedup();
    let mut ips = Vec::with_capacity(others.len() + 1);
    if let Some(primary) = primary {
        others.retain(|ip| ip != &primary);
        ips.push(primary);
    }
    ips.extend(others);
    ips
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::PeerRegistry;
    use std::time::Duration;

    #[tokio::test]
    #[ignore = "requires a working UDP multicast loopback environment"]
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
            start_listener(rx_registry, None).await.unwrap();
        });

        // 延迟后发送广播
        tokio::time::sleep(Duration::from_millis(200)).await;
        broadcast_once(&peer, None).await.unwrap();

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

    #[test]
    fn test_get_local_ips_with_bind_ip() {
        let ip = Ipv4Addr::new(192, 168, 1, 5);
        let ips = get_local_ips(Some(ip));
        assert_eq!(ips, vec!["192.168.1.5".to_string()]);
    }

    #[test]
    fn test_get_local_ips_without_bind_ip() {
        let ips = get_local_ips(None);
        if !ips.is_empty() {
            for ip in &ips {
                assert_ne!(
                    ip, "127.0.0.1",
                    "获取到的局域网 IP 列表中不应包含 Loopback 环回地址"
                );
            }
        }
    }

    #[test]
    fn test_order_local_ips_puts_primary_first() {
        let ips = order_local_ips(
            Some("192.168.100.157".to_string()),
            vec!["10.20.0.1".to_string(), "192.168.100.157".to_string()],
        );
        assert_eq!(
            ips,
            vec!["192.168.100.157".to_string(), "10.20.0.1".to_string()],
            "默认路由地址应排在首位，避免对端优先选择不可达的 VPN/隧道地址"
        );
    }

    #[test]
    fn test_order_local_ips_without_primary_is_sorted_unique() {
        let ips = order_local_ips(
            None,
            vec![
                "192.168.1.5".to_string(),
                "10.0.0.1".to_string(),
                "10.0.0.1".to_string(),
            ],
        );
        assert_eq!(
            ips,
            vec!["10.0.0.1".to_string(), "192.168.1.5".to_string()]
        );
    }

}

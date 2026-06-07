use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Peer {
    pub uuid: String,
    pub name: String,
    pub port: u16,
    pub ips: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub peer: Peer,
    pub last_seen: Instant,
}

#[derive(Clone, Default)]
pub struct PeerRegistry {
    peers: Arc<RwLock<HashMap<String, PeerInfo>>>,
}

impl PeerRegistry {
    pub fn new() -> Self {
        Self {
            peers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn register(&self, peer: Peer) {
        let mut map = self.peers.write().unwrap();
        map.insert(
            peer.uuid.clone(),
            PeerInfo {
                peer,
                last_seen: Instant::now(),
            },
        );
    }

    pub fn clean_stale(&self, timeout: Duration) {
        let mut map = self.peers.write().unwrap();
        let now = Instant::now();
        map.retain(|_, info| now.duration_since(info.last_seen) < timeout);
    }

    pub fn list(&self) -> Vec<Peer> {
        let map = self.peers.read().unwrap();
        map.values().map(|info| info.peer.clone()).collect()
    }

    pub fn find_by_name_or_ip(&self, target: &str) -> Option<Peer> {
        let map = self.peers.read().unwrap();
        // 1. 尝试匹配 UUID
        if let Some(info) = map.get(target) {
            return Some(info.peer.clone());
        }
        // 2. 尝试精确匹配名称，或者 IP / IP:Port
        for info in map.values() {
            if info.peer.name == target {
                return Some(info.peer.clone());
            }
            for ip in &info.peer.ips {
                if ip == target || format!("{}:{}", ip, info.peer.port) == target {
                    return Some(info.peer.clone());
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_peer_serialization() {
        let peer = Peer {
            uuid: "f47ac10b-58cc-4372-a567-0e02b2c3d479".to_string(),
            name: "test-node".to_string(),
            port: 8080,
            ips: vec!["192.168.1.100".to_string()],
        };
        let serialized = serde_json::to_string(&peer).unwrap();
        let deserialized: Peer = serde_json::from_str(&serialized).unwrap();
        assert_eq!(peer.uuid, deserialized.uuid);
        assert_eq!(peer.name, deserialized.name);
        assert_eq!(peer.port, deserialized.port);
        assert_eq!(peer.ips, deserialized.ips);
    }

    #[test]
    fn test_peer_registry_register_and_list() {
        let registry = PeerRegistry::new();
        let peer = Peer {
            uuid: "peer-1".to_string(),
            name: "node-1".to_string(),
            port: 8080,
            ips: vec!["192.168.1.100".to_string()],
        };
        registry.register(peer.clone());
        let list = registry.list();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0], peer);
    }

    #[test]
    fn test_peer_registry_clean_stale() {
        let registry = PeerRegistry::new();
        let peer = Peer {
            uuid: "peer-1".to_string(),
            name: "node-1".to_string(),
            port: 8080,
            ips: vec!["192.168.1.100".to_string()],
        };
        registry.register(peer);
        assert_eq!(registry.list().len(), 1);

        // Sleep for a tiny duration so that duration_since(last_seen) is positive.
        thread::sleep(Duration::from_millis(5));

        // Clean stale with a duration of 1ms, it should clean the peer because it's been 5ms.
        registry.clean_stale(Duration::from_millis(1));
        assert_eq!(registry.list().len(), 0);
    }

    #[test]
    fn test_peer_registry_find_by_name_or_ip() {
        let registry = PeerRegistry::new();
        let peer = Peer {
            uuid: "peer-1-uuid".to_string(),
            name: "node-1".to_string(),
            port: 8080,
            ips: vec!["192.168.1.100".to_string(), "10.0.0.1".to_string()],
        };
        registry.register(peer.clone());

        // Find by UUID
        assert_eq!(
            registry.find_by_name_or_ip("peer-1-uuid"),
            Some(peer.clone())
        );

        // Find by Name
        assert_eq!(registry.find_by_name_or_ip("node-1"), Some(peer.clone()));

        // Find by IP
        assert_eq!(
            registry.find_by_name_or_ip("192.168.1.100"),
            Some(peer.clone())
        );
        assert_eq!(registry.find_by_name_or_ip("10.0.0.1"), Some(peer.clone()));

        // Find by IP:Port
        assert_eq!(
            registry.find_by_name_or_ip("192.168.1.100:8080"),
            Some(peer.clone())
        );

        // Non-existent
        assert_eq!(registry.find_by_name_or_ip("non-existent"), None);
    }
}

use lan_share::peer::Peer;

fn main() {
    let _peer = Peer {
        uuid: "dummy".to_string(),
        name: "dummy".to_string(),
        port: 0,
        ips: vec![],
    };
    println!("Hello, lan-share!");
}

//! Comm cores finding each other: what is found, what is trusted, and what happens then.

use std::cell::RefCell;
use std::net::{IpAddr, SocketAddr};
use std::rc::Rc;
use std::time::Duration;

use orion_discovery::{
    Advertisement, Announcement, Discovery, DiscoveryConfig, MemoryDiscoveryBus, PeerEvent,
    PeerState,
};
use orion_keys::{Identity, Keys, KeysConfig, KeysService, SharedKeys};
use orion_msg_session::{Hello, Roles};
use orion_server::{Comm, Limits, Principal, Security};
use orion_tcp::{TcpCarrier, TcpOptions};
use orion_types::NodeName;
use orion_wire::{INTERFACE_REVISION, PROTOCOL_VERSION, encode_body, frame, kind};

const LOOPBACK: IpAddr = IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
const TTL: Duration = Duration::from_secs(120);

fn run(paused: bool, test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(paused)
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, test);
}

/// One comm core: its keys, its network way in, and discovery on `bus`.
struct Node {
    name: NodeName,
    keys: SharedKeys,
    comm: Comm,
    options: TcpOptions,
    addr: SocketAddr,
    discovery: Discovery,
    events: Rc<RefCell<Vec<PeerEvent>>>,
}

impl Node {
    async fn start(
        name: &str,
        cluster: &str,
        enrollment_key: Option<&str>,
        bus: &MemoryDiscoveryBus,
    ) -> Self {
        let name: NodeName = name.parse().expect("name");
        let config = KeysConfig {
            enrollment_key: enrollment_key.map(|key| key.as_bytes().to_vec()),
            ..KeysConfig::in_memory(cluster)
        };
        let keys = SharedKeys::new(Keys::open(config, Security::On).expect("keys"));
        let options = TcpOptions {
            security: Security::On,
            keys: keys.clone(),
            max_frame: 64 * 1024,
            handshake: Duration::from_secs(5),
        };
        let comm = Comm::new(
            name.clone(),
            Security::On,
            Some(Rc::new(keys.clone())),
            Limits::default(),
        );
        comm.add_service(KeysService::new(name.clone(), keys.clone()));
        let carrier = TcpCarrier::bind(SocketAddr::new(LOOPBACK, 0), options.clone())
            .await
            .expect("bind");
        let addr = carrier.local_addr().expect("addr");
        let serving = comm.clone();
        tokio::task::spawn_local(async move { serving.serve(carrier).await });
        let events = Rc::new(RefCell::new(Vec::new()));
        let seen = events.clone();
        let discovery = Discovery::start(
            DiscoveryConfig {
                node: name.clone(),
                port: addr.port(),
                addresses: vec![LOOPBACK],
                ttl: TTL,
                tcp: options.clone(),
            },
            Box::new(bus.backend()),
            move |event| seen.borrow_mut().push(event),
        )
        .expect("discovery");
        Self {
            name,
            keys,
            comm,
            options,
            addr,
            discovery,
            events,
        }
    }

    fn state_of(&self, peer: &Node) -> Option<PeerState> {
        self.discovery
            .peers()
            .into_iter()
            .find(|seen| seen.node == peer.name)
            .map(|seen| seen.state)
    }

    fn public(&self) -> [u8; 32] {
        self.keys.lock().identity().public()
    }
}

async fn until(what: &str, mut done: impl FnMut() -> bool) {
    for _ in 0..3000 {
        if done() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("never happened: {what}");
}

async fn settle() {
    tokio::time::sleep(Duration::from_millis(5)).await;
}

#[test]
fn two_comm_cores_find_each_other_and_enroll_with_the_shared_key() {
    run(false, async {
        let bus = MemoryDiscoveryBus::new();
        let key = "a shared enrollment key of thirty-two bytes!";
        let a = Node::start("node-a", "lab", Some(key), &bus).await;
        let b = Node::start("node-b", "lab", Some(key), &bus).await;
        until("each has enrolled the other", || {
            a.state_of(&b) == Some(PeerState::Enrolled)
                && b.state_of(&a) == Some(PeerState::Enrolled)
        })
        .await;
        // Each holds exactly the key the other has, not merely the one it announced.
        assert_eq!(a.keys.lock().peer_key(&b.name), Some(&b.public()));
        assert_eq!(b.keys.lock().peer_key(&a.name), Some(&a.public()));
        // And each was told where the other is, once.
        until("both were told", || {
            !a.events.borrow().is_empty() && !b.events.borrow().is_empty()
        })
        .await;
        let told = a.events.borrow().clone();
        let [PeerEvent::Present(peer)] = told.as_slice() else {
            panic!("{told:?}");
        };
        assert_eq!(
            (&peer.node, peer.addrs.as_slice(), peer.public_key),
            (&b.name, [b.addr].as_slice(), b.public())
        );
        assert!(peer.fingerprint.starts_with("sha256:"));

        // That is enough to connect as a peer: a signed session, known by name.
        let mut connection = orion_tcp::connect(peer.addrs[0], &a.options, Some(peer.public_key))
            .await
            .expect("connect");
        let hello = Hello {
            name: "node-a",
            roles: Roles::PEER,
            max_frame: 4096,
            revision: INTERFACE_REVISION,
            level: 0,
        };
        let mut out = [0_u8; 256];
        let len = encode_body(&hello, 1, &mut out).expect("encode");
        connection.send(&out[..len]).await.expect("send");
        let mut buf = [0_u8; 4096];
        let len = connection
            .recv(&mut buf)
            .await
            .expect("recv")
            .expect("open");
        assert_eq!(
            frame::decode(&buf[..len]).expect("frame").kind(),
            kind::WELCOME
        );
        let as_peer = b.comm.with(|sessions, _| {
            sessions
                .iter()
                .any(|session| session.open && session.principal == Principal::Peer(a.name.clone()))
        });
        assert!(as_peer, "node-b knows the caller as its peer node-a");

        // A third node without the key is found by both and trusted by neither.
        let c = Node::start("node-c", "lab", None, &bus).await;
        until("the stranger is seen", || {
            a.state_of(&c).is_some() && b.state_of(&c).is_some()
        })
        .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            (a.state_of(&c), b.state_of(&c)),
            (Some(PeerState::Discovered), Some(PeerState::Discovered))
        );
        assert_eq!(c.state_of(&a), Some(PeerState::Discovered));
        assert_eq!(a.keys.lock().peer_key(&c.name), None);
    });
}

#[test]
fn a_node_with_another_key_is_never_enrolled() {
    run(false, async {
        let bus = MemoryDiscoveryBus::new();
        let a = Node::start(
            "node-a",
            "lab",
            Some("the cluster's own enrollment key, 32 b"),
            &bus,
        )
        .await;
        let b = Node::start(
            "node-b",
            "lab",
            Some("some other key that is not the cluster's"),
            &bus,
        )
        .await;
        until("both tried and failed", || {
            let failed = |node: &Node| {
                node.discovery
                    .peers()
                    .iter()
                    .any(|peer| peer.last_enrollment_error.is_some())
            };
            failed(&a) && failed(&b)
        })
        .await;
        assert_eq!(
            (a.state_of(&b), b.state_of(&a)),
            (Some(PeerState::Discovered), Some(PeerState::Discovered))
        );
        assert_eq!(a.keys.lock().peer_key(&b.name), None);
        assert_eq!(b.keys.lock().peer_key(&a.name), None);
        assert!(a.events.borrow().is_empty() && b.events.borrow().is_empty());
        let error = a.discovery.peers()[0]
            .last_enrollment_error
            .clone()
            .expect("error");
        assert!(
            error.contains("refused") || error.contains("did not prove"),
            "{error}"
        );
    });
}

fn announcement(
    node: &str,
    cluster: &str,
    key: [u8; 32],
    wire: u8,
    addr: &str,
    port: u16,
) -> Announcement {
    let advert = Advertisement {
        node: node.parse().expect("name"),
        cluster: cluster.into(),
        public_key: key,
        wire,
        revision: 1,
        port,
    };
    Announcement {
        instance: format!("{node}._orion._tcp.local."),
        addresses: vec![addr.parse().expect("ip")],
        port,
        txt: advert.txt(),
        ttl: None,
    }
}

#[test]
fn finding_a_peer_is_not_trusting_it() {
    run(true, async {
        let bus = MemoryDiscoveryBus::new();
        // No shared key: only a person enrolls.
        let a = Node::start("node-a", "lab", None, &bus).await;
        let b_key = Identity::generate().public();
        let peer = |state| {
            let peers = a.discovery.peers();
            assert_eq!(peers.len(), 1, "{peers:?}");
            assert_eq!(peers[0].state, state);
            peers[0].clone()
        };

        // What is not a peer of this cluster is not listed at all.
        bus.inject(announcement(
            "node-x",
            "another-cluster",
            [1; 32],
            PROTOCOL_VERSION,
            "10.0.0.9",
            9200,
        ));
        bus.inject(announcement(
            "node-a",
            "lab",
            [2; 32],
            PROTOCOL_VERSION,
            "10.0.0.8",
            9200,
        ));
        let mut broken = announcement("node-y", "lab", [3; 32], PROTOCOL_VERSION, "10.0.0.7", 9200);
        broken.txt.retain(|(key, _)| key != "pk");
        bus.inject(broken);
        settle().await;
        assert!(a.discovery.peers().is_empty());
        assert_eq!(a.discovery.counters().ignored, 3);

        // A peer of this cluster is listed, and that is all.
        bus.inject(announcement(
            "node-b",
            "lab",
            b_key,
            PROTOCOL_VERSION,
            "10.0.0.2",
            9200,
        ));
        settle().await;
        let found = peer(PeerState::Discovered);
        assert_eq!(
            found.addrs,
            ["10.0.0.2:9200".parse::<SocketAddr>().expect("addr")]
        );
        assert!(
            a.events.borrow().is_empty(),
            "nobody is told about an untrusted peer"
        );

        // A person enrolls it, having compared the fingerprint: now it is a peer to connect to.
        assert!(
            a.keys
                .lock()
                .trust_peer(&found.node, found.public_key)
                .expect("trust")
        );
        a.discovery.trust_changed();
        settle().await;
        peer(PeerState::Enrolled);
        assert_eq!(a.events.borrow().len(), 1);

        // It moves to another address: the connection can follow.
        bus.inject(announcement(
            "node-b",
            "lab",
            b_key,
            PROTOCOL_VERSION,
            "10.0.0.3",
            9300,
        ));
        settle().await;
        let moved = a.events.borrow().last().cloned();
        let Some(PeerEvent::Present(now)) = moved else {
            panic!("{moved:?}");
        };
        assert_eq!(
            now.addrs,
            ["10.0.0.3:9300".parse::<SocketAddr>().expect("addr")]
        );
        // The same again is not news.
        bus.inject(announcement(
            "node-b",
            "lab",
            b_key,
            PROTOCOL_VERSION,
            "10.0.0.3",
            9300,
        ));
        settle().await;
        assert_eq!(a.events.borrow().len(), 2);

        // Something announces node-b's name with another key: not followed, and node-b is gone
        // as far as connecting goes.
        bus.inject(announcement(
            "node-b",
            "lab",
            [9; 32],
            PROTOCOL_VERSION,
            "10.0.0.66",
            9200,
        ));
        settle().await;
        peer(PeerState::KeyMismatch);
        assert_eq!(
            a.events.borrow().last(),
            Some(&PeerEvent::Gone(found.node.clone()))
        );
        // The real one again.
        bus.inject(announcement(
            "node-b",
            "lab",
            b_key,
            PROTOCOL_VERSION,
            "10.0.0.3",
            9300,
        ));
        settle().await;
        peer(PeerState::Enrolled);

        // It speaks another wire: listed, never connected to.
        bus.inject(announcement(
            "node-b",
            "lab",
            b_key,
            PROTOCOL_VERSION + 1,
            "10.0.0.3",
            9300,
        ));
        settle().await;
        peer(PeerState::Incompatible);
        bus.inject(announcement(
            "node-b",
            "lab",
            b_key,
            PROTOCOL_VERSION,
            "10.0.0.3",
            9300,
        ));
        settle().await;

        // A person revokes it: listed as revoked, and gone.
        a.keys.lock().revoke(b_key).expect("revoke");
        a.discovery.trust_changed();
        settle().await;
        peer(PeerState::Revoked);
        assert_eq!(
            a.events.borrow().last(),
            Some(&PeerEvent::Gone(found.node.clone()))
        );
    });
}

#[test]
fn a_peer_is_listed_until_it_says_goodbye_or_falls_silent() {
    run(true, async {
        let bus = MemoryDiscoveryBus::new();
        let a = Node::start("node-a", "lab", None, &bus).await;
        let (b_key, c_key) = (Identity::generate().public(), Identity::generate().public());
        for (name, key) in [("node-b", b_key), ("node-c", c_key)] {
            assert!(
                a.keys
                    .lock()
                    .trust_peer(&name.parse().expect("name"), key)
                    .expect("trust")
            );
        }
        bus.inject(announcement(
            "node-b",
            "lab",
            b_key,
            PROTOCOL_VERSION,
            "10.0.0.2",
            9200,
        ));
        bus.inject(announcement(
            "node-c",
            "lab",
            c_key,
            PROTOCOL_VERSION,
            "10.0.0.3",
            9200,
        ));
        settle().await;
        assert_eq!(a.discovery.peers().len(), 2);
        assert_eq!(a.events.borrow().len(), 2);

        // node-b says goodbye: gone at once.
        bus.inject_withdrawal("node-b._orion._tcp.local.");
        settle().await;
        assert_eq!(a.discovery.peers().len(), 1);
        assert_eq!(
            a.events.borrow().last(),
            Some(&PeerEvent::Gone("node-b".parse().expect("name")))
        );

        // node-c keeps announcing: it stays, however long.
        for _ in 0..10 {
            tokio::time::sleep(TTL / 2).await;
            bus.inject(announcement(
                "node-c",
                "lab",
                c_key,
                PROTOCOL_VERSION,
                "10.0.0.3",
                9200,
            ));
        }
        settle().await;
        assert_eq!(a.discovery.peers().len(), 1);
        assert_eq!(a.events.borrow().len(), 3);

        // Then it falls silent without a goodbye: gone when its announcement runs out.
        tokio::time::sleep(TTL - Duration::from_secs(1)).await;
        assert_eq!(a.discovery.peers().len(), 1);
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(a.discovery.peers().is_empty());
        assert_eq!(
            a.events.borrow().last(),
            Some(&PeerEvent::Gone("node-c".parse().expect("name")))
        );
        assert_eq!(a.discovery.counters().expired, 2);
        // Its key is still held: it is a peer again the moment it is back.
        bus.inject(announcement(
            "node-c",
            "lab",
            c_key,
            PROTOCOL_VERSION,
            "10.0.0.3",
            9200,
        ));
        settle().await;
        assert!(matches!(
            a.events.borrow().last(),
            Some(PeerEvent::Present(_))
        ));

        // A node that stops takes its announcement with it.
        let d = Node::start("node-d", "lab", None, &bus).await;
        settle().await;
        assert!(a.discovery.peers().iter().any(|peer| peer.node == d.name));
        drop(d);
        settle().await;
        assert!(
            !a.discovery
                .peers()
                .iter()
                .any(|peer| peer.node.as_str() == "node-d")
        );
    });
}

/// Real mDNS on this machine. Ignored by default: it needs multicast on an interface, which
/// build containers usually lack.
///
/// ```sh
/// cargo test -p orion-discovery --features mdns real_mdns -- --ignored
/// ```
#[cfg(feature = "mdns")]
#[test]
#[ignore = "needs working multicast on this machine"]
fn real_mdns_comm_cores_find_each_other_and_enroll() {
    use orion_discovery::MdnsDiscoveryBackend;
    run(false, async {
        let key = "a shared enrollment key of thirty-two bytes!";
        let unique = std::process::id();
        let start = |name: String| async move {
            let name: NodeName = name.parse().expect("name");
            let config = KeysConfig {
                enrollment_key: Some(key.as_bytes().to_vec()),
                ..KeysConfig::in_memory(&format!("mdns-test-{unique}"))
            };
            let keys = SharedKeys::new(Keys::open(config, Security::On).expect("keys"));
            let options = TcpOptions {
                security: Security::On,
                keys: keys.clone(),
                max_frame: 64 * 1024,
                handshake: Duration::from_secs(5),
            };
            let comm = Comm::new(
                name.clone(),
                Security::On,
                Some(Rc::new(keys.clone())),
                Limits::default(),
            );
            comm.add_service(KeysService::new(name.clone(), keys.clone()));
            let carrier = TcpCarrier::bind("0.0.0.0:0".parse().expect("addr"), options.clone())
                .await
                .expect("bind");
            let port = carrier.local_addr().expect("addr").port();
            tokio::task::spawn_local(async move { comm.serve(carrier).await });
            let discovery = Discovery::start(
                DiscoveryConfig {
                    node: name.clone(),
                    port,
                    addresses: Vec::new(),
                    ttl: TTL,
                    tcp: options,
                },
                Box::new(
                    MdnsDiscoveryBackend::new(Vec::new()).with_refresh(Duration::from_secs(2)),
                ),
                |_| {},
            )
            .expect("mDNS starts");
            (name, discovery)
        };
        let (a_name, a) = start(format!("mdns-a-{unique}")).await;
        let (b_name, b) = start(format!("mdns-b-{unique}")).await;
        let enrolled = |discovery: &Discovery, peer: &NodeName| {
            discovery
                .peers()
                .iter()
                .any(|seen| seen.node == *peer && seen.state == PeerState::Enrolled)
        };
        for _ in 0..600 {
            if enrolled(&a, &b_name) && enrolled(&b, &a_name) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            enrolled(&a, &b_name) && enrolled(&b, &a_name),
            "{:?} {:?}",
            a.peers(),
            b.peers()
        );
        // One stops: the other hears its goodbye.
        drop(a);
        for _ in 0..200 {
            if b.peers().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(b.peers().is_empty(), "{:?}", b.peers());
    });
}

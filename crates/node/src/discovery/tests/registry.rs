//! Advertisement encoding, the discovered set, and discovery over the in-memory bus.

use super::*;
use crate::discovery::registry::{DiscoveryRegistry, Observation};
use orion_core::CONTROL_PROTOCOL_VERSION;

fn advertisement(node_id: &str, cluster: &str) -> Advertisement {
    Advertisement {
        node_id: NodeId::new(node_id),
        cluster: cluster.into(),
        public_key: [7; 32],
        control_protocol_version: CONTROL_PROTOCOL_VERSION,
        peer_tcp_port: Some(9200),
        http_port: Some(9100),
        https: true,
    }
}

fn announcement(advertisement: &Advertisement, ttl_ms: u64) -> Announcement {
    Announcement {
        instance: format!("{}._orion._tcp.local.", advertisement.node_id),
        addresses: vec![
            "fe80::1".parse().expect("ip"),
            "2001:db8::2".parse().expect("ip"),
            "10.0.0.2".parse().expect("ip"),
        ],
        port: advertisement.srv_port(),
        txt: advertisement.txt(),
        ttl: Some(Duration::from_millis(ttl_ms)),
    }
}

#[test]
fn txt_records_roundtrip_and_build_peer_urls() {
    let ad = advertisement("node-b", CLUSTER);
    let txt = ad.txt();
    assert!(txt.iter().all(|(key, value)| key.len() + value.len() < 255));
    assert_eq!(Advertisement::from_txt(&txt), Ok(ad.clone()));
    // Keys are case-insensitive and the first occurrence wins (RFC 6763).
    let mut shouted: Vec<_> = txt
        .iter()
        .map(|(key, value)| (key.to_ascii_uppercase(), value.clone()))
        .collect();
    shouted.push(("cl".into(), "other".into()));
    assert_eq!(Advertisement::from_txt(&shouted), Ok(ad.clone()));

    let urls: Vec<String> = ad
        .peer_urls(&announcement(&ad, 1).addresses)
        .iter()
        .map(|url| url.to_string())
        .collect();
    // orion+tcp first, IPv4 before IPv6, link-local IPv6 skipped.
    assert_eq!(
        urls,
        [
            "orion+tcp://10.0.0.2:9200",
            "orion+tcp://[2001:db8::2]:9200",
            "https://10.0.0.2:9100",
            "https://[2001:db8::2]:9100",
        ]
    );

    let fingerprint = key_fingerprint(&[7; 32]);
    assert!(fingerprint.starts_with("sha256:"));
    assert_eq!(fingerprint.len(), "sha256:".len() + 32);
}

#[test]
fn malformed_txt_records_are_rejected() {
    let ad = advertisement("node-b", CLUSTER);
    let without = |key: &str| -> Vec<(String, String)> {
        ad.txt()
            .into_iter()
            .filter(|(name, _)| name != key)
            .collect()
    };
    for key in ["v", "id", "pk", "cp", "cl"] {
        assert!(Advertisement::from_txt(&without(key)).is_err(), "{key}");
    }
    let mut bad_key = ad.txt();
    bad_key[2].1 = "zz".repeat(32);
    assert!(Advertisement::from_txt(&bad_key).is_err());
    let mut bad_port = ad.txt();
    bad_port.push(("tcp".into(), "0".into()));
    bad_port.retain(|(key, value)| key != "tcp" || value == "0");
    assert!(Advertisement::from_txt(&bad_port).is_err());
    let mut future = ad.txt();
    future[0].1 = "2".into();
    assert!(Advertisement::from_txt(&future).is_err());
}

#[test]
fn registry_filters_by_cluster_refreshes_and_expires() {
    let mut registry = DiscoveryRegistry::new(
        NodeId::new("node-a"),
        CLUSTER.into(),
        Duration::from_secs(60),
    );
    let b = advertisement("node-b", CLUSTER);
    assert_eq!(
        registry.observe(&announcement(&b, 100), 1_000),
        Observation::New(NodeId::new("node-b"))
    );
    assert_eq!(
        registry.observe(&announcement(&b, 100), 1_050),
        Observation::Refreshed(NodeId::new("node-b"))
    );
    let mut moved = announcement(&b, 100);
    moved.addresses = vec!["10.0.0.9".parse().expect("ip")];
    assert_eq!(
        registry.observe(&moved, 1_060),
        Observation::Changed(NodeId::new("node-b"))
    );

    // Other clusters, the node itself and malformed records are ignored and counted.
    let other = advertisement("node-x", "other-cluster");
    assert!(matches!(
        registry.observe(&announcement(&other, 100), 1_060),
        Observation::Ignored(_)
    ));
    let own = advertisement("node-a", CLUSTER);
    assert!(matches!(
        registry.observe(&announcement(&own, 100), 1_060),
        Observation::Ignored(_)
    ));
    let mut garbage = announcement(&b, 100);
    garbage.txt.clear();
    assert!(matches!(
        registry.observe(&garbage, 1_060),
        Observation::Ignored(_)
    ));
    let counters = registry.counters();
    assert_eq!(counters.announcements_received, 3);
    assert_eq!(counters.announcements_ignored, 3);

    // Not refreshed within its TTL: dropped.
    assert!(registry.expire(1_159).is_empty());
    assert_eq!(registry.expire(1_160), vec![NodeId::new("node-b")]);
    assert!(registry.get(&NodeId::new("node-b")).is_none());

    // A goodbye drops it immediately.
    registry.observe(&announcement(&b, 100), 2_000);
    assert_eq!(
        registry.withdraw(&announcement(&b, 100).instance),
        Some(NodeId::new("node-b"))
    );
    assert_eq!(registry.counters().peers_expired, 2);
}

#[tokio::test]
async fn nodes_advertise_and_browse_without_trusting_each_other() {
    let bus = MemoryDiscoveryBus::new();
    let a = build_app("node-a", None);
    let b = build_app("node-b", None);
    let config = || discovery_config(None).with_ttl(Duration::from_millis(300));
    let a_handle = a
        .start_discovery(config(), Box::new(bus.backend()), endpoints(9201))
        .expect("discovery should start");
    let b_handle = b
        .start_discovery(config(), Box::new(bus.backend()), endpoints(9202))
        .expect("discovery should start");
    let (a_id, b_id) = (a.config.node_id.clone(), b.config.node_id.clone());

    wait_until("both nodes see each other", || {
        peer_state(&a, &b_id) == Some(DiscoveredPeerState::Discovered)
            && peer_state(&b, &a_id) == Some(DiscoveredPeerState::Discovered)
    })
    .await;
    // Discovered is not trusted: nothing is registered for sync and no key is pinned.
    assert!(!a.is_registered_peer(&b_id));
    assert!(a.trusted_key_hex(&b_id).is_none());
    let snapshot = a.query_discovery();
    assert_eq!(snapshot.backend, "memory");
    assert_eq!(snapshot.cluster, CLUSTER);
    assert!(!snapshot.enrollment_key_configured);
    let record = &snapshot.peers[0];
    assert_eq!(
        record.key_fingerprint,
        key_fingerprint(&b.security.public_key_bytes())
    );
    assert_eq!(record.peer_urls[0].as_str(), "orion+tcp://127.0.0.1:9202");
    let metrics = a.observability_snapshot().discovery;
    assert!(metrics.enabled);
    assert_eq!(metrics.discovered_peers, 1);
    assert_eq!(metrics.enrolled_peers, 0);

    // A peer of another cluster is ignored; a peer speaking another protocol version is listed
    // as incompatible.
    let foreign = |node_id: &str, cluster: &str, version: u16| {
        let ad = Advertisement {
            control_protocol_version: version,
            ..advertisement(node_id, cluster)
        };
        Announcement {
            instance: node_id.into(),
            addresses: vec![LOOPBACK],
            port: 1,
            txt: ad.txt(),
            ttl: None,
        }
    };
    let ignored_before = a.query_discovery().metrics.announcements_ignored;
    bus.inject(foreign(
        "node-other",
        "other-cluster",
        CONTROL_PROTOCOL_VERSION,
    ));
    bus.inject(foreign("node-old", CLUSTER, CONTROL_PROTOCOL_VERSION - 1));
    let old = NodeId::new("node-old");
    wait_until("the old node is listed", || {
        peer_state(&a, &old) == Some(DiscoveredPeerState::Incompatible)
    })
    .await;
    assert!(peer_state(&a, &NodeId::new("node-other")).is_none());
    assert!(a.query_discovery().metrics.announcements_ignored > ignored_before);
    assert!(
        local_control(
            &a,
            ControlMessage::EnrollDiscoveredPeer(orion::control_plane::DiscoveredPeerEnrollment {
                node_id: old.clone(),
                expected_key_fingerprint: None,
            })
        )
        .is_rejected()
    );

    // b stops answering: a drops it after the TTL while refreshes keep a alive in b.
    bus.silence(b_id.as_str());
    let deadline = Instant::now() + Duration::from_millis(1_500);
    while Instant::now() < deadline && peer_state(&a, &b_id).is_some() {
        bus.refresh();
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert!(peer_state(&a, &b_id).is_none(), "silent peer should expire");
    assert!(
        peer_state(&b, &a_id).is_some(),
        "refreshed peer should stay"
    );
    assert!(a.query_discovery().metrics.peers_expired >= 1);

    // A goodbye removes the node right away.
    a_handle.shutdown().await;
    wait_until("b sees a's goodbye", || peer_state(&b, &a_id).is_none()).await;
    b_handle.shutdown().await;
}

#[tokio::test]
async fn discovery_requires_required_peer_auth_and_a_listener() {
    let bus = MemoryDiscoveryBus::new();
    let optional = NodeApp::builder()
        .config(
            node_config("node-optional", None)
                .with_peer_authentication(PeerAuthenticationMode::Optional),
        )
        .try_build()
        .expect("node app should build");
    assert!(
        optional
            .start_discovery(
                discovery_config(None),
                Box::new(bus.backend()),
                endpoints(1)
            )
            .is_err()
    );
    let app = build_app("node-nolistener", None);
    assert!(
        app.start_discovery(
            discovery_config(None),
            Box::new(bus.backend()),
            AdvertisedEndpoints::default()
        )
        .is_err()
    );
    assert!(DiscoveryConfig::new("bad cluster!").is_err());
    assert!(EnrollmentKey::try_new("too-short").is_err());
}

trait Rejected {
    fn is_rejected(&self) -> bool;
}

impl Rejected for ControlMessage {
    fn is_rejected(&self) -> bool {
        matches!(self, ControlMessage::Rejected(_))
    }
}

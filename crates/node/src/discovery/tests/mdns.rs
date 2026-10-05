//! Real mDNS on this host (ignored by default: needs multicast on an interface, which CI
//! containers usually lack). See docs/testing.md:
//!
//! ```sh
//! cargo test -p orion-node --features discovery-mdns real_mdns -- --ignored
//! ```

use super::*;

#[tokio::test]
#[ignore = "needs working multicast on this host; see docs/testing.md"]
async fn real_mdns_nodes_discover_each_other_and_enroll_with_the_shared_key() {
    let a = TcpNode::start(&unique("node-mdns-a"), None).await;
    let b = TcpNode::start(&unique("node-mdns-b"), None).await;
    let start = |node: &TcpNode| {
        let backend = MdnsDiscoveryBackend::new(Vec::new()).with_refresh(Duration::from_secs(2));
        node.app
            .start_discovery(
                discovery_config(Some(KEY)),
                Box::new(backend),
                endpoints(node.addr.port()),
            )
            .expect("mDNS discovery should start")
    };
    let a_discovery = start(&a);
    let b_discovery = start(&b);
    wait_until("both nodes enrolled each other over real mDNS", || {
        peer_state(&a.app, &b.id()) == Some(DiscoveredPeerState::Enrolled)
            && peer_state(&b.app, &a.id()) == Some(DiscoveredPeerState::Enrolled)
    })
    .await;
    put_artifact(&a.app, "artifact.over-mdns");
    a.app
        .sync_peer(&b.id())
        .await
        .expect("peers enrolled through mDNS discovery sync");
    assert!(has_artifact(&b.app, "artifact.over-mdns"));
    a_discovery.shutdown().await;
    wait_until("b sees a's goodbye", || {
        peer_state(&b.app, &a.id()).is_none()
    })
    .await;
    b_discovery.shutdown().await;
    a.stop().await;
    b.stop().await;
}

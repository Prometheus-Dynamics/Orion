//! A cluster that mixes HTTP and `orion+tcp` peers.

use super::*;

#[tokio::test]
async fn http_and_tcp_peers_share_one_cluster() {
    let mode = crate::PeerAuthenticationMode::Optional;
    let build = |node_id: &'static str| {
        NodeApp::builder()
            .config(test_node_config_with_auth(node_id, "node-mixed", mode))
            .try_build()
            .expect("node app should build")
    };
    let (http_node, tcp_node, hub) = (build("node-http"), build("node-tcp"), build("node-hub"));
    let (http_addr, http_server) = http_node
        .start_http_server("127.0.0.1:0".parse().expect("address should parse"))
        .await
        .expect("HTTP listener should start");
    let (tcp_addr, tcp_server) = tcp_node
        .start_peer_tcp_server("127.0.0.1:0".parse().expect("address should parse"))
        .await
        .expect("peer TCP listener should start");
    let (hub_addr, hub_server) = hub
        .start_peer_tcp_server("127.0.0.1:0".parse().expect("address should parse"))
        .await
        .expect("peer TCP listener should start");
    hub.register_peer(PeerConfig::new(
        "node-http",
        orion_core::PeerBaseUrl::new(format!("http://{http_addr}")),
    ))
    .expect("peer registration should succeed");
    hub.register_peer(PeerConfig::new(
        "node-tcp",
        orion_core::PeerBaseUrl::new(format!("orion+tcp://{tcp_addr}")),
    ))
    .expect("peer registration should succeed");
    for leaf in [&http_node, &tcp_node] {
        leaf.register_peer(PeerConfig::new(
            "node-hub",
            orion_core::PeerBaseUrl::new(format!("orion+tcp://{hub_addr}")),
        ))
        .expect("peer registration should succeed");
    }

    write_local(
        &http_node,
        DesiredStateMutation::PutArtifact(artifact("artifact.http", 1)),
    );
    write_local(
        &tcp_node,
        DesiredStateMutation::PutArtifact(artifact("artifact.tcp", 1)),
    );
    write_local(
        &hub,
        DesiredStateMutation::PutArtifact(artifact("artifact.hub", 1)),
    );
    for _ in 0..2 {
        hub.sync_peer(&NodeId::new("node-http"))
            .await
            .expect("HTTP peer sync should succeed");
        hub.sync_peer(&NodeId::new("node-tcp"))
            .await
            .expect("TCP peer sync should succeed");
    }
    let state = assert_all_equal(&[http_node.clone(), tcp_node.clone(), hub.clone()]);
    assert_eq!(state.artifacts.len(), 3);
    // The HTTP leaf can also reach the hub over orion+tcp (a node with both features).
    http_node
        .sync_peer(&NodeId::new("node-hub"))
        .await
        .expect("TCP sync from the HTTP leaf should succeed");

    http_server.abort();
    tcp_server.shutdown().await.expect("listener should stop");
    hub_server.shutdown().await.expect("listener should stop");
}

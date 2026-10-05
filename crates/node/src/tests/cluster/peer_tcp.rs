//! In-process clusters over the `orion+tcp` peer transport (no HTTP feature required).

use super::*;
use crate::{PeerTcpError, peer_tcp::PeerTcpClient};
use orion::control_plane::CommunicationTransportKind;
use orion_transport_ipc::{CONTROL_PREAMBLE_BYTES, control_preamble};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct TcpNode {
    app: NodeApp,
    addr: std::net::SocketAddr,
    server: crate::app::GracefulTaskHandle<NodeError>,
}

async fn tcp_node(node_id: &'static str, mode: crate::PeerAuthenticationMode) -> TcpNode {
    let app = NodeApp::builder()
        .config(test_node_config_with_auth(node_id, "node-tcp", mode))
        .try_build()
        .expect("node app should build");
    let (addr, server) = app
        .start_peer_tcp_server("127.0.0.1:0".parse().expect("address should parse"))
        .await
        .expect("peer TCP listener should start");
    TcpNode { app, addr, server }
}

/// Registers `peer` on `node` as an `orion+tcp` peer with its public key pinned.
fn connect(node: &TcpNode, peer: &TcpNode) {
    node.app
        .register_peer(
            PeerConfig::new(
                peer.app.config.node_id.clone(),
                orion_core::PeerBaseUrl::new(format!("orion+tcp://{}", peer.addr)),
            )
            .with_trusted_public_key_hex(peer.app.security.public_key_hex()),
        )
        .expect("peer registration should succeed");
}

async fn sync(node: &TcpNode, peer: &TcpNode) {
    node.app
        .sync_peer(&peer.app.config.node_id)
        .await
        .expect("orion+tcp sync should succeed");
}

fn apps(nodes: &[&TcpNode]) -> Vec<NodeApp> {
    nodes.iter().map(|node| node.app.clone()).collect()
}

#[tokio::test]
async fn two_nodes_converge_over_orion_tcp_with_required_peer_auth() {
    let mode = crate::PeerAuthenticationMode::Required;
    let (a, b) = (
        tcp_node("node-a", mode).await,
        tcp_node("node-b", mode).await,
    );
    connect(&a, &b);
    connect(&b, &a);
    write_local(
        &a.app,
        DesiredStateMutation::PutArtifact(artifact("artifact.a", 1)),
    );
    write_local(
        &b.app,
        DesiredStateMutation::PutArtifact(artifact("artifact.b", 1)),
    );
    // One round from a pushes a's object and pulls b's.
    sync(&a, &b).await;
    let state = assert_all_equal(&apps(&[&a, &b]));
    assert_eq!(state.artifacts.len(), 2);

    let peer = a
        .app
        .peer_state_for_test(&NodeId::new("node-b"))
        .expect("peer state should exist");
    assert_eq!(peer.sync_status, PeerSyncStatus::Synced);
    let snapshot = a.app.observability_snapshot();
    let endpoint = snapshot
        .communication
        .iter()
        .find(|endpoint| endpoint.id == "tcp/peer-sync/node-b")
        .expect("peer endpoint should be reported");
    assert_eq!(endpoint.transport, CommunicationTransportKind::Tcp);
    assert!(endpoint.metrics.messages_sent_total >= 3);
    let served = b.app.observability_snapshot();
    assert!(
        served
            .communication
            .iter()
            .any(|endpoint| endpoint.id == "tcp/peer-control")
    );
    a.server.shutdown().await.expect("listener should stop");
    b.server.shutdown().await.expect("listener should stop");
}

#[tokio::test]
async fn three_nodes_with_conflicting_writes_converge_over_orion_tcp() {
    let mode = crate::PeerAuthenticationMode::Optional;
    let a = tcp_node("node-a", mode).await;
    let b = tcp_node("node-b", mode).await;
    let c = tcp_node("node-c", mode).await;
    // A line topology: a <-> b <-> c; a and c only meet through b.
    connect(&a, &b);
    connect(&b, &a);
    connect(&b, &c);
    connect(&c, &b);
    let mut contested = Vec::new();
    for (index, node) in [&a, &b, &c].into_iter().enumerate() {
        contested.extend(write_local(
            &node.app,
            DesiredStateMutation::PutArtifact(artifact("artifact.contested", index as u64)),
        ));
        write_local(
            &node.app,
            DesiredStateMutation::PutArtifact(artifact(
                ["artifact.only-a", "artifact.only-b", "artifact.only-c"][index],
                1,
            )),
        );
    }
    // c deletes its own object; the delete reaches a through b.
    write_local(
        &c.app,
        delete(&DesiredObjectKey::Artifact(ArtifactId::new(
            "artifact.only-c",
        ))),
    );
    for _ in 0..3 {
        sync(&a, &b).await;
        sync(&c, &b).await;
        sync(&b, &a).await;
        sync(&b, &c).await;
    }
    let state = assert_all_equal(&apps(&[&a, &b, &c]));
    assert!(
        state
            .artifacts
            .contains_key(&ArtifactId::new("artifact.only-a"))
    );
    assert!(
        state
            .artifacts
            .contains_key(&ArtifactId::new("artifact.only-b"))
    );
    assert!(
        !state
            .artifacts
            .contains_key(&ArtifactId::new("artifact.only-c"))
    );
    // The contested artifact went to the write with the latest stamp.
    let (winner, stamp) = contested
        .into_iter()
        .max_by_key(|(_, stamp)| *stamp)
        .expect("three writes were made");
    assert_eq!(
        DesiredStateMutation::PutArtifact(
            state.artifacts[&ArtifactId::new("artifact.contested")].clone()
        ),
        winner
    );
    assert_eq!(
        state.stamps.artifacts[&ArtifactId::new("artifact.contested")],
        stamp
    );
    for node in [a, b, c] {
        node.server.shutdown().await.expect("listener should stop");
    }
}

#[tokio::test]
async fn unsigned_responses_are_rejected_when_peer_auth_is_required() {
    let server = tcp_node("node-a", crate::PeerAuthenticationMode::Disabled).await;
    let client = tcp_node("node-b", crate::PeerAuthenticationMode::Required).await;
    connect(&client, &server);
    let err = client
        .app
        .sync_peer(&NodeId::new("node-a"))
        .await
        .expect_err("an unsigned response must not be trusted");
    assert!(
        err.to_string().contains("unsigned response"),
        "unexpected error: {err}"
    );
    let peer = client
        .app
        .peer_state_for_test(&NodeId::new("node-a"))
        .expect("peer state should exist");
    assert_eq!(
        peer.last_error_kind,
        Some(orion::control_plane::PeerSyncErrorKind::AuthPolicy)
    );
}

#[tokio::test]
async fn responses_signed_by_another_key_are_rejected() {
    let mode = crate::PeerAuthenticationMode::Required;
    let (a, b) = (
        tcp_node("node-a", mode).await,
        tcp_node("node-b", mode).await,
    );
    let impostor = tcp_node("node-x", mode).await;
    // a expects b's key, but b's address answers with the impostor's identity.
    a.app
        .register_peer(
            PeerConfig::new(
                "node-b",
                orion_core::PeerBaseUrl::new(format!("orion+tcp://{}", impostor.addr)),
            )
            .with_trusted_public_key_hex(b.app.security.public_key_hex()),
        )
        .expect("peer registration should succeed");
    let err = a
        .app
        .sync_peer(&NodeId::new("node-b"))
        .await
        .expect_err("a response from another identity must be rejected");
    assert!(
        matches!(
            &err,
            NodeError::Authentication(_)
                | NodeError::Authorization(_)
                | NodeError::PeerTcp(PeerTcpError::Remote(_))
        ),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn protocol_version_skew_is_reported_on_both_sides() {
    let node = tcp_node("node-a", crate::PeerAuthenticationMode::Optional).await;

    // A client from another release: the listener answers with its own preamble and closes.
    let mut stream = tokio::net::TcpStream::connect(node.addr)
        .await
        .expect("connect should succeed");
    let mut header = control_preamble().to_vec();
    let skewed = orion_core::CONTROL_PROTOCOL_VERSION + 1;
    header[2..CONTROL_PREAMBLE_BYTES].copy_from_slice(&skewed.to_le_bytes());
    header.extend_from_slice(&0_u32.to_le_bytes());
    stream
        .write_all(&header)
        .await
        .expect("write should succeed");
    let mut reply = Vec::new();
    stream
        .read_to_end(&mut reply)
        .await
        .expect("read should succeed");
    assert_eq!(reply[..CONTROL_PREAMBLE_BYTES], control_preamble());

    // A listener from another release: the client reports a typed mismatch.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind should succeed");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept should succeed");
        let mut request = [0_u8; 64];
        let _ = stream.read(&mut request).await;
        let _ = stream.write_all(&header).await;
    });
    let client = PeerTcpClient::new(&addr.to_string(), Duration::from_secs(2), 1 << 20);
    let err = client
        .exchange(b"\x01request")
        .await
        .map_err(PeerTcpError::from)
        .expect_err("a skewed listener must be reported");
    assert!(
        matches!(
            err,
            PeerTcpError::Frame(orion::transport::ipc::IpcTransportError::ProtocolMismatch { .. })
        ),
        "unexpected error: {err}"
    );
    node.server.shutdown().await.expect("listener should stop");
}

#[tokio::test]
async fn unreachable_tcp_peer_backs_off_with_a_connectivity_error() {
    let node = tcp_node("node-a", crate::PeerAuthenticationMode::Optional).await;
    node.app
        .register_peer(PeerConfig::new("node-gone", "orion+tcp://127.0.0.1:1"))
        .expect("peer registration should succeed");
    let err = node
        .app
        .sync_peer(&NodeId::new("node-gone"))
        .await
        .expect_err("an unreachable peer fails");
    assert!(matches!(
        err,
        NodeError::PeerTcp(PeerTcpError::Connect { .. })
    ));
    let peer = node
        .app
        .peer_state_for_test(&NodeId::new("node-gone"))
        .expect("peer state should exist");
    assert_eq!(peer.sync_status, PeerSyncStatus::BackingOff);
    assert_eq!(
        peer.last_error_kind,
        Some(orion::control_plane::PeerSyncErrorKind::TransportConnectivity)
    );
    node.server.shutdown().await.expect("listener should stop");
}

#[tokio::test]
async fn actions_are_forwarded_to_the_owning_node_over_orion_tcp() {
    use crate::tests::ipc::actions::{
        hello, polled_requests, publish_provider, wait_final, watch_requests,
    };
    use orion::control_plane::{
        ActionQuery, ActionReport, ActionRequest, ActionState, ActionTarget,
    };

    let mode = crate::PeerAuthenticationMode::Required;
    let (a, b) = (
        tcp_node("node-a", mode).await,
        tcp_node("node-b", mode).await,
    );
    connect(&a, &b);
    connect(&b, &a);
    let provider = hello(&b.app, "b-provider", ClientRole::Provider);
    publish_provider(&b.app, &provider, "provider.b");
    watch_requests(&b.app, &provider, "provider.b");
    // node-a learns that provider.b lives on node-b from the replicated desired state.
    sync(&a, &b).await;

    let accepted = a
        .app
        .run_action(
            ActionRequest::new(
                "f1",
                ActionTarget::Provider(ProviderId::new("provider.b")),
                "locate",
            )
            .with_arg("duration_ms", TypedConfigValue::UInt(1_000)),
            "operator",
        )
        .expect("action should be submitted");
    assert_eq!(accepted.state, ActionState::Accepted);
    assert_eq!(accepted.handled_by, NodeId::new("node-b"));

    let mut delivered = Vec::new();
    for _ in 0..500 {
        delivered = polled_requests(&b.app, &provider);
        if !delivered.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(delivered.len(), 1, "node-b delivers the forwarded request");
    assert_eq!(delivered[0].requested_by, "peer:node-a/local:operator");
    b.app
        .apply_local_control_message(
            &provider,
            ControlMessage::ReportActionResult(Box::new(
                ActionReport::new("f1", ActionState::Succeeded)
                    .with_output("blinks", TypedConfigValue::UInt(3)),
            )),
        )
        .expect("the handler reports on node-b");
    let done = wait_final(&a.app, "f1").await;
    assert_eq!(done.state, ActionState::Succeeded);
    assert_eq!(done.output["blinks"], TypedConfigValue::UInt(3));

    // The owner's rejection is mirrored back.
    a.app
        .run_action(
            ActionRequest::new("f2", ActionTarget::Node(NodeId::new("node-b")), "reboot"),
            "operator",
        )
        .expect("submitted");
    match wait_final(&a.app, "f2").await.state {
        ActionState::Rejected { reason } => {
            assert!(reason.contains("node-b has no handler"), "{reason}")
        }
        other => panic!("expected the owner's rejection, got {other:?}"),
    }

    // A peer that node-b has not enrolled cannot submit actions to it.
    let c = tcp_node("node-c", mode).await;
    connect(&c, &b);
    c.app
        .run_action(
            ActionRequest::new("f3", ActionTarget::Node(NodeId::new("node-b")), "reboot"),
            "operator",
        )
        .expect("submitted");
    match wait_final(&c.app, "f3").await.state {
        ActionState::Failed { reason } => assert!(reason.contains("node-b"), "{reason}"),
        other => panic!("expected the forward to fail, got {other:?}"),
    }
    assert!(b.app.query_actions(&ActionQuery::action("f3")).is_empty());
    for node in [a, b, c] {
        node.server.shutdown().await.expect("listener should stop");
    }
}

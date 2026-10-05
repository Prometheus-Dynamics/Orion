//! Remote operators (`docs/remote-operator.md`) against in-process nodes over real `orion+tcp`
//! listeners with required peer authentication, driven by the `orion-client` remote client.
//! Actions are in `remote_operator_actions.rs`.

use super::*;
use crate::HostFactsSource;
use orion::control_plane::{
    HostFacts, NodeHostFacts, OperatorEnrollment, OperatorId, OperatorPolicy, OperatorTrustState,
    StatusQuery,
};
use orion_client::remote::{NodeTrust, OperatorIdentity, RemoteError, RemoteOperator};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Publishes host facts and clock facts in the node's own observed record.
pub(super) fn publish_facts(app: &NodeApp) {
    app.refresh_host_facts_from(&EdgeHost);
    app.refresh_clock_facts_from(&crate::KernelClockStatusSource);
}

pub(super) struct OperatorNode {
    pub(super) app: NodeApp,
    pub(super) addr: std::net::SocketAddr,
    pub(super) server: crate::app::GracefulTaskHandle<NodeError>,
}

impl OperatorNode {
    pub(super) fn url(&self) -> String {
        format!("orion+tcp://{}", self.addr)
    }
}

pub(super) async fn operator_node(node_id: &'static str, state_dir: Option<PathBuf>) -> OperatorNode {
    operator_node_with(node_id, state_dir, Vec::new()).await
}

pub(super) async fn operator_node_with(
    node_id: &'static str,
    state_dir: Option<PathBuf>,
    default_actions: Vec<String>,
) -> OperatorNode {
    let mut config = test_node_config_with(
        node_id,
        format!("{node_id}-operator"),
        state_dir,
        Vec::new(),
        crate::PeerAuthenticationMode::Required,
    );
    config.runtime_tuning.actions = config
        .runtime_tuning
        .actions
        .clone()
        .with_operator_actions(default_actions);
    let app = NodeApp::builder()
        .config(config)
        .try_build()
        .expect("node app should build");
    let (addr, server) = app
        .start_peer_tcp_server("127.0.0.1:0".parse().expect("address should parse"))
        .await
        .expect("peer TCP listener should start");
    OperatorNode { app, addr, server }
}

/// Approves `identity` on `node` from its pending hello, with `policy`.
pub(super) fn approve(node: &OperatorNode, identity: &OperatorIdentity, policy: OperatorPolicy) {
    node.app
        .approve_operator(OperatorEnrollment {
            operator_id: identity.operator_id().clone(),
            public_key_hex: None,
            expected_key_fingerprint: Some(identity.fingerprint()),
            policy,
        })
        .expect("approval should succeed");
}

/// Connects `identity` and has it approved with `policy`.
pub(super) async fn enrolled_operator(
    node: &OperatorNode,
    identity: OperatorIdentity,
    policy: OperatorPolicy,
) -> RemoteOperator {
    let operator = RemoteOperator::connect(&node.url(), identity.clone(), NodeTrust::FirstUse)
        .await
        .expect("operator should connect");
    approve(node, &identity, policy);
    operator.hello().await.expect("hello should succeed");
    assert!(operator.is_enrolled());
    operator
}

struct EdgeHost;

impl HostFactsSource for EdgeHost {
    fn sample(&self) -> HostFacts {
        HostFacts {
            identity: NodeHostFacts {
                hostname: Some("edge-1".into()),
                image_version: Some("2026.10.1".into()),
                ..NodeHostFacts::default()
            },
            ..HostFacts::default()
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn operator_approval_enrolls_and_unlocks_read_apis() {
    let state_dir = temp_state_dir("operator-approval");
    let node = operator_node("node-a", Some(state_dir.clone())).await;
    publish_facts(&node.app);
    let identity = OperatorIdentity::generate("alice").expect("identity");

    let operator = RemoteOperator::connect(&node.url(), identity.clone(), NodeTrust::FirstUse)
        .await
        .expect("an unenrolled operator can still say hello");
    assert!(!operator.is_enrolled());
    assert_eq!(operator.node_id(), &NodeId::new("node-a"));
    assert_eq!(
        operator.node_public_key(),
        node.app.security.public_key_bytes()
    );
    let welcome = operator.welcome();
    assert_eq!(welcome.state, OperatorTrustState::Pending);
    assert_eq!(welcome.operator_key_fingerprint, identity.fingerprint());
    let refused = operator.nodes().await.expect_err("not enrolled yet");
    assert!(refused.is_not_enrolled(), "unexpected error: {refused}");

    // The hello left a pending entry whose fingerprint the administrator compares.
    let pending = node.app.query_operators();
    let record = pending
        .operators
        .iter()
        .find(|record| record.operator_id == *identity.operator_id())
        .expect("pending operator is listed");
    assert_eq!(record.state, OperatorTrustState::Pending);
    assert_eq!(record.key_fingerprint, Some(identity.fingerprint()));
    // A wrong fingerprint is refused.
    assert!(
        node.app
            .approve_operator(OperatorEnrollment {
                operator_id: identity.operator_id().clone(),
                public_key_hex: None,
                expected_key_fingerprint: Some("sha256:00".into()),
                policy: OperatorPolicy::default(),
            })
            .is_err()
    );
    approve(&node, &identity, OperatorPolicy::default());

    let welcome = operator.hello().await.expect("hello");
    assert_eq!(welcome.state, OperatorTrustState::Enrolled);
    assert!(welcome.read);
    let nodes = operator.nodes().await.expect("node records");
    let me = nodes
        .iter()
        .find(|record| record.node_id == NodeId::new("node-a"))
        .expect("the node's own record");
    let host = me.host.as_ref().expect("host facts are replicated");
    assert_eq!(host.hostname.as_deref(), Some("edge-1"));
    assert_eq!(host.image_version.as_deref(), Some("2026.10.1"));
    assert!(me.clock.is_some(), "clock facts are reported");
    assert_eq!(
        operator
            .node(&NodeId::new("node-a"))
            .await
            .expect("node")
            .and_then(|record| record.host)
            .and_then(|host| host.hostname),
        Some("edge-1".into())
    );
    let observability = operator.observability().await.expect("observability");
    assert_eq!(observability.node_id, NodeId::new("node-a"));
    let status = operator
        .status(StatusQuery::default())
        .await
        .expect("status lane");
    assert!(status.iter().all(|entry| !entry.key.is_empty()));

    // The enrollment survives a restart of the node.
    let OperatorNode { app, server, .. } = node;
    server.shutdown().await.expect("listener should stop");
    drop(app);
    let restarted = operator_node("node-a", Some(state_dir.clone())).await;
    let operator = RemoteOperator::connect(
        &restarted.url(),
        identity,
        NodeTrust::Node {
            node_id: NodeId::new("node-a"),
            public_key: operator.node_public_key(),
        },
    )
    .await
    .expect("the node keeps its key across restarts");
    assert!(operator.is_enrolled());
    restarted.server.shutdown().await.expect("listener stops");
    let _ = std::fs::remove_dir_all(state_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_access_can_be_withheld_and_removed_operators_are_refused() {
    let node = operator_node("node-a", None).await;
    let identity = OperatorIdentity::generate("bob").expect("identity");
    let operator = enrolled_operator(
        &node,
        identity.clone(),
        OperatorPolicy {
            read: false,
            actions: Some(vec![]),
        },
    )
    .await;
    let err = operator.nodes().await.expect_err("no read access");
    assert!(err.to_string().contains("no read access"), "{err}");

    assert!(node.app.remove_operator(identity.operator_id()).expect("remove"));
    let err = operator.hello().await.expect_err("removed operators are refused");
    assert!(err.to_string().contains("was removed"), "{err}");
    let record = node
        .app
        .query_operators()
        .operators
        .into_iter()
        .find(|record| record.operator_id == *identity.operator_id())
        .expect("revoked operators stay listed");
    assert_eq!(record.state, OperatorTrustState::Revoked);
    // An explicit approval lifts the revocation.
    node.app
        .approve_operator(OperatorEnrollment {
            operator_id: identity.operator_id().clone(),
            public_key_hex: Some(orion_core::PublicKeyHex::new(identity.public_key_hex())),
            expected_key_fingerprint: None,
            policy: OperatorPolicy::default(),
        })
        .expect("re-approval");
    operator.nodes().await.expect("read access after re-approval");
    node.server.shutdown().await.expect("listener stops");
}

/// A signed request from `identity` as the node's peer surface sees it.
fn signed(identity: &OperatorIdentity, nonce: u64, message: ControlMessage) -> HttpRequestPayload {
    let key = ed25519_dalek::SigningKey::from_bytes(&identity.secret_key_bytes());
    HttpRequestPayload::AuthenticatedPeer(
        orion_auth::crypto::sign_peer_request(
            &key,
            &identity.principal(),
            nonce,
            orion::auth::PeerRequestPayload::Control(Box::new(message)),
        )
        .expect("request signs"),
    )
}

fn serve(app: &NodeApp, request: HttpRequestPayload) -> Result<HttpResponsePayload, NodeError> {
    match app.serve_control_request(ControlRequest::from_peer_tcp_payload(request))? {
        ControlResponse::Http(response) => Ok(*response),
        ControlResponse::Local(_) => panic!("local response on the peer surface"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn operators_cannot_sync_or_write_and_are_never_peers() {
    let node = operator_node("node-a", None).await;
    let identity = OperatorIdentity::generate("carol").expect("identity");
    let _operator = enrolled_operator(&node, identity.clone(), OperatorPolicy::default()).await;

    let refused = [
        ControlMessage::Snapshot(node.app.state_snapshot()),
        ControlMessage::QueryPeerTrust,
        ControlMessage::Mutations(MutationBatch {
            base_revision: Revision::ZERO,
            mutations: vec![DesiredStateMutation::PutArtifact(artifact(
                "artifact.evil",
                1,
            ))],
            stamps: Vec::new(),
        }),
        ControlMessage::WatchActions(orion::control_plane::ActionQuery::all()),
        ControlMessage::RemoveOperator(identity.operator_id().clone()),
    ];
    for (nonce, message) in refused.into_iter().enumerate() {
        let err = serve(&node.app, signed(&identity, 100 + nonce as u64, message))
            .expect_err("operators may only read and run actions");
        assert!(matches!(err, NodeError::Authorization(_)), "{err}");
    }
    assert!(node.app.current_desired_state().artifacts.is_empty());

    // Not a peer: not registered for sync, not in the trust store, no node record, no liveness.
    assert!(node.app.peer_states().is_empty());
    assert!(
        !node
            .app
            .security
            .trust_store_node_ids()
            .expect("trust store")
            .contains(&identity.principal())
    );
    let snapshot = node.app.state_snapshot();
    assert!(!snapshot.state.observed.nodes.contains_key(&identity.principal()));
    assert!(!snapshot.state.desired.nodes.contains_key(&identity.principal()));
    assert_eq!(node.app.observability_snapshot().configured_peer_count, 0);

    // A peer id with the operator prefix cannot be configured either.
    assert!(
        node.app
            .register_peer(PeerConfig::new("operator:carol", "orion+tcp://127.0.0.1:1"))
            .is_err()
    );
    node.server.shutdown().await.expect("listener stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unenrolled_replayed_and_tampered_requests_are_refused() {
    let node = operator_node("node-a", None).await;
    let stranger = OperatorIdentity::generate("mallory").expect("identity");
    let err = serve(
        &node.app,
        signed(&stranger, 1, ControlMessage::QueryStateSnapshot),
    )
    .expect_err("unenrolled operators are refused");
    assert!(err.to_string().contains("is not enrolled"), "{err}");

    let identity = OperatorIdentity::generate("dave").expect("identity");
    let _operator = enrolled_operator(&node, identity.clone(), OperatorPolicy::default()).await;
    let request = signed(&identity, 7, ControlMessage::QueryObservability);
    serve(&node.app, request.clone()).expect("first use of a nonce");
    let err = serve(&node.app, request.clone()).expect_err("replays are refused");
    assert!(err.to_string().contains("replayed nonce"), "{err}");

    // Tampered payload: the signature no longer verifies.
    let HttpRequestPayload::AuthenticatedPeer(mut tampered) = signed(
        &identity,
        8,
        ControlMessage::QueryActions(orion::control_plane::ActionQuery::all()),
    ) else {
        unreachable!()
    };
    tampered.payload =
        orion::auth::PeerRequestPayload::Control(Box::new(ControlMessage::QueryStateSnapshot));
    let err = serve(&node.app, HttpRequestPayload::AuthenticatedPeer(tampered))
        .expect_err("tampered requests are refused");
    assert!(matches!(err, NodeError::Authentication(_)), "{err}");
    // Another key under an enrolled operator id.
    let impostor = OperatorIdentity::generate("dave").expect("identity");
    let err = serve(
        &node.app,
        signed(&impostor, 9, ControlMessage::QueryStateSnapshot),
    )
    .expect_err("the enrolled key is pinned");
    assert!(err.to_string().contains("not its enrolled key"), "{err}");
    // Unsigned operator messages are refused.
    assert!(
        serve(
            &node.app,
            HttpRequestPayload::Control(Box::new(ControlMessage::OperatorHello))
        )
        .is_err()
    );
    node.server.shutdown().await.expect("listener stops");
}

/// Reads one control frame (`[b"OC"][version u16][len u32 LE][payload]`) from `stream`.
async fn read_frame(stream: &mut tokio::net::TcpStream) -> Option<Vec<u8>> {
    let mut header = [0u8; 8];
    stream.read_exact(&mut header).await.ok()?;
    let len = u32::from_le_bytes(header[4..8].try_into().ok()?) as usize;
    let mut frame = header.to_vec();
    frame.resize(8 + len, 0);
    stream.read_exact(&mut frame[8..]).await.ok()?;
    Some(frame)
}

/// Forwards one `orion+tcp` connection to `target`, flipping the last payload byte (part of the
/// signed body) of every response frame.
async fn tampering_proxy(target: std::net::SocketAddr) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let Ok((mut client, _)) = listener.accept().await else {
            return;
        };
        let Ok(mut server) = tokio::net::TcpStream::connect(target).await else {
            return;
        };
        while let Some(request) = read_frame(&mut client).await {
            if server.write_all(&request).await.is_err() {
                return;
            }
            let Some(mut response) = read_frame(&mut server).await else {
                return;
            };
            if let Some(last) = response.last_mut() {
                *last ^= 0x01;
            }
            if client.write_all(&response).await.is_err() {
                return;
            }
        }
    });
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_client_refuses_tampered_responses_and_wrong_node_keys() {
    let node = operator_node("node-a", None).await;
    let identity = OperatorIdentity::generate("erin").expect("identity");
    let proxy = tampering_proxy(node.addr).await;
    let err = RemoteOperator::connect(
        &format!("orion+tcp://{proxy}"),
        identity.clone(),
        NodeTrust::FirstUse,
    )
    .await
    .expect_err("a tampered response must not verify");
    assert!(matches!(err, RemoteError::NodeAuthentication(_)), "{err}");

    let err = RemoteOperator::connect(&node.url(), identity.clone(), NodeTrust::Key([7; 32]))
        .await
        .expect_err("a node with another key is refused");
    assert!(matches!(err, RemoteError::NodeAuthentication(_)), "{err}");
    let err = RemoteOperator::connect(
        &node.url(),
        identity,
        NodeTrust::Node {
            node_id: NodeId::new("node-b"),
            public_key: node.app.security.public_key_bytes(),
        },
    )
    .await
    .expect_err("another node id is refused");
    assert!(matches!(err, RemoteError::NodeAuthentication(_)), "{err}");
    node.server.shutdown().await.expect("listener stops");
}

#[cfg(feature = "discovery-mdns")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn operators_enroll_with_the_shared_enrollment_key() {
    use crate::discovery::{
        AdvertisedEndpoints, DiscoveryConfig, EnrollmentKey, MemoryDiscoveryBus,
    };
    const KEY: &str = "0123456789abcdef0123456789abcdef-operator-test-key";

    let node = operator_node_with("node-a", None, vec!["locate".into()]).await;
    let bus = MemoryDiscoveryBus::new();
    let discovery = node
        .app
        .start_discovery(
            DiscoveryConfig::new("lab")
                .expect("cluster")
                .with_enrollment_key(EnrollmentKey::try_new(KEY).expect("key")),
            Box::new(bus.backend()),
            AdvertisedEndpoints {
                peer_tcp_port: Some(node.addr.port()),
                ..AdvertisedEndpoints::default()
            },
        )
        .expect("discovery starts");

    let identity = OperatorIdentity::generate("frank").expect("identity");
    let operator = RemoteOperator::connect(&node.url(), identity.clone(), NodeTrust::FirstUse)
        .await
        .expect("connect");
    assert!(operator.welcome().enrollment_key_configured);
    assert_eq!(operator.welcome().cluster, "lab");
    let err = operator
        .enroll_with_key(b"fedcba9876543210fedcba9876543210-wrong-key")
        .await
        .expect_err("a wrong key fails");
    assert!(matches!(err, RemoteError::Enrollment(_)), "{err}");
    assert!(!operator.hello().await.expect("hello").state.eq(&OperatorTrustState::Enrolled));

    let welcome = operator
        .enroll_with_key(KEY.as_bytes())
        .await
        .expect("the shared key enrolls the operator");
    assert_eq!(welcome.state, OperatorTrustState::Enrolled);
    assert_eq!(welcome.allowed_actions, vec!["locate".to_owned()]);
    operator.nodes().await.expect("read access");
    // Not a peer, even though it used the peer handshake.
    assert!(node.app.peer_states().is_empty());

    // A removed operator cannot come back with the key.
    node.app
        .remove_operator(identity.operator_id())
        .expect("remove");
    let again = RemoteOperator::connect(&node.url(), identity, NodeTrust::FirstUse).await;
    assert!(again.is_err(), "revoked operators cannot even say hello");
    let other = OperatorIdentity::generate("grace").expect("identity");
    let other = RemoteOperator::connect(&node.url(), other, NodeTrust::FirstUse)
        .await
        .expect("connect");
    other
        .enroll_with_key(KEY.as_bytes())
        .await
        .expect("other operators still enroll");
    let _ = OperatorId::try_new("grace").expect("valid id");
    discovery.shutdown().await;
    node.server.shutdown().await.expect("listener stops");
}

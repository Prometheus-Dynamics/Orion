//! Discovery and enrollment tests. Discovery runs over [`MemoryDiscoveryBus`] (no multicast);
//! the end-to-end tests use real `orion+tcp` listeners on loopback.

use super::*;
use crate::{NodeApp, NodeConfig, PeerAuthenticationMode};
use orion::{
    NodeId,
    control_plane::{
        ArtifactRecord, ClientHello, ClientRole, ControlMessage, DesiredStateMutation,
        DiscoveredPeerState, DiscoverySnapshot, MutationBatch,
    },
    transport::ipc::{ControlEnvelope, LocalAddress, UnixControlHandler},
};
use orion_core::ArtifactId;
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    time::{Duration, Instant},
};

mod enrollment;
mod mdns;
mod registry;
mod sync;

const CLUSTER: &str = "lab";
const KEY: &str = "0123456789abcdef0123456789abcdef-shared-test-key";
const OTHER_KEY: &str = "fedcba9876543210fedcba9876543210-another-test-key";
const LOOPBACK: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

fn unique(name: &str) -> String {
    format!(
        "{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos()
    )
}

fn temp_state_dir(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("orion-discovery-{}", unique(name)))
}

fn node_config(node_id: &str, state_dir: Option<PathBuf>) -> NodeConfig {
    let mut config = NodeConfig::for_local_node(NodeId::new(node_id))
        .with_http_bind_addr("127.0.0.1:0".parse().expect("address"))
        .with_ipc_socket_path(std::env::temp_dir().join(format!("{}.sock", unique(node_id))))
        .with_reconcile_interval(Duration::from_millis(50))
        .with_peer_authentication(PeerAuthenticationMode::Required);
    if let Some(state_dir) = state_dir {
        config = config.with_state_dir(state_dir);
    }
    config
}

fn build_app(node_id: &str, state_dir: Option<PathBuf>) -> NodeApp {
    NodeApp::builder()
        .config(node_config(node_id, state_dir))
        .try_build()
        .expect("node app should build")
}

fn discovery_config(key: Option<&str>) -> DiscoveryConfig {
    let config = DiscoveryConfig::new(CLUSTER)
        .expect("cluster name is valid")
        .with_tick(Duration::from_millis(20))
        .with_enrollment_retry(Duration::from_millis(100));
    match key {
        Some(key) => config.with_enrollment_key(EnrollmentKey::try_new(key).expect("valid key")),
        None => config,
    }
}

fn endpoints(port: u16) -> AdvertisedEndpoints {
    AdvertisedEndpoints {
        peer_tcp_port: Some(port),
        addresses: vec![LOOPBACK],
        ..AdvertisedEndpoints::default()
    }
}

/// A node with a running `orion+tcp` listener.
struct TcpNode {
    app: NodeApp,
    addr: SocketAddr,
    server: crate::app::GracefulTaskHandle<crate::NodeError>,
}

impl TcpNode {
    async fn start(node_id: &str, state_dir: Option<PathBuf>) -> Self {
        let app = build_app(node_id, state_dir);
        let (addr, server) = app
            .start_peer_tcp_server("127.0.0.1:0".parse().expect("address"))
            .await
            .expect("peer listener should start");
        Self { app, addr, server }
    }

    fn id(&self) -> NodeId {
        self.app.config.node_id.clone()
    }

    fn discover(&self, bus: &MemoryDiscoveryBus, key: Option<&str>) -> DiscoveryHandle {
        self.app
            .start_discovery(
                discovery_config(key),
                Box::new(bus.backend()),
                endpoints(self.addr.port()),
            )
            .expect("discovery should start")
    }

    async fn stop(self) {
        self.server.shutdown().await.expect("listener should stop");
    }
}

async fn wait_until(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn peer_state(app: &NodeApp, node_id: &NodeId) -> Option<DiscoveredPeerState> {
    app.query_discovery()
        .peers
        .into_iter()
        .find(|peer| &peer.node_id == node_id)
        .map(|peer| peer.state)
}

/// Sends `message` through the local control path as an `orionctl`-like control-plane client.
fn local_control(app: &NodeApp, message: ControlMessage) -> ControlMessage {
    let handler = app.unix_control_handler();
    let source = LocalAddress::new("orionctl.discovery-test");
    let envelope = |message| ControlEnvelope {
        source: source.clone(),
        destination: LocalAddress::new("orion"),
        message,
    };
    handler
        .handle_control(envelope(ControlMessage::ClientHello(ClientHello {
            client_name: "orionctl.discovery-test".into(),
            role: ClientRole::ControlPlane,
        })))
        .expect("client hello should be served");
    handler
        .handle_control(envelope(message))
        .expect("control request should be served")
        .message
}

fn query_discovery(app: &NodeApp) -> DiscoverySnapshot {
    match local_control(app, ControlMessage::QueryDiscovery) {
        ControlMessage::Discovery(snapshot) => *snapshot,
        other => panic!("unexpected discovery response {other:?}"),
    }
}

fn put_artifact(app: &NodeApp, id: &str) {
    let mut desired = app.state_snapshot().state.desired;
    MutationBatch::new(
        desired.revision,
        vec![DesiredStateMutation::PutArtifact(
            ArtifactRecord::builder(ArtifactId::new(id)).build(),
        )],
    )
    .apply_to(&mut desired);
    app.replace_desired(desired);
}

fn has_artifact(app: &NodeApp, id: &str) -> bool {
    app.state_snapshot()
        .state
        .desired
        .artifacts
        .contains_key(&ArtifactId::new(id))
}

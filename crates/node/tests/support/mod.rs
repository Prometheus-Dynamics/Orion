#![allow(dead_code)]

pub mod docker_cluster;

use std::{
    io::{BufRead, BufReader},
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Duration,
};

use orion::{
    NodeId, Revision, WorkloadId,
    control_plane::{ControlMessage, PeerHello},
    transport::{
        http::{HttpClient, HttpRequestPayload, HttpResponsePayload},
        ipc::{ControlEnvelope, LocalAddress, UnixControlStreamClient},
    },
};
use orion_node::NodeConfig;

pub struct SpawnedNode {
    child: Child,
    pub http_endpoint: String,
    pub probe_addr: Option<String>,
    pub ipc_socket: PathBuf,
    pub ipc_stream_socket: PathBuf,
    state_dir: Option<PathBuf>,
}

impl SpawnedNode {
    pub fn client(&self) -> HttpClient {
        HttpClient::try_new(self.http_endpoint.clone()).expect("HTTP client should build")
    }

    pub fn probe_client(&self) -> HttpClient {
        HttpClient::try_new(format!(
            "http://{}",
            self.probe_addr
                .as_ref()
                .expect("probe listener should be available")
        ))
        .expect("HTTP probe client should build")
    }

    pub fn cleanup(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(state_dir) = &self.state_dir {
            let _ = std::fs::remove_dir_all(state_dir);
        }
    }
}

impl Drop for SpawnedNode {
    fn drop(&mut self) {
        self.cleanup();
    }
}

pub fn integration_node_config(node_id: &str, state_dir: PathBuf) -> NodeConfig {
    NodeConfig::for_local_node(NodeId::new(node_id))
        .with_http_bind_addr("127.0.0.1:0".parse().expect("socket address should parse"))
        .with_state_dir(state_dir)
}

pub fn spawn_node(
    node_id: &str,
    state_dir: Option<PathBuf>,
    extra_env: &[(&str, &str)],
) -> SpawnedNode {
    let owned_state_dir = state_dir.unwrap_or_else(|| temp_state_dir(node_id));
    let mut command = Command::new(env!("CARGO_BIN_EXE_orion-node"));
    command
        .env("ORION_NODE_ID", node_id)
        .env("ORION_NODE_HTTP_ADDR", "127.0.0.1:0")
        .env("ORION_NODE_PEER_AUTH", "disabled")
        .env("ORION_NODE_RECONCILE_MS", "25")
        .env("ORION_NODE_STATE_DIR", &owned_state_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    for (key, value) in extra_env {
        command.env(key, value);
    }

    let mut child = command.spawn().expect("orion-node binary should start");
    let (http_endpoint, probe_addr, ipc_socket, ipc_stream_socket) =
        read_startup_endpoints(&mut child);

    SpawnedNode {
        child,
        http_endpoint,
        probe_addr,
        ipc_socket,
        ipc_stream_socket,
        state_dir: Some(owned_state_dir),
    }
}

pub async fn wait_for_workload(client: &HttpClient, workload_id: &str, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    let workload_id = WorkloadId::new(workload_id);

    loop {
        let current = snapshot(client).await;
        if current.state.desired.workloads.contains_key(&workload_id) {
            return;
        }

        if tokio::time::Instant::now() >= deadline {
            panic!("workload {} was not observed before timeout", workload_id);
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub async fn snapshot(client: &HttpClient) -> orion::control_plane::StateSnapshot {
    match client
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::SyncRequest(orion::control_plane::SyncRequest {
                node_id: NodeId::new("node.peer"),
                desired_revision: Revision::new(u64::MAX),
                desired_fingerprint: 0,
                desired_summary: None,
                sections: Vec::new(),
                object_selectors: Vec::new(),
            }),
        )))
        .await
        .expect("snapshot request should succeed")
    {
        HttpResponsePayload::Snapshot(snapshot) => snapshot,
        other => panic!("expected snapshot response, got {other:?}"),
    }
}

pub fn test_hello(node_id: &str) -> PeerHello {
    PeerHello {
        node_id: NodeId::new(node_id),
        desired_revision: Revision::ZERO,
        desired_fingerprint: 0,
        desired_section_fingerprints: orion::control_plane::DesiredStateSectionFingerprints {
            nodes: 0,
            artifacts: 0,
            workloads: 0,
            resources: 0,
            providers: 0,
            executors: 0,
            leases: 0,
        },
        observed_revision: Revision::ZERO,
        applied_revision: Revision::ZERO,
        transport_binding_version: None,
        transport_binding_public_key: None,
        transport_tls_cert_pem: None,
        transport_binding_signature: None,
    }
}

pub fn temp_state_dir(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "orion-node-it-{}-{}-{}",
        name,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos()
    ))
}

pub async fn recv_non_ping(
    client: &mut UnixControlStreamClient,
    local_address: &str,
) -> ControlEnvelope {
    loop {
        let envelope = client
            .recv()
            .await
            .expect("stream read should succeed")
            .expect("stream should yield a frame");
        if matches!(envelope.message, ControlMessage::Ping) {
            client
                .send(&ControlEnvelope {
                    source: LocalAddress::new(local_address),
                    destination: LocalAddress::new("orion"),
                    message: ControlMessage::Pong,
                })
                .await
                .expect("pong should send");
            continue;
        }
        return envelope;
    }
}

fn read_startup_endpoints(child: &mut Child) -> (String, Option<String>, PathBuf, PathBuf) {
    let stdout = child
        .stdout
        .take()
        .expect("orion-node child stdout should be available");
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .expect("orion-node should emit startup line");
    child.stdout = Some(reader.into_inner());

    let http_token = line
        .split_whitespace()
        .find(|token| token.starts_with("http="))
        .expect("startup line should include http address");
    let probe_token = line
        .split_whitespace()
        .find(|token| token.starts_with("http_probe="))
        .map(|token| token.trim_start_matches("http_probe=").to_owned())
        .filter(|value| value != "-");
    let ipc_token = line
        .split_whitespace()
        .find(|token| token.starts_with("ipc="))
        .expect("startup line should include ipc socket");
    let ipc_stream_token = line
        .split_whitespace()
        .find(|token| token.starts_with("ipc_stream="))
        .expect("startup line should include ipc stream socket");
    (
        http_token.trim_start_matches("http=").to_owned(),
        probe_token,
        PathBuf::from(ipc_token.trim_start_matches("ipc=")),
        PathBuf::from(ipc_stream_token.trim_start_matches("ipc_stream=")),
    )
}

use std::{path::PathBuf, process::Command, time::Duration};

use orion::{
    ArtifactId, NodeId, WorkloadId,
    control_plane::{
        ArtifactRecord, ControlMessage, DesiredStateMutation, ExecutorRecord, MutationBatch,
        SyncRequest,
    },
    transport::{
        http::{HttpClient, HttpRequestPayload, HttpResponsePayload},
        ipc::UnixControlStreamClient,
    },
};
use orion_node::{NodeApp, NodeConfig};

#[tokio::test(flavor = "multi_thread")]
async fn orionctl_get_reports_health_readiness_observability_and_snapshot() {
    let harness = TestHarness::start("node.orionctl.get").await;

    let health = run_orionctl(["get", "health", "--http", &harness.http_base()]);
    assert!(health.status.success(), "{}", output_text(&health));
    assert!(String::from_utf8_lossy(&health.stdout).contains("status=Healthy"));

    let readiness = run_orionctl(["get", "readiness", "--http", &harness.http_base()]);
    assert!(readiness.status.success(), "{}", output_text(&readiness));
    assert!(String::from_utf8_lossy(&readiness.stdout).contains("status=Ready"));

    let observability = run_orionctl(["get", "observability", "--http", &harness.http_base()]);
    assert!(
        observability.status.success(),
        "{}",
        output_text(&observability)
    );
    assert!(String::from_utf8_lossy(&observability.stdout).contains("mutation_success="));

    let snapshot = run_orionctl([
        "get",
        "snapshot",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
    ]);
    assert!(snapshot.status.success(), "{}", output_text(&snapshot));
    assert!(String::from_utf8_lossy(&snapshot.stdout).contains("desired_rev="));
}

#[tokio::test(flavor = "multi_thread")]
async fn orionctl_apply_delete_and_get_workloads_use_local_ipc() {
    let harness = TestHarness::start("node.orionctl.apply").await;
    let mut desired = harness._app.state_snapshot().state.desired;
    desired.put_executor(
        ExecutorRecord::builder("executor.cli", "node.orionctl.apply")
            .runtime_type("graph.exec.v1")
            .build(),
    );
    harness._app.replace_desired(desired);

    let put_artifact = run_orionctl([
        "apply",
        "artifact",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "--artifact-id",
        "artifact.cli",
        "--content-type",
        "application/octet-stream",
        "--size-bytes",
        "128",
    ]);
    assert!(
        put_artifact.status.success(),
        "{}",
        output_text(&put_artifact)
    );

    let put_workload = run_orionctl([
        "apply",
        "workload",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "--workload-id",
        "workload.cli",
        "--runtime-type",
        "graph.exec.v1",
        "--artifact-id",
        "artifact.cli",
        "--assigned-node",
        "node.orionctl.apply",
        "--desired-state",
        "running",
        "--restart-policy",
        "always",
    ]);
    assert!(
        put_workload.status.success(),
        "{}",
        output_text(&put_workload)
    );

    let get_workloads = run_orionctl([
        "get",
        "workloads",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "-o",
        "json",
    ]);
    assert!(
        get_workloads.status.success(),
        "{}",
        output_text(&get_workloads)
    );
    let workloads: serde_json::Value =
        serde_json::from_slice(&get_workloads.stdout).expect("json output should parse");
    let array = workloads.as_array().expect("json should be an array");
    assert_eq!(array.len(), 1);
    assert_eq!(array[0]["workload_id"], "workload.cli");
    assert_eq!(array[0]["desired_state"], "Running");

    let delete_workload = run_orionctl([
        "delete",
        "workload",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "--workload-id",
        "workload.cli",
    ]);
    assert!(
        delete_workload.status.success(),
        "{}",
        output_text(&delete_workload)
    );

    let snapshot = harness.snapshot().await;
    assert!(
        snapshot
            .state
            .desired
            .artifacts
            .contains_key(&ArtifactId::new("artifact.cli"))
    );
    assert!(
        !snapshot
            .state
            .desired
            .workloads
            .contains_key(&WorkloadId::new("workload.cli"))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn orionctl_watch_state_and_peers_list_use_local_admin_paths() {
    let harness = TestHarness::start("node.orionctl.watch").await;

    let peers = run_orionctl([
        "peers",
        "list",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "-o",
        "json",
    ]);
    assert!(peers.status.success(), "{}", output_text(&peers));
    let peers_json: serde_json::Value =
        serde_json::from_slice(&peers.stdout).expect("json output should parse");
    assert!(peers_json.get("http_mutual_tls_mode").is_some());

    let stream = UnixControlStreamClient::connect(&harness.ipc_stream_socket)
        .await
        .expect("stream should connect");
    drop(stream);

    let watch_task = tokio::task::spawn_blocking({
        let stream_socket = harness.ipc_stream_socket.to_string_lossy().to_string();
        move || {
            run_orionctl([
                "watch",
                "state",
                "--stream-socket",
                &stream_socket,
                "--client-name",
                "watcher",
                "--desired-revision",
                "0",
                "--batches",
                "1",
            ])
        }
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    let snapshot = harness.snapshot().await;
    let response = harness
        .client
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::Mutations(MutationBatch {
                base_revision: snapshot.state.desired.revision,
                mutations: vec![DesiredStateMutation::PutArtifact(
                    ArtifactRecord::builder("artifact.watch").build(),
                )],
            }),
        )))
        .await
        .expect("mutation should succeed");
    assert_eq!(response, HttpResponsePayload::Accepted);

    let output = watch_task.await.expect("watch task should join");
    assert!(output.status.success(), "{}", output_text(&output));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("state seq="), "unexpected stdout: {stdout}");
}

#[test]
fn orionctl_rejects_tls_material_for_plain_http_targets() {
    let output = run_orionctl([
        "get",
        "health",
        "--http",
        "http://127.0.0.1:9100",
        "--ca-cert",
        "/tmp/unused.pem",
    ]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("plain http:// targets do not use TLS material"));
}

#[test]
fn orionctl_requires_https_ca_when_client_identity_is_provided() {
    let output = run_orionctl([
        "get",
        "health",
        "--http",
        "https://127.0.0.1:9100",
        "--client-cert",
        "/tmp/client-cert.pem",
        "--client-key",
        "/tmp/client-key.pem",
    ]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("HTTPS trust material requires --ca-cert"));
}

struct TestHarness {
    _app: NodeApp,
    _http_task: tokio::task::JoinHandle<()>,
    _ipc_task: tokio::task::JoinHandle<()>,
    _ipc_stream_task: tokio::task::JoinHandle<()>,
    http_addr: std::net::SocketAddr,
    client: HttpClient,
    ipc_socket: PathBuf,
    ipc_stream_socket: PathBuf,
}

impl TestHarness {
    async fn start(node_id: &str) -> Self {
        let config = NodeConfig::for_local_node(NodeId::new(node_id))
            .with_http_bind_addr("127.0.0.1:0".parse().expect("socket address should parse"))
            .with_ipc_socket_path(NodeConfig::default_ipc_socket_path_for(node_id))
            .with_reconcile_interval(Duration::from_millis(10))
            .with_peer_authentication(orion_node::PeerAuthenticationMode::Disabled)
            .with_peer_sync_execution(
                NodeConfig::try_peer_sync_execution_from_env()
                    .expect("peer sync execution defaults should parse"),
            )
            .with_runtime_tuning(
                NodeConfig::try_runtime_tuning_from_env()
                    .expect("runtime tuning defaults should parse"),
            );
        let ipc_socket = config.ipc_socket_path.clone();
        let ipc_stream_socket = NodeConfig::default_ipc_stream_socket_path_for(node_id);
        let app = NodeApp::try_new(config.clone()).expect("node app should build");
        let (http_addr, http_server) = app
            .start_http_server_graceful(config.http_bind_addr)
            .await
            .expect("http server should start");
        let (_, ipc_server) = app
            .start_ipc_server_graceful(&config.ipc_socket_path)
            .await
            .expect("ipc server should start");
        let (_, ipc_stream_server) = app
            .start_ipc_stream_server_graceful(&ipc_stream_socket)
            .await
            .expect("ipc stream server should start");
        tokio::time::sleep(Duration::from_millis(50)).await;

        Self {
            _app: app,
            _http_task: tokio::spawn(async move {
                std::future::pending::<()>().await;
                let _ = http_server.shutdown().await;
            }),
            _ipc_task: tokio::spawn(async move {
                std::future::pending::<()>().await;
                let _ = ipc_server.shutdown().await;
            }),
            _ipc_stream_task: tokio::spawn(async move {
                std::future::pending::<()>().await;
                let _ = ipc_stream_server.shutdown().await;
            }),
            http_addr,
            client: HttpClient::try_new(format!("http://{http_addr}"))
                .expect("HTTP client should build"),
            ipc_socket,
            ipc_stream_socket,
        }
    }

    fn http_base(&self) -> String {
        format!("http://{}", self.http_addr)
    }

    async fn snapshot(&self) -> orion::control_plane::StateSnapshot {
        match self
            .client
            .send(&HttpRequestPayload::Control(Box::new(
                ControlMessage::SyncRequest(SyncRequest {
                    node_id: NodeId::new("orionctl.test.peer"),
                    desired_revision: orion::Revision::new(u64::MAX),
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
}

fn run_orionctl<const N: usize>(args: [&str; N]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_orionctl"))
        .args(args)
        .output()
        .expect("orionctl should run")
}

fn output_text(output: &std::process::Output) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    format!("stdout:\n{stdout}\nstderr:\n{stderr}")
}

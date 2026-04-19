use std::{path::PathBuf, process::Command, time::Duration};

use orion::{
    ArtifactId, ConfigSchemaId, NodeId, WorkloadId,
    control_plane::{
        ArtifactRecord, ControlMessage, DesiredStateMutation, MutationBatch, ResourceActionResult,
        ResourceActionStatus, ResourceOwnershipMode, ResourceRecord, ResourceState,
        TypedConfigValue, WorkloadConfig, WorkloadRecord,
    },
    transport::{
        http::{HttpClient, HttpRequestPayload, HttpResponsePayload},
        ipc::UnixControlStreamClient,
    },
};
use orion_node::{NodeApp, NodeConfig};

#[tokio::test(flavor = "multi_thread")]
async fn orionctl_reports_health_readiness_observability_and_snapshot() {
    let harness = TestHarness::start("node.orionctl.health").await;

    let health = run_orionctl(["health", "--http", &harness.http_base()]);
    assert!(
        health.status.success(),
        "health failed\n{}",
        output_text(&health)
    );
    assert!(String::from_utf8_lossy(&health.stdout).contains("status=Healthy"));

    let readiness = run_orionctl(["readiness", "--http", &harness.http_base()]);
    assert!(
        readiness.status.success(),
        "readiness failed\n{}",
        output_text(&readiness)
    );
    assert!(String::from_utf8_lossy(&readiness.stdout).contains("status=Ready"));

    let observability = run_orionctl(["observability", "--http", &harness.http_base()]);
    assert!(
        observability.status.success(),
        "observability failed\n{}",
        output_text(&observability)
    );
    assert!(String::from_utf8_lossy(&observability.stdout).contains("replay_success="));

    let snapshot = run_orionctl(["snapshot", "--http", &harness.http_base()]);
    assert!(
        snapshot.status.success(),
        "snapshot failed\n{}",
        output_text(&snapshot)
    );
    assert!(String::from_utf8_lossy(&snapshot.stdout).contains("desired_rev="));
}

#[tokio::test(flavor = "multi_thread")]
async fn orionctl_applies_artifact_and_workload_mutations() {
    let harness = TestHarness::start("node.orionctl.mutate").await;

    let put_artifact = run_orionctl([
        "mutate",
        "put-artifact",
        "--http",
        &harness.http_base(),
        "--artifact-id",
        "artifact.cli",
        "--content-type",
        "application/octet-stream",
        "--size-bytes",
        "128",
    ]);
    assert!(
        put_artifact.status.success(),
        "put-artifact failed\n{}",
        output_text(&put_artifact)
    );

    let put_workload = run_orionctl([
        "mutate",
        "put-workload",
        "--http",
        &harness.http_base(),
        "--workload-id",
        "workload.cli",
        "--runtime-type",
        "graph.exec.v1",
        "--artifact-id",
        "artifact.cli",
        "--assigned-node",
        "node.orionctl.mutate",
        "--desired-state",
        "stopped",
        "--restart-policy",
        "never",
    ]);
    assert!(
        put_workload.status.success(),
        "put-workload failed\n{}",
        output_text(&put_workload)
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
        snapshot
            .state
            .desired
            .workloads
            .contains_key(&WorkloadId::new("workload.cli"))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn orionctl_watch_state_prints_stream_events() {
    let harness = TestHarness::start("node.orionctl.watch").await;

    let stream = UnixControlStreamClient::connect(&harness.ipc_stream_socket)
        .await
        .expect("stream should connect");
    drop(stream);

    let watch_task = tokio::task::spawn_blocking({
        let socket = harness.ipc_stream_socket.to_string_lossy().to_string();
        move || {
            run_orionctl([
                "watch-state",
                "--socket",
                &socket,
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
    assert!(
        output.status.success(),
        "watch-state failed\n{}",
        output_text(&output)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("watch-state seq="),
        "unexpected stdout: {stdout}"
    );
    assert!(
        stdout.contains("desired_rev="),
        "unexpected stdout: {stdout}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn orionctl_rejects_invalid_desired_state() {
    let harness = TestHarness::start("node.orionctl.errors").await;

    let output = run_orionctl([
        "mutate",
        "put-workload",
        "--http",
        &harness.http_base(),
        "--workload-id",
        "workload.bad",
        "--runtime-type",
        "graph.exec.v1",
        "--artifact-id",
        "artifact.bad",
        "--desired-state",
        "broken",
    ]);
    assert!(
        !output.status.success(),
        "invalid desired state should fail"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("invalid value"),
        "unexpected stderr: {stderr}"
    );
    assert!(
        stderr.contains("--desired-state"),
        "unexpected stderr: {stderr}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn orionctl_snapshot_displays_schema_and_resource_provenance() {
    let harness = TestHarness::start("node.orionctl.snapshot.detail").await;

    let mut desired = harness._app.state_snapshot().state.desired;
    desired.put_workload(
        WorkloadRecord::builder("workload.camera", "camera.controller.v1", "artifact.camera")
            .config(
                WorkloadConfig::new(ConfigSchemaId::new("camera.controller.config.v1"))
                    .field("width", TypedConfigValue::UInt(1280)),
            )
            .assigned_to("node.orionctl.snapshot.detail")
            .build(),
    );
    desired.put_resource(
        ResourceRecord::builder(
            "resource.camera.stream.front",
            "camera.frame_stream",
            "provider.camera",
        )
        .ownership_mode(ResourceOwnershipMode::SharedRead)
        .realized_for_workload("workload.camera")
        .source_resource("resource.camera.raw.front")
        .source_workload("workload.camera")
        .realized_by_executor("executor.camera-stack")
        .state(
            ResourceState::new(42).with_action_result(ResourceActionResult {
                action_kind: "gpio".into(),
                status: ResourceActionStatus::Applied,
                data: Some(TypedConfigValue::Bool(true)),
                error: None,
            }),
        )
        .build(),
    );
    harness._app.replace_desired(desired);

    let snapshot = run_orionctl(["snapshot", "--http", &harness.http_base()]);
    assert!(
        snapshot.status.success(),
        "snapshot failed\n{}",
        output_text(&snapshot)
    );
    let stdout = String::from_utf8_lossy(&snapshot.stdout);
    assert!(stdout.contains("config_schema=camera.controller.config.v1"));
    assert!(stdout.contains("derived_from_workload=workload.camera"));
    assert!(stdout.contains("published_by_executor=executor.camera-stack"));
    assert!(stdout.contains("action_kind=gpio"));
    assert!(stdout.contains("action_status=Applied"));
    assert!(stdout.contains("action_data=true"));
}

struct TestHarness {
    _app: NodeApp,
    _http_task: tokio::task::JoinHandle<()>,
    _ipc_task: tokio::task::JoinHandle<()>,
    _ipc_stream_task: tokio::task::JoinHandle<()>,
    http_addr: std::net::SocketAddr,
    client: HttpClient,
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
                ControlMessage::SyncRequest(orion::control_plane::SyncRequest {
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

mod support;

use std::time::Duration;

use orion::{
    ArtifactId, NodeId, WorkloadId,
    control_plane::{
        ArtifactRecord, ControlMessage, DesiredStateMutation, ExecutorRecord, MutationBatch,
        TypedConfigValue, WorkloadConfig, WorkloadRecord,
    },
    transport::{
        http::{HttpRequestPayload, HttpResponsePayload},
        ipc::UnixControlStreamClient,
    },
};
use orion_control_plane::MaintenanceMode;
use orion_core::{ConfigSchemaId, RuntimeType};

use support::{TestHarness, output_text, run_orionctl, run_orionctl_with_env, temp_spec_path};

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
async fn orionctl_apply_workload_accepts_typed_config_flags() {
    let harness = TestHarness::start("node.orionctl.config").await;
    let mut desired = harness._app.state_snapshot().state.desired;
    desired.put_executor(
        ExecutorRecord::builder("executor.config", "node.orionctl.config")
            .runtime_type("graph.exec.v1")
            .build(),
    );
    harness._app.replace_desired(desired);

    let put_workload = run_orionctl([
        "apply",
        "workload",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "--workload-id",
        "workload.config",
        "--runtime-type",
        "graph.exec.v1",
        "--artifact-id",
        "artifact.config",
        "--config-schema",
        "graph.workload.config.v1",
        "--config-string",
        "graph.kind=inline",
        "--config-string",
        "graph.inline=demo",
        "--config-bool",
        "plugin.enabled=true",
        "--config-uint",
        "binding.count=3",
    ]);
    assert!(
        put_workload.status.success(),
        "{}",
        output_text(&put_workload)
    );

    let snapshot = harness.snapshot().await;
    let workload = snapshot
        .state
        .desired
        .workloads
        .get(&WorkloadId::new("workload.config"))
        .expect("configured workload should exist");
    let config = workload.config.as_ref().expect("config should be stored");
    assert_eq!(
        config.schema_id,
        ConfigSchemaId::new("graph.workload.config.v1")
    );
    assert_eq!(
        config.payload.get("graph.kind"),
        Some(&TypedConfigValue::String("inline".to_owned()))
    );
    assert_eq!(
        config.payload.get("graph.inline"),
        Some(&TypedConfigValue::String("demo".to_owned()))
    );
    assert_eq!(
        config.payload.get("plugin.enabled"),
        Some(&TypedConfigValue::Bool(true))
    );
    assert_eq!(
        config.payload.get("binding.count"),
        Some(&TypedConfigValue::UInt(3))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn orionctl_apply_workload_accepts_json_spec_file() {
    let harness = TestHarness::start("node.orionctl.spec").await;
    let mut desired = harness._app.state_snapshot().state.desired;
    desired.put_executor(
        ExecutorRecord::builder("executor.spec", "node.orionctl.spec")
            .runtime_type("graph.exec.v1")
            .build(),
    );
    harness._app.replace_desired(desired);

    let spec = WorkloadRecord::builder(
        WorkloadId::new("workload.spec"),
        RuntimeType::new("graph.exec.v1"),
        ArtifactId::new("artifact.spec"),
    )
    .assigned_to(NodeId::new("node.orionctl.spec"))
    .config(
        WorkloadConfig::new(ConfigSchemaId::new("graph.workload.config.v1"))
            .field("graph.kind", TypedConfigValue::String("inline".to_owned()))
            .field(
                "graph.inline",
                TypedConfigValue::String("{\"nodes\":[]}".to_owned()),
            ),
    )
    .build();
    let spec_path = temp_spec_path("orionctl-workload-spec.json");
    std::fs::write(
        &spec_path,
        serde_json::to_vec(&spec).expect("spec should serialize"),
    )
    .expect("spec file should write");

    let put_workload = run_orionctl([
        "apply",
        "workload",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "--spec",
        &spec_path.to_string_lossy(),
    ]);
    assert!(
        put_workload.status.success(),
        "{}",
        output_text(&put_workload)
    );

    let snapshot = harness.snapshot().await;
    let stored = snapshot
        .state
        .desired
        .workloads
        .get(&WorkloadId::new("workload.spec"))
        .expect("spec workload should exist");
    assert_eq!(stored, &spec);

    let _ = std::fs::remove_file(spec_path);
}

#[tokio::test(flavor = "multi_thread")]
async fn orionctl_apply_workload_accepts_yaml_spec_file() {
    let harness = TestHarness::start("node.orionctl.spec.yaml").await;
    let mut desired = harness._app.state_snapshot().state.desired;
    desired.put_executor(
        ExecutorRecord::builder("executor.spec.yaml", "node.orionctl.spec.yaml")
            .runtime_type("graph.exec.v1")
            .build(),
    );
    harness._app.replace_desired(desired);

    let spec = WorkloadRecord::builder(
        WorkloadId::new("workload.spec.yaml"),
        RuntimeType::new("graph.exec.v1"),
        ArtifactId::new("artifact.spec.yaml"),
    )
    .assigned_to(NodeId::new("node.orionctl.spec.yaml"))
    .config(
        WorkloadConfig::new(ConfigSchemaId::new("graph.workload.config.v1"))
            .field("graph.kind", TypedConfigValue::String("inline".to_owned()))
            .field("graph.inline", TypedConfigValue::String("yaml".to_owned())),
    )
    .build();
    let spec_path = temp_spec_path("orionctl-workload-spec.yaml");
    std::fs::write(
        &spec_path,
        serde_yaml::to_string(&spec).expect("yaml spec should serialize"),
    )
    .expect("yaml spec file should write");

    let put_workload = run_orionctl([
        "apply",
        "workload",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "--spec",
        &spec_path.to_string_lossy(),
    ]);
    assert!(
        put_workload.status.success(),
        "{}",
        output_text(&put_workload)
    );

    let snapshot = harness.snapshot().await;
    let stored = snapshot
        .state
        .desired
        .workloads
        .get(&WorkloadId::new("workload.spec.yaml"))
        .expect("yaml spec workload should exist");
    assert_eq!(stored, &spec);

    let _ = std::fs::remove_file(spec_path);
}

#[tokio::test(flavor = "multi_thread")]
async fn orionctl_apply_workload_accepts_toml_spec_file() {
    let harness = TestHarness::start("node.orionctl.spec.toml").await;
    let mut desired = harness._app.state_snapshot().state.desired;
    desired.put_executor(
        ExecutorRecord::builder("executor.spec.toml", "node.orionctl.spec.toml")
            .runtime_type("graph.exec.v1")
            .build(),
    );
    harness._app.replace_desired(desired);

    let spec = WorkloadRecord::builder(
        WorkloadId::new("workload.spec.toml"),
        RuntimeType::new("graph.exec.v1"),
        ArtifactId::new("artifact.spec.toml"),
    )
    .assigned_to(NodeId::new("node.orionctl.spec.toml"))
    .config(
        WorkloadConfig::new(ConfigSchemaId::new("graph.workload.config.v1"))
            .field("graph.kind", TypedConfigValue::String("inline".to_owned()))
            .field("graph.inline", TypedConfigValue::String("toml".to_owned())),
    )
    .build();
    let spec_path = temp_spec_path("orionctl-workload-spec.toml");
    std::fs::write(
        &spec_path,
        toml::to_string_pretty(&spec).expect("toml spec should serialize"),
    )
    .expect("toml spec file should write");

    let put_workload = run_orionctl([
        "apply",
        "workload",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "--spec",
        &spec_path.to_string_lossy(),
    ]);
    assert!(
        put_workload.status.success(),
        "{}",
        output_text(&put_workload)
    );

    let snapshot = harness.snapshot().await;
    let stored = snapshot
        .state
        .desired
        .workloads
        .get(&WorkloadId::new("workload.spec.toml"))
        .expect("toml spec workload should exist");
    assert_eq!(stored, &spec);

    let _ = std::fs::remove_file(spec_path);
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

#[tokio::test(flavor = "multi_thread")]
async fn orionctl_local_commands_default_to_ipc_when_http_is_omitted() {
    let harness = TestHarness::start("node.orionctl.defaults").await;

    let observability = run_orionctl_with_env(
        ["get", "observability"],
        &[
            (
                "ORION_NODE_IPC_SOCKET",
                harness.ipc_socket.to_string_lossy().as_ref(),
            ),
            (
                "ORION_NODE_IPC_STREAM_SOCKET",
                harness.ipc_stream_socket.to_string_lossy().as_ref(),
            ),
        ],
    );
    assert!(
        observability.status.success(),
        "{}",
        output_text(&observability)
    );
    assert!(String::from_utf8_lossy(&observability.stdout).contains("mutation_success="));

    let peers = run_orionctl_with_env(
        ["peers", "list", "-o", "json"],
        &[(
            "ORION_NODE_IPC_SOCKET",
            harness.ipc_socket.to_string_lossy().as_ref(),
        )],
    );
    assert!(peers.status.success(), "{}", output_text(&peers));
    let peers_json: serde_json::Value =
        serde_json::from_slice(&peers.stdout).expect("json output should parse");
    assert!(peers_json.get("http_mutual_tls_mode").is_some());

    let maintenance = run_orionctl_with_env(
        ["maintenance", "status", "-o", "json"],
        &[(
            "ORION_NODE_IPC_SOCKET",
            harness.ipc_socket.to_string_lossy().as_ref(),
        )],
    );
    assert!(
        maintenance.status.success(),
        "{}",
        output_text(&maintenance)
    );
    let maintenance_json: serde_json::Value =
        serde_json::from_slice(&maintenance.stdout).expect("json output should parse");
    assert_eq!(maintenance_json["state"]["mode"], "normal");
}

#[tokio::test(flavor = "multi_thread")]
async fn orionctl_maintenance_commands_update_local_node_state() {
    let harness = TestHarness::start("node.orionctl.maintenance").await;

    let enter = run_orionctl([
        "maintenance",
        "enter",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "--allow-runtime",
        "helios.updater.v1",
        "--allow-workload",
        "workload.updater",
        "-o",
        "json",
    ]);
    assert!(enter.status.success(), "{}", output_text(&enter));
    let entered: serde_json::Value =
        serde_json::from_slice(&enter.stdout).expect("json output should parse");
    assert_eq!(entered["state"]["mode"], "maintenance");

    let status = run_orionctl([
        "maintenance",
        "status",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "-o",
        "json",
    ]);
    assert!(status.status.success(), "{}", output_text(&status));
    let status_json: serde_json::Value =
        serde_json::from_slice(&status.stdout).expect("json output should parse");
    assert_eq!(status_json["state"]["mode"], "maintenance");
    assert_eq!(
        status_json["state"]["allow_runtime_types"][0],
        "helios.updater.v1"
    );
    assert_eq!(
        status_json["state"]["allow_workload_ids"][0],
        "workload.updater"
    );

    let isolate = run_orionctl([
        "maintenance",
        "isolate",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "-o",
        "json",
    ]);
    assert!(isolate.status.success(), "{}", output_text(&isolate));
    let isolated: serde_json::Value =
        serde_json::from_slice(&isolate.stdout).expect("json output should parse");
    assert_eq!(isolated["state"]["mode"], "isolated");
    assert_eq!(isolated["peer_sync_paused"], true);
    assert_eq!(isolated["remote_desired_state_blocked"], true);

    let exit = run_orionctl([
        "maintenance",
        "exit",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "-o",
        "json",
    ]);
    assert!(exit.status.success(), "{}", output_text(&exit));
    let exited: serde_json::Value =
        serde_json::from_slice(&exit.stdout).expect("json output should parse");
    assert_eq!(exited["state"]["mode"], "normal");

    assert_eq!(
        harness._app.maintenance_status().state.mode,
        MaintenanceMode::Normal
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn orionctl_supports_yaml_and_toml_structured_output() {
    let harness = TestHarness::start("node.orionctl.output").await;

    let yaml_output = run_orionctl([
        "get",
        "snapshot",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "-o",
        "yaml",
    ]);
    assert!(
        yaml_output.status.success(),
        "{}",
        output_text(&yaml_output)
    );
    let yaml: serde_yaml::Value =
        serde_yaml::from_slice(&yaml_output.stdout).expect("yaml output should parse");
    assert!(yaml.get("state").is_some());

    let toml_output = run_orionctl([
        "peers",
        "list",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "-o",
        "toml",
    ]);
    assert!(
        toml_output.status.success(),
        "{}",
        output_text(&toml_output)
    );
    let toml: toml::Value = toml::from_str(&String::from_utf8_lossy(&toml_output.stdout))
        .expect("toml output should parse");
    assert!(toml.get("value").is_some());
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

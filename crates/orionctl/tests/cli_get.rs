mod support;

use orion::{
    control_plane::{
        ArtifactRecord, ControlMessage, DesiredState, DesiredStateMutation, ExecutorRecord,
        MutationBatch, NodeRecord, WorkloadRecord,
    },
    transport::http::{HttpRequestPayload, HttpResponsePayload},
};

use support::{TestHarness, output_text, run_orionctl, run_orionctl_with_env};

#[tokio::test(flavor = "multi_thread")]
async fn orionctl_get_reports_health_readiness_observability_and_snapshot() {
    let harness = TestHarness::start("node.orionctl.get").await;
    let mut desired = harness._app.state_snapshot().state.desired;
    desired.put_node(
        NodeRecord::builder("node.orionctl.get")
            .label("role=edge")
            .build(),
    );
    desired.put_artifact(
        ArtifactRecord::builder("artifact.get")
            .content_type("application/octet-stream")
            .size_bytes(64)
            .build(),
    );
    desired.put_executor(
        ExecutorRecord::builder("executor.get", "node.orionctl.get")
            .runtime_type("graph.exec.v1")
            .build(),
    );
    desired.put_workload(
        WorkloadRecord::builder("workload.get", "graph.exec.v1", "artifact.get")
            .desired_state(DesiredState::Running)
            .assigned_to("node.orionctl.get")
            .build(),
    );
    harness._app.replace_desired(desired);

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

    let nodes = run_orionctl([
        "get",
        "nodes",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "-o",
        "json",
    ]);
    assert!(nodes.status.success(), "{}", output_text(&nodes));
    let nodes_json: serde_json::Value =
        serde_json::from_slice(&nodes.stdout).expect("json output should parse");
    let node_array = nodes_json.as_array().expect("json should be an array");
    assert!(
        node_array
            .iter()
            .any(|node| node["node_id"] == "node.orionctl.get"),
        "expected seeded node in get nodes output"
    );

    let artifacts = run_orionctl([
        "get",
        "artifacts",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "-o",
        "json",
    ]);
    assert!(artifacts.status.success(), "{}", output_text(&artifacts));
    let artifacts_json: serde_json::Value =
        serde_json::from_slice(&artifacts.stdout).expect("json output should parse");
    let artifact_array = artifacts_json.as_array().expect("json should be an array");
    assert!(
        artifact_array
            .iter()
            .any(|artifact| artifact["artifact_id"] == "artifact.get"),
        "expected seeded artifact in get artifacts output"
    );

    let response = harness
        .client
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::Mutations(MutationBatch {
                base_revision: harness.snapshot().await.state.desired.revision,
                mutations: vec![DesiredStateMutation::PutArtifact(
                    ArtifactRecord::builder("artifact.events").build(),
                )],
            }),
        )))
        .await
        .expect("mutation should succeed");
    assert_eq!(response, HttpResponsePayload::Accepted);

    let events = run_orionctl([
        "get",
        "events",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "-o",
        "json",
    ]);
    assert!(events.status.success(), "{}", output_text(&events));
    let events_json: serde_json::Value =
        serde_json::from_slice(&events.stdout).expect("json output should parse");
    let event_array = events_json.as_array().expect("json should be an array");
    assert!(
        !event_array.is_empty(),
        "expected at least one recent observability event"
    );

    let node = run_orionctl([
        "get",
        "node",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "node.orionctl.get",
    ]);
    assert!(node.status.success(), "{}", output_text(&node));
    let node_stdout = String::from_utf8_lossy(&node.stdout);
    assert!(node_stdout.contains("node id=node.orionctl.get"));
    assert!(node_stdout.contains("schedulable=true"));

    let artifact = run_orionctl([
        "get",
        "artifact",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "artifact.get",
    ]);
    assert!(artifact.status.success(), "{}", output_text(&artifact));
    assert!(String::from_utf8_lossy(&artifact.stdout).contains("artifact id=artifact.get"));

    let workload = run_orionctl([
        "get",
        "workload",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "workload.get",
    ]);
    assert!(workload.status.success(), "{}", output_text(&workload));
    let workload_stdout = String::from_utf8_lossy(&workload.stdout);
    assert!(workload_stdout.contains("workload id=workload.get"));
    assert!(workload_stdout.contains("assigned_node=node.orionctl.get"));

    let events_summary = run_orionctl([
        "get",
        "events",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
    ]);
    assert!(
        events_summary.status.success(),
        "{}",
        output_text(&events_summary)
    );
    let events_stdout = String::from_utf8_lossy(&events_summary.stdout);
    assert!(events_stdout.contains("kind=mutation_apply"));
    assert!(events_stdout.contains("status=success"));

    let filtered_nodes = run_orionctl([
        "get",
        "nodes",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "--label",
        "role=edge",
        "-o",
        "json",
    ]);
    assert!(
        filtered_nodes.status.success(),
        "{}",
        output_text(&filtered_nodes)
    );
    let filtered_nodes_json: serde_json::Value =
        serde_json::from_slice(&filtered_nodes.stdout).expect("json output should parse");
    assert_eq!(
        filtered_nodes_json
            .as_array()
            .expect("json should be an array")
            .len(),
        1
    );
}

#[test]
fn orionctl_help_examples_cover_singular_gets_and_filters() {
    let output = run_orionctl(["get", "--help"]);
    assert!(output.status.success(), "{}", output_text(&output));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("get workload workload.camera"));
    assert!(stdout.contains("get events --http http://127.0.0.1:9100 --kind mutation"));
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

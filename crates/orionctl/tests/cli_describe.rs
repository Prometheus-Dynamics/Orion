mod support;

use orion::{
    WorkloadId,
    control_plane::{
        ArtifactRecord, DesiredState, ExecutorRecord, NodeRecord, ProviderRecord, ResourceRecord,
        WorkloadRecord,
    },
};

use support::{TestHarness, output_text, run_orionctl};

#[tokio::test(flavor = "multi_thread")]
async fn orionctl_describe_reports_full_object_state_and_blockers() {
    let harness = TestHarness::start("node.orionctl.describe").await;
    let mut desired = harness._app.state_snapshot().state.desired;
    desired.put_node(
        NodeRecord::builder("node.orionctl.describe")
            .label("role=edge")
            .build(),
    );
    desired.put_artifact(
        ArtifactRecord::builder("artifact.describe")
            .content_type("application/octet-stream")
            .size_bytes(256)
            .build(),
    );
    desired.put_provider(
        ProviderRecord::builder("provider.describe", "node.orionctl.describe")
            .resource_type("camera.device")
            .build(),
    );
    desired.put_resource(
        ResourceRecord::builder("resource.describe", "camera.device", "provider.describe")
            .health(orion::control_plane::HealthState::Healthy)
            .availability(orion::control_plane::AvailabilityState::Available)
            .build(),
    );
    desired.put_workload(
        WorkloadRecord::builder("workload.describe", "graph.exec.v1", "artifact.describe")
            .desired_state(orion::control_plane::DesiredState::Running)
            .build(),
    );
    harness._app.replace_desired(desired);
    let snapshot = harness.snapshot().await;
    assert!(
        snapshot
            .state
            .desired
            .workloads
            .contains_key(&WorkloadId::new("workload.describe"))
    );

    let enter = run_orionctl([
        "maintenance",
        "enter",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
    ]);
    assert!(enter.status.success(), "{}", output_text(&enter));

    let workloads = run_orionctl([
        "get",
        "workloads",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "-o",
        "json",
    ]);
    assert!(workloads.status.success(), "{}", output_text(&workloads));
    assert!(
        String::from_utf8_lossy(&workloads.stdout).contains("workload.describe"),
        "{}",
        output_text(&workloads)
    );

    let workload = run_orionctl([
        "describe",
        "workload",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "workload.describe",
    ]);
    assert!(workload.status.success(), "{}", output_text(&workload));
    let workload_stdout = String::from_utf8_lossy(&workload.stdout);
    assert!(workload_stdout.contains("workload: workload.describe"));
    assert!(workload_stdout.contains("blockers:"));
    assert!(workload_stdout.contains("no assigned node"));

    let node = run_orionctl([
        "describe",
        "node",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "node.orionctl.describe",
    ]);
    assert!(node.status.success(), "{}", output_text(&node));
    let node_stdout = String::from_utf8_lossy(&node.stdout);
    assert!(node_stdout.contains("node: node.orionctl.describe"));
    assert!(node_stdout.contains("maintenance_mode: maintenance"));
    assert!(node_stdout.contains("workloads: -"));

    let artifact = run_orionctl([
        "describe",
        "artifact",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "artifact.describe",
        "-o",
        "json",
    ]);
    assert!(artifact.status.success(), "{}", output_text(&artifact));
    let artifact_json: serde_json::Value =
        serde_json::from_slice(&artifact.stdout).expect("json output should parse");
    assert_eq!(
        artifact_json["artifact"]["artifact_id"],
        "artifact.describe"
    );
    assert_eq!(
        artifact_json["referenced_by_workloads"][0],
        "workload.describe"
    );

    let resource = run_orionctl([
        "describe",
        "resource",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "resource.describe",
    ]);
    assert!(resource.status.success(), "{}", output_text(&resource));
    let resource_stdout = String::from_utf8_lossy(&resource.stdout);
    assert!(resource_stdout.contains("resource: resource.describe"));
    assert!(resource_stdout.contains("type: camera.device"));
}

#[tokio::test(flavor = "multi_thread")]
async fn orionctl_describe_reports_maintenance_blockers() {
    let harness = TestHarness::start("node.orionctl.blockers").await;
    let mut desired = harness._app.state_snapshot().state.desired;
    desired.put_node(NodeRecord::builder("node.orionctl.blockers").build());
    desired.put_artifact(ArtifactRecord::builder("artifact.blockers").build());
    desired.put_executor(
        ExecutorRecord::builder("executor.blockers", "node.orionctl.blockers")
            .runtime_type("graph.exec.v1")
            .build(),
    );
    desired.put_workload(
        WorkloadRecord::builder("workload.blockers", "graph.exec.v1", "artifact.blockers")
            .desired_state(DesiredState::Running)
            .assigned_to("node.orionctl.blockers")
            .build(),
    );
    harness._app.replace_desired(desired);

    let enter = run_orionctl([
        "maintenance",
        "enter",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
    ]);
    assert!(enter.status.success(), "{}", output_text(&enter));

    let workload = run_orionctl([
        "describe",
        "workload",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "workload.blockers",
    ]);
    assert!(workload.status.success(), "{}", output_text(&workload));
    let stdout = String::from_utf8_lossy(&workload.stdout);
    assert!(stdout.contains("assigned node node.orionctl.blockers is in maintenance mode"));
    assert!(stdout.contains("runtime graph.exec.v1 is not allowlisted during maintenance"));

    let node = run_orionctl([
        "describe",
        "node",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "node.orionctl.blockers",
    ]);
    assert!(node.status.success(), "{}", output_text(&node));
    assert!(String::from_utf8_lossy(&node.stdout).contains("maintenance_mode: maintenance"));
}

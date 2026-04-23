mod support;

use orion::{
    ArtifactId, NodeId, WorkloadId,
    control_plane::{
        ArtifactRecord, ExecutorRecord, NodeRecord, TypedConfigValue, WorkloadConfig,
        WorkloadRecord,
    },
};
use orion_core::{ConfigSchemaId, RuntimeType};

use support::{TestHarness, output_text, run_orionctl, temp_spec_path};

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
async fn orionctl_apply_and_delete_support_dry_run() {
    let harness = TestHarness::start("node.orionctl.dryrun").await;
    let mut desired = harness._app.state_snapshot().state.desired;
    desired.put_node(NodeRecord::builder("node.orionctl.dryrun").build());
    desired.put_artifact(ArtifactRecord::builder("artifact.present").build());
    desired.put_executor(
        ExecutorRecord::builder("executor.dryrun", "node.orionctl.dryrun")
            .runtime_type("graph.exec.v1")
            .build(),
    );
    desired.put_workload(
        WorkloadRecord::builder("workload.present", "graph.exec.v1", "artifact.present")
            .assigned_to("node.orionctl.dryrun")
            .build(),
    );
    harness._app.replace_desired(desired);

    let apply_ok = run_orionctl([
        "apply",
        "workload",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "--workload-id",
        "workload.preview",
        "--runtime-type",
        "graph.exec.v1",
        "--artifact-id",
        "artifact.present",
        "--assigned-node",
        "node.orionctl.dryrun",
        "--desired-state",
        "running",
        "--dry-run",
    ]);
    assert!(apply_ok.status.success(), "{}", output_text(&apply_ok));
    let apply_ok_stdout = String::from_utf8_lossy(&apply_ok.stdout);
    assert!(apply_ok_stdout.contains("dry-run: accepted"));

    let apply_bad = run_orionctl([
        "apply",
        "workload",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "--workload-id",
        "workload.preview.bad",
        "--runtime-type",
        "graph.exec.v1",
        "--artifact-id",
        "artifact.missing",
        "--assigned-node",
        "node.orionctl.dryrun",
        "--desired-state",
        "running",
        "--dry-run",
    ]);
    assert!(apply_bad.status.success(), "{}", output_text(&apply_bad));
    let apply_bad_stdout = String::from_utf8_lossy(&apply_bad.stdout);
    assert!(apply_bad_stdout.contains("dry-run: rejected"));
    assert!(apply_bad_stdout.contains("artifact artifact.missing is missing from desired state"));

    let delete_ok = run_orionctl([
        "delete",
        "workload",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "--workload-id",
        "workload.present",
        "--dry-run",
    ]);
    assert!(delete_ok.status.success(), "{}", output_text(&delete_ok));
    assert!(String::from_utf8_lossy(&delete_ok.stdout).contains("dry-run: accepted"));

    let delete_bad = run_orionctl([
        "delete",
        "artifact",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "--artifact-id",
        "artifact.absent",
        "--dry-run",
    ]);
    assert!(delete_bad.status.success(), "{}", output_text(&delete_bad));
    assert!(String::from_utf8_lossy(&delete_bad.stdout).contains("dry-run: rejected"));
}

#[test]
fn orionctl_apply_help_includes_dry_run_example() {
    let output = run_orionctl(["apply", "workload", "--help"]);
    assert!(output.status.success(), "{}", output_text(&output));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--dry-run"));
    assert!(stdout.contains(
        "apply workload --socket /run/orion/control.sock --spec workload.yaml --dry-run"
    ));
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

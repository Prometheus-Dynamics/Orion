mod support;

use orion_control_plane::MaintenanceMode;

use support::{TestHarness, output_text, run_orionctl};

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

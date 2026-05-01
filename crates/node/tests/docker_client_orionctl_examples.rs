mod support;

use orion::{
    control_plane::{ControlMessage, DesiredStateMutation, ExecutorRecord, MutationBatch},
    transport::http::{HttpRequestPayload, HttpResponsePayload},
};
use support::docker_cluster::{DockerCluster, stopped_workload};

fn output_text(output: &std::process::Output) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    format!("stdout:\n{stdout}\nstderr:\n{stderr}")
}

async fn assert_workload_absent_for(
    cluster: &DockerCluster,
    node: &str,
    workload_id: &str,
    duration: std::time::Duration,
) {
    let workload_id = orion::WorkloadId::new(workload_id);
    let deadline = std::time::Instant::now() + duration;
    loop {
        let snapshot = cluster.snapshot(node).await;
        assert!(
            !snapshot.state.desired.workloads.contains_key(&workload_id),
            "node {node} unexpectedly adopted workload {} during absence window\n{}",
            workload_id,
            cluster.compose_logs()
        );

        if std::time::Instant::now() >= deadline {
            return;
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_client_example_orionctl_get_reads_cluster_state() {
    let cluster = DockerCluster::start("client-example-orionctl-get").await;
    cluster.wait_for_all_http().await;

    let output = cluster.exec_service_output(
        "node-a",
        [
            "orionctl",
            "get",
            "snapshot",
            "--http",
            "http://node-a:9100",
            "-o",
            "json",
        ],
    );
    assert!(
        output.status.success(),
        "orionctl get snapshot failed\n{}",
        output_text(&output)
    );

    let snapshot: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("snapshot json should parse");
    assert_eq!(snapshot["state"]["desired"]["revision"], 0);

    let health = cluster.exec_service_output(
        "node-b",
        ["orionctl", "get", "health", "--http", "http://node-b:9100"],
    );
    assert!(
        health.status.success(),
        "orionctl get health failed\n{}",
        output_text(&health)
    );
    let stdout = String::from_utf8_lossy(&health.stdout);
    assert!(stdout.contains("health node=node-b"));
    assert!(stdout.contains("alive=true"));
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_client_example_orionctl_apply_delete_over_local_ipc_propagates_clusterwide() {
    let cluster = DockerCluster::start("client-example-orionctl-apply").await;
    cluster.wait_for_all_http().await;

    let seed_executor = MutationBatch {
        base_revision: cluster.snapshot("node-a").await.state.desired.revision,
        mutations: vec![DesiredStateMutation::PutExecutor(
            ExecutorRecord::builder("executor.engine", "node-a")
                .runtime_type("graph.exec.v1")
                .build(),
        )],
    };
    let response = cluster
        .client("node-a")
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::Mutations(seed_executor),
        )))
        .await
        .expect("executor seed mutation should apply");
    assert_eq!(response, HttpResponsePayload::Accepted);

    let apply_artifact = cluster.exec_service_output(
        "node-a",
        [
            "orionctl",
            "apply",
            "artifact",
            "--socket",
            "/tmp/orion-node-a-control.sock",
            "--artifact-id",
            "artifact.cluster.cli",
        ],
    );
    assert!(
        apply_artifact.status.success(),
        "orionctl apply artifact failed\n{}",
        output_text(&apply_artifact)
    );

    let apply_workload = cluster.exec_service_output(
        "node-a",
        [
            "orionctl",
            "apply",
            "workload",
            "--socket",
            "/tmp/orion-node-a-control.sock",
            "--workload-id",
            "workload.cluster.cli",
            "--runtime-type",
            "graph.exec.v1",
            "--artifact-id",
            "artifact.cluster.cli",
            "--assigned-node",
            "node-a",
            "--desired-state",
            "running",
        ],
    );
    assert!(
        apply_workload.status.success(),
        "orionctl apply workload failed\n{}",
        output_text(&apply_workload)
    );

    cluster
        .wait_for_workload(
            "node-b",
            "workload.cluster.cli",
            std::time::Duration::from_secs(10),
        )
        .await;

    let get_workloads = cluster.exec_service_output(
        "node-c",
        [
            "orionctl",
            "get",
            "workloads",
            "--socket",
            "/tmp/orion-node-c-control.sock",
            "-o",
            "json",
        ],
    );
    assert!(
        get_workloads.status.success(),
        "orionctl get workloads failed\n{}",
        output_text(&get_workloads)
    );
    let workloads: serde_json::Value =
        serde_json::from_slice(&get_workloads.stdout).expect("workloads json should parse");
    let workloads = workloads.as_array().expect("workloads should be an array");
    assert!(
        workloads
            .iter()
            .any(|workload| workload["workload_id"] == "workload.cluster.cli")
    );

    let delete_workload = cluster.exec_service_output(
        "node-a",
        [
            "orionctl",
            "delete",
            "workload",
            "--socket",
            "/tmp/orion-node-a-control.sock",
            "--workload-id",
            "workload.cluster.cli",
        ],
    );
    assert!(
        delete_workload.status.success(),
        "orionctl delete workload failed\n{}",
        output_text(&delete_workload)
    );

    cluster
        .wait_for_workload_absent(
            "node-c",
            "workload.cluster.cli",
            std::time::Duration::from_secs(10),
        )
        .await;
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_client_example_orionctl_maintenance_persists_across_live_cluster_restart() {
    let cluster = DockerCluster::start("client-example-orionctl-maintenance").await;
    cluster.wait_for_all_http().await;

    let enter = cluster.exec_service_output(
        "node-a",
        [
            "orionctl",
            "maintenance",
            "enter",
            "--socket",
            "/tmp/orion-node-a-control.sock",
            "--allow-runtime",
            "graph.exec.v1",
            "-o",
            "json",
        ],
    );
    assert!(
        enter.status.success(),
        "orionctl maintenance enter failed\n{}",
        output_text(&enter)
    );
    let entered: serde_json::Value =
        serde_json::from_slice(&enter.stdout).expect("maintenance enter json should parse");
    assert_eq!(entered["state"]["mode"], "maintenance");
    assert_eq!(entered["peer_sync_paused"], false);
    assert_eq!(entered["remote_desired_state_blocked"], false);

    cluster.restart_service("node-a");
    cluster.wait_for_http("node-a").await;

    let status = cluster.exec_service_output(
        "node-a",
        [
            "orionctl",
            "maintenance",
            "status",
            "--socket",
            "/tmp/orion-node-a-control.sock",
            "-o",
            "json",
        ],
    );
    assert!(
        status.status.success(),
        "orionctl maintenance status failed after restart\n{}",
        output_text(&status)
    );
    let status_json: serde_json::Value =
        serde_json::from_slice(&status.stdout).expect("maintenance status json should parse");
    assert_eq!(status_json["state"]["mode"], "maintenance");
    assert_eq!(
        status_json["state"]["allow_runtime_types"],
        serde_json::json!(["graph.exec.v1"])
    );

    let exit = cluster.exec_service_output(
        "node-a",
        [
            "orionctl",
            "maintenance",
            "exit",
            "--socket",
            "/tmp/orion-node-a-control.sock",
            "-o",
            "json",
        ],
    );
    assert!(
        exit.status.success(),
        "orionctl maintenance exit failed\n{}",
        output_text(&exit)
    );
    let exited: serde_json::Value =
        serde_json::from_slice(&exit.stdout).expect("maintenance exit json should parse");
    assert_eq!(exited["state"]["mode"], "normal");
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_client_example_orionctl_maintenance_cordon_preserves_peer_sync() {
    let cluster = DockerCluster::start("client-example-orionctl-cordon").await;
    cluster.wait_for_all_http().await;

    let cordon = cluster.exec_service_output(
        "node-a",
        [
            "orionctl",
            "maintenance",
            "cordon",
            "--socket",
            "/tmp/orion-node-a-control.sock",
            "-o",
            "json",
        ],
    );
    assert!(
        cordon.status.success(),
        "orionctl maintenance cordon failed\n{}",
        output_text(&cordon)
    );
    let cordoned: serde_json::Value =
        serde_json::from_slice(&cordon.stdout).expect("maintenance cordon json should parse");
    assert_eq!(cordoned["state"]["mode"], "cordoned");
    assert_eq!(cordoned["peer_sync_paused"], false);
    assert_eq!(cordoned["remote_desired_state_blocked"], false);

    cluster
        .put_workload(
            "node-b",
            stopped_workload("workload.cordoned.remote", "node-b"),
        )
        .await;

    cluster
        .wait_for_workload(
            "node-a",
            "workload.cordoned.remote",
            std::time::Duration::from_secs(10),
        )
        .await;
    cluster
        .wait_for_workload(
            "node-c",
            "workload.cordoned.remote",
            std::time::Duration::from_secs(10),
        )
        .await;

    let exit = cluster.exec_service_output(
        "node-a",
        [
            "orionctl",
            "maintenance",
            "exit",
            "--socket",
            "/tmp/orion-node-a-control.sock",
            "-o",
            "json",
        ],
    );
    assert!(
        exit.status.success(),
        "orionctl maintenance exit failed after cordon\n{}",
        output_text(&exit)
    );
    let exited: serde_json::Value =
        serde_json::from_slice(&exit.stdout).expect("maintenance exit json should parse");
    assert_eq!(exited["state"]["mode"], "normal");
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_client_example_orionctl_maintenance_drain_preserves_peer_sync() {
    let cluster = DockerCluster::start("client-example-orionctl-drain").await;
    cluster.wait_for_all_http().await;

    let drain = cluster.exec_service_output(
        "node-a",
        [
            "orionctl",
            "maintenance",
            "drain",
            "--socket",
            "/tmp/orion-node-a-control.sock",
            "-o",
            "json",
        ],
    );
    assert!(
        drain.status.success(),
        "orionctl maintenance drain failed\n{}",
        output_text(&drain)
    );
    let draining: serde_json::Value =
        serde_json::from_slice(&drain.stdout).expect("maintenance drain json should parse");
    assert_eq!(draining["state"]["mode"], "draining");
    assert_eq!(draining["peer_sync_paused"], false);
    assert_eq!(draining["remote_desired_state_blocked"], false);

    cluster
        .put_workload(
            "node-b",
            stopped_workload("workload.draining.remote", "node-b"),
        )
        .await;

    cluster
        .wait_for_workload(
            "node-a",
            "workload.draining.remote",
            std::time::Duration::from_secs(10),
        )
        .await;
    cluster
        .wait_for_workload(
            "node-c",
            "workload.draining.remote",
            std::time::Duration::from_secs(10),
        )
        .await;

    let exit = cluster.exec_service_output(
        "node-a",
        [
            "orionctl",
            "maintenance",
            "exit",
            "--socket",
            "/tmp/orion-node-a-control.sock",
            "-o",
            "json",
        ],
    );
    assert!(
        exit.status.success(),
        "orionctl maintenance exit failed after drain\n{}",
        output_text(&exit)
    );
    let exited: serde_json::Value =
        serde_json::from_slice(&exit.stdout).expect("maintenance exit json should parse");
    assert_eq!(exited["state"]["mode"], "normal");
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_client_example_orionctl_maintenance_isolate_blocks_peer_sync_until_exit() {
    let cluster = DockerCluster::start("client-example-orionctl-isolate").await;
    cluster.wait_for_all_http().await;

    let isolate = cluster.exec_service_output(
        "node-a",
        [
            "orionctl",
            "maintenance",
            "isolate",
            "--socket",
            "/tmp/orion-node-a-control.sock",
            "-o",
            "json",
        ],
    );
    assert!(
        isolate.status.success(),
        "orionctl maintenance isolate failed\n{}",
        output_text(&isolate)
    );
    let isolated: serde_json::Value =
        serde_json::from_slice(&isolate.stdout).expect("maintenance isolate json should parse");
    assert_eq!(isolated["state"]["mode"], "isolated");
    assert_eq!(isolated["peer_sync_paused"], true);
    assert_eq!(isolated["remote_desired_state_blocked"], true);

    cluster
        .put_workload(
            "node-b",
            stopped_workload("workload.isolated.remote", "node-b"),
        )
        .await;

    cluster
        .wait_for_workload(
            "node-c",
            "workload.isolated.remote",
            std::time::Duration::from_secs(10),
        )
        .await;
    assert_workload_absent_for(
        &cluster,
        "node-a",
        "workload.isolated.remote",
        std::time::Duration::from_secs(3),
    )
    .await;

    let exit = cluster.exec_service_output(
        "node-a",
        [
            "orionctl",
            "maintenance",
            "exit",
            "--socket",
            "/tmp/orion-node-a-control.sock",
        ],
    );
    assert!(
        exit.status.success(),
        "orionctl maintenance exit failed\n{}",
        output_text(&exit)
    );

    cluster
        .wait_for_workload(
            "node-a",
            "workload.isolated.remote",
            std::time::Duration::from_secs(15),
        )
        .await;
}

mod support;

use std::time::Duration;

use orion::control_plane::{ArtifactRecord, ControlMessage, DesiredStateMutation, MutationBatch};
use orion::transport::http::{HttpRequestPayload, HttpResponsePayload};

use support::{TestHarness, output_text, publish_observed_updater_state, run_orionctl};

#[tokio::test(flavor = "multi_thread")]
async fn orionctl_get_workloads_and_resources_report_observed_state() {
    let harness = TestHarness::start("node.orionctl.observed").await;
    publish_observed_updater_state(&harness).await;

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
    assert_eq!(array[0]["workload_id"], "workload.update");
    assert_eq!(array[0]["desired_state"], "Running");
    assert_eq!(array[0]["observed_state"], "Running");

    let get_resources = run_orionctl([
        "get",
        "resources",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
        "-o",
        "json",
    ]);
    assert!(
        get_resources.status.success(),
        "{}",
        output_text(&get_resources)
    );
    let resources: serde_json::Value =
        serde_json::from_slice(&get_resources.stdout).expect("json output should parse");
    let array = resources.as_array().expect("json should be an array");
    assert_eq!(array.len(), 2);
    assert!(
        array
            .iter()
            .any(|resource| resource["resource_id"] == "resource.updater.runtime")
    );
    assert!(
        array
            .iter()
            .any(|resource| resource["resource_id"] == "resource.update.execution")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn orionctl_snapshot_and_watch_summaries_include_desired_and_observed_counts() {
    let harness = TestHarness::start("node.orionctl.summary").await;
    publish_observed_updater_state(&harness).await;

    let snapshot = run_orionctl([
        "get",
        "snapshot",
        "--socket",
        &harness.ipc_socket.to_string_lossy(),
    ]);
    assert!(snapshot.status.success(), "{}", output_text(&snapshot));
    let snapshot_stdout = String::from_utf8_lossy(&snapshot.stdout);
    assert!(snapshot_stdout.contains("desired_workloads=1"));
    assert!(snapshot_stdout.contains("observed_workloads=1"));
    assert!(snapshot_stdout.contains("desired_resources=0"));
    assert!(snapshot_stdout.contains("observed_resources=2"));

    let watch_task = tokio::task::spawn_blocking({
        let stream_socket = harness.ipc_stream_socket.to_string_lossy().to_string();
        move || {
            run_orionctl([
                "watch",
                "state",
                "--stream-socket",
                &stream_socket,
                "--client-name",
                "summary-watcher",
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
                    ArtifactRecord::builder("artifact.summary").build(),
                )],
            }),
        )))
        .await
        .expect("mutation should succeed");
    assert_eq!(response, HttpResponsePayload::Accepted);

    let output = watch_task.await.expect("watch task should join");
    assert!(output.status.success(), "{}", output_text(&output));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("desired_workloads=1"),
        "unexpected stdout: {stdout}"
    );
    assert!(
        stdout.contains("observed_workloads=1"),
        "unexpected stdout: {stdout}"
    );
    assert!(
        stdout.contains("desired_resources=0"),
        "unexpected stdout: {stdout}"
    );
    assert!(
        stdout.contains("observed_resources=2"),
        "unexpected stdout: {stdout}"
    );
}

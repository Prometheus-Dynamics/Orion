mod support;

use std::time::Duration;

use orion::{
    control_plane::{ArtifactRecord, ControlMessage, DesiredStateMutation, MutationBatch},
    transport::{
        http::{HttpRequestPayload, HttpResponsePayload},
        ipc::UnixControlStreamClient,
    },
};

use support::{TestHarness, output_text, run_orionctl};

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

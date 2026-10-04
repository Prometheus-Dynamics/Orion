//! Responder side of peer sync over HTTP: what a node answers to sync requests from a peer.

use super::*;
use orion::{
    HlcTimestamp,
    control_plane::{
        DesiredStateMutation, DesiredStateObjectSelector, DesiredStateSection, SyncDiffRequest,
        SyncRequest, SyncSummaryRequest,
    },
};

fn artifact(id: &'static str) -> orion::control_plane::ArtifactRecord {
    orion::control_plane::ArtifactRecord::builder(id).build()
}

fn pose_workload(artifact: &'static str) -> WorkloadRecord {
    WorkloadRecord::builder(
        WorkloadId::new("workload.pose"),
        RuntimeType::new("graph.exec.v1"),
        ArtifactId::new(artifact),
    )
    .desired_state(DesiredState::Stopped)
    .assigned_to(NodeId::new("node-a"))
    .build()
}

async fn serve(
    node: &NodeApp,
) -> (
    HttpClient,
    tokio::task::JoinHandle<Result<(), HttpTransportError>>,
) {
    let (addr, server) = node
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("HTTP server should start");
    let client = HttpClient::try_new(format!("http://{}", addr)).expect("HTTP client should build");
    (client, server)
}

async fn send(client: &HttpClient, message: ControlMessage) -> HttpResponsePayload {
    client
        .send(&HttpRequestPayload::Control(Box::new(message)))
        .await
        .expect("peer request should succeed")
}

fn node_with_artifact_and_workload() -> NodeApp {
    let node = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build");
    let mut desired = node.state_snapshot().state.desired;
    desired.put_artifact(artifact("artifact.pose"));
    desired.put_workload(pose_workload("artifact.pose"));
    node.replace_desired(desired);
    node
}

#[tokio::test]
async fn node_http_sync_request_answers_by_fingerprint_and_summary() {
    let node = node_with_artifact_and_workload();
    let (client, server) = serve(&node).await;
    let (_, fingerprint, _) = node
        .desired_metadata_for_test()
        .expect("desired metadata should compute");

    let request = |desired_fingerprint, desired_summary| {
        ControlMessage::SyncRequest(SyncRequest {
            node_id: NodeId::new("node-b"),
            desired_revision: Revision::new(99),
            desired_fingerprint,
            desired_summary,
            sections: Vec::new(),
            object_selectors: Vec::new(),
        })
    };

    // Equal fingerprints mean equal content, whatever the revisions are.
    assert_eq!(
        send(&client, request(fingerprint, None)).await,
        HttpResponsePayload::Accepted
    );
    // Without a summary the responder cannot tell which versions are missing: full snapshot.
    match send(&client, request(0, None)).await {
        HttpResponsePayload::Snapshot(snapshot) => {
            assert!(
                snapshot
                    .state
                    .desired
                    .workloads
                    .contains_key(&WorkloadId::new("workload.pose"))
            );
        }
        other => panic!("expected snapshot response, got {other:?}"),
    }
    // With an (empty) summary it returns every version, stamped.
    match send(&client, request(0, Some(Default::default()))).await {
        HttpResponsePayload::Mutations(batch) => {
            assert!(batch.is_stamped());
            assert_eq!(batch.mutations.len(), 2);
        }
        other => panic!("expected stamped versions, got {other:?}"),
    }
    server.abort();
}

#[tokio::test]
async fn node_http_sync_diff_returns_only_versions_newer_than_the_summary() {
    let node = node_with_artifact_and_workload();
    let (client, server) = serve(&node).await;
    let current = node.state_snapshot().state.desired;
    let artifact_stamp = current.stamps.artifacts[&ArtifactId::new("artifact.pose")];
    let workload_stamp = current.stamps.workloads[&WorkloadId::new("workload.pose")];

    // The requester already has the artifact at the same version and a newer workload.
    let mut summary = DesiredStateSummary::default();
    summary.artifacts.insert(
        ArtifactId::new("artifact.pose"),
        entry_fingerprint(&artifact("artifact.pose")),
    );
    summary
        .stamps
        .artifacts
        .insert(ArtifactId::new("artifact.pose"), artifact_stamp);
    summary
        .workloads
        .insert(WorkloadId::new("workload.pose"), 7);
    summary.stamps.workloads.insert(
        WorkloadId::new("workload.pose"),
        HlcTimestamp::new(workload_stamp.physical_ms + 1, 0, 1),
    );
    let diff = |summary| {
        ControlMessage::SyncDiffRequest(SyncDiffRequest {
            node_id: NodeId::new("node-b"),
            desired_revision: Revision::ZERO,
            desired_summary: summary,
            sections: Vec::new(),
            object_selectors: Vec::new(),
        })
    };
    assert_eq!(
        send(&client, diff(summary.clone())).await,
        HttpResponsePayload::Accepted
    );

    // An older version of the workload on the requester makes the responder send its own.
    summary.stamps.workloads.insert(
        WorkloadId::new("workload.pose"),
        HlcTimestamp::new(workload_stamp.physical_ms.saturating_sub(1), 0, 1),
    );
    match send(&client, diff(summary)).await {
        HttpResponsePayload::Mutations(batch) => {
            assert_eq!(batch.mutations.len(), 1);
            assert_eq!(batch.stamps, vec![workload_stamp]);
            assert!(matches!(
                &batch.mutations[0],
                DesiredStateMutation::PutWorkload(record)
                    if record.workload_id == WorkloadId::new("workload.pose")
            ));
        }
        other => panic!("expected the newer workload version, got {other:?}"),
    }
    server.abort();
}

#[tokio::test]
async fn node_http_sync_summary_request_returns_only_requested_sections() {
    let node = node_with_artifact_and_workload();
    let (client, server) = serve(&node).await;

    let response = send(
        &client,
        ControlMessage::SyncSummaryRequest(SyncSummaryRequest {
            node_id: NodeId::new("node-b"),
            sections: vec![DesiredStateSection::Artifacts],
        }),
    )
    .await;
    match response {
        HttpResponsePayload::Summary(summary) => {
            assert_eq!(summary.artifacts.len(), 1);
            assert!(
                summary
                    .stamps
                    .artifacts
                    .contains_key(&ArtifactId::new("artifact.pose"))
            );
            assert!(summary.workloads.is_empty());
            assert!(summary.stamps.workloads.is_empty());
            assert!(summary.tombstones.is_empty());
            assert!(summary.resources.is_empty());
            assert!(summary.providers.is_empty());
            assert!(summary.executors.is_empty());
            assert!(summary.leases.is_empty());
        }
        other => panic!("expected summary response, got {other:?}"),
    }
    server.abort();
}

#[tokio::test]
async fn node_http_sync_diff_request_honors_object_selectors_for_large_sections() {
    let node = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Optional,
        ))
        .try_build()
        .expect("node app should build");
    let mut desired = node.state_snapshot().state.desired;
    desired.put_artifact(artifact("artifact.one"));
    desired.put_artifact(artifact("artifact.two"));
    desired.put_workload(pose_workload("artifact.two"));
    node.replace_desired(desired);
    let (client, server) = serve(&node).await;

    let response = send(
        &client,
        ControlMessage::SyncDiffRequest(SyncDiffRequest {
            node_id: NodeId::new("node-b"),
            desired_revision: Revision::new(1),
            desired_summary: DesiredStateSummary::default(),
            sections: vec![
                DesiredStateSection::Artifacts,
                DesiredStateSection::Workloads,
            ],
            object_selectors: vec![DesiredStateObjectSelector::Artifacts(vec![
                ArtifactId::new("artifact.two"),
            ])],
        }),
    )
    .await;
    match response {
        HttpResponsePayload::Mutations(batch) => {
            // The artifact section is limited to the selected object; the workload section has
            // no selector and is compared in full.
            assert!(batch.is_stamped());
            assert_eq!(batch.mutations.len(), 2);
            assert!(batch.mutations.iter().any(|mutation| matches!(
                mutation,
                DesiredStateMutation::PutArtifact(record)
                    if record.artifact_id == ArtifactId::new("artifact.two")
            )));
            assert!(batch.mutations.iter().any(|mutation| matches!(
                mutation,
                DesiredStateMutation::PutWorkload(record)
                    if record.workload_id == WorkloadId::new("workload.pose")
            )));
        }
        other => panic!("expected mutation diff response, got {other:?}"),
    }
    server.abort();
}

#[tokio::test]
async fn node_http_stamped_push_merges_per_object_and_ignores_stale_versions() {
    let node = node_with_artifact_and_workload();
    let (client, server) = serve(&node).await;
    let before = node.state_snapshot().state.desired;
    let workload_stamp = before.stamps.workloads[&WorkloadId::new("workload.pose")];

    let newer = HlcTimestamp::new(workload_stamp.physical_ms + 10, 0, 1);
    let older = HlcTimestamp::new(workload_stamp.physical_ms.saturating_sub(10), 0, 1);
    let batch = MutationBatch::stamped(
        Revision::new(1234),
        vec![
            (
                DesiredStateMutation::PutArtifact(artifact("artifact.remote")),
                newer,
            ),
            (
                DesiredStateMutation::PutWorkload(pose_workload("artifact.remote")),
                older,
            ),
        ],
    );
    assert_eq!(
        send(&client, ControlMessage::Mutations(batch)).await,
        HttpResponsePayload::Accepted
    );

    let after = node.state_snapshot().state.desired;
    assert!(
        after
            .artifacts
            .contains_key(&ArtifactId::new("artifact.remote"))
    );
    assert_eq!(
        after.workloads[&WorkloadId::new("workload.pose")].artifact_id,
        ArtifactId::new("artifact.pose"),
        "the older remote workload version must lose against the local one"
    );
    assert_eq!(after.revision, before.revision.next());
    let merge = node.observability_snapshot().desired_merge;
    assert_eq!(merge.remote_writes_applied, 1);
    assert_eq!(merge.stale_remote_writes_ignored, 1);
    // The clock moved past the accepted remote stamp.
    assert!(node.hlc_now() >= newer);
    server.abort();
}

#[tokio::test]
async fn node_http_unstamped_peer_batch_is_a_checked_local_write() {
    let node = node_with_artifact_and_workload();
    let (client, server) = serve(&node).await;
    let revision = node.current_desired_revision();

    let stale = client
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::Mutations(MutationBatch::new(
                Revision::new(revision.get() + 5),
                vec![DesiredStateMutation::PutArtifact(artifact("artifact.x"))],
            )),
        )))
        .await;
    assert!(stale.is_err(), "a stale base revision is rejected");

    assert_eq!(
        send(
            &client,
            ControlMessage::Mutations(MutationBatch::new(
                revision,
                vec![DesiredStateMutation::PutArtifact(artifact("artifact.x"))],
            )),
        )
        .await,
        HttpResponsePayload::Accepted
    );
    let desired = node.state_snapshot().state.desired;
    let stamp = desired.stamps.artifacts[&ArtifactId::new("artifact.x")];
    assert_eq!(stamp.node, orion::hlc_node_tag("node-a"));
    server.abort();
}

use super::*;

#[tokio::test]
async fn node_http_sync_request_returns_mutations_for_zero_base_and_snapshot_for_mismatch() {
    let node = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Optional,
        ))
        .try_build()
        .expect("node app should build");

    let mut desired = node.state_snapshot().state.desired;
    desired.put_artifact(orion::control_plane::ArtifactRecord::builder("artifact.pose").build());
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.pose"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.pose"),
        )
        .desired_state(DesiredState::Running)
        .assigned_to(NodeId::new("node-a"))
        .build(),
    );
    node.replace_desired(desired);

    let (addr, server) = node
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("HTTP server should start");
    let client = HttpClient::try_new(format!("http://{}", addr)).expect("HTTP client should build");

    let zero_base = client
        .send(&HttpRequestPayload::Control(Box::new(
            orion::control_plane::ControlMessage::SyncRequest(orion::control_plane::SyncRequest {
                node_id: NodeId::new("node-b"),
                desired_revision: orion::Revision::ZERO,
                desired_fingerprint: 0,
                desired_summary: None,
                sections: Vec::new(),
                object_selectors: Vec::new(),
            }),
        )))
        .await
        .expect("sync request should succeed");

    match zero_base {
        HttpResponsePayload::Mutations(batch) => {
            assert_eq!(batch.base_revision, orion::Revision::ZERO);
            assert_eq!(batch.mutations.len(), 2);
        }
        other => panic!("expected mutation replay response, got {other:?}"),
    }

    let mismatch = client
        .send(&HttpRequestPayload::Control(Box::new(
            orion::control_plane::ControlMessage::SyncRequest(orion::control_plane::SyncRequest {
                node_id: NodeId::new("node-b"),
                desired_revision: orion::Revision::new(1),
                desired_fingerprint: 0,
                desired_summary: None,
                sections: Vec::new(),
                object_selectors: Vec::new(),
            }),
        )))
        .await
        .expect("sync request should succeed");

    match mismatch {
        HttpResponsePayload::Snapshot(snapshot) => {
            assert!(
                snapshot
                    .state
                    .desired
                    .workloads
                    .contains_key(&WorkloadId::new("workload.pose"))
            );
        }
        other => panic!("expected snapshot fallback response, got {other:?}"),
    }

    server.abort();
}

#[tokio::test]
async fn node_http_sync_request_returns_delta_for_known_nonzero_base_revision() {
    let node = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Optional,
        ))
        .try_build()
        .expect("node app should build");

    let mut desired = node.state_snapshot().state.desired;
    desired.put_artifact(orion::control_plane::ArtifactRecord::builder("artifact.pose").build());
    node.replace_desired(desired.clone());
    let base_revision = desired.revision;

    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.pose"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.pose"),
        )
        .desired_state(DesiredState::Running)
        .assigned_to(NodeId::new("node-a"))
        .build(),
    );
    node.replace_desired(desired);

    let (addr, server) = node
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("HTTP server should start");
    let client = HttpClient::try_new(format!("http://{}", addr)).expect("HTTP client should build");

    let response = client
        .send(&HttpRequestPayload::Control(Box::new(
            orion::control_plane::ControlMessage::SyncRequest(orion::control_plane::SyncRequest {
                node_id: NodeId::new("node-b"),
                desired_revision: base_revision,
                desired_fingerprint: 0,
                desired_summary: None,
                sections: Vec::new(),
                object_selectors: Vec::new(),
            }),
        )))
        .await
        .expect("sync request should succeed");

    match response {
        HttpResponsePayload::Mutations(batch) => {
            assert_eq!(batch.base_revision, base_revision);
            assert_eq!(batch.mutations.len(), 1);
            assert!(matches!(
                &batch.mutations[0],
                orion::control_plane::DesiredStateMutation::PutWorkload(_)
            ));
        }
        other => panic!("expected delta mutation response, got {other:?}"),
    }

    server.abort();
}

#[tokio::test]
async fn node_http_sync_request_returns_accepted_when_revisions_match() {
    let node = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Optional,
        ))
        .try_build()
        .expect("node app should build");

    let mut desired = node.state_snapshot().state.desired;
    desired.put_artifact(orion::control_plane::ArtifactRecord::builder("artifact.pose").build());
    node.replace_desired(desired.clone());

    let (addr, server) = node
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("HTTP server should start");
    let client = HttpClient::try_new(format!("http://{}", addr)).expect("HTTP client should build");

    let fingerprint = desired_fingerprint(&desired);
    let response = client
        .send(&HttpRequestPayload::Control(Box::new(
            orion::control_plane::ControlMessage::SyncRequest(orion::control_plane::SyncRequest {
                node_id: NodeId::new("node-b"),
                desired_revision: desired.revision,
                desired_fingerprint: fingerprint,
                desired_summary: None,
                sections: Vec::new(),
                object_selectors: Vec::new(),
            }),
        )))
        .await
        .expect("sync request should succeed");

    assert_eq!(response, HttpResponsePayload::Accepted);
    server.abort();
}

#[tokio::test]
async fn node_http_sync_request_returns_combined_delta_for_multiple_revisions() {
    let node = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Optional,
        ))
        .try_build()
        .expect("node app should build");

    let mut desired = node.state_snapshot().state.desired;
    desired.put_artifact(orion::control_plane::ArtifactRecord::builder("artifact.one").build());
    node.replace_desired(desired.clone());
    let base_revision = desired.revision;

    desired.put_artifact(orion::control_plane::ArtifactRecord::builder("artifact.two").build());
    node.replace_desired(desired.clone());

    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.pose"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.two"),
        )
        .desired_state(DesiredState::Stopped)
        .assigned_to(NodeId::new("node-a"))
        .build(),
    );
    node.replace_desired(desired);

    let (addr, server) = node
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("HTTP server should start");
    let client = HttpClient::try_new(format!("http://{}", addr)).expect("HTTP client should build");

    let response = client
        .send(&HttpRequestPayload::Control(Box::new(
            orion::control_plane::ControlMessage::SyncRequest(orion::control_plane::SyncRequest {
                node_id: NodeId::new("node-b"),
                desired_revision: base_revision,
                desired_fingerprint: 0,
                desired_summary: None,
                sections: Vec::new(),
                object_selectors: Vec::new(),
            }),
        )))
        .await
        .expect("sync request should succeed");

    match response {
        HttpResponsePayload::Mutations(batch) => {
            assert_eq!(batch.base_revision, base_revision);
            assert_eq!(batch.mutations.len(), 2);
            assert!(matches!(
                &batch.mutations[0],
                orion::control_plane::DesiredStateMutation::PutArtifact(_)
            ));
            assert!(matches!(
                &batch.mutations[1],
                orion::control_plane::DesiredStateMutation::PutWorkload(_)
            ));
        }
        other => panic!("expected combined delta response, got {other:?}"),
    }

    server.abort();
}

#[tokio::test]
async fn node_http_sync_diff_request_returns_object_delta_without_snapshot_fallback() {
    let node = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Optional,
        ))
        .try_build()
        .expect("node app should build");

    let mut desired = node.state_snapshot().state.desired;
    let artifact_one = orion::control_plane::ArtifactRecord::builder("artifact.one").build();
    desired.put_artifact(artifact_one.clone());
    node.replace_desired(desired.clone());
    let base_revision = desired.revision;

    let artifact_two = orion::control_plane::ArtifactRecord::builder("artifact.two").build();
    desired.put_artifact(artifact_two.clone());
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.pose"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.two"),
        )
        .desired_state(DesiredState::Stopped)
        .assigned_to(NodeId::new("node-a"))
        .build(),
    );
    node.replace_desired(desired);

    let summary = DesiredStateSummary {
        revision: base_revision,
        section_fingerprints: orion::control_plane::DesiredStateSectionFingerprints {
            nodes: 0,
            artifacts: 1,
            workloads: 0,
            resources: 0,
            providers: 0,
            executors: 0,
            leases: 0,
        },
        nodes: Default::default(),
        artifacts: std::iter::once((
            ArtifactId::new("artifact.one"),
            entry_fingerprint(&artifact_one),
        ))
        .collect(),
        workloads: Default::default(),
        workload_tombstones: Default::default(),
        resources: Default::default(),
        providers: Default::default(),
        executors: Default::default(),
        leases: Default::default(),
    };

    let (addr, server) = node
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("HTTP server should start");
    let client = HttpClient::try_new(format!("http://{}", addr)).expect("HTTP client should build");

    let response = client
        .send(&HttpRequestPayload::Control(Box::new(
            orion::control_plane::ControlMessage::SyncDiffRequest(
                orion::control_plane::SyncDiffRequest {
                    node_id: NodeId::new("node-b"),
                    desired_revision: base_revision,
                    desired_summary: summary,
                    sections: vec![
                        orion::control_plane::DesiredStateSection::Artifacts,
                        orion::control_plane::DesiredStateSection::Workloads,
                    ],
                    object_selectors: vec![],
                },
            ),
        )))
        .await
        .expect("sync diff request should succeed");

    match response {
        HttpResponsePayload::Mutations(batch) => {
            assert_eq!(batch.base_revision, base_revision);
            assert_eq!(batch.mutations.len(), 2);
            assert!(matches!(
                &batch.mutations[0],
                orion::control_plane::DesiredStateMutation::PutArtifact(record)
                if record.artifact_id == ArtifactId::new("artifact.two")
            ));
            assert!(matches!(
                &batch.mutations[1],
                orion::control_plane::DesiredStateMutation::PutWorkload(record)
                if record.workload_id == WorkloadId::new("workload.pose")
            ));
        }
        other => panic!("expected mutation diff response, got {other:?}"),
    }

    server.abort();
}

#[tokio::test]
async fn node_http_sync_summary_request_returns_only_requested_sections() {
    let node = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Optional,
        ))
        .try_build()
        .expect("node app should build");

    let mut desired = node.state_snapshot().state.desired;
    desired.put_artifact(orion::control_plane::ArtifactRecord::builder("artifact.one").build());
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.pose"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.one"),
        )
        .desired_state(DesiredState::Stopped)
        .assigned_to(NodeId::new("node-a"))
        .build(),
    );
    node.replace_desired(desired);

    let (addr, server) = node
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("HTTP server should start");
    let client = HttpClient::try_new(format!("http://{}", addr)).expect("HTTP client should build");

    let response = client
        .send(&HttpRequestPayload::Control(Box::new(
            orion::control_plane::ControlMessage::SyncSummaryRequest(
                orion::control_plane::SyncSummaryRequest {
                    node_id: NodeId::new("node-b"),
                    sections: vec![orion::control_plane::DesiredStateSection::Artifacts],
                },
            ),
        )))
        .await
        .expect("sync summary request should succeed");

    match response {
        HttpResponsePayload::Summary(summary) => {
            assert_eq!(summary.artifacts.len(), 1);
            assert!(
                summary
                    .artifacts
                    .contains_key(&ArtifactId::new("artifact.one"))
            );
            assert!(summary.workloads.is_empty());
            assert!(summary.workload_tombstones.is_empty());
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
    let artifact_one = orion::control_plane::ArtifactRecord::builder("artifact.one").build();
    let artifact_two = orion::control_plane::ArtifactRecord::builder("artifact.two").build();
    desired.put_artifact(artifact_one.clone());
    desired.put_artifact(artifact_two.clone());
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.pose"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.two"),
        )
        .desired_state(DesiredState::Stopped)
        .assigned_to(NodeId::new("node-a"))
        .build(),
    );
    node.replace_desired(desired.clone());

    let summary = DesiredStateSummary {
        revision: Revision::new(1),
        section_fingerprints: orion::control_plane::DesiredStateSectionFingerprints {
            nodes: 0,
            artifacts: 0,
            workloads: 0,
            resources: 0,
            providers: 0,
            executors: 0,
            leases: 0,
        },
        nodes: Default::default(),
        artifacts: std::iter::once((
            ArtifactId::new("artifact.one"),
            entry_fingerprint(&artifact_one),
        ))
        .collect(),
        workloads: Default::default(),
        workload_tombstones: Default::default(),
        resources: Default::default(),
        providers: Default::default(),
        executors: Default::default(),
        leases: Default::default(),
    };

    let (addr, server) = node
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("HTTP server should start");
    let client = HttpClient::try_new(format!("http://{}", addr)).expect("HTTP client should build");

    let response = client
        .send(&HttpRequestPayload::Control(Box::new(
            orion::control_plane::ControlMessage::SyncDiffRequest(
                orion::control_plane::SyncDiffRequest {
                    node_id: NodeId::new("node-b"),
                    desired_revision: Revision::new(1),
                    desired_summary: summary,
                    sections: vec![
                        orion::control_plane::DesiredStateSection::Artifacts,
                        orion::control_plane::DesiredStateSection::Workloads,
                    ],
                    object_selectors: vec![
                        orion::control_plane::DesiredStateObjectSelector::Artifacts(vec![
                            ArtifactId::new("artifact.two"),
                        ]),
                    ],
                },
            ),
        )))
        .await
        .expect("sync diff request should succeed");

    match response {
        HttpResponsePayload::Mutations(batch) => {
            assert_eq!(batch.base_revision, Revision::new(1));
            assert_eq!(batch.mutations.len(), 2);
            assert!(matches!(
                &batch.mutations[0],
                orion::control_plane::DesiredStateMutation::PutArtifact(record)
                if record.artifact_id == ArtifactId::new("artifact.two")
            ));
            assert!(matches!(
                &batch.mutations[1],
                orion::control_plane::DesiredStateMutation::PutWorkload(record)
                if record.workload_id == WorkloadId::new("workload.pose")
            ));
        }
        other => panic!("expected mutation diff response, got {other:?}"),
    }

    server.abort();
}

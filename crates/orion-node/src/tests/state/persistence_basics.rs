use super::*;

#[test]
fn node_persists_and_replays_state_and_artifacts() {
    let state_dir = temp_state_dir("persist");
    let app = NodeApp::builder()
        .config(test_node_config_with_state_dir_and_auth(
            "node-a",
            "node-test",
            state_dir.clone(),
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build");

    app.register_provider(TestProvider)
        .expect("provider registration should succeed");
    app.register_executor(TestExecutor::new())
        .expect("executor registration should succeed");
    app.put_artifact_content(
        &orion::control_plane::ArtifactRecord::builder("artifact.pose")
            .content_type("application/octet-stream")
            .size_bytes(4)
            .build(),
        &[1, 2, 3, 4],
    )
    .expect("artifact should persist");

    let mut desired = app.state_snapshot().state.desired;
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.pose"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.pose"),
        )
        .desired_state(DesiredState::Running)
        .assigned_to(NodeId::new("node-a"))
        .require_resource(ResourceType::new("imu.sample"), 1)
        .build(),
    );
    app.replace_desired(desired.clone());
    app.persist_state().expect("state should persist");

    let replayed = NodeApp::builder()
        .config(test_node_config_with_state_dir_and_auth(
            "node-a",
            "node-test",
            state_dir.clone(),
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build");
    assert!(
        replayed
            .replay_state()
            .expect("state replay should succeed")
    );

    let snapshot = replayed.state_snapshot();
    assert!(
        snapshot
            .state
            .desired
            .workloads
            .contains_key(&WorkloadId::new("workload.pose"))
    );

    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime should build");
    let delta = runtime.block_on(async {
        let (addr, server) = replayed
            .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
            .await
            .expect("HTTP server should start");
        let client =
            HttpClient::try_new(format!("http://{}", addr)).expect("HTTP client should build");
        let response = client
            .send(&HttpRequestPayload::Control(Box::new(
                orion::control_plane::ControlMessage::SyncRequest(
                    orion::control_plane::SyncRequest {
                        node_id: NodeId::new("node-b"),
                        desired_revision: orion::Revision::new(3),
                        desired_fingerprint: 0,
                        desired_summary: None,
                        sections: Vec::new(),
                        object_selectors: Vec::new(),
                    },
                ),
            )))
            .await
            .expect("sync request should succeed");
        server.abort();
        response
    });
    match delta {
        HttpResponsePayload::Mutations(batch) => {
            assert_eq!(batch.base_revision, orion::Revision::new(3));
            assert_eq!(batch.mutations.len(), 1);
        }
        other => panic!("expected persisted mutation replay, got {other:?}"),
    }

    let (artifact, payload) = replayed
        .artifact_content(&ArtifactId::new("artifact.pose"))
        .expect("artifact lookup should succeed")
        .expect("artifact should exist");
    assert_eq!(artifact.artifact_id.as_str(), "artifact.pose");
    assert_eq!(payload, vec![1, 2, 3, 4]);

    let _ = fs::remove_dir_all(state_dir);
}

#[test]
fn replay_rejects_corrupt_snapshot_file() {
    let state_dir = temp_state_dir("corrupt");
    fs::create_dir_all(&state_dir).expect("state dir should be created");
    fs::write(state_dir.join("snapshot-manifest.rkyv"), b"not-rkyv")
        .expect("corrupt snapshot should be written");

    let app = NodeApp::builder()
        .config(test_node_config_with_state_dir_and_auth(
            "node-a",
            "node-test",
            state_dir.clone(),
            crate::PeerAuthenticationMode::Optional,
        ))
        .try_build()
        .expect("node app should build");

    let err = app
        .replay_state()
        .expect_err("corrupt snapshot should fail");
    assert!(matches!(err, NodeError::Storage(_)));

    let _ = fs::remove_dir_all(state_dir);
}

#[test]
fn replay_ignores_orphaned_interrupted_write_temp_files() {
    let state_dir = temp_state_dir("orphaned-temp-files");
    let app = NodeApp::builder()
        .config(test_node_config_with_state_dir_and_auth(
            "node-a",
            "node-test",
            state_dir.clone(),
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build");

    app.put_artifact_content(
        &orion::control_plane::ArtifactRecord::builder("artifact.pose").build(),
        &[1, 2, 3, 4],
    )
    .expect("artifact should persist");

    let mut desired = app.state_snapshot().state.desired;
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
    app.replace_desired(desired);
    app.persist_state().expect("state should persist");
    drop(app);

    fs::write(
        state_dir.join("snapshot-manifest.rkyv.interrupted.tmp"),
        b"partial-manifest",
    )
    .expect("orphaned manifest temp file should be written");
    fs::write(
        state_dir.join("snapshot-desired.rkyv.interrupted.tmp"),
        b"partial-desired",
    )
    .expect("orphaned desired temp file should be written");

    let replayed = NodeApp::builder()
        .config(test_node_config_with_state_dir_and_auth(
            "node-a",
            "node-test",
            state_dir.clone(),
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build");
    assert!(
        replayed
            .replay_state()
            .expect("state replay should succeed with orphaned temp files present")
    );
    assert!(
        replayed
            .state_snapshot()
            .state
            .desired
            .workloads
            .contains_key(&WorkloadId::new("workload.pose"))
    );

    let _ = fs::remove_dir_all(state_dir);
}

#[test]
fn duplicate_artifact_put_does_not_bump_desired_revision() {
    let state_dir = temp_state_dir("artifact-dedup");
    let app = NodeApp::builder()
        .config(test_node_config_with_state_dir_and_auth(
            "node-a",
            "node-test",
            state_dir.clone(),
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build");

    let artifact = orion::control_plane::ArtifactRecord::builder("artifact.pose")
        .content_type("application/octet-stream")
        .size_bytes(4)
        .build();

    app.put_artifact_content(&artifact, &[1, 2, 3, 4])
        .expect("initial artifact put should succeed");
    let revision_after_first_put = app.state_snapshot().state.desired.revision;

    app.put_artifact_content(&artifact, &[1, 2, 3, 4])
        .expect("duplicate artifact put should succeed");
    assert_eq!(
        app.state_snapshot().state.desired.revision,
        revision_after_first_put
    );

    let _ = fs::remove_dir_all(state_dir);
}

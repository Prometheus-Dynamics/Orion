use super::*;

#[tokio::test]
async fn node_sync_peer_pulls_remote_snapshot_when_peer_is_ahead() {
    let node_a = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build");
    let node_b = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-b",
            "node-test",
            crate::PeerAuthenticationMode::Optional,
        ))
        .try_build()
        .expect("node app should build");

    let mut desired = node_a.state_snapshot().state.desired;
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
    node_a.replace_desired(desired);

    let (addr_a, server_a) = node_a
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("node A server should start");

    node_b
        .register_peer(PeerConfig::new("node-a", format!("http://{}", addr_a)))
        .expect("peer registration should succeed");

    node_b
        .sync_peer(&NodeId::new("node-a"))
        .await
        .expect("peer sync should succeed");

    let snapshot = node_b.state_snapshot();
    assert!(
        snapshot
            .state
            .desired
            .workloads
            .contains_key(&WorkloadId::new("workload.pose"))
    );

    let peer = node_b
        .peer_states()
        .into_iter()
        .find(|peer| peer.node_id == NodeId::new("node-a"))
        .expect("peer state should exist");
    assert_eq!(peer.sync_status, PeerSyncStatus::Synced);
    assert_eq!(
        peer.desired_revision,
        node_a.state_snapshot().state.desired.revision
    );

    server_a.abort();
}

#[tokio::test]
async fn node_sync_peer_remote_ahead_does_not_require_summary_round_trip() {
    #[derive(Clone)]
    struct NoSummaryRemoteAhead;

    impl HttpControlHandler for NoSummaryRemoteAhead {
        fn handle_payload(
            &self,
            payload: HttpRequestPayload,
        ) -> Result<HttpResponsePayload, HttpTransportError> {
            match peer_control_message(payload)? {
                ControlMessage::Hello(_) => Ok(HttpResponsePayload::Hello(
                    orion::control_plane::PeerHello {
                        node_id: NodeId::new("node-a"),
                        desired_revision: Revision::new(2),
                        desired_fingerprint: 99,
                        desired_section_fingerprints:
                            orion::control_plane::DesiredStateSectionFingerprints {
                                nodes: 0,
                                artifacts: 1,
                                workloads: 2,
                                resources: 0,
                                providers: 0,
                                executors: 0,
                                leases: 0,
                            },
                        observed_revision: Revision::ZERO,
                        applied_revision: Revision::ZERO,
                        transport_binding_version: None,
                        transport_binding_public_key: None,
                        transport_tls_cert_pem: None,
                        transport_binding_signature: None,
                    },
                )),
                ControlMessage::SyncRequest(request) => {
                    assert_eq!(request.desired_revision, Revision::ZERO);
                    assert!(request.desired_summary.is_some());
                    assert!(
                        request
                            .sections
                            .contains(&orion::control_plane::DesiredStateSection::Artifacts)
                    );
                    assert!(
                        request
                            .sections
                            .contains(&orion::control_plane::DesiredStateSection::Workloads)
                    );
                    assert!(request.object_selectors.is_empty());
                    Ok(HttpResponsePayload::Mutations(MutationBatch {
                        base_revision: Revision::ZERO,
                        mutations: vec![
                            orion::control_plane::DesiredStateMutation::PutArtifact(
                                orion::control_plane::ArtifactRecord::builder("artifact.pose")
                                    .build(),
                            ),
                            orion::control_plane::DesiredStateMutation::PutWorkload(
                                WorkloadRecord::builder(
                                    WorkloadId::new("workload.pose"),
                                    RuntimeType::new("graph.exec.v1"),
                                    ArtifactId::new("artifact.pose"),
                                )
                                .desired_state(DesiredState::Running)
                                .assigned_to(NodeId::new("node-a"))
                                .build(),
                            ),
                        ],
                    }))
                }
                ControlMessage::SyncSummaryRequest(_) => Err(HttpTransportError::request_failed(
                    "summary request should not be used",
                )),
                other => Err(HttpTransportError::request_failed(format!(
                    "unexpected payload: {other:?}"
                ))),
            }
        }
    }

    let (addr, server, listener) = HttpServer::bind(
        "127.0.0.1:0".parse().expect("socket address should parse"),
        Arc::new(NoSummaryRemoteAhead),
    )
    .await
    .expect("custom HTTP server should bind");
    let server_task = tokio::spawn(server.serve(listener));

    let node = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-b"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-test"),
            reconcile_interval: std::time::Duration::from_millis(50),
            state_dir: None,
            peers: vec![PeerConfig::new("node-a", format!("http://{}", addr))],
            peer_authentication: crate::PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .try_build()
        .expect("node app should build");

    node.sync_peer(&NodeId::new("node-a"))
        .await
        .expect("peer sync should succeed without summary round-trip");

    let snapshot = node.state_snapshot();
    assert!(
        snapshot
            .state
            .desired
            .artifacts
            .contains_key(&ArtifactId::new("artifact.pose"))
    );
    assert!(
        snapshot
            .state
            .desired
            .workloads
            .contains_key(&WorkloadId::new("workload.pose"))
    );
    let peer = node
        .peer_state_for_test(&NodeId::new("node-a"))
        .expect("peer state should exist");
    let (_, desired_fingerprint, desired_section_fingerprints) = node
        .desired_metadata_for_test()
        .expect("desired metadata should compute");
    assert_eq!(peer.desired_revision, snapshot.state.desired.revision);
    assert_eq!(peer.desired_fingerprint, desired_fingerprint);
    assert_eq!(
        peer.desired_section_fingerprints,
        desired_section_fingerprints
    );

    server_task.abort();
}

#[tokio::test]
async fn node_sync_peer_pushes_local_snapshot_when_local_is_ahead() {
    let node_a = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-a"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-test"),
            reconcile_interval: std::time::Duration::from_millis(50),
            state_dir: None,
            peers: vec![PeerConfig::new("node-b", "http://127.0.0.1:1")],
            peer_authentication: crate::PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .try_build()
        .expect("node app should build");
    let node_b = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-b",
            "node-test",
            crate::PeerAuthenticationMode::Optional,
        ))
        .try_build()
        .expect("node app should build");

    let mut desired = node_b.state_snapshot().state.desired;
    desired.put_artifact(orion::control_plane::ArtifactRecord::builder("artifact.pose").build());
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.pose"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.pose"),
        )
        .desired_state(DesiredState::Running)
        .assigned_to(NodeId::new("node-b"))
        .build(),
    );
    node_b.replace_desired(desired);

    let (addr_a, server_a) = node_a
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("node A server should start");

    node_b
        .register_peer(PeerConfig::new("node-a", format!("http://{}", addr_a)))
        .expect("peer registration should succeed");

    node_b
        .sync_peer(&NodeId::new("node-a"))
        .await
        .expect("peer sync should succeed");

    let snapshot = node_a.state_snapshot();
    assert!(
        snapshot
            .state
            .desired
            .workloads
            .contains_key(&WorkloadId::new("workload.pose"))
    );

    server_a.abort();
}

#[tokio::test]
async fn node_sync_peer_local_ahead_without_history_does_not_require_summary_round_trip() {
    #[derive(Clone)]
    struct NoSummaryRemoteBehind {
        batches: Arc<Mutex<Vec<MutationBatch>>>,
    }

    impl HttpControlHandler for NoSummaryRemoteBehind {
        fn handle_payload(
            &self,
            payload: HttpRequestPayload,
        ) -> Result<HttpResponsePayload, HttpTransportError> {
            match peer_control_message(payload)? {
                ControlMessage::Hello(_) => Ok(HttpResponsePayload::Hello(
                    orion::control_plane::PeerHello {
                        node_id: NodeId::new("node-a"),
                        desired_revision: Revision::ZERO,
                        desired_fingerprint: 0,
                        desired_section_fingerprints:
                            orion::control_plane::DesiredStateSectionFingerprints {
                                nodes: 0,
                                artifacts: 0,
                                workloads: 0,
                                resources: 0,
                                providers: 0,
                                executors: 0,
                                leases: 0,
                            },
                        observed_revision: Revision::ZERO,
                        applied_revision: Revision::ZERO,
                        transport_binding_version: None,
                        transport_binding_public_key: None,
                        transport_tls_cert_pem: None,
                        transport_binding_signature: None,
                    },
                )),
                ControlMessage::Mutations(batch) => {
                    self.batches
                        .lock()
                        .expect("mutation batch log lock should not be poisoned")
                        .push(batch);
                    Ok(HttpResponsePayload::Accepted)
                }
                ControlMessage::SyncSummaryRequest(_) => Err(HttpTransportError::request_failed(
                    "summary request should not be used",
                )),
                other => Err(HttpTransportError::request_failed(format!(
                    "unexpected payload: {other:?}"
                ))),
            }
        }
    }

    let batches = Arc::new(Mutex::new(Vec::new()));
    let (addr, server, listener) = HttpServer::bind(
        "127.0.0.1:0".parse().expect("socket address should parse"),
        Arc::new(NoSummaryRemoteBehind {
            batches: batches.clone(),
        }),
    )
    .await
    .expect("custom HTTP server should bind");
    let server_task = tokio::spawn(server.serve(listener));

    let mut desired = orion::control_plane::DesiredClusterState::default();
    desired.put_artifact(orion::control_plane::ArtifactRecord::builder("artifact.pose").build());
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.pose"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.pose"),
        )
        .desired_state(DesiredState::Running)
        .assigned_to(NodeId::new("node-b"))
        .build(),
    );

    let node = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-b"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-test"),
            reconcile_interval: std::time::Duration::from_millis(50),
            state_dir: None,
            peers: vec![PeerConfig::new("node-a", format!("http://{}", addr))],
            peer_authentication: crate::PeerAuthenticationMode::Disabled,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .desired(desired)
        .try_build()
        .expect("node app should build");

    node.sync_peer(&NodeId::new("node-a"))
        .await
        .expect("peer sync should succeed without summary round-trip");

    let batches = batches
        .lock()
        .expect("mutation batch log lock should not be poisoned");
    assert_eq!(batches.len(), 1);
    assert!(batches[0].mutations.iter().any(|mutation| matches!(
        mutation,
        orion::control_plane::DesiredStateMutation::PutArtifact(record)
        if record.artifact_id == ArtifactId::new("artifact.pose")
    )));
    assert!(batches[0].mutations.iter().any(|mutation| matches!(
        mutation,
        orion::control_plane::DesiredStateMutation::PutWorkload(record)
        if record.workload_id == WorkloadId::new("workload.pose")
    )));

    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_sync_peer_makes_progress_while_remote_desired_state_is_mutating() {
    let node_a = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-sync-churn-a",
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build");
    let node_b = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-b",
            "node-sync-churn-b",
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build");

    let (addr_a, server_a) = node_a
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("node A server should start");
    node_b
        .register_peer(PeerConfig::new("node-a", format!("http://{}", addr_a)))
        .expect("peer registration should succeed");

    let last_node_id = Arc::new(Mutex::new(NodeId::new("node-a")));
    let mutator_app = node_a.clone();
    let last_node_id_mutator = last_node_id.clone();
    let mutator = tokio::spawn(async move {
        for index in 0..20_u32 {
            let churn_id = NodeId::new(format!("node-sync-churn-{index}"));
            let mut desired = mutator_app.state_snapshot().state.desired;
            desired.put_node(orion::control_plane::NodeRecord::builder(churn_id.clone()).build());
            mutator_app.replace_desired(desired);
            *last_node_id_mutator
                .lock()
                .expect("last node id lock should not be poisoned") = churn_id;
            tokio::task::yield_now().await;
        }
    });

    let sync_app = node_b.clone();
    let syncer = tokio::spawn(async move {
        for _ in 0..12 {
            sync_app.sync_peer(&NodeId::new("node-a")).await?;
            tokio::task::yield_now().await;
        }
        Ok::<(), NodeError>(())
    });

    tokio::time::timeout(Duration::from_secs(3), async {
        mutator.await.expect("mutator task should join");
        syncer
            .await
            .expect("sync task should join")
            .expect("peer sync loop should succeed");
    })
    .await
    .expect("peer sync and remote mutation load should complete");

    node_b
        .sync_peer(&NodeId::new("node-a"))
        .await
        .expect("final peer sync should succeed");

    let expected_node = last_node_id
        .lock()
        .expect("last node id lock should not be poisoned")
        .clone();
    let snapshot_a = node_a.state_snapshot();
    let snapshot_b = node_b.state_snapshot();
    assert_eq!(
        snapshot_b.state.desired.revision,
        snapshot_a.state.desired.revision
    );
    assert!(
        snapshot_b.state.desired.nodes.contains_key(&expected_node),
        "final sync should converge to the latest mutating remote desired state"
    );

    server_a.abort();
}

use super::*;

#[tokio::test]
async fn node_sync_peer_uses_cached_peer_state_to_skip_hello_when_remote_is_ahead() {
    #[derive(Clone)]
    struct CachedRemoteAheadPeer;

    impl HttpControlHandler for CachedRemoteAheadPeer {
        fn handle_payload(
            &self,
            payload: HttpRequestPayload,
        ) -> Result<HttpResponsePayload, HttpTransportError> {
            match peer_control_message(payload)? {
                ControlMessage::SyncRequest(request) => {
                    assert_eq!(request.desired_revision, Revision::ZERO);
                    assert!(request.desired_summary.is_some());
                    assert!(!request.sections.is_empty());
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
                ControlMessage::Hello(_) => Err(HttpTransportError::request_failed(
                    "hello should not be used",
                )),
                other => Err(HttpTransportError::request_failed(format!(
                    "unexpected payload: {other:?}"
                ))),
            }
        }
    }

    let (addr, server, listener) = HttpServer::bind(
        "127.0.0.1:0".parse().expect("socket address should parse"),
        Arc::new(CachedRemoteAheadPeer),
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
            peers: vec![PeerConfig::new(
                "node-a",
                orion_core::PeerBaseUrl::new(format!("http://{}", addr)),
            )],
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

    node.record_peer_hello(
        &NodeId::new("node-a"),
        &orion::control_plane::PeerHello {
            node_id: NodeId::new("node-a"),
            desired_revision: Revision::new(2),
            desired_fingerprint: 99,
            desired_section_fingerprints: orion::control_plane::DesiredStateSectionFingerprints {
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
        CompatibilityState::Preferred,
    )
    .expect("peer metadata should record");

    node.sync_peer(&NodeId::new("node-a"))
        .await
        .expect("cached remote-ahead sync should succeed without hello");

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

    server_task.abort();
}

#[tokio::test]
async fn node_sync_peer_uses_cached_peer_state_to_skip_hello_for_equal_revision_conflict() {
    #[derive(Clone)]
    struct CachedEqualConflictPeer;

    impl HttpControlHandler for CachedEqualConflictPeer {
        fn handle_payload(
            &self,
            payload: HttpRequestPayload,
        ) -> Result<HttpResponsePayload, HttpTransportError> {
            match peer_control_message(payload)? {
                ControlMessage::SyncRequest(request) => {
                    assert_eq!(request.desired_revision, Revision::new(1));
                    assert!(request.desired_summary.is_none());
                    Ok(HttpResponsePayload::Snapshot(
                        orion::control_plane::StateSnapshot {
                            state: orion::control_plane::ClusterStateEnvelope {
                                desired: {
                                    let mut desired =
                                        orion::control_plane::DesiredClusterState::default();
                                    desired.put_artifact(
                                        orion::control_plane::ArtifactRecord::builder(
                                            "artifact.pose",
                                        )
                                        .build(),
                                    );
                                    desired
                                },
                                observed: orion::control_plane::ObservedClusterState::default(),
                                applied: orion::control_plane::AppliedClusterState::default(),
                            },
                        },
                    ))
                }
                ControlMessage::Snapshot(_) => Ok(HttpResponsePayload::Accepted),
                ControlMessage::Hello(_) => Err(HttpTransportError::request_failed(
                    "hello should not be used",
                )),
                other => Err(HttpTransportError::request_failed(format!(
                    "unexpected payload: {other:?}"
                ))),
            }
        }
    }

    let mut desired = orion::control_plane::DesiredClusterState::default();
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

    let local_fingerprint = desired_fingerprint(&desired);
    let local_section_fingerprints = orion::control_plane::DesiredStateSectionFingerprints {
        nodes: entry_fingerprint(&desired.nodes),
        artifacts: entry_fingerprint(&desired.artifacts),
        workloads: entry_fingerprint(&(
            desired.workloads.clone(),
            desired.workload_tombstones.clone(),
        )),
        resources: entry_fingerprint(&desired.resources),
        providers: entry_fingerprint(&desired.providers),
        executors: entry_fingerprint(&desired.executors),
        leases: entry_fingerprint(&desired.leases),
    };

    let (addr, server, listener) = HttpServer::bind(
        "127.0.0.1:0".parse().expect("socket address should parse"),
        Arc::new(CachedEqualConflictPeer),
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
            peers: vec![PeerConfig::new(
                "node-a",
                orion_core::PeerBaseUrl::new(format!("http://{}", addr)),
            )],
            peer_authentication: crate::PeerAuthenticationMode::Optional,
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

    node.record_peer_hello(
        &NodeId::new("node-a"),
        &orion::control_plane::PeerHello {
            node_id: NodeId::new("node-a"),
            desired_revision: Revision::new(1),
            desired_fingerprint: local_fingerprint ^ 1,
            desired_section_fingerprints: local_section_fingerprints,
            observed_revision: Revision::ZERO,
            applied_revision: Revision::ZERO,
            transport_binding_version: None,
            transport_binding_public_key: None,
            transport_tls_cert_pem: None,
            transport_binding_signature: None,
        },
        CompatibilityState::Preferred,
    )
    .expect("peer metadata should record");

    node.sync_peer(&NodeId::new("node-a"))
        .await
        .expect("cached equal-revision conflict sync should succeed without hello");

    server_task.abort();
}

#[tokio::test]
async fn node_sync_peer_noops_when_revisions_already_match() {
    let node_a = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Optional,
        ))
        .try_build()
        .expect("node app should build");

    let mut desired = node_a.state_snapshot().state.desired;
    desired.put_artifact(orion::control_plane::ArtifactRecord::builder("artifact.pose").build());
    node_a.replace_desired(desired);

    let (addr_a, server_a) = node_a
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("node A server should start");

    let node_b = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-b"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-test"),
            reconcile_interval: std::time::Duration::from_millis(50),
            state_dir: None,
            peers: vec![PeerConfig::new(
                "node-a",
                orion_core::PeerBaseUrl::new(format!("http://{}", addr_a)),
            )],
            peer_authentication: crate::PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .desired(node_a.state_snapshot().state.desired.clone())
        .try_build()
        .expect("node app should build");

    let before = node_b.state_snapshot();
    node_b
        .sync_peer(&NodeId::new("node-a"))
        .await
        .expect("sync should succeed");
    let after = node_b.state_snapshot();

    assert_eq!(before.state.desired, after.state.desired);
    let peer = node_b
        .peer_states()
        .into_iter()
        .find(|peer| peer.node_id == NodeId::new("node-a"))
        .expect("peer state should exist");
    assert_eq!(peer.sync_status, PeerSyncStatus::Synced);

    server_a.abort();
}

#[tokio::test]
async fn node_sync_peer_uses_cached_peer_state_to_skip_hello_when_state_matches() {
    #[derive(Clone)]
    struct SyncOnlyPeer;

    impl HttpControlHandler for SyncOnlyPeer {
        fn handle_payload(
            &self,
            payload: HttpRequestPayload,
        ) -> Result<HttpResponsePayload, HttpTransportError> {
            match peer_control_message(payload)? {
                ControlMessage::SyncRequest(_) => Ok(HttpResponsePayload::Accepted),
                ControlMessage::Hello(_) => Err(HttpTransportError::request_failed(
                    "hello should not be used",
                )),
                other => Err(HttpTransportError::request_failed(format!(
                    "unexpected payload: {other:?}"
                ))),
            }
        }
    }

    let mut desired = orion::control_plane::DesiredClusterState::default();
    desired.put_artifact(orion::control_plane::ArtifactRecord::builder("artifact.pose").build());

    let local_fingerprint = desired_fingerprint(&desired);
    let section_fingerprints = orion::control_plane::DesiredStateSectionFingerprints {
        nodes: entry_fingerprint(&desired.nodes),
        artifacts: entry_fingerprint(&desired.artifacts),
        workloads: entry_fingerprint(&(
            desired.workloads.clone(),
            desired.workload_tombstones.clone(),
        )),
        resources: entry_fingerprint(&desired.resources),
        providers: entry_fingerprint(&desired.providers),
        executors: entry_fingerprint(&desired.executors),
        leases: entry_fingerprint(&desired.leases),
    };

    let (addr, server, listener) = HttpServer::bind(
        "127.0.0.1:0".parse().expect("socket address should parse"),
        Arc::new(SyncOnlyPeer),
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
            peers: vec![PeerConfig::new(
                "node-a",
                orion_core::PeerBaseUrl::new(format!("http://{}", addr)),
            )],
            peer_authentication: crate::PeerAuthenticationMode::Optional,
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

    node.record_peer_hello(
        &NodeId::new("node-a"),
        &orion::control_plane::PeerHello {
            node_id: NodeId::new("node-a"),
            desired_revision: Revision::new(1),
            desired_fingerprint: local_fingerprint,
            desired_section_fingerprints: section_fingerprints,
            observed_revision: Revision::ZERO,
            applied_revision: Revision::ZERO,
            transport_binding_version: None,
            transport_binding_public_key: None,
            transport_tls_cert_pem: None,
            transport_binding_signature: None,
        },
        CompatibilityState::Preferred,
    )
    .expect("peer metadata should record");

    node.sync_peer(&NodeId::new("node-a"))
        .await
        .expect("cached steady-state sync should succeed without hello");

    server_task.abort();
}

#[tokio::test]
async fn node_sync_peer_uses_cached_peer_state_to_skip_hello_when_local_history_can_replay() {
    #[derive(Clone)]
    struct MutationOnlyPeer {
        batches: Arc<Mutex<Vec<MutationBatch>>>,
    }

    impl HttpControlHandler for MutationOnlyPeer {
        fn handle_payload(
            &self,
            payload: HttpRequestPayload,
        ) -> Result<HttpResponsePayload, HttpTransportError> {
            match peer_control_message(payload)? {
                ControlMessage::Mutations(batch) => {
                    self.batches
                        .lock()
                        .expect("mutation batch log lock should not be poisoned")
                        .push(batch);
                    Ok(HttpResponsePayload::Accepted)
                }
                ControlMessage::Hello(_) => Err(HttpTransportError::request_failed(
                    "hello should not be used",
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
        Arc::new(MutationOnlyPeer {
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
            peers: vec![PeerConfig::new(
                "node-a",
                orion_core::PeerBaseUrl::new(format!("http://{}", addr)),
            )],
            peer_authentication: crate::PeerAuthenticationMode::Optional,
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

    node.record_peer_hello(
        &NodeId::new("node-a"),
        &orion::control_plane::PeerHello {
            node_id: NodeId::new("node-a"),
            desired_revision: Revision::ZERO,
            desired_fingerprint: 0,
            desired_section_fingerprints: orion::control_plane::DesiredStateSectionFingerprints {
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
        CompatibilityState::Preferred,
    )
    .expect("peer metadata should record");

    node.sync_peer(&NodeId::new("node-a"))
        .await
        .expect("cached local-ahead sync should succeed without hello");

    let batches = batches
        .lock()
        .expect("mutation batch log lock should not be poisoned");
    assert_eq!(batches.len(), 1);
    assert!(!batches[0].mutations.is_empty());
    let peer = node
        .peer_state_for_test(&NodeId::new("node-a"))
        .expect("peer state should exist");
    let (_, desired_fingerprint, desired_section_fingerprints) = node
        .desired_metadata_for_test()
        .expect("desired metadata should compute");
    assert_eq!(
        peer.desired_revision,
        node.state_snapshot().state.desired.revision
    );
    assert_eq!(peer.desired_fingerprint, desired_fingerprint);
    assert_eq!(
        peer.desired_section_fingerprints,
        desired_section_fingerprints
    );

    server_task.abort();
}

#[tokio::test]
async fn node_sync_peer_remote_snapshot_preserves_local_registrations() {
    #[derive(Clone)]
    struct NodeBProvider;

    impl ProviderIntegration for NodeBProvider {
        fn provider_record(&self) -> ProviderRecord {
            ProviderRecord::builder("provider.local.b", "node-b")
                .resource_type(ResourceType::new("imu.sample"))
                .build()
        }

        fn snapshot(&self) -> ProviderSnapshot {
            ProviderSnapshot {
                provider: self.provider_record(),
                resources: vec![
                    ResourceRecord::builder("resource.imu-b-1", "imu.sample", "provider.local.b")
                        .health(HealthState::Healthy)
                        .availability(AvailabilityState::Available)
                        .lease_state(LeaseState::Unleased)
                        .build(),
                ],
            }
        }
    }

    #[derive(Clone)]
    struct NodeBExecutor;

    impl ExecutorIntegration for NodeBExecutor {
        fn executor_record(&self) -> ExecutorRecord {
            ExecutorRecord::builder("executor.local.b", "node-b")
                .runtime_type(RuntimeType::new("graph.exec.v1"))
                .build()
        }

        fn snapshot(&self) -> ExecutorSnapshot {
            ExecutorSnapshot {
                executor: self.executor_record(),
                workloads: Vec::new(),
                resources: Vec::new(),
            }
        }
    }

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

    let mut desired = node_a.state_snapshot().state.desired;
    desired.put_artifact(orion::control_plane::ArtifactRecord::builder("artifact.pose").build());
    node_a.replace_desired(desired);

    let (addr_a, server_a) = node_a
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("node A server should start");

    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-b"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-test"),
            reconcile_interval: std::time::Duration::from_millis(50),
            state_dir: None,
            peers: vec![PeerConfig::new(
                "node-a",
                orion_core::PeerBaseUrl::new(format!("http://{}", addr_a)),
            )],
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
    app.register_provider(NodeBProvider)
        .expect("provider registration should succeed");
    app.register_executor(NodeBExecutor)
        .expect("executor registration should succeed");

    app.sync_peer(&NodeId::new("node-a"))
        .await
        .expect("sync should succeed");

    let snapshot = app.state_snapshot();
    assert!(
        snapshot
            .state
            .desired
            .providers
            .contains_key(&ProviderId::new("provider.local.b"))
    );
    assert!(
        snapshot
            .state
            .desired
            .executors
            .contains_key(&ExecutorId::new("executor.local.b"))
    );

    server_a.abort();
}

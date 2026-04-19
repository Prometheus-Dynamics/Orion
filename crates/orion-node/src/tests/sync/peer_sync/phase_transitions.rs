use super::*;

fn zero_section_fingerprints() -> orion::control_plane::DesiredStateSectionFingerprints {
    orion::control_plane::DesiredStateSectionFingerprints {
        nodes: 0,
        artifacts: 0,
        workloads: 0,
        resources: 0,
        providers: 0,
        executors: 0,
        leases: 0,
    }
}

#[derive(Clone)]
struct SequencePeer {
    events: Arc<Mutex<Vec<&'static str>>>,
    handler: Arc<
        dyn Fn(ControlMessage) -> Result<HttpResponsePayload, HttpTransportError> + Send + Sync,
    >,
}

impl HttpControlHandler for SequencePeer {
    fn handle_payload(
        &self,
        payload: HttpRequestPayload,
    ) -> Result<HttpResponsePayload, HttpTransportError> {
        let message = peer_control_message(payload)?;
        self.events
            .lock()
            .expect("sequence log should not be poisoned")
            .push(match &message {
                ControlMessage::Hello(_) => "hello",
                ControlMessage::SyncRequest(_) => "sync_request",
                ControlMessage::Mutations(_) => "mutations",
                ControlMessage::Snapshot(_) => "snapshot",
                other => panic!("unexpected peer-sync control message: {other:?}"),
            });
        (self.handler)(message)
    }
}

#[tokio::test]
async fn node_sync_peer_phase_cached_fast_path_skips_hello() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let handler = SequencePeer {
        events: events.clone(),
        handler: Arc::new(|message| match message {
            ControlMessage::SyncRequest(_) => Ok(HttpResponsePayload::Accepted),
            other => Err(HttpTransportError::request_failed(format!(
                "unexpected payload: {other:?}"
            ))),
        }),
    };

    let (addr, server, listener) = HttpServer::bind(
        "127.0.0.1:0".parse().expect("socket address should parse"),
        Arc::new(handler),
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

    let (_, desired_fingerprint, desired_section_fingerprints) = node
        .desired_metadata_for_test()
        .expect("desired metadata should compute");
    node.record_peer_hello(
        &NodeId::new("node-a"),
        &orion::control_plane::PeerHello {
            node_id: NodeId::new("node-a"),
            desired_revision: Revision::ZERO,
            desired_fingerprint,
            desired_section_fingerprints,
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
        .expect("cached fast-path sync should succeed");

    assert_eq!(
        *events.lock().expect("sequence log should not be poisoned"),
        vec!["sync_request"]
    );

    server_task.abort();
}

#[tokio::test]
async fn node_sync_peer_phase_hello_then_remote_ahead_exchange() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let handler = SequencePeer {
        events: events.clone(),
        handler: Arc::new(|message| match message {
            ControlMessage::Hello(_) => Ok(HttpResponsePayload::Hello(
                orion::control_plane::PeerHello {
                    node_id: NodeId::new("node-a"),
                    desired_revision: Revision::new(1),
                    desired_fingerprint: 99,
                    desired_section_fingerprints:
                        orion::control_plane::DesiredStateSectionFingerprints {
                            nodes: 0,
                            artifacts: 1,
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
            ControlMessage::SyncRequest(_) => Ok(HttpResponsePayload::Mutations(MutationBatch {
                base_revision: Revision::ZERO,
                mutations: vec![orion::control_plane::DesiredStateMutation::PutArtifact(
                    ArtifactRecord::builder("artifact.phase.remote").build(),
                )],
            })),
            other => Err(HttpTransportError::request_failed(format!(
                "unexpected payload: {other:?}"
            ))),
        }),
    };

    let (addr, server, listener) = HttpServer::bind(
        "127.0.0.1:0".parse().expect("socket address should parse"),
        Arc::new(handler),
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
        .expect("remote-ahead phase sync should succeed");

    assert_eq!(
        *events.lock().expect("sequence log should not be poisoned"),
        vec!["hello", "sync_request"]
    );

    server_task.abort();
}

#[tokio::test]
async fn node_sync_peer_phase_local_ahead_replays_mutations_after_hello() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let handler = SequencePeer {
        events: events.clone(),
        handler: Arc::new(|message| match message {
            ControlMessage::Hello(_) => Ok(HttpResponsePayload::Hello(
                orion::control_plane::PeerHello {
                    node_id: NodeId::new("node-a"),
                    desired_revision: Revision::ZERO,
                    desired_fingerprint: 0,
                    desired_section_fingerprints: zero_section_fingerprints(),
                    observed_revision: Revision::ZERO,
                    applied_revision: Revision::ZERO,
                    transport_binding_version: None,
                    transport_binding_public_key: None,
                    transport_tls_cert_pem: None,
                    transport_binding_signature: None,
                },
            )),
            ControlMessage::Mutations(_) => Ok(HttpResponsePayload::Accepted),
            other => Err(HttpTransportError::request_failed(format!(
                "unexpected payload: {other:?}"
            ))),
        }),
    };

    let (addr, server, listener) = HttpServer::bind(
        "127.0.0.1:0".parse().expect("socket address should parse"),
        Arc::new(handler),
    )
    .await
    .expect("custom HTTP server should bind");
    let server_task = tokio::spawn(server.serve(listener));

    let mut desired = orion::control_plane::DesiredClusterState::default();
    desired.put_artifact(ArtifactRecord::builder("artifact.phase.local").build());

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
        .desired(desired)
        .try_build()
        .expect("node app should build");

    node.sync_peer(&NodeId::new("node-a"))
        .await
        .expect("local-ahead phase sync should succeed");

    assert_eq!(
        *events.lock().expect("sequence log should not be poisoned"),
        vec!["hello", "mutations"]
    );

    server_task.abort();
}

#[tokio::test]
async fn node_sync_peer_phase_equal_revision_conflict_requests_snapshot_reconciliation() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let handler = SequencePeer {
        events: events.clone(),
        handler: Arc::new(|message| match message {
            ControlMessage::Hello(_) => Ok(HttpResponsePayload::Hello(
                orion::control_plane::PeerHello {
                    node_id: NodeId::new("node-a"),
                    desired_revision: Revision::new(1),
                    desired_fingerprint: 7,
                    desired_section_fingerprints: zero_section_fingerprints(),
                    observed_revision: Revision::ZERO,
                    applied_revision: Revision::ZERO,
                    transport_binding_version: None,
                    transport_binding_public_key: None,
                    transport_tls_cert_pem: None,
                    transport_binding_signature: None,
                },
            )),
            ControlMessage::SyncRequest(_) => Ok(HttpResponsePayload::Snapshot(
                orion::control_plane::StateSnapshot {
                    state: orion::control_plane::ClusterStateEnvelope {
                        desired: {
                            let mut desired = orion::control_plane::DesiredClusterState::default();
                            desired.put_artifact(
                                ArtifactRecord::builder("artifact.phase.conflict").build(),
                            );
                            desired
                        },
                        observed: orion::control_plane::ObservedClusterState::default(),
                        applied: orion::control_plane::AppliedClusterState::default(),
                    },
                },
            )),
            ControlMessage::Snapshot(_) => Ok(HttpResponsePayload::Accepted),
            other => Err(HttpTransportError::request_failed(format!(
                "unexpected payload: {other:?}"
            ))),
        }),
    };

    let (addr, server, listener) = HttpServer::bind(
        "127.0.0.1:0".parse().expect("socket address should parse"),
        Arc::new(handler),
    )
    .await
    .expect("custom HTTP server should bind");
    let server_task = tokio::spawn(server.serve(listener));

    let mut desired = orion::control_plane::DesiredClusterState::default();
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.phase.conflict"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.phase.conflict"),
        )
        .desired_state(DesiredState::Stopped)
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

    node.sync_peer(&NodeId::new("node-a"))
        .await
        .expect("equal-revision conflict phase sync should succeed");

    assert_eq!(
        *events.lock().expect("sequence log should not be poisoned"),
        vec!["hello", "sync_request", "snapshot"]
    );

    server_task.abort();
}

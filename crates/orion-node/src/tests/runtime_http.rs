use super::*;

#[tokio::test]
async fn node_http_host_applies_mutations_and_serves_snapshots() {
    let app = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build");

    let executor = TestExecutor::new();
    let command_log = executor.commands.clone();
    app.register_provider(TestProvider)
        .expect("provider registration should succeed");
    app.register_executor(executor)
        .expect("executor registration should succeed");

    let (addr, server) = app
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("HTTP server should start");

    let client = HttpClient::try_new(format!("http://{}", addr)).expect("HTTP client should build");
    let mut mutations = orion::control_plane::MutationBatch {
        base_revision: app.snapshot().desired_revision,
        mutations: Vec::new(),
    };
    mutations
        .mutations
        .push(orion::control_plane::DesiredStateMutation::PutArtifact(
            orion::control_plane::ArtifactRecord::builder("artifact.pose").build(),
        ));
    mutations
        .mutations
        .push(orion::control_plane::DesiredStateMutation::PutWorkload(
            WorkloadRecord::builder(
                WorkloadId::new("workload.pose"),
                RuntimeType::new("graph.exec.v1"),
                ArtifactId::new("artifact.pose"),
            )
            .desired_state(DesiredState::Running)
            .assigned_to(NodeId::new("node-a"))
            .require_resource(ResourceType::new("imu.sample"), 1)
            .observed_state(WorkloadObservedState::Pending)
            .build(),
        ));

    let response = client
        .send(
            &app.security
                .wrap_http_payload(HttpRequestPayload::Control(Box::new(
                    orion::control_plane::ControlMessage::Mutations(mutations),
                )))
                .expect("mutation request should be signed"),
        )
        .await
        .expect("mutation request should succeed");
    assert_eq!(response, HttpResponsePayload::Accepted);

    let snapshot = client
        .send(&HttpRequestPayload::Control(Box::new(
            orion::control_plane::ControlMessage::Hello(orion::control_plane::PeerHello {
                node_id: NodeId::new("node-b"),
                desired_revision: orion::Revision::ZERO,
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
                observed_revision: orion::Revision::ZERO,
                applied_revision: orion::Revision::ZERO,
                transport_binding_version: None,
                transport_binding_public_key: None,
                transport_tls_cert_pem: None,
                transport_binding_signature: None,
            }),
        )))
        .await
        .expect("hello request should yield metadata");

    match snapshot {
        HttpResponsePayload::Hello(hello) => {
            assert_eq!(
                hello.desired_revision,
                app.state_snapshot().state.desired.revision
            );
        }
        other => panic!("expected hello response, got {other:?}"),
    }

    assert_eq!(
        command_log
            .lock()
            .expect("test executor command log lock should not be poisoned")
            .len(),
        1
    );

    server.abort();
}

#[tokio::test]
async fn two_nodes_can_exchange_snapshots_over_http() {
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
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build");

    node_a
        .register_provider(TestProvider)
        .expect("provider registration should succeed");
    node_a
        .register_executor(TestExecutor::new())
        .expect("executor registration should succeed");

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
        .require_resource(ResourceType::new("imu.sample"), 1)
        .build(),
    );
    node_a.replace_desired(desired);

    let (addr_a, server_a) = node_a
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("node A server should start");
    let client_a =
        HttpClient::try_new(format!("http://{}", addr_a)).expect("HTTP client should build");

    let snapshot = match client_a
        .send(&HttpRequestPayload::Control(Box::new(
            orion::control_plane::ControlMessage::SyncRequest(orion::control_plane::SyncRequest {
                node_id: NodeId::new("node-b"),
                desired_revision: orion::Revision::new(u64::MAX),
                desired_fingerprint: 0,
                desired_summary: None,
                sections: Vec::new(),
                object_selectors: Vec::new(),
            }),
        )))
        .await
        .expect("sync request should return snapshot")
    {
        HttpResponsePayload::Snapshot(snapshot) => snapshot,
        other => panic!("expected snapshot response, got {other:?}"),
    };

    let response = node_b
        .http_control_handler()
        .handle_payload(
            node_a
                .security
                .wrap_http_payload(HttpRequestPayload::Control(Box::new(
                    orion::control_plane::ControlMessage::Snapshot(snapshot),
                )))
                .expect("snapshot request should be signed"),
        )
        .expect("snapshot should apply on node B");
    assert_eq!(response, HttpResponsePayload::Accepted);

    let node_b_snapshot = node_b.state_snapshot();
    assert!(
        node_b_snapshot
            .state
            .desired
            .workloads
            .contains_key(&WorkloadId::new("workload.pose"))
    );

    server_a.abort();
}

#[tokio::test]
async fn two_nodes_can_exchange_snapshots_over_https() {
    let (tls_dir, cert_path, key_path) = write_test_tls_files("https-sync");
    let node_a = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Disabled,
        ))
        .with_http_tls_files(cert_path.clone(), key_path.clone())
        .try_build()
        .expect("node app should build");
    node_a
        .register_provider(TestProvider)
        .expect("provider registration should succeed");
    node_a
        .register_executor(TestExecutor::new())
        .expect("executor registration should succeed");

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
        .require_resource(ResourceType::new("imu.sample"), 1)
        .build(),
    );
    node_a.replace_desired(desired);

    let (addr_a, server_a) = node_a
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("node A TLS server should start");
    let node_b = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-b"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-test"),
            reconcile_interval: std::time::Duration::from_millis(50),
            state_dir: None,
            peers: vec![
                PeerConfig::new("node-a", format!("https://localhost:{}", addr_a.port()))
                    .with_tls_root_cert_path(cert_path.display().to_string()),
            ],
            peer_authentication: crate::PeerAuthenticationMode::Disabled,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .try_build()
        .expect("node app should build");

    node_b
        .sync_peer(&NodeId::new("node-a"))
        .await
        .expect("sync over HTTPS should succeed");

    let desired = node_b.state_snapshot().state.desired;
    assert!(
        desired
            .artifacts
            .contains_key(&ArtifactId::new("artifact.pose"))
    );
    assert!(
        desired
            .workloads
            .contains_key(&WorkloadId::new("workload.pose"))
    );

    server_a.abort();
    let _ = fs::remove_dir_all(tls_dir);
}

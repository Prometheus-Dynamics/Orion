use super::*;

#[test]
fn required_peer_auth_rejects_unsigned_http_requests() {
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-main"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-main"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(temp_state_dir("required-http-reject")),
            peers: vec![PeerConfig::new("node-peer", "http://127.0.0.1:9101")],
            peer_authentication: PeerAuthenticationMode::Required,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .try_build()
        .expect("node app should build");

    let err = app
        .http_control_handler()
        .handle_payload(orion::transport::http::HttpRequestPayload::Control(
            Box::new(ControlMessage::Mutations(MutationBatch {
                base_revision: Revision::ZERO,
                mutations: Vec::new(),
            })),
        ))
        .expect_err("unsigned request should be rejected");

    assert!(err.to_string().contains("authentication required"));

    if let Some(state_dir) = app.config.state_dir.as_ref() {
        let _ = std::fs::remove_dir_all(state_dir);
    }
}

#[test]
fn optional_peer_auth_still_rejects_unsigned_peer_write_operations() {
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-main"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-main"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: None,
            peers: Vec::new(),
            peer_authentication: PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .try_build()
        .expect("node app should build");

    let mutation_err = app
        .http_control_handler()
        .handle_payload(orion::transport::http::HttpRequestPayload::Control(
            Box::new(ControlMessage::Mutations(MutationBatch {
                base_revision: Revision::ZERO,
                mutations: Vec::new(),
            })),
        ))
        .expect_err("unsigned peer mutation writes should be rejected");
    assert!(
        mutation_err
            .to_string()
            .contains("authenticated peer identity is required")
    );

    let observed_err = app
        .http_control_handler()
        .handle_payload(orion::transport::http::HttpRequestPayload::ObservedUpdate(
            ObservedStateUpdate {
                observed: orion::control_plane::ObservedClusterState::default(),
                applied: orion::control_plane::AppliedClusterState::default(),
            },
        ))
        .expect_err("unsigned observed updates should be rejected");
    assert!(
        observed_err
            .to_string()
            .contains("authenticated peer identity is required")
    );
}

#[test]
fn authenticated_but_unconfigured_peer_cannot_write_desired_state() {
    let main = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-main"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-main"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: None,
            peers: Vec::new(),
            peer_authentication: PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .try_build()
        .expect("node app should build");
    let unknown_peer = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-unknown"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-unknown"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: None,
            peers: Vec::new(),
            peer_authentication: PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .try_build()
        .expect("node app should build");

    let err = main
        .http_control_handler()
        .handle_payload(signed_peer_payload(
            &unknown_peer,
            orion::transport::http::HttpRequestPayload::Control(Box::new(
                ControlMessage::Mutations(MutationBatch {
                    base_revision: Revision::ZERO,
                    mutations: Vec::new(),
                }),
            )),
        ))
        .expect_err("unconfigured peer should not be allowed to write desired state");

    assert!(
        err.to_string()
            .contains("configured peer identity is required")
    );
}

#[test]
fn peer_http_rejects_local_only_control_messages() {
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-main"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-main"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: None,
            peers: Vec::new(),
            peer_authentication: PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .try_build()
        .expect("node app should build");

    let err = app
        .http_control_handler()
        .handle_payload(orion::transport::http::HttpRequestPayload::Control(
            Box::new(ControlMessage::ClientHello(ClientHello {
                client_name: "bad-http-client".into(),
                role: ClientRole::ControlPlane,
            })),
        ))
        .expect_err("peer http should reject local-only control messages");

    assert!(err.to_string().contains("is not allowed"));
}

#[test]
fn peer_http_rejects_mismatched_embedded_peer_identity() {
    let main = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-main"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-main"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: None,
            peers: vec![PeerConfig::new("node-peer", "http://127.0.0.1:9001")],
            peer_authentication: PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .try_build()
        .expect("node app should build");
    let peer = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-peer"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-peer"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: None,
            peers: Vec::new(),
            peer_authentication: PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .try_build()
        .expect("node app should build");

    let err = main
        .http_control_handler()
        .handle_payload(signed_peer_payload(
            &peer,
            orion::transport::http::HttpRequestPayload::Control(Box::new(ControlMessage::Hello(
                PeerHello {
                    node_id: NodeId::new("node-spoofed"),
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
            ))),
        ))
        .expect_err("peer with mismatched embedded identity should be rejected");

    assert!(
        err.to_string()
            .contains("authenticated peer node-peer cannot send payload for node-spoofed")
    );
}

#[test]
fn configured_peer_cannot_publish_foreign_observed_state() {
    let main = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-main"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-main"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: None,
            peers: vec![PeerConfig::new("node-peer", "http://127.0.0.1:9001")],
            peer_authentication: PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .try_build()
        .expect("node app should build");
    let peer = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-peer"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-peer"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: None,
            peers: Vec::new(),
            peer_authentication: PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .try_build()
        .expect("node app should build");
    let mut observed = orion::control_plane::ObservedClusterState::default();
    observed.put_node(
        NodeRecord::builder("node-other")
            .health(HealthState::Healthy)
            .build(),
    );

    let err = main
        .http_control_handler()
        .handle_payload(signed_peer_payload(
            &peer,
            orion::transport::http::HttpRequestPayload::ObservedUpdate(ObservedStateUpdate {
                observed,
                applied: orion::control_plane::AppliedClusterState::default(),
            }),
        ))
        .expect_err("peer should not be allowed to publish foreign observed node records");

    assert!(
        err.to_string()
            .contains("peer node-peer cannot publish observed node record for node-other")
    );
}

#[tokio::test]
async fn required_peer_auth_allows_signed_http_peer_sync() {
    let state_dir_a = temp_state_dir("signed-sync-a");
    let state_dir_b = temp_state_dir("signed-sync-b");
    let node_a = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-a"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-a"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(state_dir_a.clone()),
            peers: vec![PeerConfig::new("node-b", "http://127.0.0.1:1")],
            peer_authentication: PeerAuthenticationMode::Required,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .desired({
            let mut desired = DesiredClusterState::default();
            desired.put_artifact(ArtifactRecord::builder("artifact.auth").build());
            desired
        })
        .try_build()
        .expect("node app should build");
    let (addr_a, server_a) = node_a
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("node a http server should start");

    let node_b = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-b"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-b"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(state_dir_b.clone()),
            peers: vec![PeerConfig::new("node-a", format!("http://{}", addr_a))],
            peer_authentication: PeerAuthenticationMode::Required,
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
        .expect("signed peer sync should succeed");

    assert!(
        node_b
            .state_snapshot()
            .state
            .desired
            .artifacts
            .contains_key(&orion::ArtifactId::new("artifact.auth"))
    );
    assert!(
        NodeStorage::new(state_dir_a.clone())
            .trust_store_path()
            .exists()
    );

    server_a.abort();
    let _ = std::fs::remove_dir_all(state_dir_a);
    let _ = std::fs::remove_dir_all(state_dir_b);
}

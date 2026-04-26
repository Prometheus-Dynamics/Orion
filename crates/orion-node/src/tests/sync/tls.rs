use super::*;

#[test]
fn auto_http_tls_persists_generated_server_identity_across_restarts() {
    let state_dir = temp_state_dir("auto-http-tls");
    let config = test_node_config_with_state_dir_and_auth(
        "node-auto",
        "node-auto",
        state_dir.clone(),
        crate::PeerAuthenticationMode::Disabled,
    );

    let app = NodeApp::builder()
        .config(config.clone())
        .with_auto_http_tls(true)
        .try_build()
        .expect("node app should build");
    let cert_path = app
        .http_tls_cert_path()
        .expect("auto HTTP TLS should resolve a cert path")
        .to_path_buf();
    let key_path = app
        .http_tls_key_path()
        .expect("auto HTTP TLS should resolve a key path")
        .to_path_buf();
    let cert_bytes = fs::read(&cert_path).expect("generated cert should be readable");
    let key_bytes = fs::read(&key_path).expect("generated key should be readable");

    let restarted = NodeApp::builder()
        .config(config)
        .with_auto_http_tls(true)
        .try_build()
        .expect("node app should build");
    let restarted_cert_path = restarted
        .http_tls_cert_path()
        .expect("restarted node should reuse generated cert path");
    let restarted_key_path = restarted
        .http_tls_key_path()
        .expect("restarted node should reuse generated key path");

    assert_eq!(restarted_cert_path, cert_path.as_path());
    assert_eq!(restarted_key_path, key_path.as_path());
    assert_eq!(
        fs::read(restarted_cert_path).expect("restarted cert should be readable"),
        cert_bytes
    );
    assert_eq!(
        fs::read(restarted_key_path).expect("restarted key should be readable"),
        key_bytes
    );

    let _ = fs::remove_dir_all(state_dir);
}

#[test]
fn auto_http_tls_rotation_rewrites_server_identity() {
    let state_dir = temp_state_dir("auto-http-tls-rotate");
    let app = NodeApp::builder()
        .config(test_node_config_with_state_dir_and_auth(
            "node-auto-rotate",
            "node-auto-rotate",
            state_dir.clone(),
            crate::PeerAuthenticationMode::Disabled,
        ))
        .with_auto_http_tls(true)
        .try_build()
        .expect("node app should build");
    let cert_path = app
        .http_tls_cert_path()
        .expect("auto HTTP TLS should resolve a cert path");
    let key_path = app
        .http_tls_key_path()
        .expect("auto HTTP TLS should resolve a key path");
    let cert_before = fs::read(cert_path).expect("generated cert should be readable");
    let key_before = fs::read(key_path).expect("generated key should be readable");

    app.rotate_http_tls_identity()
        .expect("HTTP TLS identity rotation should succeed");

    let cert_after = fs::read(cert_path).expect("rotated cert should be readable");
    let key_after = fs::read(key_path).expect("rotated key should be readable");
    assert_ne!(cert_before, cert_after);
    assert_ne!(key_before, key_after);

    let _ = fs::remove_dir_all(state_dir);
}

#[tokio::test]
async fn two_nodes_can_exchange_snapshots_over_https_with_identity_bootstrap() {
    let state_dir_a = temp_state_dir("auto-https-a");
    let state_dir_b = temp_state_dir("auto-https-b");
    let node_a = NodeApp::builder()
        .config(test_node_config_with_state_dir_and_auth(
            "node-a",
            "node-auto-a",
            state_dir_a.clone(),
            crate::PeerAuthenticationMode::Optional,
        ))
        .with_auto_http_tls(true)
        .with_http_mutual_tls_mode(crate::app::HttpMutualTlsMode::Optional)
        .try_build()
        .expect("node app should build");
    node_a
        .register_provider(TestProvider)
        .expect("provider registration should succeed");
    node_a
        .register_executor(TestExecutor::new())
        .expect("executor registration should succeed");

    let mut desired = node_a.state_snapshot().state.desired;
    desired.put_artifact(ArtifactRecord::builder("artifact.auto").build());
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.auto"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.auto"),
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
        .expect("node A auto TLS server should start");
    let node_b = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-b"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-auto-b"),
            reconcile_interval: std::time::Duration::from_millis(50),
            state_dir: Some(state_dir_b.clone()),
            peers: vec![
                PeerConfig::new(
                    "node-a",
                    orion_core::PeerBaseUrl::new(format!("https://localhost:{}", addr_a.port())),
                )
                .with_trusted_public_key_hex(node_a.security.public_key_hex()),
            ],
            peer_authentication: crate::PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .with_auto_http_tls(true)
        .with_http_mutual_tls_mode(crate::app::HttpMutualTlsMode::Optional)
        .try_build()
        .expect("node app should build");

    node_b
        .sync_peer(&NodeId::new("node-a"))
        .await
        .expect("sync over auto HTTPS should succeed");

    let desired = node_b.state_snapshot().state.desired;
    assert!(
        desired
            .artifacts
            .contains_key(&ArtifactId::new("artifact.auto"))
    );
    assert!(
        desired
            .workloads
            .contains_key(&WorkloadId::new("workload.auto"))
    );
    assert!(
        node_b
            .security
            .trusted_peer_tls_root_cert_pem(&NodeId::new("node-a"))
            .expect("learned peer TLS cert should be readable")
            .is_some()
    );
    assert!(
        node_a
            .security
            .trusted_peer_tls_root_cert_pem(&NodeId::new("node-b"))
            .expect("learned client TLS cert should be readable")
            .is_some()
    );

    server_a.abort();
    let _ = fs::remove_dir_all(state_dir_a);
    let _ = fs::remove_dir_all(state_dir_b);
}

#[tokio::test]
async fn optional_mtls_peer_rebinds_after_remote_http_tls_rotation() {
    let state_dir_a = temp_state_dir("optional-rebind-a");
    let state_dir_b = temp_state_dir("optional-rebind-b");
    let node_a = NodeApp::builder()
        .config(test_node_config_with_state_dir_and_auth(
            "node-a",
            "node-rebind-a",
            state_dir_a.clone(),
            crate::PeerAuthenticationMode::Optional,
        ))
        .with_auto_http_tls(true)
        .with_http_mutual_tls_mode(crate::app::HttpMutualTlsMode::Optional)
        .try_build()
        .expect("node app should build");
    node_a
        .register_provider(TestProvider)
        .expect("provider registration should succeed");
    node_a
        .register_executor(TestExecutor::new())
        .expect("executor registration should succeed");
    let mut desired = node_a.state_snapshot().state.desired;
    desired.put_artifact(ArtifactRecord::builder("artifact.rebind").build());
    node_a.replace_desired(desired);

    let (addr_a, server_a) = node_a
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("node A HTTPS server should start");
    let node_b = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-b"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-rebind-b"),
            reconcile_interval: Duration::from_millis(50),
            state_dir: Some(state_dir_b.clone()),
            peers: vec![
                PeerConfig::new(
                    "node-a",
                    orion_core::PeerBaseUrl::new(format!("https://localhost:{}", addr_a.port())),
                )
                .with_trusted_public_key_hex(node_a.security.public_key_hex()),
            ],
            peer_authentication: crate::PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .with_auto_http_tls(true)
        .with_http_mutual_tls_mode(crate::app::HttpMutualTlsMode::Optional)
        .try_build()
        .expect("node app should build");

    node_b
        .sync_peer(&NodeId::new("node-a"))
        .await
        .expect("initial sync should succeed");
    let fingerprint_before = node_b
        .peer_trust_status(&NodeId::new("node-a"))
        .expect("peer trust lookup should succeed")
        .expect("peer trust status should exist")
        .learned_tls_root_cert_fingerprint
        .expect("initial learned TLS fingerprint should exist");

    node_a
        .rotate_http_tls_identity()
        .expect("node A TLS rotation should succeed");
    node_b.evict_all_peer_clients();

    node_b
        .sync_peer(&NodeId::new("node-a"))
        .await
        .expect("sync after TLS rotation should succeed via rebind");
    let fingerprint_after = node_b
        .peer_trust_status(&NodeId::new("node-a"))
        .expect("peer trust lookup should succeed")
        .expect("peer trust status should exist")
        .learned_tls_root_cert_fingerprint
        .expect("rotated learned TLS fingerprint should exist");
    assert_ne!(fingerprint_before, fingerprint_after);

    server_a.abort();
    let _ = fs::remove_dir_all(state_dir_a);
    let _ = fs::remove_dir_all(state_dir_b);
}

#[tokio::test]
async fn two_nodes_can_exchange_snapshots_over_https_with_required_mtls() {
    let state_dir_a = temp_state_dir("required-mtls-a");
    let state_dir_b = temp_state_dir("required-mtls-b");
    let node_a = NodeApp::builder()
        .config(test_node_config_with_state_dir_and_auth(
            "node-a",
            "node-required-a",
            state_dir_a.clone(),
            crate::PeerAuthenticationMode::Required,
        ))
        .with_auto_http_tls(true)
        .with_http_mutual_tls_mode(crate::app::HttpMutualTlsMode::Required)
        .try_build()
        .expect("node app should build");
    let node_b = NodeApp::builder()
        .config(test_node_config_with_state_dir_and_auth(
            "node-b",
            "node-required-b",
            state_dir_b.clone(),
            crate::PeerAuthenticationMode::Required,
        ))
        .with_auto_http_tls(true)
        .with_http_mutual_tls_mode(crate::app::HttpMutualTlsMode::Required)
        .try_build()
        .expect("node app should build");

    let node_a_cert = fs::read(
        node_a
            .http_tls_cert_path()
            .expect("node A should resolve an HTTPS cert path"),
    )
    .expect("node A cert should be readable");
    let node_b_cert = fs::read(
        node_b
            .http_tls_cert_path()
            .expect("node B should resolve an HTTPS cert path"),
    )
    .expect("node B cert should be readable");

    node_a
        .enroll_peer(
            PeerConfig::new("node-b", "https://localhost:0")
                .with_trusted_public_key_hex(node_b.security.public_key_hex()),
        )
        .expect("node A should enroll node B");
    node_b
        .enroll_peer(
            PeerConfig::new("node-a", "https://localhost:0")
                .with_trusted_public_key_hex(node_a.security.public_key_hex()),
        )
        .expect("node B should enroll node A");

    let node_a_binding = node_a
        .security
        .transport_binding(&node_a_cert)
        .expect("node A transport binding should sign");
    let node_b_binding = node_b
        .security
        .transport_binding(&node_b_cert)
        .expect("node B transport binding should sign");
    node_a
        .security
        .validate_or_update_transport_binding(&NodeId::new("node-b"), &node_b_binding)
        .expect("node A should trust node B client cert");
    node_b
        .security
        .validate_or_update_transport_binding(&NodeId::new("node-a"), &node_a_binding)
        .expect("node B should trust node A server cert");

    node_a
        .register_provider(TestProvider)
        .expect("provider registration should succeed");
    node_a
        .register_executor(TestExecutor::new())
        .expect("executor registration should succeed");
    let mut desired = node_a.state_snapshot().state.desired;
    desired.put_artifact(ArtifactRecord::builder("artifact.required").build());
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.required"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.required"),
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
        .expect("node A required mTLS server should start");
    node_b
        .enroll_peer(
            PeerConfig::new(
                "node-a",
                orion_core::PeerBaseUrl::new(format!("https://localhost:{}", addr_a.port())),
            )
            .with_trusted_public_key_hex(node_a.security.public_key_hex()),
        )
        .expect("node B should update node A endpoint");

    node_b
        .sync_peer(&NodeId::new("node-a"))
        .await
        .expect("sync over required mTLS should succeed");

    let desired = node_b.state_snapshot().state.desired;
    assert!(
        desired
            .artifacts
            .contains_key(&ArtifactId::new("artifact.required"))
    );
    assert!(
        desired
            .workloads
            .contains_key(&WorkloadId::new("workload.required"))
    );

    server_a.abort();
    let _ = fs::remove_dir_all(state_dir_a);
    let _ = fs::remove_dir_all(state_dir_b);
}

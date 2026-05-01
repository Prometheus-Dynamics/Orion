use super::*;

#[test]
fn node_sync_status_and_error_updates_reject_unknown_peers() {
    let app = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Optional,
        ))
        .try_build()
        .expect("node app should build");

    let err = app
        .set_peer_sync_status(&NodeId::new("node-missing"), PeerSyncStatus::Syncing)
        .expect_err("unknown peer status update should fail");
    assert!(matches!(err, NodeError::UnknownPeer(_)));

    let err = app
        .record_peer_error(&NodeId::new("node-missing"), "boom")
        .expect_err("unknown peer error update should fail");
    assert!(matches!(err, NodeError::UnknownPeer(_)));
}

#[test]
fn node_app_tracks_configured_and_registered_peers() {
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-a"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-test"),
            reconcile_interval: std::time::Duration::from_millis(50),
            state_dir: None,
            peers: vec![PeerConfig::new("node-b", "http://127.0.0.1:9201")],
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

    assert_eq!(app.snapshot().registered_peers, 1);
    assert_eq!(app.peer_states().len(), 1);

    app.register_peer(PeerConfig::new("node-c", "http://127.0.0.1:9202"))
        .expect("peer registration should succeed");

    assert_eq!(app.snapshot().registered_peers, 2);
}

#[test]
fn node_app_records_peer_hello_and_sync_status() {
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-a"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-test"),
            reconcile_interval: std::time::Duration::from_millis(50),
            state_dir: None,
            peers: vec![PeerConfig::new("node-b", "http://127.0.0.1:9201")],
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

    app.set_peer_sync_status(&NodeId::new("node-b"), PeerSyncStatus::Negotiating)
        .expect("peer sync status should update");
    app.record_peer_hello(
        &NodeId::new("node-b"),
        &orion::control_plane::PeerHello {
            node_id: NodeId::new("node-b"),
            desired_revision: orion::Revision::new(5),
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
            observed_revision: orion::Revision::new(4),
            applied_revision: orion::Revision::new(4),
            transport_binding_version: None,
            transport_binding_public_key: None,
            transport_tls_cert_pem: None,
            transport_binding_signature: None,
        },
        CompatibilityState::Downgraded,
    )
    .expect("peer hello should record");

    let peer = app
        .peer_states()
        .into_iter()
        .find(|peer| peer.node_id == NodeId::new("node-b"))
        .expect("peer state should exist");
    assert_eq!(peer.compatibility, Some(CompatibilityState::Downgraded));
    assert_eq!(peer.desired_revision, orion::Revision::new(5));
    assert_eq!(peer.sync_status, PeerSyncStatus::Ready);
}

#[test]
fn node_app_enroll_peer_upserts_runtime_peer_and_trust_state() {
    let app = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Optional,
        ))
        .try_build()
        .expect("node app should build");

    app.enroll_peer(
        PeerConfig::new("node-b", "http://127.0.0.1:9201").with_trusted_public_key_hex(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ),
    )
    .expect("first peer enrollment should succeed");
    app.enroll_peer(
        PeerConfig::new("node-b", "http://127.0.0.1:9301").with_trusted_public_key_hex(
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        ),
    )
    .expect("second peer enrollment should update existing peer");

    let peer = app
        .peer_states()
        .into_iter()
        .find(|peer| peer.node_id == NodeId::new("node-b"))
        .expect("peer state should exist");
    assert_eq!(peer.base_url, "http://127.0.0.1:9301");

    let status = app
        .peer_trust_status(&NodeId::new("node-b"))
        .expect("peer trust status lookup should succeed")
        .expect("peer trust status should exist");
    assert_eq!(
        status.configured_public_key_hex.as_deref(),
        Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
    );
    assert_eq!(status.base_url.as_deref(), Some("http://127.0.0.1:9301"));
    assert!(!status.revoked);
}

#[test]
fn peer_trust_statuses_include_revoked_unconfigured_peers() {
    let app = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Optional,
        ))
        .try_build()
        .expect("node app should build");

    app.replace_peer_identity_hex(
        &NodeId::new("node-revoked"),
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
    )
    .expect("trusted peer key replacement should succeed");
    app.revoke_peer_identity(&NodeId::new("node-revoked"))
        .expect("peer revocation should succeed");

    let status = app
        .peer_trust_status(&NodeId::new("node-revoked"))
        .expect("peer trust status lookup should succeed")
        .expect("revoked peer trust status should exist");
    assert!(status.revoked);
    assert_eq!(status.base_url, None);
    assert_eq!(status.configured_public_key_hex, None);
    assert_eq!(status.trusted_public_key_hex, None);
    assert_eq!(status.configured_tls_root_cert_path, None);
    assert_eq!(status.learned_tls_root_cert_fingerprint, None);
    assert_eq!(status.sync_status, None);
}

#[test]
fn peer_trust_query_exposes_mtls_mode_and_learned_tls_binding() {
    let state_dir_a = temp_state_dir("trust-query-a");
    let state_dir_b = temp_state_dir("trust-query-b");
    let node_a = NodeApp::builder()
        .config(test_node_config_with_state_dir_and_auth(
            "node-a",
            "node-trust-a",
            state_dir_a.clone(),
            crate::PeerAuthenticationMode::Optional,
        ))
        .with_auto_http_tls(true)
        .with_http_mutual_tls_mode(crate::app::HttpMutualTlsMode::Optional)
        .try_build()
        .expect("node app should build");
    let node_b = NodeApp::builder()
        .config(test_node_config_with_state_dir_and_auth(
            "node-b",
            "node-trust-b",
            state_dir_b.clone(),
            crate::PeerAuthenticationMode::Optional,
        ))
        .with_auto_http_tls(true)
        .with_http_mutual_tls_mode(crate::app::HttpMutualTlsMode::Optional)
        .try_build()
        .expect("node app should build");

    let node_b_cert = fs::read(
        node_b
            .http_tls_cert_path()
            .expect("node B should resolve an HTTPS cert path"),
    )
    .expect("node B cert should be readable");
    node_a
        .enroll_peer(
            PeerConfig::new("node-b", "https://localhost:9443")
                .with_trusted_public_key_hex(node_b.security.public_key_hex()),
        )
        .expect("peer enrollment should succeed");
    let binding = node_b
        .security
        .transport_binding(&node_b_cert)
        .expect("transport binding should sign");
    node_a
        .security
        .validate_or_update_transport_binding(&NodeId::new("node-b"), &binding)
        .expect("transport binding should be trusted");

    let status = node_a
        .peer_trust_status(&NodeId::new("node-b"))
        .expect("peer trust status lookup should succeed")
        .expect("peer trust status should exist");
    assert_eq!(
        status.configured_tls_root_cert_path, None,
        "this peer should rely on learned transport trust, not configured CA path"
    );
    assert!(status.learned_tls_root_cert_fingerprint.is_some());

    let response = node_a
        .serve_local_control_message(
            ControlSurface::LocalIpc,
            LocalAddress::new("trust-reader"),
            LocalAddress::new("orion"),
            ControlMessage::ClientHello(ClientHello {
                client_name: "trust-reader".into(),
                role: ClientRole::ControlPlane,
            }),
        )
        .and_then(|_| {
            node_a.serve_local_control_message(
                ControlSurface::LocalIpc,
                LocalAddress::new("trust-reader"),
                LocalAddress::new("orion"),
                ControlMessage::QueryPeerTrust,
            )
        })
        .expect("query peer trust should succeed");
    match response {
        ControlMessage::PeerTrust(snapshot) => {
            assert_eq!(
                snapshot.http_mutual_tls_mode,
                orion::control_plane::HttpMutualTlsMode::Optional
            );
            let peer = snapshot
                .peers
                .into_iter()
                .find(|peer| peer.node_id == NodeId::new("node-b"))
                .expect("peer trust snapshot should include node-b");
            assert_eq!(peer.configured_tls_root_cert_path, None);
            assert!(peer.learned_tls_root_cert_fingerprint.is_some());
        }
        other => panic!("expected peer trust response, got {other:?}"),
    }

    let _ = fs::remove_dir_all(state_dir_a);
    let _ = fs::remove_dir_all(state_dir_b);
}

#[test]
fn local_enroll_peer_can_pretrust_tls_root_cert() {
    let app = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-enroll-tls",
            crate::PeerAuthenticationMode::Optional,
        ))
        .with_http_mutual_tls_mode(crate::app::HttpMutualTlsMode::Required)
        .try_build()
        .expect("node app should build");

    let rcgen::CertifiedKey { cert, .. } =
        generate_simple_self_signed(vec!["peer.local".to_owned()])
            .expect("peer TLS test certificate should generate");
    let tls_root_cert_pem = cert.pem().into_bytes();
    app.serve_local_control_message(
        ControlSurface::LocalIpc,
        LocalAddress::new("trust-enroll"),
        LocalAddress::new("orion"),
        ControlMessage::ClientHello(ClientHello {
            client_name: "trust-enroll".into(),
            role: ClientRole::ControlPlane,
        }),
    )
    .expect("client hello should succeed");
    app.serve_local_control_message(
        ControlSurface::LocalIpc,
        LocalAddress::new("trust-enroll"),
        LocalAddress::new("orion"),
        ControlMessage::EnrollPeer(PeerEnrollment {
            node_id: NodeId::new("node-b"),
            base_url: "https://localhost:9443".into(),
            trusted_public_key_hex: Some(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            ),
            trusted_tls_root_cert_pem: Some(tls_root_cert_pem.clone()),
        }),
    )
    .expect("peer enrollment should succeed");

    let status = app
        .peer_trust_status(&NodeId::new("node-b"))
        .expect("peer trust status lookup should succeed")
        .expect("peer trust status should exist");
    assert_eq!(status.configured_tls_root_cert_path, None);
    let mut hasher = DefaultHasher::new();
    tls_root_cert_pem.hash(&mut hasher);
    assert_eq!(
        status.learned_tls_root_cert_fingerprint,
        Some(format!("{:016x}", hasher.finish()))
    );
}

#[test]
fn peer_trust_status_classifies_sync_errors_for_troubleshooting() {
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-a"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-troubleshoot"),
            reconcile_interval: Duration::from_millis(50),
            state_dir: None,
            peers: vec![
                PeerConfig::new("node-b", "https://localhost:9443").with_trusted_public_key_hex(
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                ),
            ],
            peer_authentication: crate::PeerAuthenticationMode::Required,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .with_http_mutual_tls_mode(crate::app::HttpMutualTlsMode::Required)
        .try_build()
        .expect("node app should build");

    app.record_peer_error(
        &NodeId::new("node-b"),
        "HTTPS peer node-b requires prior TLS enrollment before mTLS-required sync",
    )
    .expect("peer sync error should record");

    let status = app
        .peer_trust_status(&NodeId::new("node-b"))
        .expect("peer trust status lookup should succeed")
        .expect("peer trust status should exist");
    assert_eq!(
        status.last_error_kind,
        Some(orion::control_plane::PeerSyncErrorKind::TlsTrust)
    );
    assert!(
        status
            .troubleshooting_hint
            .as_deref()
            .unwrap_or_default()
            .contains("trust enroll --tls-root-cert")
    );
    assert!(status.last_error.is_some());
}

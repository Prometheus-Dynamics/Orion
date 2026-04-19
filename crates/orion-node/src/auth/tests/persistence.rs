use super::*;
use std::fs;

#[test]
fn node_identity_persists_when_state_dir_is_reused() {
    let state_dir = temp_state_dir("identity-persist");
    let storage = NodeStorage::new(state_dir.clone());

    let first = NodeSecurity::load_or_create(
        NodeId::new("node-a"),
        PeerAuthenticationMode::Optional,
        &[],
        Some(storage.clone()),
        128,
    )
    .expect("first security initialization should succeed");
    let second = NodeSecurity::load_or_create(
        NodeId::new("node-a"),
        PeerAuthenticationMode::Optional,
        &[],
        Some(storage.clone()),
        128,
    )
    .expect("second security initialization should succeed");

    assert_eq!(first.public_key_hex(), second.public_key_hex());

    let _ = std::fs::remove_dir_all(state_dir);
}

#[test]
fn node_identity_load_ignores_stale_temp_files() {
    let state_dir = temp_state_dir("identity-stale-temp");
    let storage = NodeStorage::new(state_dir.clone());

    let first = NodeSecurity::load_or_create(
        NodeId::new("node-a"),
        PeerAuthenticationMode::Optional,
        &[],
        Some(storage.clone()),
        128,
    )
    .expect("initial security initialization should succeed");

    let stale_tmp = storage.identity_path().with_extension("bin.stale.tmp");
    fs::write(&stale_tmp, [9_u8; 32]).expect("stale temp file should be written");

    let second = NodeSecurity::load_or_create(
        NodeId::new("node-a"),
        PeerAuthenticationMode::Optional,
        &[],
        Some(storage.clone()),
        128,
    )
    .expect("security initialization with stale temp file should succeed");

    assert_eq!(first.public_key_hex(), second.public_key_hex());
    let _ = fs::remove_dir_all(state_dir);
}

#[test]
fn outbound_nonce_persists_when_state_dir_is_reused() {
    let state_dir = temp_state_dir("nonce-persist");
    let storage = NodeStorage::new(state_dir.clone());

    let first = NodeSecurity::load_or_create(
        NodeId::new("node-a"),
        PeerAuthenticationMode::Optional,
        &[],
        Some(storage.clone()),
        128,
    )
    .expect("first security initialization should succeed");
    let first_request = first
        .wrap_http_payload(orion::transport::http::HttpRequestPayload::Control(
            Box::new(ControlMessage::Ping),
        ))
        .expect("first signed payload should succeed");
    let first_nonce = match first_request {
        orion::transport::http::HttpRequestPayload::AuthenticatedPeer(request) => {
            request.auth.nonce
        }
        other => panic!("expected authenticated peer payload, got {other:?}"),
    };

    let second = NodeSecurity::load_or_create(
        NodeId::new("node-a"),
        PeerAuthenticationMode::Optional,
        &[],
        Some(storage.clone()),
        128,
    )
    .expect("second security initialization should succeed");
    let second_request = second
        .wrap_http_payload(orion::transport::http::HttpRequestPayload::Control(
            Box::new(ControlMessage::Ping),
        ))
        .expect("second signed payload should succeed");
    let second_nonce = match second_request {
        orion::transport::http::HttpRequestPayload::AuthenticatedPeer(request) => {
            request.auth.nonce
        }
        other => panic!("expected authenticated peer payload, got {other:?}"),
    };

    assert!(second_nonce > first_nonce);

    let _ = std::fs::remove_dir_all(state_dir);
}

#[test]
fn trusted_peer_store_load_ignores_stale_temp_files() {
    let state_dir = temp_state_dir("trust-store-stale-temp");
    let storage = NodeStorage::new(state_dir.clone());
    let peer_id = NodeId::new("node-peer");

    let main = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-main"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-main"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(state_dir.clone()),
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
    let peer = NodeApp::builder()
        .config(NodeConfig {
            node_id: peer_id.clone(),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-peer"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(temp_state_dir("trust-store-stale-temp-peer")),
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

    main.http_control_handler()
        .handle_payload(signed_peer_hello(&peer))
        .expect("trusted peer state should persist");
    let expected_key = main
        .trusted_peer_public_key_hex(&peer_id)
        .expect("trusted peer lookup should succeed");

    let stale_tmp = storage.trust_store_path().with_extension("rkyv.stale.tmp");
    fs::write(&stale_tmp, b"corrupt temp state").expect("stale trust-store temp file should write");

    let restarted = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-main"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-main"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(state_dir.clone()),
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
        .expect("restarted node app should build");

    assert_eq!(
        restarted
            .trusted_peer_public_key_hex(&peer_id)
            .expect("trusted peer lookup after restart should succeed"),
        expected_key
    );

    let _ = fs::remove_dir_all(state_dir);
}

#[test]
fn replayed_nonce_is_rejected_after_restart() {
    let state_dir_main = temp_state_dir("nonce-replay-main");
    let state_dir_peer = temp_state_dir("nonce-replay-peer");

    let main = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-main"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-main"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(state_dir_main.clone()),
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
    let peer = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-peer"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-peer"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(state_dir_peer.clone()),
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

    let signed = signed_peer_hello(&peer);
    main.http_control_handler()
        .handle_payload(signed.clone())
        .expect("first signed peer request should succeed");

    let restarted_main = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-main"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-main"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(state_dir_main.clone()),
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

    let err = restarted_main
        .http_control_handler()
        .handle_payload(signed)
        .expect_err("replayed signed peer request should be rejected after restart");
    assert!(err.to_string().contains("replayed nonce"));

    let _ = std::fs::remove_dir_all(state_dir_main);
    let _ = std::fs::remove_dir_all(state_dir_peer);
}

#[test]
fn revoked_peer_is_rejected_until_it_is_replaced() {
    let state_dir_main = temp_state_dir("revoked-peer-main");
    let state_dir_old = temp_state_dir("revoked-peer-old");
    let state_dir_new = temp_state_dir("revoked-peer-new");

    let main = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-main"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-main"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(state_dir_main.clone()),
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
    let old_peer = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-peer"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-peer-old"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(state_dir_old.clone()),
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
    let new_peer = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-peer"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-peer-new"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(state_dir_new.clone()),
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

    let initial_payload = signed_peer_hello(&old_peer);
    main.http_control_handler()
        .handle_payload(initial_payload)
        .expect("initial signed peer hello should be accepted");

    let peer_id = NodeId::new("node-peer");
    assert_eq!(
        main.trusted_peer_public_key_hex(&peer_id)
            .expect("trusted peer key query should succeed"),
        Some(old_peer.local_public_key_hex())
    );

    assert!(
        main.revoke_peer_identity(&peer_id)
            .expect("peer revocation should succeed")
    );
    assert!(
        main.is_peer_revoked(&peer_id)
            .expect("revocation status query should succeed")
    );
    assert_eq!(
        main.trusted_peer_public_key_hex(&peer_id)
            .expect("trusted peer key query should succeed"),
        None
    );

    let revoked_payload = signed_peer_hello(&old_peer);
    let revoked_err = main
        .http_control_handler()
        .handle_payload(revoked_payload)
        .expect_err("revoked peer should be rejected");
    assert!(
        revoked_err
            .to_string()
            .contains("peer node-peer is revoked")
    );

    let untrusted_new_payload = signed_peer_hello(&new_peer);
    let untrusted_new_err = main
        .http_control_handler()
        .handle_payload(untrusted_new_payload)
        .expect_err("replacement peer should still be rejected before trust is replaced");
    assert!(
        untrusted_new_err
            .to_string()
            .contains("peer node-peer is revoked")
    );

    main.replace_peer_identity_hex(&peer_id, &new_peer.local_public_key_hex())
        .expect("peer key replacement should succeed");
    assert!(
        !main
            .is_peer_revoked(&peer_id)
            .expect("revocation status query should succeed")
    );
    assert_eq!(
        main.trusted_peer_public_key_hex(&peer_id)
            .expect("trusted peer key query should succeed"),
        Some(new_peer.local_public_key_hex())
    );

    let replaced_payload = signed_peer_hello(&new_peer);
    main.http_control_handler()
        .handle_payload(replaced_payload)
        .expect("replacement peer should be accepted");

    let old_payload_after_replace = signed_peer_hello(&old_peer);
    let old_err = main
        .http_control_handler()
        .handle_payload(old_payload_after_replace)
        .expect_err("old key should be rejected after replacement");
    assert!(
        old_err
            .to_string()
            .contains("peer node-peer presented a different trusted key")
    );

    let reloaded = NodeSecurity::load_or_create(
        NodeId::new("node-main"),
        PeerAuthenticationMode::Optional,
        &[],
        Some(NodeStorage::new(state_dir_main.clone())),
        128,
    )
    .expect("reloaded security should succeed");
    assert_eq!(
        reloaded
            .trusted_peer_public_key_hex(&peer_id)
            .expect("trusted peer key query should succeed"),
        Some(new_peer.local_public_key_hex())
    );
    assert!(
        !reloaded
            .is_peer_revoked(&peer_id)
            .expect("revocation status query should succeed")
    );

    let _ = std::fs::remove_dir_all(state_dir_main);
    let _ = std::fs::remove_dir_all(state_dir_old);
    let _ = std::fs::remove_dir_all(state_dir_new);
}

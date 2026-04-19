use super::*;
use crate::auth::crypto::{current_effective_gid, current_effective_uid};

#[test]
fn local_ipc_rejects_wrong_client_role_before_app_logic() {
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

    let source = LocalAddress::new("provider-client");
    let destination = LocalAddress::new("orion");
    let _ = app
        .serve_local_control_message(
            crate::ControlSurface::LocalIpc,
            source.clone(),
            destination.clone(),
            ControlMessage::ClientHello(ClientHello {
                client_name: "provider-client".into(),
                role: ClientRole::Provider,
            }),
        )
        .expect("client hello should register local client");

    let err = app
        .serve_local_control_message(
            crate::ControlSurface::LocalIpc,
            source,
            destination,
            ControlMessage::WatchState(StateWatch {
                desired_revision: Revision::ZERO,
            }),
        )
        .expect_err("provider role should not be allowed to watch control-plane state");

    assert!(matches!(err, NodeError::ClientRoleMismatch { .. }));
}

#[test]
fn local_ipc_rejects_mismatched_unix_peer_uid_before_app_logic() {
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
        .serve_control_request(crate::service::ControlRequest::from_local_message(
            crate::ControlSurface::LocalIpc,
            LocalAddress::new("cli"),
            LocalAddress::new("orion"),
            ControlMessage::ClientHello(ClientHello {
                client_name: "cli".into(),
                role: ClientRole::ControlPlane,
            }),
            Some(UnixPeerIdentity {
                pid: Some(std::process::id()),
                uid: current_effective_uid().saturating_add(1),
                gid: 0,
            }),
        ))
        .expect_err("mismatched unix peer uid should be rejected");

    assert!(
        matches!(err, NodeError::Authorization(message) if message.contains("does not satisfy"))
    );
}

#[test]
fn local_ipc_same_user_or_group_mode_allows_matching_group() {
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
        .with_local_authentication_mode(LocalAuthenticationMode::SameUserOrGroup)
        .try_build()
        .expect("node app should build");

    let response = app
        .serve_control_request(crate::service::ControlRequest::from_local_message(
            crate::ControlSurface::LocalIpc,
            LocalAddress::new("cli"),
            LocalAddress::new("orion"),
            ControlMessage::ClientHello(ClientHello {
                client_name: "cli".into(),
                role: ClientRole::ControlPlane,
            }),
            Some(UnixPeerIdentity {
                pid: Some(std::process::id()),
                uid: current_effective_uid().saturating_add(1),
                gid: current_effective_gid(),
            }),
        ))
        .expect("matching gid should satisfy same-user-or-group policy");

    match response {
        crate::service::ControlResponse::Local(message)
            if matches!(message.as_ref(), ControlMessage::ClientWelcome(_)) => {}
        other => panic!("expected local client welcome response, got {other:?}"),
    }
}

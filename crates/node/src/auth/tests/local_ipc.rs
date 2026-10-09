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
            ControlMessage::WatchState(StateWatch::desired(Revision::ZERO)),
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
                groups: Vec::new(),
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
                groups: Vec::new(),
            }),
        ))
        .expect("matching gid should satisfy same-user-or-group policy");

    match response {
        crate::service::ControlResponse::Local(message)
            if matches!(message.as_ref(), ControlMessage::ClientWelcome(_)) => {}
        other => panic!("expected local client welcome response, got {other:?}"),
    }
}

fn local_auth_test_app(
    mode: LocalAuthenticationMode,
    allow: Option<crate::auth::LocalAccessAllowList>,
) -> NodeApp {
    let mut builder = NodeApp::builder()
        .config(NodeConfig::for_local_node(NodeId::new("node-main")))
        .with_local_authentication_mode(mode);
    if let Some(allow) = allow {
        builder = builder.with_local_access_allow(allow);
    }
    builder.try_build().expect("node app should build")
}

/// Sends a `ClientHello` from a caller with these credentials; `Ok` when admitted.
fn hello_as(app: &NodeApp, uid: u32, gid: u32, groups: &[u32]) -> Result<(), NodeError> {
    app.serve_control_request(crate::service::ControlRequest::from_local_message(
        crate::ControlSurface::LocalIpc,
        LocalAddress::new("cli"),
        LocalAddress::new("orion"),
        ControlMessage::ClientHello(ClientHello {
            client_name: "cli".into(),
            role: ClientRole::ControlPlane,
        }),
        Some(UnixPeerIdentity {
            pid: Some(std::process::id()),
            uid,
            gid,
            groups: groups.to_vec(),
        }),
    ))
    .map(|_| ())
}

/// A uid and gid that are neither the test process's nor root's.
fn stranger() -> (u32, u32) {
    let uid = current_effective_uid().wrapping_add(4242).max(1);
    let gid = current_effective_gid().wrapping_add(4242).max(1);
    (uid, gid)
}

#[test]
fn same_user_or_group_admits_supplementary_members_of_the_node_group() {
    let app = local_auth_test_app(LocalAuthenticationMode::SameUserOrGroup, None);
    let (uid, gid) = stranger();
    hello_as(&app, uid, gid, &[current_effective_gid()])
        .expect("a supplementary member of the node's group is admitted");
    let err = hello_as(&app, uid, gid, &[gid]).expect_err("a stranger is refused");
    assert!(
        matches!(&err, NodeError::Authorization(message) if message.contains("ORION_NODE_LOCAL_AUTH_ALLOW")),
        "the refusal names the knobs: {err:?}"
    );
}

#[test]
fn root_needs_the_or_root_mode_or_an_allow_list_entry() {
    let node_is_root = current_effective_uid() == 0 || current_effective_gid() == 0;
    if !node_is_root {
        let app = local_auth_test_app(LocalAuthenticationMode::SameUserOrGroup, None);
        hello_as(&app, 0, 0, &[]).expect_err("root is not the node's user or group");
        let app = local_auth_test_app(LocalAuthenticationMode::SameUser, None);
        hello_as(&app, 0, 0, &[]).expect_err("root is not the node's user");
    }
    let app = local_auth_test_app(LocalAuthenticationMode::SameUserOrGroupOrRoot, None);
    hello_as(&app, 0, 0, &[]).expect("root is admitted");
    let (uid, gid) = stranger();
    hello_as(&app, uid, gid, &[]).expect_err("other users still are not");

    let app = local_auth_test_app(
        LocalAuthenticationMode::SameUser,
        Some(crate::auth::LocalAccessAllowList::new().allow_uid(0)),
    );
    hello_as(&app, 0, 0, &[]).expect("root is admitted by the allow-list");
}

#[test]
fn allow_list_admits_listed_users_and_group_members() {
    let (uid, gid) = stranger();
    let operators = gid.wrapping_add(1);
    let app = local_auth_test_app(
        LocalAuthenticationMode::SameUser,
        Some(
            crate::auth::LocalAccessAllowList::new()
                .allow_uid(uid)
                .allow_gid(operators),
        ),
    );
    hello_as(&app, uid, gid, &[]).expect("listed uid");
    hello_as(&app, uid.wrapping_add(1), operators, &[]).expect("primary member of a listed gid");
    hello_as(&app, uid.wrapping_add(1), gid, &[7, operators])
        .expect("supplementary member of a listed gid");
    hello_as(&app, uid.wrapping_add(1), gid, &[7]).expect_err("not listed");
}

#[test]
fn disabled_mode_ignores_credentials() {
    let app = local_auth_test_app(LocalAuthenticationMode::Disabled, None);
    let (uid, gid) = stranger();
    hello_as(&app, uid, gid, &[]).expect("disabled admits every caller");
}

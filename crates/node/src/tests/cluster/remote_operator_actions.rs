//! Remote operators running actions: per-operator authorization, an out-of-process node action
//! handler (a local IPC client that claimed the action name), and forwarding to the node that
//! owns the target (`docs/remote-operator.md`, `docs/actions.md`).

use super::remote_operator::{
    OperatorNode, enrolled_operator, operator_node, operator_node_with, publish_facts,
};
use super::*;
use orion::control_plane::{
    ActionQuery, ActionReport, ActionRequest, ActionState, ActionTarget, OperatorPolicy,
    ProviderRecord, StatusEntry, StatusQuery, StatusSubject,
};
use orion_client::remote::{OperatorIdentity, RemoteError};
use orion_client::{LocalNodeRuntime, LocalProviderService, LocalServiceRetryPolicy};

/// A device-manager client that claimed node actions over real local IPC sockets.
struct DeviceManager {
    claims: orion_client::ActionRequestWatch,
    _servers: [crate::app::GracefulTaskHandle<orion::transport::ipc::IpcTransportError>; 2],
    sockets: [PathBuf; 2],
}

impl Drop for DeviceManager {
    fn drop(&mut self) {
        for socket in &self.sockets {
            let _ = std::fs::remove_file(socket);
        }
    }
}

/// Serves local IPC on `node` and claims `names` from a device-manager client (the claim is
/// released when the returned watch is dropped).
async fn claim_node_actions(node: &OperatorNode, names: &[&str]) -> DeviceManager {
    let socket = temp_socket_path("operator-claim");
    let stream_socket = temp_socket_path("operator-claim-stream");
    let (_, unary) = node
        .app
        .start_ipc_server_graceful(&socket)
        .await
        .expect("ipc server should start");
    let (_, stream) = node
        .app
        .start_ipc_stream_server_graceful(&stream_socket)
        .await
        .expect("ipc stream server should start");
    let runtime = LocalNodeRuntime::new(&socket, &stream_socket);
    let manager = LocalProviderService::new(
        runtime,
        "device-manager",
        ProviderRecord::builder(
            ProviderId::new("provider.device-manager"),
            node.app.config.node_id.clone(),
        )
        .build(),
    )
    .with_retry_policy(
        LocalServiceRetryPolicy::fixed_delay(Duration::from_millis(20)).with_max_attempts(10),
    );
    manager.register().await.expect("register");
    let claims = manager
        .claim_node_actions(names.iter().copied())
        .await
        .expect("claim");
    DeviceManager {
        claims,
        _servers: [unary, stream],
        sockets: [socket, stream_socket],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn operator_policies_gate_actions_and_claimed_handlers_run_them() {
    let node = operator_node_with("node-a", None, vec!["self-*".into()]).await;
    let mut manager = claim_node_actions(&node, &["locate", "reboot", "self-test"]).await;
    let claims = &mut manager.claims;
    let alice = enrolled_operator(
        &node,
        OperatorIdentity::generate("alice").expect("identity"),
        OperatorPolicy {
            read: false,
            actions: Some(vec!["locate".into()]),
        },
    )
    .await;
    let target = ActionTarget::Node(NodeId::new("node-a"));

    // Not in the policy: refused before anything is tracked.
    let err = alice
        .run_action(ActionRequest::new("reboot-1", target.clone(), "reboot"))
        .await
        .expect_err("reboot is not allowed");
    assert!(
        matches!(&err, RemoteError::Rejected(message) if message.contains("may not run action `reboot`")),
        "{err}"
    );
    assert!(
        node.app
            .query_actions(&ActionQuery::action("reboot-1"))
            .is_empty()
    );

    // Allowed: delivered to the client that claimed the name, which reports the outcome.
    let accepted = alice
        .run_action(
            ActionRequest::new("locate-1", target.clone(), "locate")
                .with_arg("duration_ms", TypedConfigValue::UInt(500)),
        )
        .await
        .expect("locate is allowed");
    assert_eq!(accepted.state, ActionState::Accepted);
    assert_eq!(accepted.requested_by, "operator:alice");
    let request = tokio::time::timeout(Duration::from_secs(5), claims.next())
        .await
        .expect("request should arrive")
        .expect("request");
    assert_eq!(request.action_id, "locate-1");
    assert_eq!(request.requested_by, "operator:alice");
    claims
        .report(
            ActionReport::new("locate-1", ActionState::Succeeded)
                .with_output("blinks", TypedConfigValue::UInt(4)),
        )
        .await
        .expect("report");
    let done = alice
        .wait_for_action("locate-1", Duration::from_secs(5))
        .await
        .expect("the action finishes");
    assert_eq!(done.state, ActionState::Succeeded);
    assert_eq!(done.output["blinks"], TypedConfigValue::UInt(4));
    let mut watch = alice.watch_actions(ActionQuery::all());
    let seen = watch.next().await.expect("watch");
    assert!(seen.iter().any(|result| result.action_id == "locate-1"));

    // Without read access an operator only sees its own actions; the node default patterns
    // apply to operators without their own.
    node.app
        .run_action(
            ActionRequest::new("local-1", target.clone(), "locate"),
            "console",
        )
        .expect("local action");
    let own = alice
        .query_actions(ActionQuery::all())
        .await
        .expect("query");
    assert!(
        own.iter()
            .all(|result| result.requested_by == "operator:alice")
    );
    let bob = enrolled_operator(
        &node,
        OperatorIdentity::generate("bob").expect("identity"),
        OperatorPolicy::default(),
    )
    .await;
    assert_eq!(bob.welcome().allowed_actions, vec!["self-*".to_owned()]);
    assert!(
        bob.run_action(ActionRequest::new("locate-2", target.clone(), "locate"))
            .await
            .is_err()
    );
    bob.run_action(ActionRequest::new("self-test-1", target, "self-test"))
        .await
        .expect("the node default allows self-*");
    let everything = bob.query_actions(ActionQuery::all()).await.expect("query");
    assert!(
        everything
            .iter()
            .any(|result| result.action_id == "local-1")
    );

    drop(manager);
    node.server.shutdown().await.expect("listener stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn operators_reach_other_nodes_through_the_connected_node() {
    use crate::tests::ipc::actions::{hello, polled_requests, publish_provider, watch_requests};

    let (a, b) = (
        operator_node("node-a", None).await,
        operator_node("node-b", None).await,
    );
    for (node, peer) in [(&a, &b), (&b, &a)] {
        node.app
            .register_peer(
                PeerConfig::new(
                    peer.app.config.node_id.clone(),
                    orion_core::PeerBaseUrl::new(peer.url()),
                )
                .with_trusted_public_key_hex(peer.app.security.public_key_hex()),
            )
            .expect("peer registration");
    }
    publish_facts(&a.app);
    publish_facts(&b.app);
    let provider = hello(&b.app, "b-provider", ClientRole::Provider);
    publish_provider(&b.app, &provider, "provider.b");
    watch_requests(&b.app, &provider, "provider.b");
    a.app
        .sync_peer(&NodeId::new("node-b"))
        .await
        .expect("sync a <-> b");
    b.app
        .sync_peer(&NodeId::new("node-a"))
        .await
        .expect("sync b <-> a");

    // Enrolled on node-a only.
    let operator = enrolled_operator(
        &a,
        OperatorIdentity::generate("ops").expect("identity"),
        OperatorPolicy {
            read: true,
            actions: Some(vec!["*".into()]),
        },
    )
    .await;
    let nodes = operator.nodes().await.expect("cluster-wide node records");
    let ids: Vec<_> = nodes.iter().map(|node| node.node_id.as_str()).collect();
    assert!(
        ids.contains(&"node-a") && ids.contains(&"node-b"),
        "{ids:?}"
    );
    assert!(
        nodes.iter().all(|node| node.host.is_some()),
        "host facts of every node replicate to node-a"
    );
    assert!(!ids.iter().any(|id| id.starts_with("operator:")));

    let accepted = operator
        .run_action(ActionRequest::new(
            "fwd-1",
            ActionTarget::Provider(ProviderId::new("provider.b")),
            "locate",
        ))
        .await
        .expect("node-a forwards the action");
    assert_eq!(accepted.handled_by, NodeId::new("node-b"));
    let mut delivered = Vec::new();
    for _ in 0..500 {
        delivered = polled_requests(&b.app, &provider);
        if !delivered.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(delivered.len(), 1, "node-b delivers the forwarded request");
    assert_eq!(delivered[0].requested_by, "peer:node-a/operator:ops");
    b.app
        .apply_local_control_message(
            &provider,
            ControlMessage::ReportActionResult(Box::new(ActionReport::new(
                "fwd-1",
                ActionState::Succeeded,
            ))),
        )
        .expect("report");
    let done = operator
        .wait_for_action("fwd-1", Duration::from_secs(10))
        .await
        .expect("node-a mirrors the owner's result");
    assert_eq!(done.state, ActionState::Succeeded);
    assert_eq!(done.handled_by, NodeId::new("node-b"));

    // The status lane is node-local: queries for subjects owned by node-b are forwarded once.
    b.app
        .apply_local_control_message(
            &provider,
            ControlMessage::PublishStatus(vec![StatusEntry::new(
                StatusSubject::Provider(ProviderId::new("provider.b")),
                "update.phase",
                TypedConfigValue::String("download".into()),
            )]),
        )
        .expect("publish status");
    let host = operator
        .status(StatusQuery::subject(StatusSubject::Node(NodeId::new(
            "node-b",
        ))))
        .await
        .expect("node-b's host metrics through node-a");
    assert!(
        host.iter().any(|entry| entry.key == "host.uptime_seconds"
            && entry.subject == StatusSubject::Node(NodeId::new("node-b"))),
        "{host:?}"
    );
    let update = operator
        .status(
            StatusQuery::subject(StatusSubject::Provider(ProviderId::new("provider.b")))
                .with_key_prefix("update."),
        )
        .await
        .expect("node-b's provider status through node-a");
    assert_eq!(update.len(), 1, "{update:?}");
    assert_eq!(update[0].value, TypedConfigValue::String("download".into()));
    // Unknown owners are answered locally (nothing), and node-a's own lane stays node-a's.
    assert!(
        operator
            .status(StatusQuery::subject(StatusSubject::Provider(
                ProviderId::new("provider.nowhere")
            )))
            .await
            .expect("query")
            .is_empty()
    );
    // Local control-plane clients get the same routing.
    let console = crate::tests::ipc::actions::hello(&a.app, "console", ClientRole::ControlPlane);
    match a
        .app
        .apply_local_control_message(
            &console,
            ControlMessage::QueryStatus(
                StatusQuery::subject(StatusSubject::Provider(ProviderId::new("provider.b")))
                    .with_key_prefix("update."),
            ),
        )
        .expect("local query")
    {
        ControlMessage::Status(entries) => assert_eq!(entries.len(), 1),
        other => panic!("unexpected response {other:?}"),
    }
    assert_eq!(
        operator
            .query_action("fwd-1")
            .await
            .expect("query")
            .map(|result| result.state),
        Some(ActionState::Succeeded)
    );

    // The operator is no peer of node-b, which refuses it directly.
    let direct = orion_client::remote::RemoteOperator::connect(
        &b.url(),
        operator.identity().clone(),
        orion_client::remote::NodeTrust::FirstUse,
    )
    .await
    .expect("hello");
    assert!(!direct.is_enrolled());
    assert!(direct.nodes().await.is_err());
    a.server.shutdown().await.expect("listener stops");
    b.server.shutdown().await.expect("listener stops");
}

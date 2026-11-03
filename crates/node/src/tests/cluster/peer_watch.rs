//! Peer watches (`docs/actions.md`, "Peer watches"): a configured peer's push session carries
//! only the results of the actions that peer forwarded, pushed as they change.

use super::*;
use crate::actions::ActionOutcome;
use crate::app::ActionOrigin;
use orion::control_plane::{
    ActionQuery, ActionRequest, ActionResult, ActionState, ActionTarget, ClientEventKind,
    ControlMessage, SessionOpen,
};
use orion::transport::http::HttpResponsePayload;
use orion::transport::ipc::{ControlEnvelope, LocalAddress};
use orion_core::PeerBaseUrl;
use std::net::SocketAddr;
use std::time::Duration;

/// How long the owner's `slow` action runs: longer than a forwarder's first `RunAction` wait, so
/// the forwarder must complete it by push (or by resends, when push is off).
const SLOW_ACTION: Duration = Duration::from_millis(1500);

/// An owner with a `slow` action, served on a real `orion+tcp` listener.
async fn owner_node(
    node_id: &'static str,
) -> (
    NodeApp,
    SocketAddr,
    crate::app::GracefulTaskHandle<NodeError>,
) {
    let app = NodeApp::builder()
        .config(test_node_config_with_auth(
            node_id,
            "node-push",
            crate::PeerAuthenticationMode::Required,
        ))
        .with_action_handler("slow", |_request, _context| async {
            tokio::time::sleep(SLOW_ACTION).await;
            ActionOutcome::succeeded()
        })
        .try_build()
        .expect("node app should build");
    let (addr, server) = app
        .start_peer_tcp_server("127.0.0.1:0".parse().expect("address should parse"))
        .await
        .expect("peer TCP listener should start");
    (app, addr, server)
}

/// A forwarder that knows `owner` at `addr` (with its pinned key), and `owner` that knows it back.
fn forwarder_node(node_id: &'static str, owner: &NodeApp, addr: SocketAddr) -> NodeApp {
    let app = NodeApp::builder()
        .config(test_node_config_with_auth(
            node_id,
            "node-push",
            crate::PeerAuthenticationMode::Required,
        ))
        .try_build()
        .expect("node app should build");
    app.register_peer(
        PeerConfig::new(
            owner.config.node_id.clone(),
            PeerBaseUrl::new(format!("orion+tcp://{addr}")),
        )
        .with_trusted_public_key_hex(owner.security.public_key_hex()),
    )
    .expect("peer registration should succeed");
    owner
        .register_peer(
            PeerConfig::new(
                app.config.node_id.clone(),
                PeerBaseUrl::new("orion+tcp://127.0.0.1:9"),
            )
            .with_trusted_public_key_hex(app.security.public_key_hex()),
        )
        .expect("peer registration should succeed");
    app
}

/// Forwards a `slow` action from `forwarder` to `owner` and returns its final result.
async fn forward_slow(forwarder: &NodeApp, action_id: &str) -> ActionResult {
    forwarder
        .submit_action(
            ActionRequest::new(
                action_id,
                ActionTarget::Node(NodeId::new("node-owner")),
                "slow",
            ),
            ActionOrigin::Local("test".to_owned()),
        )
        .expect("the forwarded action is accepted");
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(result) = forwarder
                .query_actions(&ActionQuery::action(action_id))
                .into_iter()
                .find(|result| result.state.is_terminal())
            {
                return result;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the forwarded action finishes in time")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_forwarded_action_completes_by_push_without_resends() {
    let (owner, addr, _server) = owner_node("node-owner").await;
    let forwarder = forwarder_node("node-forwarder", &owner, addr);

    let result = forward_slow(&forwarder, "slow-push").await;
    assert_eq!(result.state, ActionState::Succeeded);
    assert_eq!(
        forwarder.action_forward_resends_for_test(),
        0,
        "the owner's push session delivered the result: no RunAction was resent"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_a_push_session_the_forwarder_resends_and_still_completes() {
    let (owner, addr, _server) = owner_node("node-owner").await;
    let forwarder = forwarder_node("node-forwarder", &owner, addr);
    forwarder.disable_push_for_test();

    let result = forward_slow(&forwarder, "slow-resend").await;
    assert_eq!(result.state, ActionState::Succeeded);
    assert!(
        forwarder.action_forward_resends_for_test() > 0,
        "without push, the forwarder resends RunAction until the action is final"
    );
}

/// The action ids in a pushed `ClientEvents` envelope.
fn pushed_action_ids(envelope: ControlEnvelope) -> Vec<String> {
    let ControlMessage::ClientEvents(events) = envelope.message else {
        panic!("a session pushes client events");
    };
    events
        .into_iter()
        .flat_map(|event| match event.event {
            ClientEventKind::ActionResults(results) => results
                .into_iter()
                .map(|result| result.action_id)
                .collect::<Vec<_>>(),
            _ => Vec::new(),
        })
        .collect()
}

#[tokio::test]
async fn a_peer_session_watches_only_the_actions_that_peer_forwarded() {
    let owner = cluster_node("node-b", |config| config);
    let forwarder = NodeId::new("node-a");
    let other_peer = NodeId::new("node-c");
    let target = ActionTarget::Node(NodeId::new("node-b"));

    owner
        .submit_action(
            ActionRequest::new("fwd-1", target.clone(), "locate"),
            ActionOrigin::Peer(forwarder.clone()),
        )
        .expect("a forwarded action is accepted");
    owner
        .submit_action(
            ActionRequest::new("local-1", target.clone(), "locate"),
            ActionOrigin::Local("cli".to_owned()),
        )
        .expect("a local action is accepted");

    let HttpResponsePayload::Session(welcome) = owner
        .open_peer_session(
            &forwarder,
            SessionOpen {
                client_name: String::new(),
            },
        )
        .expect("a configured peer may open a session")
    else {
        panic!("OpenSession answers with the session");
    };
    let session = LocalAddress::new(welcome.session_id);
    let (sender, mut pushed) = tokio::sync::mpsc::channel(16);
    owner
        .attach_remote_session(&session, sender)
        .expect("the session attaches");

    // A peer may not watch another peer's forwarded actions, use another peer's session, or send
    // anything but a watch or a ping on its session.
    assert!(
        owner
            .apply_peer_session_message(
                &forwarder,
                Some(&session),
                ControlMessage::WatchActions(ActionQuery::all().forwarded_by(other_peer.clone())),
            )
            .is_err()
    );
    assert!(
        owner
            .apply_peer_session_message(
                &other_peer,
                Some(&session),
                ControlMessage::WatchActions(ActionQuery::all()),
            )
            .is_err()
    );
    assert!(
        owner
            .apply_peer_session_message(&forwarder, Some(&session), ControlMessage::QueryPeerTrust,)
            .is_err()
    );

    // The watch is narrowed to the forwarder even though its query asked for everything. Its
    // bootstrap carries the forwarded action only.
    owner
        .apply_peer_session_message(
            &forwarder,
            Some(&session),
            ControlMessage::WatchActions(ActionQuery::all()),
        )
        .expect("the peer's watch is accepted");
    let bootstrap = tokio::time::timeout(Duration::from_secs(5), pushed.recv())
        .await
        .expect("the bootstrap is pushed in time")
        .expect("the session stays open");
    assert_eq!(pushed_action_ids(bootstrap), vec!["fwd-1".to_owned()]);

    // Later changes follow the same rule: a local action is not pushed, a forwarded one is.
    owner
        .submit_action(
            ActionRequest::new("local-2", target.clone(), "locate"),
            ActionOrigin::Local("cli".to_owned()),
        )
        .expect("a local action is accepted");
    owner
        .submit_action(
            ActionRequest::new("fwd-2", target, "locate"),
            ActionOrigin::Peer(forwarder),
        )
        .expect("a forwarded action is accepted");
    let update = tokio::time::timeout(Duration::from_secs(5), pushed.recv())
        .await
        .expect("the forwarded update is pushed in time")
        .expect("the session stays open");
    assert_eq!(pushed_action_ids(update), vec!["fwd-2".to_owned()]);
}

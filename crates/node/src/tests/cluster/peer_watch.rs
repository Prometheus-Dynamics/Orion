//! Peer watches (`docs/actions.md`, "Peer watches"): a configured peer's push session carries
//! only the results of the actions that peer forwarded, pushed as they change.

use super::*;
use crate::app::ActionOrigin;
use orion::control_plane::{
    ActionQuery, ActionRequest, ActionTarget, ClientEventKind, ControlMessage, SessionOpen,
};
use orion::transport::http::HttpResponsePayload;
use orion::transport::ipc::{ControlEnvelope, LocalAddress};
use std::time::Duration;

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

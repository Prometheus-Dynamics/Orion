//! Remote push sessions (`docs/remote-operator.md`, "Push sessions") against an in-process node
//! over a real `orion+tcp` listener: watches answered on the session's connection, their events
//! pushed (no polling), signed and numbered.

use super::remote_operator::{enrolled_operator, operator_node, publish_facts, serve, signed};
use super::*;
use orion::control_plane::{ClientEventKind, OperatorPolicy, StatusQuery, StatusSubject};
use orion_client::remote::{OperatorIdentity, RemoteError};
use std::time::Duration;

/// The next status change pushed on `session`, within `within`.
async fn next_status(
    session: &mut orion_client::remote::RemoteSession,
    within: Duration,
) -> orion::control_plane::StatusChange {
    tokio::time::timeout(within, async {
        loop {
            let event = session.next_event().await.expect("the session stays open");
            if let ClientEventKind::Status(change) = event.event {
                return change;
            }
        }
    })
    .await
    .expect("a status change is pushed in time")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_pushes_status_changes_as_they_happen() {
    let node = operator_node("node-a", None).await;
    let identity = OperatorIdentity::generate("alice").expect("identity");
    let operator = enrolled_operator(&node, identity, OperatorPolicy::default()).await;

    let mut session = operator
        .open_session("operator-console")
        .await
        .expect("session opens");
    assert!(
        session
            .session_id()
            .starts_with("remote-session/operator:alice/")
    );
    assert_eq!(session.node_id(), &NodeId::new("node-a"));
    session
        .watch_status(StatusQuery::subject(StatusSubject::Node(NodeId::new(
            "node-a",
        ))))
        .await
        .expect("an operator with read access may watch the status lane");
    let bootstrap = next_status(&mut session, Duration::from_secs(5)).await;
    assert!(bootstrap.bootstrap);

    // Something publishes on the node: the change arrives on the open connection.
    publish_facts(&node.app);
    let change = next_status(&mut session, Duration::from_secs(5)).await;
    assert!(!change.bootstrap);
    assert!(
        change
            .updated
            .iter()
            .any(|entry| entry.key == "host.uptime_seconds"),
        "host metrics are pushed: {:?}",
        change
            .updated
            .iter()
            .map(|e| e.key.as_str())
            .collect::<Vec<_>>()
    );

    // The node keeps the session's client while it is open, and forgets it once it closes.
    let open = |app: &NodeApp| {
        let clients = app.observability_snapshot().client_sessions;
        clients.registered_clients > 0 && clients.live_stream_clients > 0
    };
    assert!(
        open(&node.app),
        "the session is a registered client with a stream"
    );
    drop(session);
    tokio::time::timeout(Duration::from_secs(5), async {
        while open(&node.app) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("a closed session is forgotten");
    node.server.shutdown().await.expect("listener stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watches_need_a_session_and_read_access() {
    let node = operator_node("node-a", None).await;
    let alice = OperatorIdentity::generate("alice").expect("identity");
    let reader = enrolled_operator(&node, alice.clone(), OperatorPolicy::default()).await;
    // Outside a session a watch is refused: it would have nowhere to push to.
    let watch = || ControlMessage::WatchStatus(StatusQuery::all());
    serve(&node.app, signed(&alice, 1, watch())).expect_err("a watch outside a session is refused");

    // Another operator can't use alice's session, and nothing local can speak as one.
    let session = reader
        .open_session("operator-console")
        .await
        .expect("session opens");
    let address = LocalAddress::new(session.session_id());
    let mallory = OperatorIdentity::generate("mallory").expect("identity");
    enrolled_operator(&node, mallory.clone(), OperatorPolicy::default()).await;
    let hijack = node
        .app
        .serve_control_request(ControlRequest::from_session_payload(
            signed(&mallory, 2, watch()),
            address.clone(),
        ));
    assert!(hijack.is_err(), "another operator's session is refused");
    let spoofed = node.app.serve_local_control_message(
        ControlSurface::LocalIpc,
        address.clone(),
        LocalAddress::new("orion-node"),
        watch(),
    );
    assert!(
        matches!(&spoofed, Ok(ControlMessage::Rejected(_)) | Err(_)),
        "a local client using a session's address is refused: {spoofed:?}"
    );
    drop(session);

    let blind = enrolled_operator(
        &node,
        OperatorIdentity::generate("bob").expect("identity"),
        OperatorPolicy {
            read: false,
            actions: Some(vec![]),
        },
    )
    .await;
    let refused = blind
        .open_session("operator-console")
        .await
        .expect_err("no read access and no actions: no session");
    assert!(matches!(refused, RemoteError::Rejected(_)), "{refused}");
    node.server.shutdown().await.expect("listener stops");
}

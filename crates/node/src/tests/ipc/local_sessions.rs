use super::*;

#[tokio::test]
async fn node_local_client_reconnect_before_ttl_resumes_queued_events() {
    let app = build_local_session_app("resume-before-ttl", Duration::from_secs(1));

    let source = LocalAddress::new("cli-resume");
    let welcome = app
        .apply_local_control_message(
            &source,
            ControlMessage::ClientHello(ClientHello {
                client_name: "cli-resume".into(),
                role: ClientRole::ControlPlane,
            }),
        )
        .expect("client hello should succeed");
    let ControlMessage::ClientWelcome(first_session) = welcome else {
        panic!("expected client welcome");
    };

    app.apply_local_control_message(
        &source,
        ControlMessage::WatchState(orion::control_plane::StateWatch {
            desired_revision: Revision::ZERO,
        }),
    )
    .expect("watch registration should succeed");

    let mut desired = app.state_snapshot().state.desired;
    desired.put_node(orion::control_plane::NodeRecord::builder("node-reconnect").build());
    app.replace_desired(desired);

    let welcome = app
        .apply_local_control_message(
            &source,
            ControlMessage::ClientHello(ClientHello {
                client_name: "cli-resume".into(),
                role: ClientRole::ControlPlane,
            }),
        )
        .expect("reconnect hello should succeed");
    let ControlMessage::ClientWelcome(resumed_session) = welcome else {
        panic!("expected resumed client welcome");
    };
    assert_eq!(resumed_session.session_id, first_session.session_id);

    let response = app
        .apply_local_control_message(
            &source,
            ControlMessage::PollClientEvents(ClientEventPoll {
                after_sequence: 0,
                max_events: 8,
            }),
        )
        .expect("event poll should succeed");
    let ControlMessage::ClientEvents(events) = response else {
        panic!("expected client events");
    };
    assert_eq!(events.len(), 1);
}

#[tokio::test]
async fn node_local_client_reconnect_after_ttl_drops_resumable_state() {
    let app = build_local_session_app("resume-after-ttl", Duration::from_millis(25));

    let source = LocalAddress::new("cli-expire");
    app.apply_local_control_message(
        &source,
        ControlMessage::ClientHello(ClientHello {
            client_name: "cli-expire".into(),
            role: ClientRole::ControlPlane,
        }),
    )
    .expect("client hello should succeed");
    app.apply_local_control_message(
        &source,
        ControlMessage::WatchState(orion::control_plane::StateWatch {
            desired_revision: Revision::ZERO,
        }),
    )
    .expect("watch registration should succeed");

    let mut desired = app.state_snapshot().state.desired;
    desired.put_node(orion::control_plane::NodeRecord::builder("node-expire").build());
    app.replace_desired(desired);

    tokio::time::sleep(Duration::from_millis(40)).await;

    let welcome = app
        .apply_local_control_message(
            &source,
            ControlMessage::ClientHello(ClientHello {
                client_name: "cli-expire".into(),
                role: ClientRole::ControlPlane,
            }),
        )
        .expect("post-expiry hello should succeed");
    let ControlMessage::ClientWelcome(_) = welcome else {
        panic!("expected client welcome");
    };

    let response = app
        .apply_local_control_message(
            &source,
            ControlMessage::PollClientEvents(ClientEventPoll {
                after_sequence: 0,
                max_events: 8,
            }),
        )
        .expect("event poll should succeed");
    assert_eq!(response, ControlMessage::ClientEvents(Vec::new()));
}

#[tokio::test]
async fn node_local_client_reconnect_coalesces_superseded_state_snapshots() {
    let app = build_local_session_app("resume-coalesced-state", Duration::from_secs(1));

    let source = LocalAddress::new("cli-coalesced");
    app.apply_local_control_message(
        &source,
        ControlMessage::ClientHello(ClientHello {
            client_name: "cli-coalesced".into(),
            role: ClientRole::ControlPlane,
        }),
    )
    .expect("client hello should succeed");
    app.apply_local_control_message(
        &source,
        ControlMessage::WatchState(orion::control_plane::StateWatch {
            desired_revision: Revision::ZERO,
        }),
    )
    .expect("watch registration should succeed");

    let mut desired = app.state_snapshot().state.desired;
    desired.put_node(orion::control_plane::NodeRecord::builder("node-coalesced-1").build());
    app.replace_desired(desired);

    let mut desired = app.state_snapshot().state.desired;
    desired.put_node(orion::control_plane::NodeRecord::builder("node-coalesced-2").build());
    app.replace_desired(desired);

    let response = app
        .apply_local_control_message(
            &source,
            ControlMessage::PollClientEvents(ClientEventPoll {
                after_sequence: 0,
                max_events: 8,
            }),
        )
        .expect("event poll should succeed");
    let ControlMessage::ClientEvents(events) = response else {
        panic!("expected client events");
    };
    assert_eq!(events.len(), 1);
    match &events[0].event {
        ClientEventKind::StateSnapshot(snapshot) => {
            assert_eq!(snapshot.state.desired.revision, Revision::new(2));
            assert!(
                snapshot
                    .state
                    .desired
                    .nodes
                    .contains_key(&NodeId::new("node-coalesced-2"))
            );
        }
        other => panic!("expected coalesced state snapshot, got {other:?}"),
    }
}

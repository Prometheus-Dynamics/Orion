//! End to end: two nodes with `orion+tcp` listeners discover each other over the in-memory bus
//! and sync only once they are enrolled.

use super::*;
use orion::control_plane::DiscoveredPeerEnrollment;

fn enroll(app: &NodeApp, peer: &NodeId, fingerprint: Option<String>) -> ControlMessage {
    local_control(
        app,
        ControlMessage::EnrollDiscoveredPeer(DiscoveredPeerEnrollment {
            node_id: peer.clone(),
            expected_key_fingerprint: fingerprint,
        }),
    )
}

/// Syncs `from` -> `to` until `artifact` arrived (a failed round leaves the peer in backoff, during
/// which `sync_peer` skips the round).
async fn sync_until_present(from: &TcpNode, to: &TcpNode, artifact: &str) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !has_artifact(&to.app, artifact) {
        assert!(
            Instant::now() < deadline,
            "{artifact} never reached {}",
            to.id()
        );
        let _ = from.app.sync_peer(&to.id()).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn fingerprint_of(app: &NodeApp, peer: &NodeId) -> String {
    query_discovery(app)
        .peers
        .into_iter()
        .find(|record| &record.node_id == peer)
        .expect("peer should be discovered")
        .key_fingerprint
}

#[tokio::test]
async fn shared_enrollment_key_enrolls_both_nodes_and_they_sync() {
    let bus = MemoryDiscoveryBus::new();
    let a = TcpNode::start("node-a", None).await;
    let b = TcpNode::start("node-b", None).await;
    put_artifact(&a.app, "artifact.from-a");
    let a_discovery = a.discover(&bus, Some(KEY));
    let b_discovery = b.discover(&bus, Some(KEY));

    wait_until("both nodes enrolled each other", || {
        peer_state(&a.app, &b.id()) == Some(DiscoveredPeerState::Enrolled)
            && peer_state(&b.app, &a.id()) == Some(DiscoveredPeerState::Enrolled)
    })
    .await;
    assert_eq!(
        a.app
            .registered_peer_base_url(&b.id())
            .map(|url| url.to_string()),
        Some(format!("orion+tcp://{}", b.addr))
    );
    // The responder learned the initiator's URL from the handshake.
    assert!(b.app.is_registered_peer(&a.id()));

    a.app
        .sync_peer(&b.id())
        .await
        .expect("enrolled peers sync with required peer auth");
    assert!(has_artifact(&b.app, "artifact.from-a"));
    put_artifact(&b.app, "artifact.from-b");
    b.app
        .sync_peer(&a.id())
        .await
        .expect("sync works in the other direction too");
    assert!(has_artifact(&a.app, "artifact.from-b"));

    let metrics = a.app.observability_snapshot().discovery;
    assert_eq!(metrics.enrolled_peers, 1);
    assert!(metrics.enrollment_successes >= 1);
    a_discovery.shutdown().await;
    b_discovery.shutdown().await;
    a.stop().await;
    b.stop().await;
}

#[tokio::test]
async fn different_enrollment_keys_never_enroll() {
    let bus = MemoryDiscoveryBus::new();
    let a = TcpNode::start("node-a", None).await;
    let b = TcpNode::start("node-b", None).await;
    let a_discovery = a.discover(&bus, Some(KEY));
    let b_discovery = b.discover(&bus, Some(OTHER_KEY));
    wait_until("enrollment attempts failed on both sides", || {
        let a_metrics = a.app.query_discovery().metrics;
        let b_metrics = b.app.query_discovery().metrics;
        a_metrics.enrollment_failures >= 2 && b_metrics.enrollment_failures >= 2
    })
    .await;
    assert!(!a.app.is_registered_peer(&b.id()));
    assert!(!b.app.is_registered_peer(&a.id()));
    assert_eq!(
        peer_state(&a.app, &b.id()),
        Some(DiscoveredPeerState::Discovered)
    );
    let record = query_discovery(&a.app)
        .peers
        .into_iter()
        .find(|record| record.node_id == b.id())
        .expect("b is discovered");
    assert!(record.last_enrollment_error.is_some());
    a_discovery.shutdown().await;
    b_discovery.shutdown().await;
    a.stop().await;
    b.stop().await;
}

#[tokio::test]
async fn operator_enrollment_gates_sync_and_removal_stops_it() {
    let bus = MemoryDiscoveryBus::new();
    let a = TcpNode::start("node-a", None).await;
    let b = TcpNode::start("node-b", None).await;
    put_artifact(&a.app, "artifact.from-a");
    let a_discovery = a.discover(&bus, None);
    let b_discovery = b.discover(&bus, None);
    wait_until("both nodes discovered each other", || {
        peer_state(&a.app, &b.id()).is_some() && peer_state(&b.app, &a.id()).is_some()
    })
    .await;
    assert!(
        a.app.sync_all_peers().await.is_empty(),
        "nothing to sync with yet"
    );

    // A stale fingerprint is refused; the one shown by `get discovered-peers` is accepted.
    assert!(matches!(
        enroll(&a.app, &b.id(), Some("sha256:00".into())),
        ControlMessage::Rejected(_)
    ));
    let shown = fingerprint_of(&a.app, &b.id());
    assert_eq!(
        shown,
        query_discovery(&b.app).local_key_fingerprint,
        "operators compare this with the fingerprint b reports for itself"
    );
    assert_eq!(
        enroll(&a.app, &b.id(), Some(shown)),
        ControlMessage::Accepted
    );
    assert_eq!(
        peer_state(&a.app, &b.id()),
        Some(DiscoveredPeerState::Enrolled)
    );

    // Only a trusts b so far: b refuses requests from the peer it has not enrolled.
    let err = a
        .app
        .sync_peer(&b.id())
        .await
        .expect_err("b must not accept an unenrolled peer");
    assert!(err.to_string().contains("unknown peer"), "{err}");
    assert!(!has_artifact(&b.app, "artifact.from-a"));

    let shown = fingerprint_of(&b.app, &a.id());
    assert_eq!(
        enroll(&b.app, &a.id(), Some(shown)),
        ControlMessage::Accepted
    );
    // Mutually enrolled peers sync (once a's backoff from the refused round has passed).
    sync_until_present(&a, &b, "artifact.from-a").await;

    // Removal on b: b stops syncing with a and rejects a's requests.
    assert_eq!(
        local_control(&b.app, ControlMessage::RemovePeer(a.id())),
        ControlMessage::Accepted
    );
    assert!(!b.app.is_registered_peer(&a.id()));
    assert_eq!(
        peer_state(&b.app, &a.id()),
        Some(DiscoveredPeerState::Revoked)
    );
    put_artifact(&a.app, "artifact.after-removal");
    assert!(a.app.sync_peer(&b.id()).await.is_err());
    assert!(!has_artifact(&b.app, "artifact.after-removal"));

    // Enrolling again is an explicit operator decision that lifts the revocation.
    let shown = fingerprint_of(&b.app, &a.id());
    assert_eq!(
        enroll(&b.app, &a.id(), Some(shown)),
        ControlMessage::Accepted
    );
    sync_until_present(&a, &b, "artifact.after-removal").await;

    let metrics = query_discovery(&a.app).metrics;
    assert_eq!(metrics.enrollment_attempts, 2);
    assert_eq!(metrics.enrollment_successes, 1);
    assert_eq!(metrics.enrollment_failures, 1);
    a_discovery.shutdown().await;
    b_discovery.shutdown().await;
    a.stop().await;
    b.stop().await;
}

#[tokio::test]
async fn removed_peers_are_not_re_enrolled_by_the_shared_key() {
    let bus = MemoryDiscoveryBus::new();
    let a = TcpNode::start("node-a", None).await;
    let b = TcpNode::start("node-b", None).await;
    let a_discovery = a.discover(&bus, Some(KEY));
    let b_discovery = b.discover(&bus, Some(KEY));
    wait_until("both nodes enrolled each other", || {
        a.app.is_registered_peer(&b.id()) && b.app.is_registered_peer(&a.id())
    })
    .await;
    a.app.remove_peer(&b.id()).expect("remove should succeed");
    let attempts = a.app.query_discovery().metrics.enrollment_attempts;
    // Several ticks and retry periods: a does not start an enrollment with a removed peer.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        a.app.query_discovery().metrics.enrollment_attempts,
        attempts
    );
    assert!(!a.app.is_registered_peer(&b.id()));
    assert_eq!(
        peer_state(&a.app, &b.id()),
        Some(DiscoveredPeerState::Revoked)
    );
    // And refuses a handshake that b starts.
    let state = b.app.discovery_state().expect("b runs discovery");
    let peer_a = state
        .registry()
        .get(&a.id())
        .cloned()
        .expect("b discovered a");
    let err = b
        .app
        .enroll_with_shared_key(&state, &peer_a)
        .await
        .expect_err("a must refuse a removed peer");
    assert!(err.to_string().contains("removed"), "{err}");
    assert!(!a.app.is_registered_peer(&b.id()));
    a_discovery.shutdown().await;
    b_discovery.shutdown().await;
    a.stop().await;
    b.stop().await;
}

#[tokio::test]
async fn discovered_enrollments_survive_a_restart() {
    let bus = MemoryDiscoveryBus::new();
    let state_dir = temp_state_dir("restart");
    let a = TcpNode::start("node-a", Some(state_dir.clone())).await;
    let b = TcpNode::start("node-b", None).await;
    let a_discovery = a.discover(&bus, Some(KEY));
    let b_discovery = b.discover(&bus, Some(KEY));
    wait_until("a enrolled b", || a.app.is_registered_peer(&b.id())).await;
    a_discovery.shutdown().await;
    a.stop().await;

    // A new process on the same state directory trusts b again without rediscovering it.
    let restarted = build_app("node-a", Some(state_dir.clone()));
    assert!(!restarted.is_registered_peer(&b.id()));
    assert_eq!(
        restarted
            .restore_discovered_enrollments()
            .expect("restore should succeed"),
        1
    );
    assert!(restarted.is_registered_peer(&b.id()));
    put_artifact(&restarted, "artifact.after-restart");
    restarted
        .sync_peer(&b.id())
        .await
        .expect("restored peers sync");
    assert!(has_artifact(&b.app, "artifact.after-restart"));

    // Removal also forgets the enrollment, so it is not restored again.
    restarted
        .remove_peer(&b.id())
        .expect("remove should succeed");
    let again = build_app("node-a", Some(state_dir.clone()));
    assert_eq!(again.restore_discovered_enrollments().expect("restore"), 0);
    assert!(!again.is_registered_peer(&b.id()));

    b_discovery.shutdown().await;
    b.stop().await;
    let _ = std::fs::remove_dir_all(state_dir);
}

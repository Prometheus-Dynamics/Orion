//! The stale-peer guard (`docs/peer-sync.md`, "Stale peers"): a peer whose last completed sync
//! round is older than `ORION_NODE_TOMBSTONE_RETENTION_MS` is refused in both directions, and its
//! clock survives restarts and is reset only by removing and re-adding the peer.

use super::*;
use crate::peer::PeerState;
use orion::control_plane::{ControlMessage, MutationBatch};
use std::path::Path;

/// Base URL of the test peers. Their requests go through the in-memory transport, so only a
/// scheme this build supports is needed.
#[cfg(feature = "transport-http")]
const MEMORY_PEER_URL: &str = "http://memory.invalid:1";
#[cfg(not(feature = "transport-http"))]
const MEMORY_PEER_URL: &str = "orion+tcp://memory.invalid:1";

/// Retention used by the tests that let a tombstone expire. Sleeping past it is what expires it.
const SHORT_RETENTION: Duration = Duration::from_millis(50);
/// An age far beyond the default retention, so the clock is stale whatever the timing.
const LONG_AGO_MS: u64 = 3_600_000;

fn stale_guard_node(node_id: &'static str, state_dir: &Path, peers: Vec<PeerConfig>) -> NodeApp {
    let mut config = test_node_config_with_state_dir_and_auth(
        node_id,
        "node-stale-peer",
        state_dir.to_path_buf(),
        crate::PeerAuthenticationMode::Optional,
    );
    config.peers = peers;
    config =
        config.with_runtime_tuning_mut(|tuning| tuning.with_tombstone_retention(SHORT_RETENTION));
    NodeApp::builder()
        .config(config)
        .try_build()
        .expect("node app should build")
}

/// Rewrites the persisted sync clock of `node_id` in `state_dir`, as if the peer had been silent
/// for `age_ms` when the node next starts.
fn age_persisted_clock(state_dir: &Path, node_id: &str, age_ms: u64) {
    let at = NodeApp::wall_clock_ms().saturating_sub(age_ms);
    crate::peer_sync_times::update(state_dir, |records| {
        records.insert(node_id.to_owned(), at);
    })
    .expect("clock should be written");
}

fn peer_sync_status(node: &NodeApp, peer: &str) -> PeerState {
    node.peer_state_for_test(&NodeId::new(peer))
        .expect("peer should be registered")
}

/// Node `a` (which knows `b` from its state directory) whose clock for `b` is stale.
fn node_with_stale_peer(state_dir: &Path) -> NodeApp {
    let peers = vec![PeerConfig::new("node-b", MEMORY_PEER_URL)];
    let first = stale_guard_node("node-a", state_dir, peers.clone());
    drop(first);
    age_persisted_clock(state_dir, "node-b", LONG_AGO_MS);
    stale_guard_node("node-a", state_dir, peers)
}

#[tokio::test]
async fn stale_peer_is_refused_and_cannot_resurrect_a_collected_tombstone() {
    let state_dir = temp_state_dir("stale-peer-tombstone");
    let a = node_with_stale_peer(&state_dir);
    let b = cluster_node("node-b", |config| config);
    b.register_peer(PeerConfig::new("node-a", MEMORY_PEER_URL))
        .expect("peer registration should succeed");
    let key = DesiredObjectKey::Artifact(ArtifactId::new("artifact.gone"));

    // a deletes the artifact and its tombstone expires and is collected.
    write_local(
        &a,
        DesiredStateMutation::PutArtifact(artifact("artifact.gone", 1)),
    );
    write_local(&a, delete(&key)).expect("delete should apply");
    std::thread::sleep(SHORT_RETENTION * 2);
    assert_eq!(a.collect_expired_tombstones(), 1);
    assert!(a.state_snapshot().state.desired.tombstones.is_empty());

    // b still holds the artifact from before the delete and pushes it to a.
    write_local(
        &b,
        DesiredStateMutation::PutArtifact(artifact("artifact.gone", 1)),
    );
    let old_write = HlcTimestamp::new(NodeApp::wall_clock_ms() - 1_000, 0, 9);
    let push = ControlMessage::Mutations(MutationBatch::stamped(
        Revision::ZERO,
        vec![(
            DesiredStateMutation::PutArtifact(artifact("artifact.gone", 1)),
            old_write,
        )],
    ));
    let refused = a.apply_control_message(Some(NodeId::new("node-b")), push);
    assert!(refused.is_err(), "a stale peer's push must be refused");
    assert!(
        !a.state_snapshot()
            .state
            .desired
            .artifacts
            .contains_key(&ArtifactId::new("artifact.gone"))
    );

    // a neither pulls from nor pushes to b.
    a.sync_peer(&NodeId::new("node-b"))
        .await
        .expect("a stale peer is skipped, not failed");
    let peer = peer_sync_status(&a, "node-b");
    assert_eq!(peer.sync_status, PeerSyncStatus::Error);
    assert!(
        peer.last_error
            .as_deref()
            .is_some_and(|error| error.contains("ORION_NODE_TOMBSTONE_RETENTION_MS")),
        "the refusal should say why: {:?}",
        peer.last_error
    );
    let peer = peer_sync_status(&a, "node-b");
    let round = a
        .sync_peer_over(
            &NodeId::new("node-b"),
            &peer,
            &MemoryTransport { remote: b.clone() },
        )
        .await;
    assert!(round.is_err(), "a sync round with a stale peer must fail");
    assert!(
        !a.state_snapshot()
            .state
            .desired
            .artifacts
            .contains_key(&ArtifactId::new("artifact.gone"))
    );
    let _ = std::fs::remove_dir_all(&state_dir);
}

#[tokio::test]
async fn removing_and_re_adding_a_stale_peer_lets_sync_proceed() {
    let state_dir = temp_state_dir("stale-peer-readd");
    let a = node_with_stale_peer(&state_dir);
    let b = cluster_node("node-b", |config| config);
    b.register_peer(PeerConfig::new("node-a", MEMORY_PEER_URL))
        .expect("peer registration should succeed");
    a.sync_peer(&NodeId::new("node-b"))
        .await
        .expect("a stale peer is skipped, not failed");
    assert_eq!(
        peer_sync_status(&a, "node-b").sync_status,
        PeerSyncStatus::Error
    );

    // The operator resets the peer: removing it forgets its clock, and enrolling it again starts
    // a fresh one (and lifts the revocation that the removal recorded).
    assert!(
        a.remove_peer(&NodeId::new("node-b"))
            .expect("remove should succeed")
    );
    assert!(
        !crate::peer_sync_times::load(&state_dir)
            .expect("store should load")
            .contains_key("node-b"),
        "removal forgets the peer's clock"
    );
    a.register_peer(PeerConfig::new("node-b", MEMORY_PEER_URL))
        .expect("re-registration should succeed");
    a.security
        .replace_trusted_peer_key(&NodeId::new("node-b"), b.security.public_key_bytes())
        .expect("re-trusting the peer should succeed");

    memory_round(&a, &b).await;
    let peer = peer_sync_status(&a, "node-b");
    assert_ne!(peer.sync_status, PeerSyncStatus::Error);
    assert!(peer.last_error.is_none());
    assert!(peer.last_successful_sync_ms.is_some());
    let _ = std::fs::remove_dir_all(&state_dir);
}

#[tokio::test]
async fn peer_within_retention_syncs_normally() {
    let a = cluster_node("node-a", |config| config);
    let b = cluster_node("node-b", |config| config);
    a.register_peer(PeerConfig::new("node-b", MEMORY_PEER_URL))
        .expect("peer registration should succeed");
    b.register_peer(PeerConfig::new("node-a", MEMORY_PEER_URL))
        .expect("peer registration should succeed");
    write_local(
        &b,
        DesiredStateMutation::PutArtifact(artifact("artifact.fresh", 1)),
    );

    memory_round(&a, &b).await;

    assert!(
        a.state_snapshot()
            .state
            .desired
            .artifacts
            .contains_key(&ArtifactId::new("artifact.fresh"))
    );
    let peer = peer_sync_status(&a, "node-b");
    assert_ne!(peer.sync_status, PeerSyncStatus::Error);
    assert!(peer.last_successful_sync_ms.is_some());
}

#[tokio::test]
async fn restart_does_not_reset_a_stale_peer_clock() {
    let state_dir = temp_state_dir("stale-peer-restart");
    let a = node_with_stale_peer(&state_dir);
    let persisted = crate::peer_sync_times::load(&state_dir).expect("store should load");
    assert!(
        persisted
            .get("node-b")
            .is_some_and(|at| { NodeApp::wall_clock_ms().saturating_sub(*at) >= LONG_AGO_MS }),
        "the stale clock is what the restart loads"
    );
    assert_eq!(
        peer_sync_status(&a, "node-b").last_successful_sync_ms,
        persisted.get("node-b").copied()
    );
    drop(a);

    // Restarting again keeps the old clock: the window is not renewed by restarts.
    let restarted = stale_guard_node(
        "node-a",
        &state_dir,
        vec![PeerConfig::new("node-b", MEMORY_PEER_URL)],
    );
    restarted
        .sync_peer(&NodeId::new("node-b"))
        .await
        .expect("a stale peer is skipped, not failed");
    assert_eq!(
        peer_sync_status(&restarted, "node-b").sync_status,
        PeerSyncStatus::Error
    );
    let _ = std::fs::remove_dir_all(&state_dir);
}

#[tokio::test]
async fn upgrade_starts_configured_peers_without_a_clock_record_at_load_time() {
    // A state directory from before the sync clock existed: no `peer-sync-times.json`.
    let state_dir = temp_state_dir("stale-peer-upgrade");
    std::fs::create_dir_all(&state_dir).expect("state dir should exist");
    assert!(!state_dir.join("peer-sync-times.json").exists());

    let before = NodeApp::wall_clock_ms();
    let app = stale_guard_node(
        "node-a",
        &state_dir,
        vec![PeerConfig::new("node-b", MEMORY_PEER_URL)],
    );
    let after = NodeApp::wall_clock_ms();

    let clock = peer_sync_status(&app, "node-b")
        .last_successful_sync_ms
        .expect("the peer should start its clock at load");
    assert!((before..=after).contains(&clock));
    // The fill-in is persisted, so restarts cannot keep pushing the window forward.
    let persisted = crate::peer_sync_times::load(&state_dir).expect("store should load");
    assert_eq!(persisted.get("node-b"), Some(&clock));
    let _ = std::fs::remove_dir_all(&state_dir);
}

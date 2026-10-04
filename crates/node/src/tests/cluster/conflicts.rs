use super::*;
use orion::control_plane::{DesiredClusterState, MutationBatch, NodeRecord};

const KEYS: &[&str] = &["artifact.a", "artifact.b", "artifact.c", "artifact.d"];
const NODE_KEYS: &[&str] = &["node.x", "node.y"];

fn random_mutation(rng: &mut SplitMix64) -> DesiredStateMutation {
    match rng.below(5) {
        0 => delete(&DesiredObjectKey::Artifact(ArtifactId::new(
            KEYS[rng.below(KEYS.len())],
        ))),
        1 => DesiredStateMutation::PutNode(
            NodeRecord::builder(NODE_KEYS[rng.below(NODE_KEYS.len())])
                .label(format!("rev-{}", rng.below(1000)))
                .build(),
        ),
        2 => delete(&DesiredObjectKey::Node(NodeId::new(
            NODE_KEYS[rng.below(NODE_KEYS.len())],
        ))),
        _ => DesiredStateMutation::PutArtifact(artifact(
            KEYS[rng.below(KEYS.len())],
            rng.below(1000) as u64,
        )),
    }
}

/// The merge result every node must reach: all versions ever written, folded with the merge
/// rule (in any order, since the rule is commutative).
fn oracle(versions: &[(DesiredStateMutation, HlcTimestamp)]) -> DesiredClusterState {
    let mut state = DesiredClusterState::default();
    for (mutation, stamp) in versions {
        state.apply_stamped(mutation.clone(), *stamp);
    }
    desired_content(&state)
}

/// Random local writes on three nodes interleaved with random sync rounds between random pairs;
/// afterwards every node holds exactly the per-object last writer of all writes.
#[tokio::test]
async fn concurrent_writes_converge_to_the_last_writer_for_every_interleaving() {
    for seed in 0..12_u64 {
        let nodes = memory_cluster(&["node-a", "node-b", "node-c"]);
        let mut rng = SplitMix64(seed);
        let mut versions = Vec::new();
        for _ in 0..40 {
            if rng.below(3) == 0 {
                let local = rng.below(nodes.len());
                let remote = (local + 1 + rng.below(nodes.len() - 1)) % nodes.len();
                memory_round(&nodes[local], &nodes[remote]).await;
            } else {
                let node = &nodes[rng.below(nodes.len())];
                versions.extend(write_local(node, random_mutation(&mut rng)));
            }
        }
        converge_memory(&nodes).await;
        let converged = assert_all_equal(&nodes);
        let mut shuffled = versions.clone();
        rng.shuffle(&mut shuffled);
        assert_eq!(converged, oracle(&shuffled), "seed {seed}");
    }
}

/// The same set of stamped versions delivered to fresh nodes in different orders and batch
/// splits (and repeated) always produces the same state.
#[tokio::test]
async fn delivery_order_and_duplication_do_not_change_the_merge_result() {
    let now = NodeApp::wall_clock_ms();
    let mut rng = SplitMix64(42);
    let mut versions = Vec::new();
    for index in 0..60_u64 {
        // Few distinct timestamps and node tags, so equal stamps (ties) happen often.
        let stamp = HlcTimestamp::new(
            now - 10 + rng.below(4) as u64,
            rng.below(2) as u32,
            1 + index % 2,
        );
        versions.push((random_mutation(&mut rng), stamp));
    }
    let expected = oracle(&versions);
    for trial in 0..8 {
        let node = cluster_node("node-r", |c| c);
        let mut order = versions.clone();
        rng.shuffle(&mut order);
        if trial % 2 == 1 {
            order.extend(order.clone());
        }
        for chunk in order.chunks(1 + rng.below(9)) {
            node.apply_control_message(
                None,
                ControlMessage::Mutations(MutationBatch::stamped(Revision::ZERO, chunk.to_vec())),
            )
            .expect("stamped batch should merge");
        }
        assert_eq!(
            desired_content(&node.state_snapshot().state.desired),
            expected,
            "trial {trial}"
        );
    }
}

#[tokio::test]
async fn a_later_update_beats_a_concurrent_delete_and_a_later_delete_beats_an_update() {
    let nodes = memory_cluster(&["node-a", "node-b"]);
    let key = DesiredObjectKey::Artifact(ArtifactId::new("artifact.shared"));
    write_local(
        &nodes[0],
        DesiredStateMutation::PutArtifact(artifact("artifact.shared", 1)),
    );
    converge_memory(&nodes).await;

    // node-a deletes, then node-b updates later: the update wins everywhere.
    write_local(&nodes[0], delete(&key)).expect("delete should apply");
    std::thread::sleep(Duration::from_millis(3));
    write_local(
        &nodes[1],
        DesiredStateMutation::PutArtifact(artifact("artifact.shared", 2)),
    )
    .expect("update should apply");
    converge_memory(&nodes).await;
    let state = assert_all_equal(&nodes);
    assert_eq!(
        state.artifacts[&ArtifactId::new("artifact.shared")].size_bytes,
        Some(2)
    );
    assert!(state.tombstones.get(&key).is_none());

    // node-b updates, then node-a deletes later: the delete wins and leaves a tombstone.
    write_local(
        &nodes[1],
        DesiredStateMutation::PutArtifact(artifact("artifact.shared", 3)),
    )
    .expect("update should apply");
    std::thread::sleep(Duration::from_millis(3));
    write_local(&nodes[0], delete(&key)).expect("delete should apply");
    converge_memory(&nodes).await;
    let state = assert_all_equal(&nodes);
    assert!(state.artifacts.is_empty());
    assert!(state.tombstones.get(&key).is_some());
}

#[tokio::test]
async fn versions_beyond_the_maximum_drift_are_rejected_and_counted() {
    let strict = cluster_node("node-a", |config| {
        config.with_runtime_tuning_mut(|tuning| tuning.with_hlc_max_drift(Duration::from_secs(1)))
    });
    let now = NodeApp::wall_clock_ms();
    let far = HlcTimestamp::new(now + 60_000, 0, 7);
    let near = HlcTimestamp::new(now + 500, 0, 7);
    strict
        .apply_control_message(
            None,
            ControlMessage::Mutations(MutationBatch::stamped(
                Revision::ZERO,
                vec![
                    (
                        DesiredStateMutation::PutArtifact(artifact("artifact.future", 1)),
                        far,
                    ),
                    (
                        DesiredStateMutation::PutArtifact(artifact("artifact.near", 1)),
                        near,
                    ),
                ],
            )),
        )
        .expect("the batch is accepted; only the skewed version is dropped");
    let state = strict.state_snapshot().state.desired;
    assert!(
        !state
            .artifacts
            .contains_key(&ArtifactId::new("artifact.future"))
    );
    assert!(
        state
            .artifacts
            .contains_key(&ArtifactId::new("artifact.near"))
    );
    let merge = strict.observability_snapshot().desired_merge;
    assert_eq!(merge.clock_skew_rejections, 1);
    assert_eq!(merge.max_drift_ms, 1_000);
    assert!(merge.last_clock_skew.is_some());
    // The rejected timestamp did not move the local clock; the accepted one did.
    assert!(strict.hlc_now() < far);
    assert!(strict.hlc_now() >= near);

    // A peer holding a far-future version: the round completes but does not converge.
    let lenient = cluster_node("node-b", |config| {
        config
            .with_runtime_tuning_mut(|tuning| tuning.with_hlc_max_drift(Duration::from_secs(3_600)))
    });
    lenient
        .apply_control_message(
            None,
            ControlMessage::Mutations(MutationBatch::stamped(
                Revision::ZERO,
                vec![(
                    DesiredStateMutation::PutArtifact(artifact("artifact.future", 2)),
                    far,
                )],
            )),
        )
        .expect("within the lenient node's drift");
    for (node, peer) in [(&strict, &lenient), (&lenient, &strict)] {
        node.register_peer(PeerConfig::new(
            peer.config.node_id.clone(),
            "http://memory.invalid:1",
        ))
        .expect("peer registration should succeed");
    }
    let peer = strict
        .peer_state_for_test(&NodeId::new("node-b"))
        .expect("peer should exist");
    let round = strict
        .sync_peer_over(
            &NodeId::new("node-b"),
            &peer,
            &MemoryTransport {
                remote: lenient.clone(),
            },
        )
        .await
        .expect("sync round should complete");
    assert!(!round.converged);
    assert!(
        !strict
            .state_snapshot()
            .state
            .desired
            .artifacts
            .contains_key(&ArtifactId::new("artifact.future"))
    );
    assert_eq!(
        strict
            .observability_snapshot()
            .desired_merge
            .clock_skew_rejections,
        2
    );
}

#[tokio::test]
async fn tombstones_are_collected_after_retention_and_expired_deletes_are_not_reimported() {
    let node = cluster_node("node-a", |config| {
        config.with_runtime_tuning_mut(|tuning| {
            tuning.with_tombstone_retention(Duration::from_millis(20))
        })
    });
    let key = DesiredObjectKey::Artifact(ArtifactId::new("artifact.gone"));
    write_local(
        &node,
        DesiredStateMutation::PutArtifact(artifact("artifact.gone", 1)),
    );
    write_local(&node, delete(&key)).expect("delete should apply");
    let revision = node.current_desired_revision();
    assert_eq!(node.state_snapshot().state.desired.tombstones.len(), 1);
    assert_eq!(
        node.collect_expired_tombstones(),
        0,
        "still within retention"
    );

    std::thread::sleep(Duration::from_millis(40));
    assert_eq!(node.collect_expired_tombstones(), 1);
    let state = node.state_snapshot().state.desired;
    assert!(state.tombstones.is_empty());
    assert_eq!(state.revision, revision, "collection is not a write");

    // A peer still sending the old delete does not bring the tombstone back...
    let old_stamp = HlcTimestamp::new(NodeApp::wall_clock_ms() - 1_000, 0, 9);
    node.apply_control_message(
        None,
        ControlMessage::Mutations(MutationBatch::stamped(
            Revision::ZERO,
            vec![(delete(&key), old_stamp)],
        )),
    )
    .expect("batch should merge");
    assert!(node.state_snapshot().state.desired.tombstones.is_empty());
    // ...but an expired delete still removes an older live copy.
    node.apply_control_message(
        None,
        ControlMessage::Mutations(MutationBatch::stamped(
            Revision::ZERO,
            vec![
                (
                    DesiredStateMutation::PutArtifact(artifact("artifact.old", 1)),
                    HlcTimestamp::new(old_stamp.physical_ms - 10, 0, 9),
                ),
                (
                    delete(&DesiredObjectKey::Artifact(ArtifactId::new("artifact.old"))),
                    old_stamp,
                ),
            ],
        )),
    )
    .expect("batch should merge");
    assert!(
        !node
            .state_snapshot()
            .state
            .desired
            .artifacts
            .contains_key(&ArtifactId::new("artifact.old"))
    );
    // That delete's tombstone is already expired, so the next collection (run by the reconcile
    // pass after the merge) drops it as well.
    node.collect_expired_tombstones();
    assert!(node.state_snapshot().state.desired.tombstones.is_empty());
    let merge = node.observability_snapshot().desired_merge;
    assert_eq!(merge.tombstones_collected, 2);
    assert_eq!(merge.expired_tombstones_ignored, 1);
}

#[tokio::test]
async fn restart_keeps_versions_and_orders_new_writes_after_old_ones() {
    let state_dir = temp_state_dir("hlc-restart");
    let config = test_node_config_with_state_dir_and_auth(
        "node-a",
        "node-hlc-restart",
        state_dir.clone(),
        crate::PeerAuthenticationMode::Optional,
    );
    let node = NodeApp::builder()
        .config(config.clone())
        .try_build()
        .expect("node app should build");
    // A remote version slightly in the future (within drift) moves the clock ahead of the wall.
    let ahead = HlcTimestamp::new(NodeApp::wall_clock_ms() + 2_000, 5, 9);
    node.apply_control_message(
        None,
        ControlMessage::Mutations(MutationBatch::stamped(
            Revision::ZERO,
            vec![(
                DesiredStateMutation::PutArtifact(artifact("artifact.ahead", 1)),
                ahead,
            )],
        )),
    )
    .expect("batch should merge");
    let before = node.state_snapshot().state.desired;
    drop(node);

    let restarted = NodeApp::builder()
        .config(config)
        .try_build()
        .expect("node app should rebuild");
    assert_eq!(restarted.state_snapshot().state.desired, before);
    assert!(
        restarted.hlc_now() >= ahead,
        "the clock is seeded from stored stamps"
    );
    let (_, stamp) = write_local(
        &restarted,
        DesiredStateMutation::PutArtifact(artifact("artifact.ahead", 2)),
    )
    .expect("local write should apply");
    assert!(
        stamp > ahead,
        "a local write after restart wins over stored versions"
    );
    let _ = fs::remove_dir_all(state_dir);
}

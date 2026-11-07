//! Update intents (`docs/update-recovery.md`, "Data model" and "Requester authorization"): which
//! remote operators may write them, what the audit log records, and the statuses a node reports
//! for its own intents.

use super::remote_operator::{OperatorNode, enrolled_operator, publish_facts};
use super::*;
use orion::control_plane::{
    DesiredStateMutation, MutationBatch, OperatorId, OperatorPolicy, UpdateIntentRecord,
    UpdatePhase,
};
use orion_client::remote::OperatorIdentity;
use orion_core::UpdateIntentId;

async fn node_with_requesters(
    requesters: Vec<OperatorId>,
    audit_log: Option<PathBuf>,
) -> OperatorNode {
    let mut config = test_node_config_with(
        "node-a",
        "node-a-update-intents",
        None,
        Vec::new(),
        crate::PeerAuthenticationMode::Required,
    );
    config.runtime_tuning.actions = config
        .runtime_tuning
        .actions
        .clone()
        .with_update_requesters(requesters);
    let mut builder = NodeApp::builder().config(config);
    if let Some(path) = audit_log {
        builder = builder.with_audit_log_path(path);
    }
    let app = builder.try_build().expect("node app should build");
    let (addr, server) = app
        .start_peer_tcp_server("127.0.0.1:0".parse().expect("address should parse"))
        .await
        .expect("peer TCP listener should start");
    OperatorNode { app, addr, server }
}

fn revision(node: &OperatorNode) -> orion::Revision {
    node.app.current_desired_revision()
}

fn intent_id(node: &str, component: &str) -> UpdateIntentId {
    UpdateIntentId::new(format!("{node}/{component}"))
}

/// Reads the audit log until it holds `needle` (the worker thread appends asynchronously).
async fn audit_log_contains(path: &std::path::Path, needle: &str) -> bool {
    for _ in 0..100 {
        if std::fs::read_to_string(path).is_ok_and(|text| text.contains(needle)) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_listed_operators_write_update_intents_and_nothing_else() {
    let alice = OperatorIdentity::generate("alice").expect("identity");
    let bob = OperatorIdentity::generate("bob").expect("identity");
    let node = node_with_requesters(vec![alice.operator_id().clone()], None).await;
    let alice_op = enrolled_operator(&node, alice.clone(), OperatorPolicy::default()).await;
    let bob_op = enrolled_operator(&node, bob.clone(), OperatorPolicy::default()).await;

    // A listed requester writes an intent for the node.
    let os = UpdateIntentRecord::new("node-a", "os", "2.0");
    alice_op
        .apply_update_intents(MutationBatch::new(
            revision(&node),
            vec![DesiredStateMutation::PutUpdateIntent(os.clone())],
        ))
        .await
        .expect("a listed requester may write intents");
    let desired = node.app.current_desired_state();
    assert_eq!(desired.update_intents.get(&os.intent_id), Some(&os));

    // An enrolled operator that is not listed may not, and nothing changes.
    let mcu = UpdateIntentRecord::new("node-a", "mcu/motor-a", "1.4");
    let refused = bob_op
        .apply_update_intents(MutationBatch::new(
            revision(&node),
            vec![DesiredStateMutation::PutUpdateIntent(mcu.clone())],
        ))
        .await;
    assert!(refused.is_err(), "an unlisted operator must be refused");
    assert!(
        !node
            .app
            .current_desired_state()
            .update_intents
            .contains_key(&mcu.intent_id)
    );

    // Even a listed requester may only write update intents: a batch with anything else is refused
    // as a whole.
    let mixed = alice_op
        .apply_update_intents(MutationBatch::new(
            revision(&node),
            vec![
                DesiredStateMutation::PutUpdateIntent(mcu.clone()),
                DesiredStateMutation::PutArtifact(
                    orion::control_plane::ArtifactRecord::builder("artifact.evil").build(),
                ),
            ],
        ))
        .await;
    assert!(mixed.is_err(), "a mixed batch must be refused");
    let after = node.app.current_desired_state();
    assert!(after.artifacts.is_empty());
    assert!(!after.update_intents.contains_key(&mcu.intent_id));

    // A listed requester may remove its intent (no opinion; the node keeps what it runs).
    alice_op
        .apply_update_intents(MutationBatch::new(
            revision(&node),
            vec![DesiredStateMutation::RemoveUpdateIntent(
                os.intent_id.clone(),
            )],
        ))
        .await
        .expect("a listed requester may remove intents");
    let after = node.app.current_desired_state();
    assert!(!after.update_intents.contains_key(&os.intent_id));
    assert!(after.tombstones.update_intents.contains_key(&os.intent_id));

    // A malformed intent is refused even from a listed requester (the id must be node/component).
    let mut malformed = UpdateIntentRecord::new("node-a", "os", "2.0");
    malformed.intent_id = intent_id("node-b", "os");
    assert!(
        alice_op
            .apply_update_intents(MutationBatch::new(
                revision(&node),
                vec![DesiredStateMutation::PutUpdateIntent(malformed)],
            ))
            .await
            .is_err()
    );
    node.server.shutdown().await.expect("listener stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_intent_writes_are_audited() {
    let dir = crate::test_temp::dir().join(format!(
        "orion-update-audit-{}-{}",
        std::process::id(),
        NodeApp::wall_clock_ms()
    ));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let audit = dir.join("events.jsonl");
    let alice = OperatorIdentity::generate("alice").expect("identity");
    let node = node_with_requesters(vec![alice.operator_id().clone()], Some(audit.clone())).await;
    let alice_op = enrolled_operator(&node, alice.clone(), OperatorPolicy::default()).await;

    alice_op
        .apply_update_intents(MutationBatch::new(
            revision(&node),
            vec![DesiredStateMutation::PutUpdateIntent(
                UpdateIntentRecord::new("node-a", "os", "2.0"),
            )],
        ))
        .await
        .expect("write");
    assert!(
        audit_log_contains(&audit, "\"kind\":\"update_intent_written\"").await,
        "the write must be audited"
    );
    assert!(audit_log_contains(&audit, "\"subject\":\"node-a/os\"").await);
    assert!(audit_log_contains(&audit, "operator `operator:alice`").await);

    alice_op
        .apply_update_intents(MutationBatch::new(
            revision(&node),
            vec![DesiredStateMutation::RemoveUpdateIntent(intent_id(
                "node-a", "os",
            ))],
        ))
        .await
        .expect("remove");
    assert!(audit_log_contains(&audit, "\"kind\":\"update_intent_removed\"").await);
    node.server.shutdown().await.expect("listener stops");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn requesters_are_optional_and_none_by_default() {
    // Nothing listed: no operator writes intents, even enrolled ones.
    let node = node_with_requesters(Vec::new(), None).await;
    let carol = OperatorIdentity::generate("carol").expect("identity");
    let carol_op = enrolled_operator(&node, carol.clone(), OperatorPolicy::default()).await;
    assert!(
        carol_op
            .apply_update_intents(MutationBatch::new(
                revision(&node),
                vec![DesiredStateMutation::PutUpdateIntent(
                    UpdateIntentRecord::new("node-a", "os", "2.0",)
                )],
            ))
            .await
            .is_err()
    );
    assert!(node.app.current_desired_state().update_intents.is_empty());
    node.server.shutdown().await.expect("listener stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_node_reports_idle_or_pending_for_its_own_intents() {
    let node = node_with_requesters(Vec::new(), None).await;
    // Host facts publish `image_version` 2026.10.1 for the node's own record.
    publish_facts(&node.app);

    let mut desired = orion::control_plane::DesiredClusterState::default();
    desired.put_update_intent(UpdateIntentRecord::new("node-a", "os", "2026.10.1"));
    desired.put_update_intent(UpdateIntentRecord::new("node-a", "mcu/motor-a", "1.4"));
    desired.put_update_intent(UpdateIntentRecord::new("node-b", "os", "2026.11"));
    node.app.replace_desired(desired);

    assert!(node.app.publish_local_update_statuses());
    let observed = node.app.state_snapshot().state.observed;
    let status = |id: &str| observed.update_statuses.get(&UpdateIntentId::new(id));
    let os = status("node-a/os").expect("status of the local os intent");
    assert_eq!(
        os.phase,
        UpdatePhase::Idle,
        "the booted image already has the target"
    );
    assert_eq!(os.installed_version.as_deref(), Some("2026.10.1"));
    let mcu = status("node-a/mcu/motor-a").expect("status of the local mcu intent");
    assert_eq!(mcu.phase, UpdatePhase::Pending);
    assert_eq!(
        mcu.installed_version, None,
        "no installed-version source for mcu components"
    );
    assert!(
        status("node-b/os").is_none(),
        "a node reports only the intents it acts on"
    );
    // Nothing changed: no new write.
    assert!(!node.app.publish_local_update_statuses());

    // A newer target makes the os intent pending; a removed intent loses its status.
    let mut desired = node.app.current_desired_state();
    desired.put_update_intent(UpdateIntentRecord::new("node-a", "os", "2026.11"));
    desired.remove_update_intent(&intent_id("node-a", "mcu/motor-a"));
    node.app.replace_desired(desired);
    assert!(node.app.publish_local_update_statuses());
    let observed = node.app.state_snapshot().state.observed;
    let os = observed
        .update_statuses
        .get(&intent_id("node-a", "os"))
        .expect("status");
    assert_eq!(os.phase, UpdatePhase::Pending);
    assert_eq!(os.installed_version.as_deref(), Some("2026.10.1"));
    assert!(
        !observed
            .update_statuses
            .contains_key(&intent_id("node-a", "mcu/motor-a"))
    );
    node.server.shutdown().await.expect("listener stops");
}

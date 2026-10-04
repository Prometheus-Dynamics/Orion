//! Coalesced observed/applied persistence (`ORION_NODE_OBSERVED_PERSIST_INTERVAL_MS`).

use super::*;
use crate::storage::NodeStorage;
use orion::control_plane::{ClientHello, ClientRole, NodeRecord, ProviderStateUpdate};
use std::time::Instant;

const PROVIDER: &str = "provider.coalesce";

fn coalescing_app(node_id: &str, state_dir: PathBuf, interval: Duration) -> NodeApp {
    let config = test_node_config_with_state_dir_and_auth(
        NodeId::new(node_id),
        node_id,
        state_dir,
        crate::PeerAuthenticationMode::Disabled,
    )
    .with_runtime_tuning_mut(|tuning| {
        tuning
            .with_observed_persist_interval(interval)
            .with_reconcile_backstop_interval(Duration::from_secs(60))
    });
    NodeApp::builder()
        .config(config)
        .try_build()
        .expect("node app should build")
}

fn provider_update(app: &NodeApp, generation: u64) -> ControlMessage {
    let health = if generation.is_multiple_of(2) {
        HealthState::Healthy
    } else {
        HealthState::Degraded
    };
    ControlMessage::ProviderState(ProviderStateUpdate {
        provider: ProviderRecord::builder(PROVIDER, app.config.node_id.clone())
            .resource_type(ResourceType::new("camera.frame"))
            .build(),
        resources: vec![
            ResourceRecord::builder("resource.coalesce.camera", "camera.frame", PROVIDER)
                .health(health)
                .availability(AvailabilityState::Available)
                .label(format!("generation={generation}"))
                .build(),
        ],
    })
}

fn register_provider_client(app: &NodeApp, source: &LocalAddress) {
    app.apply_local_control_message(
        source,
        ControlMessage::ClientHello(ClientHello {
            client_name: "coalesce-provider".into(),
            role: ClientRole::Provider,
        }),
    )
    .expect("hello should be accepted");
}

fn state_writes(app: &NodeApp) -> u64 {
    app.observability_snapshot()
        .persistence
        .state_persist
        .success_count
}

fn observed_label(app: &NodeApp) -> Option<String> {
    app.state_snapshot()
        .state
        .observed
        .resources
        .get(&ResourceId::new("resource.coalesce.camera"))
        .and_then(|resource| resource.labels.first().cloned())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rapid_observed_updates_are_coalesced_and_flushed_on_shutdown() {
    let state_dir = temp_state_dir("observed-coalesce");
    let interval = Duration::from_millis(100);
    let app = coalescing_app("node.coalesce", state_dir.clone(), interval);
    let source = LocalAddress::new("coalesce-provider");
    register_provider_client(&app, &source);
    let handle = app.spawn_reconcile_loop(Duration::from_millis(5));
    app.apply_local_control_message(&source, provider_update(&app, 0))
        .expect("first provider state should apply");
    tokio::time::sleep(interval * 2).await;

    let writes_before = state_writes(&app);
    let started = Instant::now();
    let updates = 60;
    for generation in 1..=updates {
        app.apply_local_control_message(&source, provider_update(&app, generation))
            .expect("provider state should apply");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let elapsed = started.elapsed();
    let usage = app
        .observability_snapshot()
        .resource_usage
        .observed_persistence;
    assert!(usage.coalescing, "the reconcile loop runs the coalescer");
    assert!(
        usage.coalesced_changes_total >= updates,
        "every observed change should be deferred, got {usage:?}"
    );

    // Let the last deferred change flush, then count writes.
    tokio::time::sleep(interval * 2).await;
    let writes = state_writes(&app) - writes_before;
    let bound = elapsed.as_millis().div_ceil(interval.as_millis()) as u64 + 2;
    assert!(
        writes <= bound,
        "{updates} observed updates over {elapsed:?} caused {writes} writes, expected at most \
         {bound} with a {interval:?} interval"
    );
    assert!(writes >= 1, "coalesced changes must still be written");

    // The first change after an idle interval is written at once; one right behind it waits for
    // the interval and is flushed by the shutdown instead of being lost.
    app.apply_local_control_message(&source, provider_update(&app, updates + 1))
        .expect("provider state should apply");
    tokio::time::sleep(Duration::from_millis(20)).await;
    app.apply_local_control_message(&source, provider_update(&app, updates + 2))
        .expect("provider state should apply");
    assert!(
        app.observability_snapshot()
            .resource_usage
            .observed_persistence
            .pending
    );
    handle.shutdown().await;
    let usage = app
        .observability_snapshot()
        .resource_usage
        .observed_persistence;
    assert!(!usage.pending, "shutdown flushes pending observed state");
    assert!(!usage.coalescing);
    let expected = observed_label(&app);
    drop(app);

    let replayed = coalescing_app("node.coalesce", state_dir.clone(), interval);
    assert!(replayed.replay_state().expect("replay should succeed"));
    assert_eq!(observed_label(&replayed), expected);
    assert_eq!(
        expected.as_deref(),
        Some(format!("generation={}", updates + 2).as_str())
    );
    let _ = std::fs::remove_dir_all(state_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zero_interval_keeps_immediate_observed_writes() {
    let state_dir = temp_state_dir("observed-immediate");
    let app = coalescing_app("node.immediate", state_dir.clone(), Duration::ZERO);
    let source = LocalAddress::new("coalesce-provider");
    register_provider_client(&app, &source);
    let handle = app.spawn_reconcile_loop(Duration::from_millis(5));
    app.apply_local_control_message(&source, provider_update(&app, 0))
        .expect("provider state should apply");
    tokio::time::sleep(Duration::from_millis(50)).await;
    let before = state_writes(&app);
    for generation in 1..=5 {
        app.apply_local_control_message(&source, provider_update(&app, generation))
            .expect("provider state should apply");
    }
    assert!(
        state_writes(&app) - before >= 5,
        "each observed change is written immediately"
    );
    let usage = app
        .observability_snapshot()
        .resource_usage
        .observed_persistence;
    assert_eq!(usage.interval_ms, 0);
    assert!(!usage.coalescing);
    assert_eq!(usage.coalesced_changes_total, 0);
    handle.shutdown().await;
    let _ = std::fs::remove_dir_all(state_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn desired_commits_stay_durable_and_carry_pending_observed_state() {
    let state_dir = temp_state_dir("observed-desired");
    let app = coalescing_app("node.desired", state_dir.clone(), Duration::from_secs(60));
    let source = LocalAddress::new("coalesce-provider");
    register_provider_client(&app, &source);
    let handle = app.spawn_reconcile_loop(Duration::from_millis(5));
    tokio::time::sleep(Duration::from_millis(20)).await;
    app.apply_local_control_message(&source, provider_update(&app, 0))
        .expect("provider state should apply");
    // The first deferred change after an idle period is flushed at once; the next one waits for
    // the (60 s) interval.
    tokio::time::sleep(Duration::from_millis(50)).await;
    app.apply_local_control_message(&source, provider_update(&app, 1))
        .expect("provider state should apply");
    tokio::time::sleep(Duration::from_millis(50)).await;
    let usage = app
        .observability_snapshot()
        .resource_usage
        .observed_persistence;
    assert!(usage.pending, "the second change waits for the interval");
    let storage = NodeStorage::new(&state_dir);
    let persisted_label = |storage: &NodeStorage| {
        storage
            .load_snapshot_sections()
            .expect("snapshot should load")
            .and_then(|sections| {
                sections
                    .observed
                    .resources
                    .get(&ResourceId::new("resource.coalesce.camera"))
                    .and_then(|resource| resource.labels.first().cloned())
            })
    };
    assert_eq!(persisted_label(&storage).as_deref(), Some("generation=0"));

    let before = state_writes(&app);
    let mut desired = app.state_snapshot().state.desired;
    desired.put_node(NodeRecord::builder(NodeId::new("node.desired.extra")).build());
    app.replace_desired(desired);
    assert!(
        state_writes(&app) > before,
        "desired-state commits are written immediately"
    );
    assert!(
        app.observability_snapshot()
            .resource_usage
            .observed_persistence
            .absorbed_flushes_total
            >= 1
    );
    // The durable desired write carried the pending observed change with it.
    assert_eq!(persisted_label(&storage).as_deref(), Some("generation=1"));

    handle.shutdown().await;
    let _ = std::fs::remove_dir_all(state_dir);
}

use super::*;
use orion::control_plane::{NodeRecord, ObservabilityEventKind, ObservedClusterState};
use std::time::Instant;

fn loop_test_app(node_id: &str, backstop: Duration) -> NodeApp {
    NodeApp::builder()
        .config(
            NodeConfig::for_local_node(NodeId::new(node_id)).with_runtime_tuning_mut(|tuning| {
                tuning.with_reconcile_backstop_interval(backstop)
            }),
        )
        .try_build()
        .expect("node app should build")
}

fn reconcile_count(app: &NodeApp) -> u64 {
    app.observability_snapshot().reconcile.success_count
}

async fn wait_for_reconciles(app: &NodeApp, at_least: u64, timeout: Duration) -> Duration {
    let started = Instant::now();
    while reconcile_count(app) < at_least {
        assert!(
            started.elapsed() < timeout,
            "expected at least {at_least} reconciles within {timeout:?}, observed {}",
            reconcile_count(app)
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    started.elapsed()
}

fn add_node(app: &NodeApp, index: u32) {
    let mut desired = app.state_snapshot().state.desired;
    desired.put_node(NodeRecord::builder(NodeId::new(format!("node.extra.{index}"))).build());
    app.replace_desired(desired);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_reconcile_loop_only_runs_backstop_passes() {
    let app = loop_test_app("node.loop.idle", Duration::from_millis(200));
    let handle = app.spawn_reconcile_loop(Duration::from_millis(10));

    tokio::time::sleep(Duration::from_millis(500)).await;
    let reconciles = reconcile_count(&app);
    handle.shutdown().await;

    // One startup pass plus two backstop passes; fixed 10ms polling would have run ~50.
    assert!(
        (1..=4).contains(&reconciles),
        "idle loop should only run startup and backstop passes, observed {reconciles}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn desired_state_mutation_wakes_reconcile_loop_promptly() {
    let app = loop_test_app("node.loop.mutation", Duration::from_secs(60));
    let handle = app.spawn_reconcile_loop(Duration::from_millis(10));
    wait_for_reconciles(&app, 1, Duration::from_secs(2)).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(reconcile_count(&app), 1, "idle loop should not poll");

    add_node(&app, 0);
    let elapsed = wait_for_reconciles(&app, 2, Duration::from_secs(2)).await;
    let snapshot = app.state_snapshot();
    handle.shutdown().await;

    assert!(
        elapsed < Duration::from_millis(500),
        "mutation should wake the loop well before the backstop, took {elapsed:?}"
    );
    assert_eq!(
        snapshot.state.applied.revision, snapshot.state.desired.revision,
        "woken pass should apply the new desired revision"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observed_update_wakes_reconcile_loop() {
    let app = loop_test_app("node.loop.observed", Duration::from_secs(60));
    let handle = app.spawn_reconcile_loop(Duration::from_millis(10));
    wait_for_reconciles(&app, 1, Duration::from_secs(2)).await;

    let mut observed = ObservedClusterState::default();
    observed.set_revision(Revision::new(1));
    app.apply_observed_update(orion::control_plane::ObservedStateUpdate {
        observed,
        applied: Default::default(),
    })
    .expect("observed update should apply");
    let elapsed = wait_for_reconciles(&app, 2, Duration::from_secs(2)).await;
    handle.shutdown().await;

    assert!(
        elapsed < Duration::from_millis(500),
        "observed update should wake the loop well before the backstop, took {elapsed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn burst_of_mutations_coalesces_into_few_reconciles() {
    let app = loop_test_app("node.loop.burst", Duration::from_secs(60));
    let interval = Duration::from_millis(50);
    let handle = app.spawn_reconcile_loop(interval);
    wait_for_reconciles(&app, 1, Duration::from_secs(2)).await;
    let before = reconcile_count(&app);

    let started = std::time::Instant::now();
    for index in 0..25 {
        add_node(&app, index);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let elapsed = started.elapsed();
    let reconciles = reconcile_count(&app);
    let snapshot = app.state_snapshot();
    handle.shutdown().await;

    // Passes are spaced at least `interval` apart, so the burst can trigger at most one pass per
    // interval of wall time (plus the one already pending). Under a loaded test runner the burst
    // itself can take longer, so bound by elapsed time rather than a fixed count.
    let burst_passes = reconciles - before;
    let max_passes = (elapsed.as_millis() / interval.as_millis()) as u64 + 1;
    assert!(
        (1..=max_passes).contains(&burst_passes),
        "25 back-to-back mutations should coalesce to at most one pass per {interval:?} \
         ({max_passes} over {elapsed:?}), observed {burst_passes}"
    );
    assert!(
        burst_passes < 25,
        "mutations must not each trigger their own pass, observed {burst_passes}"
    );
    assert_eq!(
        snapshot.state.applied.revision, snapshot.state.desired.revision,
        "coalesced passes should still converge on the final desired revision"
    );
}

#[test]
fn unchanged_reconcile_updates_metrics_without_recording_an_event() {
    let app = loop_test_app("node.loop.events", Duration::from_secs(60));
    add_node(&app, 0);
    app.tick()
        .expect("first tick should apply the desired revision");
    let reconcile_events = |app: &NodeApp| {
        app.observability_snapshot()
            .recent_events
            .iter()
            .filter(|event| event.kind == ObservabilityEventKind::Reconcile)
            .count()
    };
    assert_eq!(
        reconcile_events(&app),
        1,
        "changing pass should record an event"
    );

    for _ in 0..5 {
        app.tick().expect("idle tick should succeed");
    }

    assert_eq!(
        reconcile_events(&app),
        1,
        "idle passes should not push reconcile events"
    );
    assert_eq!(app.observability_snapshot().reconcile.success_count, 6);
}

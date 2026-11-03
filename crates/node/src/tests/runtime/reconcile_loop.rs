use super::*;
use orion::control_plane::{NodeRecord, ObservabilityEventKind, ObservedClusterState};
use std::time::Instant;

/// A backstop no test outlives: with it, any pass after the first was woken by a trigger.
const NO_BACKSTOP: Duration = Duration::from_secs(3600);
/// How long a test waits for a pass it expects. Only reached on failure, so it is generous:
/// a loaded runner delays a woken pass by hundreds of milliseconds without anything being wrong.
const PASS_WAIT: Duration = Duration::from_secs(30);

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

    let started = Instant::now();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let reconciles = reconcile_count(&app);
    let elapsed = started.elapsed();
    handle.shutdown().await;

    // The startup pass, one per backstop period that actually elapsed (a loaded machine
    // oversleeps), and one for rounding: 4 after 500 ms, where fixed 10ms polling runs ~50.
    let most = 2 + u64::try_from(elapsed.as_millis() / 200).unwrap_or(u64::MAX);
    assert!(
        (1..=most).contains(&reconciles),
        "idle loop should only run startup and backstop passes, observed {reconciles} in {elapsed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn desired_state_mutation_wakes_reconcile_loop_promptly() {
    let app = loop_test_app("node.loop.mutation", NO_BACKSTOP);
    let handle = app.spawn_reconcile_loop(Duration::from_millis(10));
    wait_for_reconciles(&app, 1, PASS_WAIT).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(reconcile_count(&app), 1, "idle loop should not poll");

    // With no backstop in reach, the second pass can only be the mutation's wake-up.
    add_node(&app, 0);
    wait_for_reconciles(&app, 2, PASS_WAIT).await;
    let snapshot = app.state_snapshot();
    handle.shutdown().await;

    assert_eq!(
        snapshot.state.applied.revision, snapshot.state.desired.revision,
        "woken pass should apply the new desired revision"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observed_update_wakes_reconcile_loop() {
    let app = loop_test_app("node.loop.observed", NO_BACKSTOP);
    let handle = app.spawn_reconcile_loop(Duration::from_millis(10));
    wait_for_reconciles(&app, 1, PASS_WAIT).await;

    let mut observed = ObservedClusterState::default();
    observed.set_revision(Revision::new(1));
    app.apply_observed_update(orion::control_plane::ObservedStateUpdate {
        observed,
        applied: Default::default(),
        host: None,
    })
    .expect("observed update should apply");
    // With no backstop in reach, the second pass can only be the update's wake-up.
    wait_for_reconciles(&app, 2, PASS_WAIT).await;
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn burst_of_mutations_coalesces_into_few_reconciles() {
    let app = loop_test_app("node.loop.burst", NO_BACKSTOP);
    let interval = Duration::from_millis(50);
    let handle = app.spawn_reconcile_loop(interval);
    wait_for_reconciles(&app, 1, PASS_WAIT).await;
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
    let app = loop_test_app("node.loop.events", NO_BACKSTOP);
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

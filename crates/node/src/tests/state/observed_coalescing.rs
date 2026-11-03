//! Coalesced observed/applied persistence (`ORION_NODE_OBSERVED_PERSIST_INTERVAL_MS`).

use super::*;
use orion::control_plane::{ClientHello, ClientRole, ProviderStateUpdate};
use std::time::Instant;

const PROVIDER: &str = "provider.coalesce";

pub(super) fn coalescing_app(node_id: &str, state_dir: PathBuf, interval: Duration) -> NodeApp {
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

pub(super) fn provider_update(app: &NodeApp, generation: u64) -> ControlMessage {
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

pub(super) fn register_provider_client(app: &NodeApp, source: &LocalAddress) {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_node_does_no_state_writes_across_reconcile_passes() {
    let state_dir = temp_state_dir("observed-idle");
    let config = test_node_config_with_state_dir_and_auth(
        NodeId::new("node.idle"),
        "node.idle",
        state_dir.clone(),
        crate::PeerAuthenticationMode::Disabled,
    )
    .with_runtime_tuning_mut(|tuning| {
        tuning
            .with_observed_persist_interval(Duration::from_millis(2000))
            .with_reconcile_backstop_interval(Duration::from_millis(20))
    });
    let app = NodeApp::builder()
        .config(config)
        .try_build()
        .expect("node app should build");
    let source = LocalAddress::new("coalesce-provider");
    register_provider_client(&app, &source);
    app.apply_local_control_message(&source, provider_update(&app, 0))
        .expect("provider state should apply");
    let handle = app.spawn_reconcile_loop(Duration::from_millis(5));

    // Let the first change flush and the coalescer drain before the measured window starts.
    let settle_deadline = Instant::now() + Duration::from_secs(10);
    while app
        .observability_snapshot()
        .resource_usage
        .observed_persistence
        .pending
    {
        assert!(
            Instant::now() < settle_deadline,
            "the first change should flush before the idle window"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let writes_before = state_writes(&app);
    let passes_before = app.reconcile_loop_heartbeat();
    // Identical re-publishes, as provider clients send several times a second.
    for _ in 0..120 {
        app.apply_local_control_message(&source, provider_update(&app, 0))
            .expect("identical provider state should apply");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let passes = app.reconcile_loop_heartbeat() - passes_before;
    let writes = state_writes(&app) - writes_before;

    handle.shutdown().await;
    assert!(
        passes >= 10,
        "the window should span several reconcile passes, saw {passes}"
    );
    assert_eq!(writes, 0, "an idle node should write nothing");
    let _ = std::fs::remove_dir_all(state_dir);
}

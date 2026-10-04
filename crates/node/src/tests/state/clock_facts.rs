use super::*;
use crate::clock::{STA_NANO, STA_UNSYNC};
use crate::{ClockStatusSource, KernelClockReading};
use orion::control_plane::{ClockSourceKind, render_observability_metrics};
use std::sync::Mutex;

struct FakeClock(Mutex<Option<KernelClockReading>>);

impl FakeClock {
    fn new(reading: KernelClockReading) -> Self {
        Self(Mutex::new(Some(reading)))
    }

    fn set(&self, reading: KernelClockReading) {
        *self.0.lock().expect("fake clock lock") = Some(reading);
    }
}

impl ClockStatusSource for FakeClock {
    fn read(&self) -> Option<KernelClockReading> {
        *self.0.lock().expect("fake clock lock")
    }
}

const SYNCED: KernelClockReading = KernelClockReading {
    state: 0,
    status: STA_NANO,
    offset: 2_000,
    max_error_us: 20_000,
    estimated_error_us: 5,
};

fn clock_app() -> NodeApp {
    let mut config = test_node_config("node-clock", "node-clock");
    config.runtime_tuning = config
        .runtime_tuning
        .with_clock_source(Some(ClockSourceKind::Ptp))
        .with_clock_timebase(Some("TAI".into()));
    NodeApp::builder()
        .config(config)
        .try_build()
        .expect("node app should build")
}

#[test]
fn clock_facts_are_published_only_on_meaningful_change() {
    let app = clock_app();
    let clock = FakeClock::new(SYNCED);

    assert!(app.refresh_clock_facts_from(&clock));
    let published = app
        .published_clock_facts()
        .expect("clock facts should be published");
    assert_eq!(published.source, ClockSourceKind::Ptp);
    assert_eq!(published.timebase.as_deref(), Some("TAI"));
    assert_eq!(published.synchronized, Some(true));
    assert_eq!(published.offset_ns, Some(2_000));

    // Sub-millisecond jitter updates the observability sample but not the observed record.
    clock.set(KernelClockReading {
        offset: 300_000,
        ..SYNCED
    });
    assert!(!app.refresh_clock_facts_from(&clock));
    assert_eq!(app.published_clock_facts(), Some(published.clone()));
    let sampled = app
        .observability_snapshot()
        .clock
        .expect("observability should carry the latest sample");
    assert_eq!(sampled.offset_ns, Some(300_000));

    clock.set(KernelClockReading {
        status: STA_NANO | STA_UNSYNC,
        ..SYNCED
    });
    assert!(app.refresh_clock_facts_from(&clock));
    assert_eq!(
        app.published_clock_facts()
            .and_then(|facts| facts.synchronized),
        Some(false)
    );
    assert!(!app.refresh_clock_facts_from(&clock));

    let metrics = render_observability_metrics(&app.observability_snapshot());
    assert!(metrics.contains("orion_node_clock_synchronized{node_id=\"node-clock\"} 0\n"));
    assert!(metrics.contains(
        "orion_node_clock_info{node_id=\"node-clock\",source=\"ptp\",timebase=\"TAI\"} 1\n"
    ));
}

#[test]
fn clock_facts_ride_in_observed_state_and_survive_desired_replacement() {
    let app = clock_app();
    assert!(app.refresh_clock_facts_from(&FakeClock::new(SYNCED)));

    app.replace_desired(DesiredClusterState::default());

    let observed = app.state_snapshot().state.observed;
    let record = observed
        .nodes
        .get(&NodeId::new("node-clock"))
        .expect("local observed node record should be kept");
    assert_eq!(
        record.clock.as_ref().map(|facts| facts.source.clone()),
        Some(ClockSourceKind::Ptp)
    );
}

#[test]
fn peer_clock_facts_merge_through_observed_updates() {
    let app = clock_app();
    let mut observed = orion::control_plane::ObservedClusterState::default();
    observed.put_node(
        orion::control_plane::NodeRecord::builder("node-peer")
            .clock(
                orion::control_plane::NodeClockFacts::unknown(7)
                    .with_source(ClockSourceKind::Chrony)
                    .with_synchronized(true)
                    .with_stratum(2),
            )
            .build(),
    );
    app.apply_observed_update_from_peer(
        Some(&NodeId::new("node-peer")),
        orion::control_plane::ObservedStateUpdate {
            observed,
            applied: Default::default(),
        },
    )
    .expect("peer observed update should apply");

    let snapshot = app.state_snapshot();
    let peer_clock = snapshot
        .state
        .observed
        .nodes
        .get(&NodeId::new("node-peer"))
        .and_then(|record| record.clock.clone())
        .expect("peer clock facts should be stored");
    assert_eq!(peer_clock.source, ClockSourceKind::Chrony);
    assert_eq!(peer_clock.stratum, Some(2));
}

#[test]
fn missing_kernel_reading_publishes_unknown_facts() {
    let app = NodeApp::builder()
        .config(test_node_config("node-clock-none", "node-clock-none"))
        .try_build()
        .expect("node app should build");
    assert!(app.refresh_clock_facts_from(&FakeClock(Mutex::new(None))));
    let facts = app.published_clock_facts().expect("facts should publish");
    assert_eq!(facts.source, ClockSourceKind::Unknown);
    assert_eq!(facts.synchronized, None);
}

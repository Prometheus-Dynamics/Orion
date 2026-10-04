//! Prometheus rendering is host-only, so these tests need the `std` feature.
#![cfg(feature = "std")]

use orion_control_plane::{ClockSourceKind, NodeClockFacts, render_clock_metrics};
use orion_core::NodeId;

#[test]
fn clock_metrics_render_sync_state_offset_and_errors() {
    let facts = NodeClockFacts::unknown(1_000)
        .with_source(ClockSourceKind::Ptp)
        .with_timebase("TAI")
        .with_synchronized(true)
        .with_offset_ns(-1_500)
        .with_max_error_ns(16_000_000)
        .with_estimated_error_ns(2_000);
    let rendered = render_clock_metrics(&NodeId::new("node-a"), Some(&facts));

    assert!(
        rendered.contains(
            "orion_node_clock_info{node_id=\"node-a\",source=\"ptp\",timebase=\"TAI\"} 1\n"
        )
    );
    assert!(rendered.contains("# TYPE orion_node_clock_synchronized gauge\n"));
    assert!(rendered.contains("orion_node_clock_synchronized{node_id=\"node-a\"} 1\n"));
    assert!(rendered.contains("orion_node_clock_offset_seconds{node_id=\"node-a\"} -0.0000015\n"));
    assert!(rendered.contains("orion_node_clock_max_error_seconds{node_id=\"node-a\"} 0.016\n"));
    assert!(
        rendered
            .contains("orion_node_clock_estimated_error_seconds{node_id=\"node-a\"} 0.000002\n")
    );
    assert!(!rendered.contains("orion_node_clock_stratum"));
}

#[test]
fn clock_metrics_omit_unknown_values() {
    assert!(render_clock_metrics(&NodeId::new("node-a"), None).is_empty());

    let rendered = render_clock_metrics(&NodeId::new("node-a"), Some(&NodeClockFacts::unknown(0)));
    assert!(rendered.contains(
        "orion_node_clock_info{node_id=\"node-a\",source=\"unknown\",timebase=\"\"} 1\n"
    ));
    assert!(!rendered.contains("orion_node_clock_synchronized"));
    assert!(!rendered.contains("orion_node_clock_offset_seconds"));

    let unsynced = NodeClockFacts::unknown(0)
        .with_source(ClockSourceKind::System)
        .with_synchronized(false);
    let rendered = render_clock_metrics(&NodeId::new("node-a"), Some(&unsynced));
    assert!(rendered.contains("orion_node_clock_synchronized{node_id=\"node-a\"} 0\n"));
}

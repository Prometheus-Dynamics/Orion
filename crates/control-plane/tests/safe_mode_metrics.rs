//! Prometheus rendering is host-only, so these tests need the `std` feature.
#![cfg(feature = "std")]

use orion_control_plane::render_safe_mode_metrics;
use orion_core::NodeId;

#[test]
fn safe_mode_metric_is_one_with_the_cause_and_zero_otherwise() {
    let node = NodeId::new("node-a");

    let in_safe_mode = render_safe_mode_metrics(&node, Some("state_quarantined"));
    assert!(in_safe_mode.contains("# TYPE orion_safe_mode gauge\n"));
    assert!(
        in_safe_mode
            .contains("orion_safe_mode{node_id=\"node-a\",cause=\"state_quarantined\"} 1\n"),
        "{in_safe_mode}"
    );

    let normal = render_safe_mode_metrics(&node, None);
    assert!(
        normal.contains("orion_safe_mode{node_id=\"node-a\",cause=\"\"} 0\n"),
        "{normal}"
    );
}

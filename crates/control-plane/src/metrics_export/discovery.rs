//! Prometheus text export for `DiscoveryMetricsSnapshot` (peer discovery and enrollment, see
//! `docs/discovery.md`).

use super::format::{gauge, metric_help, metric_type, sample};
use crate::DiscoveryMetricsSnapshot;
use orion_core::NodeId;

pub(super) fn append_discovery_metrics(
    out: &mut String,
    node_id: &NodeId,
    discovery: &DiscoveryMetricsSnapshot,
) {
    if !discovery.enabled {
        return;
    }
    let labels = [("node_id", node_id.as_str())];
    gauge(
        out,
        "orion_node_discovered_peers",
        "Peers of the local cluster currently in the discovered set.",
        &labels,
        discovery.discovered_peers,
    );
    gauge(
        out,
        "orion_node_discovered_enrolled_peers",
        "Discovered peers that are enrolled.",
        &labels,
        discovery.enrolled_peers,
    );
    for (name, help, value) in [
        (
            "orion_node_discovery_announcements_total",
            "Valid announcements of the local cluster received.",
            discovery.announcements_received,
        ),
        (
            "orion_node_discovery_announcements_ignored_total",
            "Announcements ignored (other cluster, malformed, or the node's own).",
            discovery.announcements_ignored,
        ),
        (
            "orion_node_discovery_peers_expired_total",
            "Discovered peers dropped after their announcement expired or was withdrawn.",
            discovery.peers_expired,
        ),
        (
            "orion_node_enrollment_attempts_total",
            "Peer enrollments started (operator approvals and shared-key handshakes).",
            discovery.enrollment_attempts,
        ),
        (
            "orion_node_enrollment_successes_total",
            "Peer enrollments that completed.",
            discovery.enrollment_successes,
        ),
        (
            "orion_node_enrollment_failures_total",
            "Peer enrollments that failed.",
            discovery.enrollment_failures,
        ),
    ] {
        metric_help(out, name, help);
        metric_type(out, name, "counter");
        sample(out, name, &labels, value);
    }
}

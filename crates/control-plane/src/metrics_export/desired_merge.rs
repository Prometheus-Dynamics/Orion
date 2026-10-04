//! Prometheus text export for `DesiredStateMergeSnapshot` (hybrid logical clock and per-object
//! merge counters, see `docs/peer-sync.md`).

use super::format::{gauge, metric_help, metric_type, sample};
use crate::DesiredStateMergeSnapshot;
use orion_core::NodeId;

pub(super) fn append_desired_merge_metrics(
    out: &mut String,
    node_id: &NodeId,
    merge: &DesiredStateMergeSnapshot,
) {
    let labels = [("node_id", node_id.as_str())];
    gauge(
        out,
        "orion_node_hlc_physical_ms",
        "Physical part (Unix milliseconds) of the latest hybrid-logical-clock timestamp.",
        &labels,
        merge.hlc.physical_ms,
    );
    gauge(
        out,
        "orion_node_hlc_max_drift_ms",
        "Largest accepted distance of a peer timestamp ahead of the local wall clock.",
        &labels,
        merge.max_drift_ms,
    );
    gauge(
        out,
        "orion_node_desired_tombstones",
        "Desired-state tombstones currently retained.",
        &labels,
        merge.tombstones,
    );
    for (name, help, value) in [
        (
            "orion_node_desired_merge_local_writes_total",
            "Desired-state object versions written by this node.",
            merge.local_writes,
        ),
        (
            "orion_node_desired_merge_remote_applied_total",
            "Object versions from peers that replaced the local version.",
            merge.remote_writes_applied,
        ),
        (
            "orion_node_desired_merge_remote_stale_total",
            "Object versions from peers that lost against a newer local version.",
            merge.stale_remote_writes_ignored,
        ),
        (
            "orion_node_desired_merge_clock_skew_rejections_total",
            "Object versions from peers rejected because their timestamp exceeded the maximum drift.",
            merge.clock_skew_rejections,
        ),
        (
            "orion_node_desired_merge_expired_tombstones_ignored_total",
            "Deletes from peers that were already past tombstone retention.",
            merge.expired_tombstones_ignored,
        ),
        (
            "orion_node_desired_tombstones_collected_total",
            "Desired-state tombstones dropped after the retention period.",
            merge.tombstones_collected,
        ),
    ] {
        metric_help(out, name, help);
        metric_type(out, name, "counter");
        sample(out, name, &labels, value);
    }
}

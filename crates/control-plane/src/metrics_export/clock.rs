use super::format::{gauge, optional_gauge};
use crate::NodeClockFacts;
use orion_core::NodeId;

const NANOS_PER_SECOND: f64 = 1_000_000_000.0;

fn nanos_to_seconds_i64(value: i64) -> f64 {
    value as f64 / NANOS_PER_SECOND
}

fn nanos_to_seconds_u64(value: u64) -> f64 {
    value as f64 / NANOS_PER_SECOND
}

/// Renders the clock families of [`append_clock_metrics`] on their own.
pub fn render_clock_metrics(node_id: &NodeId, clock: Option<&NodeClockFacts>) -> String {
    let mut out = String::new();
    append_clock_metrics(&mut out, node_id, clock);
    out
}

/// Clock facts as Prometheus families. Nothing is emitted before the first clock check, and each
/// measurement is emitted only when the source reports it.
pub(super) fn append_clock_metrics(
    out: &mut String,
    node_id: &NodeId,
    clock: Option<&NodeClockFacts>,
) {
    let Some(clock) = clock else {
        return;
    };
    let node = node_id.as_str();
    gauge(
        out,
        "orion_node_clock_info",
        "Clock source and declared timebase of the node (always 1).",
        &[
            ("node_id", node),
            ("source", clock.source.as_str()),
            ("timebase", clock.timebase.as_deref().unwrap_or("")),
        ],
        1,
    );
    optional_gauge(
        out,
        "orion_node_clock_synchronized",
        "1 when the node clock reports synchronized to its reference, 0 when not.",
        &[("node_id", node)],
        clock.synchronized.map(u8::from),
    );
    optional_gauge(
        out,
        "orion_node_clock_offset_seconds",
        "Estimated offset of the node clock from its reference in seconds.",
        &[("node_id", node)],
        clock.offset_ns.map(nanos_to_seconds_i64),
    );
    optional_gauge(
        out,
        "orion_node_clock_max_error_seconds",
        "Upper bound on the node clock error in seconds.",
        &[("node_id", node)],
        clock.max_error_ns.map(nanos_to_seconds_u64),
    );
    optional_gauge(
        out,
        "orion_node_clock_estimated_error_seconds",
        "Estimated node clock error in seconds.",
        &[("node_id", node)],
        clock.estimated_error_ns.map(nanos_to_seconds_u64),
    );
    optional_gauge(
        out,
        "orion_node_clock_stratum",
        "NTP stratum of the node clock.",
        &[("node_id", node)],
        clock.stratum,
    );
}

//! Prometheus text export for `NodeResourceUsageSnapshot`.
//!
//! Process counters already exported by the host metrics (`orion_process_rss_bytes`,
//! `orion_process_pss_bytes`, `orion_process_vm_hwm_bytes`, ...) are not repeated here; this
//! module adds the RSS/PSS breakdown plus state, history, stream, queue, and registry gauges, and
//! the status-lane and observed-persistence counters.

use super::format::{gauge, metric_help, metric_type, optional_gauge, sample};
use crate::{NodeResourceUsageSnapshot, StateSectionCounts};
use orion_core::NodeId;

/// Renders only the resource-usage metric families for one node.
pub fn render_resource_usage_metrics(
    node_id: &NodeId,
    usage: &NodeResourceUsageSnapshot,
) -> String {
    let mut out = String::new();
    append_resource_usage_metrics(&mut out, node_id, usage);
    out
}

pub(super) fn append_resource_usage_metrics(
    out: &mut String,
    node_id: &NodeId,
    usage: &NodeResourceUsageSnapshot,
) {
    let node = node_id.as_str();
    append_process_breakdown(out, node, usage);
    append_state_metrics(out, node, usage);
    append_mutation_history_metrics(out, node, usage);
    append_local_stream_metrics(out, node, usage);
    append_worker_queue_metrics(out, node, usage);
    append_registry_metrics(out, node, usage);
    append_status_and_persistence_metrics(out, node, usage);
}

fn append_process_breakdown(out: &mut String, node: &str, usage: &NodeResourceUsageSnapshot) {
    let process = &usage.process;
    let labels = [("node_id", node)];
    for (name, help, value) in [
        (
            "orion_process_rss_anon_bytes",
            "Orion process resident anonymous memory in bytes.",
            process.rss_anon_bytes,
        ),
        (
            "orion_process_rss_file_bytes",
            "Orion process resident file-backed memory in bytes.",
            process.rss_file_bytes,
        ),
        (
            "orion_process_rss_shmem_bytes",
            "Orion process resident shared memory in bytes.",
            process.rss_shmem_bytes,
        ),
        (
            "orion_process_pss_anon_bytes",
            "Orion process proportional anonymous memory in bytes.",
            process.pss_anon_bytes,
        ),
        (
            "orion_process_pss_file_bytes",
            "Orion process proportional file-backed memory in bytes.",
            process.pss_file_bytes,
        ),
    ] {
        optional_gauge(out, name, help, &labels, value);
    }
}

fn append_state_metrics(out: &mut String, node: &str, usage: &NodeResourceUsageSnapshot) {
    metric_help(
        out,
        "orion_state_records",
        "In-memory cluster-state record count by state view and record kind.",
    );
    metric_type(out, "orion_state_records", "gauge");
    append_state_counts(out, node, "desired", &usage.state.desired, false);
    append_state_counts(out, node, "observed", &usage.state.observed, true);

    let persisted = [
        ("snapshot", usage.state.persisted_snapshot_bytes),
        (
            "mutation_history",
            usage.state.persisted_mutation_history_bytes,
        ),
    ];
    if persisted.iter().any(|(_, value)| value.is_some()) {
        metric_help(
            out,
            "orion_state_persisted_bytes",
            "On-disk size of persisted node state by section.",
        );
        metric_type(out, "orion_state_persisted_bytes", "gauge");
        for (section, value) in persisted {
            if let Some(value) = value {
                sample(
                    out,
                    "orion_state_persisted_bytes",
                    &[("node_id", node), ("section", section)],
                    value,
                );
            }
        }
    }
}

/// Observed state only tracks nodes, workloads, resources, and leases; the other kinds are
/// skipped for that view instead of being exported as misleading zeroes.
fn append_state_counts(
    out: &mut String,
    node: &str,
    view: &str,
    counts: &StateSectionCounts,
    observed_view: bool,
) {
    const OBSERVED_KINDS: &[&str] = &["nodes", "workloads", "resources", "leases"];
    for (kind, value) in [
        ("nodes", counts.nodes),
        ("artifacts", counts.artifacts),
        ("workloads", counts.workloads),
        ("tombstones", counts.tombstones),
        ("resources", counts.resources),
        ("providers", counts.providers),
        ("executors", counts.executors),
        ("leases", counts.leases),
    ] {
        if observed_view && !OBSERVED_KINDS.contains(&kind) {
            continue;
        }
        sample(
            out,
            "orion_state_records",
            &[("node_id", node), ("view", view), ("kind", kind)],
            value,
        );
    }
}

fn append_mutation_history_metrics(
    out: &mut String,
    node: &str,
    usage: &NodeResourceUsageSnapshot,
) {
    let history = &usage.mutation_history;
    let labels = [("node_id", node)];
    gauge(
        out,
        "orion_mutation_history_batches",
        "Retained desired-state mutation batches.",
        &labels,
        history.batches,
    );
    gauge(
        out,
        "orion_mutation_history_max_batches",
        "Configured mutation-history batch cap.",
        &labels,
        history.max_batches,
    );
    gauge(
        out,
        "orion_mutation_history_mutations",
        "Mutations contained in the retained history batches.",
        &labels,
        history.mutations,
    );
    optional_gauge(
        out,
        "orion_mutation_history_encoded_bytes",
        "Encoded size of the retained mutation history in bytes.",
        &labels,
        history.encoded_bytes,
    );
    gauge(
        out,
        "orion_mutation_history_max_bytes",
        "Configured mutation-history encoded byte cap.",
        &labels,
        history.max_bytes,
    );
}

fn append_local_stream_metrics(out: &mut String, node: &str, usage: &NodeResourceUsageSnapshot) {
    let streams = &usage.local_streams;
    metric_help(
        out,
        "orion_local_stream_subscribers",
        "Local IPC clients, attached streams, and watchers by kind.",
    );
    metric_type(out, "orion_local_stream_subscribers", "gauge");
    for (kind, value) in [
        ("registered_clients", streams.registered_clients),
        ("attached_streams", streams.attached_streams),
        ("state_watchers", streams.state_watchers),
        ("executor_watchers", streams.executor_watchers),
        ("provider_watchers", streams.provider_watchers),
    ] {
        sample(
            out,
            "orion_local_stream_subscribers",
            &[("node_id", node), ("kind", kind)],
            value,
        );
    }
    append_total_max(
        out,
        node,
        "orion_local_stream_send_queue_depth",
        "Envelopes waiting in local stream send queues, summed or per-stream maximum.",
        streams.send_queue_depth_total,
        streams.send_queue_depth_max,
    );
    gauge(
        out,
        "orion_local_stream_send_queue_capacity",
        "Configured per-stream send queue capacity.",
        &[("node_id", node)],
        streams.send_queue_capacity,
    );
    append_total_max(
        out,
        node,
        "orion_local_client_event_queue_depth",
        "Pending local client events, summed or per-client maximum.",
        streams.queued_client_events_total,
        streams.queued_client_events_max,
    );
    gauge(
        out,
        "orion_local_client_event_queue_limit",
        "Configured per-client pending event limit.",
        &[("node_id", node)],
        streams.client_event_queue_limit,
    );
    gauge(
        out,
        "orion_local_client_dropped_events",
        "Events dropped from full client queues, summed over currently registered clients.",
        &[("node_id", node)],
        streams.dropped_client_events_total,
    );
}

fn append_total_max(out: &mut String, node: &str, name: &str, help: &str, total: u64, max: u64) {
    metric_help(out, name, help);
    metric_type(out, name, "gauge");
    sample(out, name, &[("node_id", node), ("stat", "total")], total);
    sample(out, name, &[("node_id", node), ("stat", "max")], max);
}

fn append_worker_queue_metrics(out: &mut String, node: &str, usage: &NodeResourceUsageSnapshot) {
    if usage.worker_queues.is_empty() {
        return;
    }
    metric_help(
        out,
        "orion_worker_queue_depth",
        "Commands waiting in a bounded background worker queue.",
    );
    metric_type(out, "orion_worker_queue_depth", "gauge");
    for queue in &usage.worker_queues {
        sample(
            out,
            "orion_worker_queue_depth",
            &[("node_id", node), ("queue", queue.name.as_str())],
            queue.depth,
        );
    }
    metric_help(
        out,
        "orion_worker_queue_capacity",
        "Configured capacity of a bounded background worker queue.",
    );
    metric_type(out, "orion_worker_queue_capacity", "gauge");
    for queue in &usage.worker_queues {
        sample(
            out,
            "orion_worker_queue_capacity",
            &[("node_id", node), ("queue", queue.name.as_str())],
            queue.capacity,
        );
    }
    if usage
        .worker_queues
        .iter()
        .any(|queue| queue.dropped_total.is_some())
    {
        metric_help(
            out,
            "orion_worker_queue_dropped_total",
            "Commands dropped by a worker queue overload policy.",
        );
        metric_type(out, "orion_worker_queue_dropped_total", "counter");
        for queue in &usage.worker_queues {
            if let Some(dropped) = queue.dropped_total {
                sample(
                    out,
                    "orion_worker_queue_dropped_total",
                    &[("node_id", node), ("queue", queue.name.as_str())],
                    dropped,
                );
            }
        }
    }
}

fn append_registry_metrics(out: &mut String, node: &str, usage: &NodeResourceUsageSnapshot) {
    let registries = &usage.registries;
    metric_help(
        out,
        "orion_registry_entries",
        "Entries held in bounded or traffic-driven in-memory registries.",
    );
    metric_type(out, "orion_registry_entries", "gauge");
    for (registry, value) in [
        ("peers", registries.peers),
        ("local_clients", registries.local_clients),
        ("local_providers", registries.local_providers),
        ("local_executors", registries.local_executors),
        (
            "communication_endpoints",
            registries.communication_endpoints,
        ),
        ("recent_events", registries.recent_events),
        ("auth_nonce_peers", registries.auth_nonce_peers),
        ("auth_seen_nonces", registries.auth_seen_nonces),
    ] {
        sample(
            out,
            "orion_registry_entries",
            &[("node_id", node), ("registry", registry)],
            value,
        );
    }
    metric_help(
        out,
        "orion_registry_limit",
        "Configured cap for bounded in-memory registries.",
    );
    metric_type(out, "orion_registry_limit", "gauge");
    for (registry, value) in [
        (
            "communication_endpoints",
            registries.communication_endpoint_limit,
        ),
        ("recent_events", registries.recent_event_limit),
    ] {
        sample(
            out,
            "orion_registry_limit",
            &[("node_id", node), ("registry", registry)],
            value,
        );
    }
}

fn append_status_and_persistence_metrics(
    out: &mut String,
    node: &str,
    usage: &NodeResourceUsageSnapshot,
) {
    let labels = [("node_id", node)];
    let status = &usage.status_lane;
    gauge(
        out,
        "orion_status_lane_entries",
        "Entries held in the volatile status lane.",
        &labels,
        status.entries,
    );
    gauge(
        out,
        "orion_status_lane_max_entries",
        "Configured node-wide status lane entry cap.",
        &labels,
        status.max_entries,
    );
    let persistence = &usage.observed_persistence;
    for (name, help, value) in [
        (
            "orion_status_lane_published_total",
            "Status entries accepted (new or updated values).",
            status.published_total,
        ),
        (
            "orion_status_lane_expired_total",
            "Status entries dropped because their TTL ran out.",
            status.expired_total,
        ),
        (
            "orion_status_lane_dropped_total",
            "Status entries refused because a cap was reached or the entry was invalid.",
            status.dropped_total,
        ),
        (
            "orion_observed_persist_coalesced_total",
            "Observed or applied state changes deferred to a coalesced write.",
            persistence.coalesced_changes_total,
        ),
        (
            "orion_observed_persist_flushes_total",
            "Coalesced observed/applied state writes.",
            persistence.flushes_total,
        ),
    ] {
        metric_help(out, name, help);
        metric_type(out, name, "counter");
        sample(out, name, &labels, value);
    }
}

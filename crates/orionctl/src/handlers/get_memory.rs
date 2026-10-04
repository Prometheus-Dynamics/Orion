//! `orionctl get memory`: node memory, state-size, and backlog diagnostics.

use orion_control_plane::{
    NodeObservabilitySnapshot, NodeResourceUsageSnapshot, render_host_metrics,
    render_resource_usage_metrics,
};

use crate::{
    cli::{OutputFormat, StateQueryArgs},
    render::print_structured,
};

use super::get::fetch_observability_snapshot;

pub(super) async fn run(args: StateQueryArgs) -> Result<(), String> {
    let snapshot = fetch_observability_snapshot(&args).await?;
    match args.output {
        OutputFormat::Summary => {
            print!("{}", render_memory_summary(&snapshot));
            Ok(())
        }
        OutputFormat::Json | OutputFormat::Yaml | OutputFormat::Toml => {
            print_structured(&snapshot.resource_usage, args.output)
        }
        OutputFormat::Metrics => {
            print!(
                "{}{}",
                render_host_metrics(&snapshot.node_id, &snapshot.host),
                render_resource_usage_metrics(&snapshot.node_id, &snapshot.resource_usage)
            );
            Ok(())
        }
    }
}

/// Renders one `key=value` line per diagnostic section, matching other `get` summaries.
fn render_memory_summary(snapshot: &NodeObservabilitySnapshot) -> String {
    let usage: &NodeResourceUsageSnapshot = &snapshot.resource_usage;
    let process = &usage.process;
    let state = &usage.state;
    let history = &usage.mutation_history;
    let streams = &usage.local_streams;
    let registries = &usage.registries;
    let mut out = String::new();

    out.push_str(&format!(
        "memory node={} vm_rss_bytes={} vm_hwm_bytes={} rss_anon_bytes={} rss_file_bytes={} rss_shmem_bytes={} pss_bytes={} pss_anon_bytes={} pss_file_bytes={} private_dirty_bytes={} vm_data_bytes={} threads={}\n",
        snapshot.node_id,
        option_u64(process.vm_rss_bytes),
        option_u64(process.vm_hwm_bytes),
        option_u64(process.rss_anon_bytes),
        option_u64(process.rss_file_bytes),
        option_u64(process.rss_shmem_bytes),
        option_u64(process.pss_bytes),
        option_u64(process.pss_anon_bytes),
        option_u64(process.pss_file_bytes),
        option_u64(process.private_dirty_bytes),
        option_u64(process.vm_data_bytes),
        option_u64(process.threads),
    ));
    out.push_str(&format!(
        "state desired_records={} desired_workloads={} desired_resources={} desired_providers={} desired_executors={} desired_artifacts={} desired_leases={} desired_nodes={} desired_tombstones={} observed_records={} observed_workloads={} observed_resources={} observed_leases={} observed_nodes={} persisted_snapshot_bytes={} persisted_mutation_history_bytes={}\n",
        state.desired.total(),
        state.desired.workloads,
        state.desired.resources,
        state.desired.providers,
        state.desired.executors,
        state.desired.artifacts,
        state.desired.leases,
        state.desired.nodes,
        state.desired.tombstones,
        state.observed.total(),
        state.observed.workloads,
        state.observed.resources,
        state.observed.leases,
        state.observed.nodes,
        option_u64(state.persisted_snapshot_bytes),
        option_u64(state.persisted_mutation_history_bytes),
    ));
    out.push_str(&format!(
        "mutation_history batches={}/{} mutations={} encoded_bytes={}/{}\n",
        history.batches,
        history.max_batches,
        history.mutations,
        option_u64(history.encoded_bytes),
        history.max_bytes,
    ));
    out.push_str(&format!(
        "local_streams clients={} attached_streams={} state_watchers={} executor_watchers={} provider_watchers={} send_queue_depth_total={} send_queue_depth_max={}/{} queued_events_total={} queued_events_max={}/{} dropped_events={}\n",
        streams.registered_clients,
        streams.attached_streams,
        streams.state_watchers,
        streams.executor_watchers,
        streams.provider_watchers,
        streams.send_queue_depth_total,
        streams.send_queue_depth_max,
        streams.send_queue_capacity,
        streams.queued_client_events_total,
        streams.queued_client_events_max,
        streams.client_event_queue_limit,
        streams.dropped_client_events_total,
    ));
    for queue in &usage.worker_queues {
        out.push_str(&format!(
            "worker_queue name={} depth={}/{} dropped={}\n",
            queue.name,
            queue.depth,
            queue.capacity,
            option_u64(queue.dropped_total),
        ));
    }
    out.push_str(&format!(
        "registries peers={} local_clients={} local_providers={} local_executors={} communication_endpoints={}/{} recent_events={}/{} auth_nonce_peers={} auth_seen_nonces={}\n",
        registries.peers,
        registries.local_clients,
        registries.local_providers,
        registries.local_executors,
        registries.communication_endpoints,
        registries.communication_endpoint_limit,
        registries.recent_events,
        registries.recent_event_limit,
        registries.auth_nonce_peers,
        registries.auth_seen_nonces,
    ));
    out
}

fn option_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_owned())
}

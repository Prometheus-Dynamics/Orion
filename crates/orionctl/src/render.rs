use orion_control_plane::{
    ClientEvent, ClientEventKind, MaintenanceStatus, PeerTrustSnapshot, StateSnapshot,
};

use crate::cli::OutputFormat;

pub(crate) fn print_snapshot_summary(snapshot: &StateSnapshot) {
    println!(
        "snapshot desired_rev={} observed_rev={} applied_rev={} desired_nodes={} observed_nodes={} desired_artifacts={} desired_workloads={} observed_workloads={} desired_resources={} observed_resources={} desired_providers={} desired_executors={} desired_leases={} observed_leases={}",
        snapshot.state.desired.revision,
        snapshot.state.observed.revision,
        snapshot.state.applied.revision,
        snapshot.state.desired.nodes.len(),
        snapshot.state.observed.nodes.len(),
        snapshot.state.desired.artifacts.len(),
        snapshot.state.desired.workloads.len(),
        snapshot.state.observed.workloads.len(),
        snapshot.state.desired.resources.len(),
        snapshot.state.observed.resources.len(),
        snapshot.state.desired.providers.len(),
        snapshot.state.desired.executors.len(),
        snapshot.state.desired.leases.len(),
        snapshot.state.observed.leases.len(),
    );
}

pub(crate) fn print_peer_summary(snapshot: &PeerTrustSnapshot) {
    println!("peers http_mtls_mode={}", snapshot.http_mutual_tls_mode);
    for peer in &snapshot.peers {
        println!(
            "peer node={} base_url={} configured_key={} trusted_key={} configured_tls_root={} learned_tls_fingerprint={} revoked={} sync_status={} last_error_kind={} last_error={} hint={}",
            peer.node_id,
            peer.base_url
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "-".to_owned()),
            peer.configured_public_key_hex
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "-".to_owned()),
            peer.trusted_public_key_hex
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "-".to_owned()),
            peer.configured_tls_root_cert_path
                .clone()
                .unwrap_or_else(|| "-".to_owned()),
            peer.learned_tls_root_cert_fingerprint
                .clone()
                .unwrap_or_else(|| "-".to_owned()),
            peer.revoked,
            peer.sync_status
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "-".to_owned()),
            peer.last_error_kind
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "-".to_owned()),
            peer.last_error.clone().unwrap_or_else(|| "-".to_owned()),
            peer.troubleshooting_hint
                .clone()
                .unwrap_or_else(|| "-".to_owned()),
        );
    }
}

pub(crate) fn print_event_summary(event: &ClientEvent) {
    match &event.event {
        ClientEventKind::StateSnapshot(snapshot) => {
            println!(
                "state seq={} desired_rev={} observed_rev={} desired_workloads={} observed_workloads={} desired_resources={} observed_resources={} desired_providers={} desired_executors={} desired_leases={} observed_leases={}",
                event.sequence,
                snapshot.state.desired.revision,
                snapshot.state.observed.revision,
                snapshot.state.desired.workloads.len(),
                snapshot.state.observed.workloads.len(),
                snapshot.state.desired.resources.len(),
                snapshot.state.observed.resources.len(),
                snapshot.state.desired.providers.len(),
                snapshot.state.desired.executors.len(),
                snapshot.state.desired.leases.len(),
                snapshot.state.observed.leases.len(),
            );
        }
        ClientEventKind::ExecutorWorkloads {
            executor_id,
            workloads,
        } => {
            println!(
                "state seq={} executor={} workloads={}",
                event.sequence,
                executor_id,
                workloads.len(),
            );
        }
        ClientEventKind::ProviderLeases {
            provider_id,
            leases,
        } => {
            println!(
                "state seq={} provider={} leases={}",
                event.sequence,
                provider_id,
                leases.len(),
            );
        }
    }
}

pub(crate) fn print_maintenance_summary(status: &MaintenanceStatus) {
    println!(
        "maintenance mode={} peer_sync_paused={} remote_desired_state_blocked={} allow_runtimes={} allow_workloads={}",
        status.state.mode,
        status.peer_sync_paused,
        status.remote_desired_state_blocked,
        join_display(&status.state.allow_runtime_types),
        join_display(&status.state.allow_workload_ids),
    );
}

#[derive(serde::Serialize)]
struct TomlEnvelope<'a, T> {
    value: &'a T,
}

pub(crate) fn print_structured<T: serde::Serialize>(
    value: &T,
    format: OutputFormat,
) -> Result<(), String> {
    let rendered = match format {
        OutputFormat::Summary => {
            return Err("summary output is not a structured serialization format".to_owned());
        }
        OutputFormat::Json => {
            serde_json::to_string_pretty(value).map_err(|error| error.to_string())?
        }
        OutputFormat::Yaml => serde_yaml::to_string(value).map_err(|error| error.to_string())?,
        OutputFormat::Toml => {
            toml::to_string_pretty(&TomlEnvelope { value }).map_err(|error| error.to_string())?
        }
    };
    println!("{rendered}");
    Ok(())
}

pub(crate) fn join_or_dash(values: &[String]) -> String {
    if values.is_empty() {
        "-".to_owned()
    } else {
        values.join(",")
    }
}

pub(crate) fn join_display<T: std::fmt::Display>(values: &[T]) -> String {
    if values.is_empty() {
        "-".to_owned()
    } else {
        values
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",")
    }
}

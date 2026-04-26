use orion_control_plane::{
    AvailabilityState, ClientEvent, ClientEventKind, DesiredState, HealthState, LeaseState,
    MaintenanceStatus, ObservabilityEvent, ObservabilityEventKind, PeerTrustSnapshot,
    RestartPolicy, StateSnapshot, WorkloadObservedState,
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

pub(crate) fn print_observability_event_summary(event: &ObservabilityEvent) {
    println!(
        "event seq={} ts_ms={} kind={} status={} duration_ms={} subject={} correlation_id={} detail={}",
        event.sequence,
        event.timestamp_ms,
        render_observability_event_kind(&event.kind),
        if event.success { "success" } else { "failed" },
        event
            .duration_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_owned()),
        event.subject.as_deref().unwrap_or("-"),
        event.correlation_id.as_deref().unwrap_or("-"),
        event.detail,
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
        OutputFormat::Metrics => {
            return Err("metrics output is supported only for observability views".to_owned());
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

pub(crate) fn render_desired_state(state: DesiredState) -> &'static str {
    match state {
        DesiredState::Running => "running",
        DesiredState::Stopped => "stopped",
    }
}

pub(crate) fn render_observed_state(state: WorkloadObservedState) -> &'static str {
    match state {
        WorkloadObservedState::Pending => "pending",
        WorkloadObservedState::Assigned => "assigned",
        WorkloadObservedState::Starting => "starting",
        WorkloadObservedState::Running => "running",
        WorkloadObservedState::Stopped => "stopped",
        WorkloadObservedState::Completed => "completed",
        WorkloadObservedState::Failed => "failed",
    }
}

pub(crate) fn render_restart_policy(policy: RestartPolicy) -> &'static str {
    match policy {
        RestartPolicy::Never => "never",
        RestartPolicy::OnFailure => "on_failure",
        RestartPolicy::Always => "always",
    }
}

pub(crate) fn render_health_state(state: HealthState) -> &'static str {
    match state {
        HealthState::Healthy => "healthy",
        HealthState::Degraded => "degraded",
        HealthState::Failed => "failed",
        HealthState::Unknown => "unknown",
    }
}

pub(crate) fn render_observability_event_kind(kind: &ObservabilityEventKind) -> &'static str {
    match kind {
        ObservabilityEventKind::Replay => "replay",
        ObservabilityEventKind::PeerSync => "peer_sync",
        ObservabilityEventKind::Reconcile => "reconcile",
        ObservabilityEventKind::MutationApply => "mutation_apply",
        ObservabilityEventKind::Persistence => "persistence",
        ObservabilityEventKind::TransportSecurity => "transport_security",
        ObservabilityEventKind::ClientRegistration => "client_registration",
        ObservabilityEventKind::ClientStreamAttach => "client_stream_attach",
        ObservabilityEventKind::ClientStreamDetach => "client_stream_detach",
        ObservabilityEventKind::ClientStaleEviction => "client_stale_eviction",
        ObservabilityEventKind::ClientRateLimited => "client_rate_limited",
    }
}

pub(crate) fn render_availability_state(state: AvailabilityState) -> &'static str {
    match state {
        AvailabilityState::Available => "available",
        AvailabilityState::Busy => "busy",
        AvailabilityState::Unavailable => "unavailable",
        AvailabilityState::Unknown => "unknown",
    }
}

pub(crate) fn render_lease_state(state: LeaseState) -> &'static str {
    match state {
        LeaseState::Unleased => "unleased",
        LeaseState::Leased => "leased",
        LeaseState::Contended => "contended",
    }
}

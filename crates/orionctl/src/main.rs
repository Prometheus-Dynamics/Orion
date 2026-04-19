mod cli;

use clap::Parser;
use cli::{Cli, Command, MutateCommand, TrustCommand};
use orion_client::{ControlPlaneEventStream, LocalControlPlaneClient};
use orion_control_plane::{
    ArtifactRecord, ClientEvent, ClientEventKind, ControlMessage, DesiredStateMutation,
    MutationBatch, PeerEnrollment, PeerIdentityUpdate, ResourceState, StateSnapshot,
    TypedConfigValue, WorkloadRecord,
};
use orion_core::{ArtifactId, NodeId, Revision, RuntimeType};
use orion_transport_http::{ControlRoute, HttpResponsePayload};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let cli = Cli::parse();
    match cli.command {
        Command::Hello(args) => {
            let response = args.hello().await?;
            match response {
                HttpResponsePayload::Hello(hello) => {
                    println!(
                        "hello node={} desired_rev={} observed_rev={} applied_rev={}",
                        hello.node_id,
                        hello.desired_revision,
                        hello.observed_revision,
                        hello.applied_revision
                    );
                    Ok(())
                }
                other => Err(format!("expected hello response, got {other:?}")),
            }
        }
        Command::Health(args) => {
            let response = args.get_route(ControlRoute::Health).await?;
            match response {
                HttpResponsePayload::Health(snapshot) => {
                    println!(
                        "health node={} status={:?} alive={} replay_completed={} replay_successful={} http_bound={} ipc_bound={} ipc_stream_bound={} degraded_peers={} reasons={}",
                        snapshot.node_id,
                        snapshot.status,
                        snapshot.alive,
                        snapshot.replay_completed,
                        snapshot.replay_successful,
                        snapshot.http_bound,
                        snapshot.ipc_bound,
                        snapshot.ipc_stream_bound,
                        snapshot.degraded_peer_count,
                        join_or_dash(&snapshot.reasons),
                    );
                    Ok(())
                }
                other => Err(format!("expected health response, got {other:?}")),
            }
        }
        Command::Readiness(args) => {
            let response = args.get_route(ControlRoute::Readiness).await?;
            match response {
                HttpResponsePayload::Readiness(snapshot) => {
                    println!(
                        "readiness node={} status={:?} ready={} replay_completed={} replay_successful={} initial_sync_complete={} ready_peers={} pending_peers={} degraded_peers={} reasons={}",
                        snapshot.node_id,
                        snapshot.status,
                        snapshot.ready,
                        snapshot.replay_completed,
                        snapshot.replay_successful,
                        snapshot.initial_sync_complete,
                        snapshot.ready_peer_count,
                        snapshot.pending_peer_count,
                        snapshot.degraded_peer_count,
                        join_or_dash(&snapshot.reasons),
                    );
                    Ok(())
                }
                other => Err(format!("expected readiness response, got {other:?}")),
            }
        }
        Command::Observability(args) => {
            let response = args
                .send_control(ControlMessage::QueryObservability)
                .await?;
            match response {
                HttpResponsePayload::Observability(snapshot) => {
                    println!(
                        "observability node={} replay_success={} replay_failures={} sync_success={} sync_failures={} reconcile_success={} reconcile_failures={} mutation_success={} mutation_failures={} persistence_queue_capacity={} audit_queue_capacity={} audit_dropped={} clients_registered={} clients_live={} rate_limited={} recent_events={}",
                        snapshot.node_id,
                        snapshot.replay.success_count,
                        snapshot.replay.failure_count,
                        snapshot.peer_sync.success_count,
                        snapshot.peer_sync.failure_count,
                        snapshot.reconcile.success_count,
                        snapshot.reconcile.failure_count,
                        snapshot.mutation_apply.success_count,
                        snapshot.mutation_apply.failure_count,
                        snapshot.persistence.worker_queue_capacity,
                        snapshot.audit_log_queue_capacity,
                        snapshot.audit_log_dropped_records,
                        snapshot.client_sessions.registered_clients,
                        snapshot.client_sessions.live_stream_clients,
                        snapshot.client_sessions.rate_limited_total,
                        snapshot.recent_events.len(),
                    );
                    Ok(())
                }
                other => Err(format!("expected observability response, got {other:?}")),
            }
        }
        Command::Snapshot(args) => {
            let snapshot = args.target.fetch_snapshot(&args.peer_node_id).await?;
            print_snapshot("snapshot", &snapshot);
            Ok(())
        }
        Command::Mutate { command } => match command {
            MutateCommand::PutArtifact(args) => {
                let snapshot = args.target.fetch_snapshot("orionctl.put-artifact").await?;
                let mut record = ArtifactRecord::builder(args.artifact_id);
                if let Some(content_type) = args.content_type {
                    record = record.content_type(content_type);
                }
                if let Some(size_bytes) = args.size_bytes {
                    record = record.size_bytes(size_bytes);
                }
                args.target
                    .apply_batch(MutationBatch {
                        base_revision: snapshot.state.desired.revision,
                        mutations: vec![DesiredStateMutation::PutArtifact(record.build())],
                    })
                    .await?;
                println!("mutate put-artifact accepted");
                Ok(())
            }
            MutateCommand::PutWorkload(args) => {
                let snapshot = args.target.fetch_snapshot("orionctl.put-workload").await?;
                let mut builder = WorkloadRecord::builder(
                    args.workload_id,
                    RuntimeType::new(args.runtime_type),
                    ArtifactId::new(args.artifact_id),
                )
                .desired_state(args.desired_state.into())
                .restart_policy(args.restart_policy.into());
                if let Some(node_id) = args.assigned_node {
                    builder = builder.assigned_to(NodeId::new(node_id));
                }
                args.target
                    .apply_batch(MutationBatch {
                        base_revision: snapshot.state.desired.revision,
                        mutations: vec![DesiredStateMutation::PutWorkload(builder.build())],
                    })
                    .await?;
                println!("mutate put-workload accepted");
                Ok(())
            }
        },
        Command::Trust { command } => match command {
            TrustCommand::List(args) => {
                let client = local_control_plane_client(&args.socket, &args.client_name)?;
                let snapshot = client
                    .query_peer_trust()
                    .await
                    .map_err(|error| error.to_string())?;
                println!("http_mtls_mode={}", snapshot.http_mutual_tls_mode);
                for record in snapshot.peers {
                    println!(
                        "peer node={} base_url={} configured_key={} trusted_key={} configured_tls_root={} learned_tls_fingerprint={} revoked={} sync_status={} last_error_kind={} last_error={} hint={}",
                        record.node_id,
                        record
                            .base_url
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "-".to_owned()),
                        record
                            .configured_public_key_hex
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "-".to_owned()),
                        record
                            .trusted_public_key_hex
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "-".to_owned()),
                        record
                            .configured_tls_root_cert_path
                            .unwrap_or_else(|| "-".to_owned()),
                        record
                            .learned_tls_root_cert_fingerprint
                            .unwrap_or_else(|| "-".to_owned()),
                        record.revoked,
                        record
                            .sync_status
                            .map(|status| status.to_string())
                            .unwrap_or_else(|| "-".to_owned()),
                        record
                            .last_error_kind
                            .map(|kind| kind.to_string())
                            .unwrap_or_else(|| "-".to_owned()),
                        record.last_error.unwrap_or_else(|| "-".to_owned()),
                        record
                            .troubleshooting_hint
                            .unwrap_or_else(|| "-".to_owned()),
                    );
                }
                Ok(())
            }
            TrustCommand::Enroll(args) => {
                let client =
                    local_control_plane_client(&args.local.socket, &args.local.client_name)?;
                client
                    .enroll_peer(PeerEnrollment {
                        node_id: NodeId::new(args.node_id),
                        base_url: args.base_url.into(),
                        trusted_public_key_hex: args.public_key.map(Into::into),
                        trusted_tls_root_cert_pem: args
                            .tls_root_cert
                            .as_deref()
                            .map(std::fs::read)
                            .transpose()
                            .map_err(|error| error.to_string())?,
                    })
                    .await
                    .map_err(|error| error.to_string())?;
                println!("trust enroll accepted");
                Ok(())
            }
            TrustCommand::Revoke(args) => {
                let client =
                    local_control_plane_client(&args.local.socket, &args.local.client_name)?;
                client
                    .revoke_peer(NodeId::new(args.node_id))
                    .await
                    .map_err(|error| error.to_string())?;
                println!("trust revoke accepted");
                Ok(())
            }
            TrustCommand::ReplaceKey(args) => {
                let client =
                    local_control_plane_client(&args.local.socket, &args.local.client_name)?;
                client
                    .replace_peer_identity(PeerIdentityUpdate {
                        node_id: NodeId::new(args.node_id),
                        public_key_hex: args.public_key.into(),
                    })
                    .await
                    .map_err(|error| error.to_string())?;
                println!("trust replace-key accepted");
                Ok(())
            }
            TrustCommand::RotateHttpTls(args) => {
                let client = local_control_plane_client(&args.socket, &args.client_name)?;
                client
                    .rotate_http_tls_identity()
                    .await
                    .map_err(|error| error.to_string())?;
                println!("trust rotate-http-tls accepted");
                Ok(())
            }
        },
        Command::WatchState(args) => {
            let mut stream = ControlPlaneEventStream::connect_at_with_local_address(
                &args.socket,
                args.client_name.clone(),
                format!("{}.control", args.client_name),
            )
            .await
            .map_err(|error| error.to_string())?;
            stream
                .subscribe_state(Revision::new(args.desired_revision))
                .await
                .map_err(|error| error.to_string())?;
            for _ in 0..args.batches {
                let events = stream
                    .next_events()
                    .await
                    .map_err(|error| error.to_string())?;
                for event in &events {
                    print_event(event);
                }
            }
            Ok(())
        }
    }
}

fn local_control_plane_client(
    socket: &std::path::Path,
    client_name: &str,
) -> Result<LocalControlPlaneClient, String> {
    LocalControlPlaneClient::connect_at(socket, client_name.to_owned())
        .map_err(|error| error.to_string())
}

fn print_snapshot(prefix: &str, snapshot: &StateSnapshot) {
    println!(
        "{prefix} desired_rev={} observed_rev={} applied_rev={} nodes={} artifacts={} workloads={} resources={} providers={} executors={} leases={}",
        snapshot.state.desired.revision,
        snapshot.state.observed.revision,
        snapshot.state.applied.revision,
        snapshot.state.desired.nodes.len(),
        snapshot.state.desired.artifacts.len(),
        snapshot.state.desired.workloads.len(),
        snapshot.state.desired.resources.len(),
        snapshot.state.desired.providers.len(),
        snapshot.state.desired.executors.len(),
        snapshot.state.desired.leases.len(),
    );
    for workload in snapshot.state.desired.workloads.values() {
        let schema = workload
            .config
            .as_ref()
            .map(|config| config.schema_id.to_string())
            .unwrap_or_else(|| "-".to_owned());
        println!(
            "workload id={} runtime={} artifact={} assigned_node={} config_schema={}",
            workload.workload_id,
            workload.runtime_type,
            workload.artifact_id,
            workload
                .assigned_node_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "-".to_owned()),
            schema,
        );
    }
    for resource in snapshot.state.desired.resources.values() {
        println!(
            "resource id={} type={} provider={} ownership={:?} owner_workload={} derived_from_resource={} derived_from_workload={} published_by_executor={}",
            resource.resource_id,
            resource.resource_type,
            resource.provider_id,
            resource.ownership_mode,
            resource
                .realized_for_workload_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "-".to_owned()),
            resource
                .source_resource_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "-".to_owned()),
            resource
                .source_workload_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "-".to_owned()),
            resource
                .realized_by_executor_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "-".to_owned()),
        );
        if let Some(state) = &resource.state {
            print_resource_state(state);
        }
    }
}

fn print_resource_state(state: &ResourceState) {
    if let Some(action_result) = &state.action_result {
        println!(
            "resource_state observed_at_ms={} action_kind={} action_status={:?} action_data={} action_error={}",
            state.observed_at_ms,
            action_result.action_kind,
            action_result.status,
            action_result
                .data
                .as_ref()
                .map(format_typed_config_value)
                .unwrap_or_else(|| "-".to_owned()),
            action_result.error.as_deref().unwrap_or("-"),
        );
    } else {
        println!(
            "resource_state observed_at_ms={} action_kind=- action_status=- action_data=- action_error=-",
            state.observed_at_ms,
        );
    }

    if let Some(config) = &state.config {
        let rendered = if config.payload.is_empty() {
            "-".to_owned()
        } else {
            config
                .payload
                .iter()
                .map(|(key, value)| format!("{key}={}", format_typed_config_value(value)))
                .collect::<Vec<_>>()
                .join(",")
        };
        println!("resource_config {}", rendered);
    }
}

fn format_typed_config_value(value: &TypedConfigValue) -> String {
    match value {
        TypedConfigValue::Bool(value) => value.to_string(),
        TypedConfigValue::Int(value) => value.to_string(),
        TypedConfigValue::UInt(value) => value.to_string(),
        TypedConfigValue::String(value) => value.clone(),
        TypedConfigValue::Bytes(bytes) => bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<Vec<_>>()
            .join(""),
    }
}

fn print_event(event: &ClientEvent) {
    match &event.event {
        ClientEventKind::StateSnapshot(snapshot) => {
            println!(
                "watch-state seq={} desired_rev={} workloads={} providers={} executors={} leases={}",
                event.sequence,
                snapshot.state.desired.revision,
                snapshot.state.desired.workloads.len(),
                snapshot.state.desired.providers.len(),
                snapshot.state.desired.executors.len(),
                snapshot.state.desired.leases.len(),
            );
        }
        ClientEventKind::ExecutorWorkloads {
            executor_id,
            workloads,
        } => {
            println!(
                "watch-state seq={} executor={} workloads={}",
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
                "watch-state seq={} provider={} leases={}",
                event.sequence,
                provider_id,
                leases.len(),
            );
        }
    }
}

fn join_or_dash(values: &[String]) -> String {
    if values.is_empty() {
        "-".to_owned()
    } else {
        values.join(",")
    }
}

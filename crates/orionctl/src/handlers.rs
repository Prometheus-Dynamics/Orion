use clap::Parser;
use orion_client::ControlPlaneEventStream;
use orion_control_plane::{
    ArtifactRecord, ControlMessage, DesiredStateMutation, MutationBatch, PeerEnrollment,
    PeerIdentityUpdate, TypedConfigValue, WorkloadConfig, WorkloadRecord, WorkloadRequirement,
};
use orion_core::{ArtifactId, ConfigSchemaId, NodeId, Revision, RuntimeType, WorkloadId};
use orion_transport_http::{ControlRoute, HttpResponsePayload};
use serde_json::json;
use std::collections::BTreeMap;

use crate::{
    cli::{
        ApplyCommand, Cli, Command, DeleteCommand, GetCommand, OutputFormat, PeerCommand,
        WatchCommand,
    },
    render::{
        join_display, join_or_dash, print_event_summary, print_json, print_peer_summary,
        print_snapshot_summary,
    },
};

pub(crate) async fn run() -> Result<(), String> {
    let cli = Cli::parse();
    match cli.command {
        Command::Get { command } => run_get(command).await,
        Command::Watch { command } => run_watch(command).await,
        Command::Apply { command } => run_apply(*command).await,
        Command::Delete { command } => run_delete(command).await,
        Command::Peers { command } => run_peers(command).await,
    }
}

async fn run_get(command: GetCommand) -> Result<(), String> {
    match command {
        GetCommand::Health(args) => match args.get_route(ControlRoute::Health).await? {
            HttpResponsePayload::Health(snapshot) => match args.output {
                OutputFormat::Summary => {
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
                OutputFormat::Json => print_json(&snapshot),
            },
            other => Err(format!("expected health response, got {other:?}")),
        },
        GetCommand::Readiness(args) => match args.get_route(ControlRoute::Readiness).await? {
            HttpResponsePayload::Readiness(snapshot) => match args.output {
                OutputFormat::Summary => {
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
                OutputFormat::Json => print_json(&snapshot),
            },
            other => Err(format!("expected readiness response, got {other:?}")),
        },
        GetCommand::Observability(args) => {
            let snapshot = if let Some(http_target) = args.http_target()? {
                match http_target
                    .send_control(ControlMessage::QueryObservability)
                    .await?
                {
                    HttpResponsePayload::Observability(snapshot) => *snapshot,
                    other => return Err(format!("expected observability response, got {other:?}")),
                }
            } else {
                args.local_client()?
                    .query_observability()
                    .await
                    .map_err(|error| error.to_string())?
            };
            match args.output {
                OutputFormat::Summary => {
                    println!(
                        "observability node={} desired_rev={} observed_rev={} applied_rev={} replay_success={} replay_failures={} sync_success={} sync_failures={} reconcile_success={} reconcile_failures={} mutation_success={} mutation_failures={} peers_configured={} peers_ready={} peers_degraded={} clients_registered={} clients_live={} recent_events={}",
                        snapshot.node_id,
                        snapshot.desired_revision,
                        snapshot.observed_revision,
                        snapshot.applied_revision,
                        snapshot.replay.success_count,
                        snapshot.replay.failure_count,
                        snapshot.peer_sync.success_count,
                        snapshot.peer_sync.failure_count,
                        snapshot.reconcile.success_count,
                        snapshot.reconcile.failure_count,
                        snapshot.mutation_apply.success_count,
                        snapshot.mutation_apply.failure_count,
                        snapshot.configured_peer_count,
                        snapshot.ready_peer_count,
                        snapshot.degraded_peer_count,
                        snapshot.client_sessions.registered_clients,
                        snapshot.client_sessions.live_stream_clients,
                        snapshot.recent_events.len(),
                    );
                    Ok(())
                }
                OutputFormat::Json => print_json(&snapshot),
            }
        }
        GetCommand::Snapshot(args) => {
            let snapshot = args.fetch_snapshot().await?;
            match args.output {
                OutputFormat::Summary => {
                    print_snapshot_summary(&snapshot);
                    Ok(())
                }
                OutputFormat::Json => print_json(&snapshot),
            }
        }
        GetCommand::Workloads(args) => {
            let snapshot = args.source.fetch_snapshot().await?;
            let workloads = snapshot
                .state
                .desired
                .workloads
                .values()
                .cloned()
                .collect::<Vec<_>>();
            match args.source.output {
                OutputFormat::Summary => {
                    println!("workloads count={}", workloads.len());
                    for workload in workloads {
                        println!(
                            "workload id={} runtime={} artifact={} desired={:?} observed={:?} assigned_node={} restart_policy={:?} requirements={} bindings={}",
                            workload.workload_id,
                            workload.runtime_type,
                            workload.artifact_id,
                            workload.desired_state,
                            workload.observed_state,
                            workload
                                .assigned_node_id
                                .as_ref()
                                .map(ToString::to_string)
                                .unwrap_or_else(|| "-".to_owned()),
                            workload.restart_policy,
                            workload.requirements.len(),
                            workload.resource_bindings.len(),
                        );
                    }
                    Ok(())
                }
                OutputFormat::Json => print_json(&workloads),
            }
        }
        GetCommand::Resources(args) => {
            let snapshot = args.source.fetch_snapshot().await?;
            let resources = snapshot
                .state
                .desired
                .resources
                .values()
                .cloned()
                .collect::<Vec<_>>();
            match args.source.output {
                OutputFormat::Summary => {
                    println!("resources count={}", resources.len());
                    for resource in resources {
                        println!(
                            "resource id={} type={} provider={} ownership={:?} health={:?} availability={:?} lease_state={:?} owner_workload={} source_workload={} source_resource={} executor={}",
                            resource.resource_id,
                            resource.resource_type,
                            resource.provider_id,
                            resource.ownership_mode,
                            resource.health,
                            resource.availability,
                            resource.lease_state,
                            resource
                                .realized_for_workload_id
                                .as_ref()
                                .map(ToString::to_string)
                                .unwrap_or_else(|| "-".to_owned()),
                            resource
                                .source_workload_id
                                .as_ref()
                                .map(ToString::to_string)
                                .unwrap_or_else(|| "-".to_owned()),
                            resource
                                .source_resource_id
                                .as_ref()
                                .map(ToString::to_string)
                                .unwrap_or_else(|| "-".to_owned()),
                            resource
                                .realized_by_executor_id
                                .as_ref()
                                .map(ToString::to_string)
                                .unwrap_or_else(|| "-".to_owned()),
                        );
                    }
                    Ok(())
                }
                OutputFormat::Json => print_json(&resources),
            }
        }
        GetCommand::Providers(args) => {
            let snapshot = args.source.fetch_snapshot().await?;
            let providers = snapshot
                .state
                .desired
                .providers
                .values()
                .cloned()
                .collect::<Vec<_>>();
            match args.source.output {
                OutputFormat::Summary => {
                    println!("providers count={}", providers.len());
                    for provider in providers {
                        println!(
                            "provider id={} node={} resource_types={}",
                            provider.provider_id,
                            provider.node_id,
                            join_display(&provider.resource_types),
                        );
                    }
                    Ok(())
                }
                OutputFormat::Json => print_json(&providers),
            }
        }
        GetCommand::Executors(args) => {
            let snapshot = args.source.fetch_snapshot().await?;
            let executors = snapshot
                .state
                .desired
                .executors
                .values()
                .cloned()
                .collect::<Vec<_>>();
            match args.source.output {
                OutputFormat::Summary => {
                    println!("executors count={}", executors.len());
                    for executor in executors {
                        println!(
                            "executor id={} node={} runtime_types={}",
                            executor.executor_id,
                            executor.node_id,
                            join_display(&executor.runtime_types),
                        );
                    }
                    Ok(())
                }
                OutputFormat::Json => print_json(&executors),
            }
        }
        GetCommand::Leases(args) => {
            let snapshot = args.source.fetch_snapshot().await?;
            let leases = snapshot
                .state
                .desired
                .leases
                .values()
                .cloned()
                .collect::<Vec<_>>();
            match args.source.output {
                OutputFormat::Summary => {
                    println!("leases count={}", leases.len());
                    for lease in leases {
                        println!(
                            "lease resource={} state={:?} holder_node={} holder_workload={}",
                            lease.resource_id,
                            lease.lease_state,
                            lease
                                .holder_node_id
                                .as_ref()
                                .map(ToString::to_string)
                                .unwrap_or_else(|| "-".to_owned()),
                            lease
                                .holder_workload_id
                                .as_ref()
                                .map(ToString::to_string)
                                .unwrap_or_else(|| "-".to_owned()),
                        );
                    }
                    Ok(())
                }
                OutputFormat::Json => print_json(&leases),
            }
        }
    }
}

async fn run_watch(command: WatchCommand) -> Result<(), String> {
    match command {
        WatchCommand::State(args) => {
            let mut stream = ControlPlaneEventStream::connect_at_with_local_address(
                &args.stream_socket,
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
                for event in events {
                    match args.output {
                        OutputFormat::Summary => print_event_summary(&event),
                        OutputFormat::Json => print_json(&event)?,
                    }
                }
            }
            Ok(())
        }
    }
}

async fn run_apply(command: ApplyCommand) -> Result<(), String> {
    match command {
        ApplyCommand::Artifact(args) => {
            let client = args.local.client()?;
            let snapshot = client
                .fetch_state_snapshot()
                .await
                .map_err(|error| error.to_string())?;
            let mut builder = ArtifactRecord::builder(args.artifact_id.clone());
            if let Some(content_type) = args.content_type {
                builder = builder.content_type(content_type);
            }
            if let Some(size_bytes) = args.size_bytes {
                builder = builder.size_bytes(size_bytes);
            }
            let artifact = builder.build();
            client
                .apply_mutations(MutationBatch {
                    base_revision: snapshot.state.desired.revision,
                    mutations: vec![DesiredStateMutation::PutArtifact(artifact.clone())],
                })
                .await
                .map_err(|error| error.to_string())?;
            match args.local.output {
                OutputFormat::Summary => {
                    println!("apply artifact accepted id={}", artifact.artifact_id);
                    Ok(())
                }
                OutputFormat::Json => print_json(&artifact),
            }
        }
        ApplyCommand::Workload(args) => {
            let client = args.local.client()?;
            let output = args.local.output;
            let snapshot = client
                .fetch_state_snapshot()
                .await
                .map_err(|error| error.to_string())?;
            let workload = workload_from_apply_args(*args)?;
            client
                .apply_mutations(MutationBatch {
                    base_revision: snapshot.state.desired.revision,
                    mutations: vec![DesiredStateMutation::PutWorkload(workload.clone())],
                })
                .await
                .map_err(|error| error.to_string())?;
            match output {
                OutputFormat::Summary => {
                    println!("apply workload accepted id={}", workload.workload_id);
                    Ok(())
                }
                OutputFormat::Json => print_json(&workload),
            }
        }
    }
}

fn workload_from_apply_args(args: crate::cli::ApplyWorkloadArgs) -> Result<WorkloadRecord, String> {
    if let Some(spec_path) = args.spec.as_ref() {
        reject_inline_workload_spec_conflicts(&args)?;
        let bytes = std::fs::read(spec_path).map_err(|error| {
            format!(
                "failed to read workload spec {}: {error}",
                spec_path.display()
            )
        })?;
        return serde_json::from_slice(&bytes).map_err(|error| {
            format!(
                "failed to parse workload spec {}: {error}",
                spec_path.display()
            )
        });
    }

    let config = workload_config_from_apply_args(&args)?;
    let workload_id = args
        .workload_id
        .ok_or_else(|| "--workload-id is required when --spec is not used".to_owned())?;
    let runtime_type = args
        .runtime_type
        .ok_or_else(|| "--runtime-type is required when --spec is not used".to_owned())?;
    let artifact_id = args
        .artifact_id
        .ok_or_else(|| "--artifact-id is required when --spec is not used".to_owned())?;

    let mut builder = WorkloadRecord::builder(
        workload_id,
        RuntimeType::new(runtime_type),
        ArtifactId::new(artifact_id),
    )
    .desired_state(args.desired_state.into())
    .restart_policy(args.restart_policy.into());
    if let Some(node_id) = args.assigned_node {
        builder = builder.assigned_to(node_id);
    }
    for requirement in args.requirements {
        builder = builder.require_claim(WorkloadRequirement::new(
            requirement.resource_type,
            requirement.count,
        ));
    }
    for binding in args.bindings {
        builder = builder.bind_resource(binding.resource_id, binding.node_id);
    }
    if let Some(config) = config {
        builder = builder.config(config);
    }
    Ok(builder.build())
}

fn reject_inline_workload_spec_conflicts(
    args: &crate::cli::ApplyWorkloadArgs,
) -> Result<(), String> {
    let has_inline_fields = args.workload_id.is_some()
        || args.runtime_type.is_some()
        || args.artifact_id.is_some()
        || args.assigned_node.is_some()
        || !args.requirements.is_empty()
        || !args.bindings.is_empty()
        || args.config_schema.is_some()
        || !args.config_bools.is_empty()
        || !args.config_ints.is_empty()
        || !args.config_uints.is_empty()
        || !args.config_strings.is_empty()
        || !args.config_bytes_hex.is_empty();
    if has_inline_fields {
        return Err(
            "--spec cannot be combined with workload field/config flags; provide either a full spec or inline flags"
                .to_owned(),
        );
    }
    Ok(())
}

fn workload_config_from_apply_args(
    args: &crate::cli::ApplyWorkloadArgs,
) -> Result<Option<WorkloadConfig>, String> {
    let mut payload = BTreeMap::<String, TypedConfigValue>::new();
    for field in args
        .config_bools
        .iter()
        .chain(args.config_ints.iter())
        .chain(args.config_uints.iter())
        .chain(args.config_strings.iter())
        .chain(args.config_bytes_hex.iter())
    {
        if payload
            .insert(field.key.clone(), field.value.clone())
            .is_some()
        {
            return Err(format!("duplicate config key `{}`", field.key));
        }
    }

    match (args.config_schema.as_ref(), payload.is_empty()) {
        (None, true) => Ok(None),
        (Some(schema_id), _) => Ok(Some(WorkloadConfig {
            schema_id: ConfigSchemaId::new(schema_id.clone()),
            payload,
        })),
        (None, false) => {
            Err("--config-schema is required when any --config-* flags are used".to_owned())
        }
    }
}

async fn run_delete(command: DeleteCommand) -> Result<(), String> {
    match command {
        DeleteCommand::Artifact(args) => {
            let client = args.local.client()?;
            let snapshot = client
                .fetch_state_snapshot()
                .await
                .map_err(|error| error.to_string())?;
            let artifact_id = ArtifactId::new(args.artifact_id);
            client
                .apply_mutations(MutationBatch {
                    base_revision: snapshot.state.desired.revision,
                    mutations: vec![DesiredStateMutation::RemoveArtifact(artifact_id.clone())],
                })
                .await
                .map_err(|error| error.to_string())?;
            match args.local.output {
                OutputFormat::Summary => {
                    println!("delete artifact accepted id={artifact_id}");
                    Ok(())
                }
                OutputFormat::Json => print_json(&json!({ "artifact_id": artifact_id })),
            }
        }
        DeleteCommand::Workload(args) => {
            let client = args.local.client()?;
            let snapshot = client
                .fetch_state_snapshot()
                .await
                .map_err(|error| error.to_string())?;
            let workload_id = WorkloadId::new(args.workload_id);
            client
                .apply_mutations(MutationBatch {
                    base_revision: snapshot.state.desired.revision,
                    mutations: vec![DesiredStateMutation::RemoveWorkload(workload_id.clone())],
                })
                .await
                .map_err(|error| error.to_string())?;
            match args.local.output {
                OutputFormat::Summary => {
                    println!("delete workload accepted id={workload_id}");
                    Ok(())
                }
                OutputFormat::Json => print_json(&json!({ "workload_id": workload_id })),
            }
        }
    }
}

async fn run_peers(command: PeerCommand) -> Result<(), String> {
    match command {
        PeerCommand::List(args) => {
            let snapshot = args
                .local
                .client()?
                .query_peer_trust()
                .await
                .map_err(|error| error.to_string())?;
            match args.local.output {
                OutputFormat::Summary => {
                    print_peer_summary(&snapshot);
                    Ok(())
                }
                OutputFormat::Json => print_json(&snapshot),
            }
        }
        PeerCommand::Enroll(args) => {
            let client = args.local.client()?;
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
            println!("peers enroll accepted");
            Ok(())
        }
        PeerCommand::Revoke(args) => {
            args.local
                .client()?
                .revoke_peer(NodeId::new(args.node_id))
                .await
                .map_err(|error| error.to_string())?;
            println!("peers revoke accepted");
            Ok(())
        }
        PeerCommand::ReplaceKey(args) => {
            args.local
                .client()?
                .replace_peer_identity(PeerIdentityUpdate {
                    node_id: NodeId::new(args.node_id),
                    public_key_hex: args.public_key.into(),
                })
                .await
                .map_err(|error| error.to_string())?;
            println!("peers replace-key accepted");
            Ok(())
        }
        PeerCommand::RotateHttpTls(args) => {
            args.client()?
                .rotate_http_tls_identity()
                .await
                .map_err(|error| error.to_string())?;
            println!("peers rotate-http-tls accepted");
            Ok(())
        }
    }
}

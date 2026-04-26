use orion_control_plane::{
    ControlMessage, MetricsExportConfig, NodeObservabilitySnapshot,
    render_communication_metrics_with_config, render_host_metrics,
    render_observability_metrics_with_config,
};
use orion_transport_http::{ControlRoute, HttpResponsePayload};

use crate::{
    cli::{CommunicationView, GetCommand, ListArgs, OutputFormat, StateQueryArgs},
    render::{
        join_display, join_or_dash, print_observability_event_summary, print_snapshot_summary,
        print_structured, render_availability_state, render_health_state, render_lease_state,
        render_observability_event_kind, render_observed_state, render_restart_policy,
    },
};

use super::{
    effective_workloads,
    get_communication::{communication_peer_summaries, print_peer_communication_summary},
    get_communication::{filtered_communication, print_communication_summary},
};

pub(super) async fn run(command: GetCommand) -> Result<(), String> {
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
                OutputFormat::Json
                | OutputFormat::Yaml
                | OutputFormat::Toml
                | OutputFormat::Metrics => print_structured(&snapshot, args.output),
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
                OutputFormat::Json
                | OutputFormat::Yaml
                | OutputFormat::Toml
                | OutputFormat::Metrics => print_structured(&snapshot, args.output),
            },
            other => Err(format!("expected readiness response, got {other:?}")),
        },
        GetCommand::Host(args) => {
            let snapshot = fetch_observability_snapshot(&args).await?;
            match args.output {
                OutputFormat::Summary => {
                    print_host_summary(&snapshot);
                    Ok(())
                }
                OutputFormat::Json | OutputFormat::Yaml | OutputFormat::Toml => {
                    print_structured(&snapshot.host, args.output)
                }
                OutputFormat::Metrics => {
                    print!("{}", render_host_metrics(&snapshot.node_id, &snapshot.host));
                    Ok(())
                }
            }
        }
        GetCommand::Observability(args) => {
            let snapshot = fetch_observability_snapshot(&args).await?;
            match args.output {
                OutputFormat::Summary => print_observability_summary(&snapshot),
                OutputFormat::Json | OutputFormat::Yaml | OutputFormat::Toml => {
                    print_structured(&snapshot, args.output)
                }
                OutputFormat::Metrics => {
                    let config = metrics_export_config(&args)?;
                    print!(
                        "{}",
                        render_observability_metrics_with_config(&snapshot, &config)
                    );
                    Ok(())
                }
            }
        }
        GetCommand::Communication(args) => {
            let snapshot = fetch_observability_snapshot(&args.source).await?;
            let communication = filtered_communication(&snapshot, &args)?;
            match args.source.output {
                OutputFormat::Summary => {
                    match args.view {
                        CommunicationView::Endpoints => print_communication_summary(&communication),
                        CommunicationView::Peers => print_peer_communication_summary(
                            &communication_peer_summaries(&communication, args.sort, args.limit),
                        ),
                    }
                    Ok(())
                }
                OutputFormat::Json | OutputFormat::Yaml | OutputFormat::Toml => match args.view {
                    CommunicationView::Endpoints => {
                        print_structured(&communication, args.source.output)
                    }
                    CommunicationView::Peers => print_structured(
                        &communication_peer_summaries(&communication, args.sort, args.limit),
                        args.source.output,
                    ),
                },
                OutputFormat::Metrics => {
                    let config = metrics_export_config(&args.source)?;
                    print!(
                        "{}",
                        render_communication_metrics_with_config(
                            &snapshot.node_id,
                            &communication,
                            &config
                        )
                    );
                    Ok(())
                }
            }
        }
        GetCommand::Snapshot(args) => {
            let snapshot = args.fetch_snapshot().await?;
            match args.output {
                OutputFormat::Summary => {
                    print_snapshot_summary(&snapshot);
                    Ok(())
                }
                OutputFormat::Json
                | OutputFormat::Yaml
                | OutputFormat::Toml
                | OutputFormat::Metrics => print_structured(&snapshot, args.output),
            }
        }
        GetCommand::Node(args) => {
            let snapshot = args.source.fetch_snapshot().await?;
            let node = snapshot
                .state
                .desired
                .nodes
                .get(&orion_core::NodeId::new(args.id.clone()))
                .cloned()
                .ok_or_else(|| format!("node `{}` not found", args.id))?;
            match args.source.output {
                OutputFormat::Summary => {
                    println!(
                        "node id={} health={} schedulable={} labels={}",
                        node.node_id,
                        render_health_state(node.health),
                        node.schedulable,
                        join_or_dash(&node.labels),
                    );
                    Ok(())
                }
                OutputFormat::Json
                | OutputFormat::Yaml
                | OutputFormat::Toml
                | OutputFormat::Metrics => print_structured(&node, args.source.output),
            }
        }
        GetCommand::Artifact(args) => {
            let snapshot = args.source.fetch_snapshot().await?;
            let artifact = snapshot
                .state
                .desired
                .artifacts
                .get(&orion_core::ArtifactId::new(args.id.clone()))
                .cloned()
                .ok_or_else(|| format!("artifact `{}` not found", args.id))?;
            match args.source.output {
                OutputFormat::Summary => {
                    println!(
                        "artifact id={} labels={} content_type={} size_bytes={}",
                        artifact.artifact_id,
                        join_or_dash(&artifact.labels),
                        artifact.content_type.as_deref().unwrap_or("-"),
                        artifact
                            .size_bytes
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "-".to_owned()),
                    );
                    Ok(())
                }
                OutputFormat::Json
                | OutputFormat::Yaml
                | OutputFormat::Toml
                | OutputFormat::Metrics => print_structured(&artifact, args.source.output),
            }
        }
        GetCommand::Workload(args) => {
            let snapshot = args.source.fetch_snapshot().await?;
            let workload = effective_workloads(&snapshot)
                .into_iter()
                .find(|workload| workload.workload_id.as_str() == args.id)
                .ok_or_else(|| format!("workload `{}` not found", args.id))?;
            match args.source.output {
                OutputFormat::Summary => {
                    println!(
                        "workload id={} runtime={} artifact={} desired={} observed={} assigned_node={} restart_policy={} requirements={} bindings={}",
                        workload.workload_id,
                        workload.runtime_type,
                        workload.artifact_id,
                        crate::render::render_desired_state(workload.desired_state),
                        render_observed_state(workload.observed_state),
                        workload
                            .assigned_node_id
                            .as_ref()
                            .map(ToString::to_string)
                            .unwrap_or_else(|| "-".to_owned()),
                        render_restart_policy(workload.restart_policy),
                        workload.requirements.len(),
                        workload.resource_bindings.len(),
                    );
                    Ok(())
                }
                OutputFormat::Json
                | OutputFormat::Yaml
                | OutputFormat::Toml
                | OutputFormat::Metrics => print_structured(&workload, args.source.output),
            }
        }
        GetCommand::Nodes(args) => {
            let snapshot = args.source.fetch_snapshot().await?;
            let nodes = snapshot
                .state
                .desired
                .nodes
                .values()
                .filter(|node| match_list_filters(&args, None, &node.labels))
                .cloned()
                .collect::<Vec<_>>();
            match args.source.output {
                OutputFormat::Summary => {
                    println!("nodes count={}", nodes.len());
                    for node in nodes {
                        println!(
                            "node id={} health={} schedulable={} labels={}",
                            node.node_id,
                            render_health_state(node.health),
                            node.schedulable,
                            join_or_dash(&node.labels),
                        );
                    }
                    Ok(())
                }
                OutputFormat::Json
                | OutputFormat::Yaml
                | OutputFormat::Toml
                | OutputFormat::Metrics => print_structured(&nodes, args.source.output),
            }
        }
        GetCommand::Artifacts(args) => {
            let snapshot = args.source.fetch_snapshot().await?;
            let artifacts = snapshot
                .state
                .desired
                .artifacts
                .values()
                .filter(|artifact| {
                    match_list_filters(&args, None, &artifact.labels)
                        && args
                            .artifact
                            .as_ref()
                            .map(|value| artifact.artifact_id.as_str().contains(value))
                            .unwrap_or(true)
                })
                .cloned()
                .collect::<Vec<_>>();
            match args.source.output {
                OutputFormat::Summary => {
                    println!("artifacts count={}", artifacts.len());
                    for artifact in artifacts {
                        println!(
                            "artifact id={} labels={} content_type={} size_bytes={}",
                            artifact.artifact_id,
                            join_or_dash(&artifact.labels),
                            artifact.content_type.as_deref().unwrap_or("-"),
                            artifact
                                .size_bytes
                                .map(|value| value.to_string())
                                .unwrap_or_else(|| "-".to_owned()),
                        );
                    }
                    Ok(())
                }
                OutputFormat::Json
                | OutputFormat::Yaml
                | OutputFormat::Toml
                | OutputFormat::Metrics => print_structured(&artifacts, args.source.output),
            }
        }
        GetCommand::Workloads(args) => {
            let snapshot = args.source.fetch_snapshot().await?;
            let workloads = effective_workloads(&snapshot);
            let workloads = workloads
                .into_iter()
                .filter(|workload| {
                    match_list_filters(
                        &args,
                        workload.assigned_node_id.as_ref().map(ToString::to_string),
                        &[],
                    ) && args
                        .runtime
                        .as_ref()
                        .map(|value| workload.runtime_type.as_str().contains(value))
                        .unwrap_or(true)
                        && args
                            .artifact
                            .as_ref()
                            .map(|value| workload.artifact_id.as_str().contains(value))
                            .unwrap_or(true)
                })
                .collect::<Vec<_>>();
            match args.source.output {
                OutputFormat::Summary => {
                    println!("workloads count={}", workloads.len());
                    for workload in workloads {
                        println!(
                            "workload id={} runtime={} artifact={} desired={} observed={} assigned_node={} restart_policy={} requirements={} bindings={}",
                            workload.workload_id,
                            workload.runtime_type,
                            workload.artifact_id,
                            crate::render::render_desired_state(workload.desired_state),
                            render_observed_state(workload.observed_state),
                            workload
                                .assigned_node_id
                                .as_ref()
                                .map(ToString::to_string)
                                .unwrap_or_else(|| "-".to_owned()),
                            render_restart_policy(workload.restart_policy),
                            workload.requirements.len(),
                            workload.resource_bindings.len(),
                        );
                    }
                    Ok(())
                }
                OutputFormat::Json
                | OutputFormat::Yaml
                | OutputFormat::Toml
                | OutputFormat::Metrics => print_structured(&workloads, args.source.output),
            }
        }
        GetCommand::Resources(args) => {
            let snapshot = args.source.fetch_snapshot().await?;
            let resources = snapshot
                .state
                .observed
                .resources
                .values()
                .filter(|resource| {
                    let provider_node = snapshot
                        .state
                        .desired
                        .providers
                        .get(&resource.provider_id)
                        .map(|provider| provider.node_id.to_string());
                    match_list_filters(&args, provider_node, &resource.labels)
                        && args
                            .resource_type
                            .as_ref()
                            .map(|value| resource.resource_type.as_str().contains(value))
                            .unwrap_or(true)
                        && args
                            .provider
                            .as_ref()
                            .map(|value| resource.provider_id.as_str().contains(value))
                            .unwrap_or(true)
                })
                .cloned()
                .collect::<Vec<_>>();
            match args.source.output {
                OutputFormat::Summary => {
                    println!("resources count={}", resources.len());
                    for resource in resources {
                        println!(
                            "resource id={} type={} provider={} ownership={:?} health={} availability={} lease_state={} owner_workload={} source_workload={} source_resource={} executor={}",
                            resource.resource_id,
                            resource.resource_type,
                            resource.provider_id,
                            resource.ownership_mode,
                            render_health_state(resource.health),
                            render_availability_state(resource.availability),
                            render_lease_state(resource.lease_state),
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
                OutputFormat::Json
                | OutputFormat::Yaml
                | OutputFormat::Toml
                | OutputFormat::Metrics => print_structured(&resources, args.source.output),
            }
        }
        GetCommand::Providers(args) => {
            let snapshot = args.source.fetch_snapshot().await?;
            let providers = snapshot
                .state
                .desired
                .providers
                .values()
                .filter(|provider| {
                    match_list_filters(&args, Some(provider.node_id.to_string()), &[])
                        && args
                            .resource_type
                            .as_ref()
                            .map(|value| {
                                provider
                                    .resource_types
                                    .iter()
                                    .any(|resource_type| resource_type.as_str().contains(value))
                            })
                            .unwrap_or(true)
                })
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
                OutputFormat::Json
                | OutputFormat::Yaml
                | OutputFormat::Toml
                | OutputFormat::Metrics => print_structured(&providers, args.source.output),
            }
        }
        GetCommand::Executors(args) => {
            let snapshot = args.source.fetch_snapshot().await?;
            let executors = snapshot
                .state
                .desired
                .executors
                .values()
                .filter(|executor| {
                    match_list_filters(&args, Some(executor.node_id.to_string()), &[])
                        && args
                            .runtime
                            .as_ref()
                            .map(|value| {
                                executor
                                    .runtime_types
                                    .iter()
                                    .any(|runtime_type| runtime_type.as_str().contains(value))
                            })
                            .unwrap_or(true)
                })
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
                OutputFormat::Json
                | OutputFormat::Yaml
                | OutputFormat::Toml
                | OutputFormat::Metrics => print_structured(&executors, args.source.output),
            }
        }
        GetCommand::Leases(args) => {
            let snapshot = args.source.fetch_snapshot().await?;
            let leases = snapshot
                .state
                .desired
                .leases
                .values()
                .filter(|lease| {
                    match_list_filters(
                        &args,
                        lease.holder_node_id.as_ref().map(ToString::to_string),
                        &[],
                    )
                })
                .cloned()
                .collect::<Vec<_>>();
            match args.source.output {
                OutputFormat::Summary => {
                    println!("leases count={}", leases.len());
                    for lease in leases {
                        println!(
                            "lease resource={} state={} holder_node={} holder_workload={}",
                            lease.resource_id,
                            render_lease_state(lease.lease_state),
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
                OutputFormat::Json
                | OutputFormat::Yaml
                | OutputFormat::Toml
                | OutputFormat::Metrics => print_structured(&leases, args.source.output),
            }
        }
        GetCommand::Events(args) => {
            let snapshot = fetch_observability_snapshot(&args.source).await?;
            let events = snapshot
                .recent_events
                .iter()
                .rev()
                .filter(|event| {
                    args.kind
                        .as_ref()
                        .map(|value| {
                            render_observability_event_kind(&event.kind)
                                .contains(&value.to_lowercase())
                        })
                        .unwrap_or(true)
                        && args
                            .subject
                            .as_ref()
                            .map(|value| {
                                event
                                    .subject
                                    .as_ref()
                                    .map(|subject| subject.contains(value))
                                    .unwrap_or(false)
                            })
                            .unwrap_or(true)
                        && (!args.failed || !event.success)
                })
                .cloned()
                .collect::<Vec<_>>();
            match args.source.output {
                OutputFormat::Summary => {
                    println!("events count={}", events.len());
                    for event in &events {
                        print_observability_event_summary(event);
                    }
                    Ok(())
                }
                OutputFormat::Json
                | OutputFormat::Yaml
                | OutputFormat::Toml
                | OutputFormat::Metrics => print_structured(&events, args.source.output),
            }
        }
    }
}

fn metrics_export_config(args: &StateQueryArgs) -> Result<MetricsExportConfig, String> {
    let mut config = MetricsExportConfig::try_from_env()?;
    if let Some(value) = args.metrics_max_communication_endpoints {
        config.max_communication_endpoints = value;
    }
    if let Some(value) = args.metrics_max_label_value_len {
        config.max_label_value_len = value.max(24);
    }
    Ok(config)
}

fn match_list_filters(args: &ListArgs, node: Option<String>, labels: &[String]) -> bool {
    args.node
        .as_ref()
        .map(|value| node.as_deref() == Some(value.as_str()))
        .unwrap_or(true)
        && args
            .label
            .as_ref()
            .map(|value| labels.iter().any(|label| label.contains(value)))
            .unwrap_or(true)
}

fn print_host_summary(snapshot: &NodeObservabilitySnapshot) {
    let host = &snapshot.host;
    println!(
        "host node={} hostname={} os={} os_version={} kernel={} arch={} uptime_seconds={} load_1_milli={} load_5_milli={} load_15_milli={} memory_total_bytes={} memory_available_bytes={} swap_total_bytes={} swap_free_bytes={} process_id={} process_rss_bytes={}",
        snapshot.node_id,
        host.hostname.as_deref().unwrap_or("-"),
        host.os_name,
        host.os_version.as_deref().unwrap_or("-"),
        host.kernel_version.as_deref().unwrap_or("-"),
        host.architecture,
        option_u64(host.uptime_seconds),
        option_u64(host.load_1_milli),
        option_u64(host.load_5_milli),
        option_u64(host.load_15_milli),
        option_u64(host.memory_total_bytes),
        option_u64(host.memory_available_bytes),
        option_u64(host.swap_total_bytes),
        option_u64(host.swap_free_bytes),
        host.process_id,
        option_u64(host.process_rss_bytes),
    );
}

fn option_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_owned())
}

fn print_observability_summary(snapshot: &NodeObservabilitySnapshot) -> Result<(), String> {
    println!(
        "observability node={} desired_rev={} observed_rev={} applied_rev={} maintenance_mode={} peer_sync_paused={} remote_blocked={} replay_success={} replay_failures={} sync_success={} sync_failures={} reconcile_success={} reconcile_failures={} mutation_success={} mutation_failures={} peers_configured={} peers_ready={} peers_degraded={} clients_registered={} clients_live={} communication_endpoints={} recent_events={}",
        snapshot.node_id,
        snapshot.desired_revision,
        snapshot.observed_revision,
        snapshot.applied_revision,
        snapshot.maintenance.mode,
        snapshot.peer_sync_paused,
        snapshot.remote_desired_state_blocked,
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
        snapshot.communication.len(),
        snapshot.recent_events.len(),
    );
    Ok(())
}

pub(crate) async fn fetch_observability_snapshot(
    args: &StateQueryArgs,
) -> Result<NodeObservabilitySnapshot, String> {
    if let Some(http_target) = args.http_target()? {
        match http_target
            .send_control(ControlMessage::QueryObservability)
            .await?
        {
            HttpResponsePayload::Observability(snapshot) => Ok(*snapshot),
            other => Err(format!("expected observability response, got {other:?}")),
        }
    } else {
        args.local_client()?
            .query_observability()
            .await
            .map_err(|error| error.to_string())
    }
}

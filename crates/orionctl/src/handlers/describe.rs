use orion::OrionConfigDecode;
use orion_control_plane::{
    ArtifactRecord, MaintenanceMode, NodeRecord, ResourceRecord, StateSnapshot,
    WorkloadObservedState, WorkloadRecord,
};
use orion_core::{ArtifactId, NodeId, ResourceId, WorkloadId};

use crate::{
    cli::{DescribeCommand, OutputFormat},
    render::{
        join_display, join_or_dash, print_structured, render_availability_state,
        render_desired_state, render_health_state, render_lease_state, render_observed_state,
        render_restart_policy,
    },
};

use super::{effective_workloads, get::fetch_observability_snapshot};

#[derive(serde::Serialize)]
struct WorkloadDescribeReport {
    workload: WorkloadRecord,
    blockers: Vec<String>,
}

#[derive(serde::Serialize)]
struct NodeDescribeReport {
    node: NodeRecord,
    observed: Option<NodeRecord>,
    maintenance_mode: MaintenanceMode,
    peer_sync_paused: bool,
    remote_desired_state_blocked: bool,
    executors: Vec<String>,
    providers: Vec<String>,
    workloads: Vec<String>,
    resources: Vec<String>,
}

#[derive(serde::Serialize)]
struct ArtifactDescribeReport {
    artifact: ArtifactRecord,
    referenced_by_workloads: Vec<String>,
}

#[derive(serde::Serialize)]
struct ResourceDescribeReport {
    resource: ResourceRecord,
    lease_holder_node: Option<String>,
    lease_holder_workload: Option<String>,
}

#[derive(OrionConfigDecode)]
struct GraphWorkloadScalarConfig {
    #[orion(path = "graph.kind")]
    graph_kind: String,
    #[orion(path = "binding.count")]
    binding_count: Option<u64>,
}

#[derive(OrionConfigDecode)]
#[orion(tag = "graph.kind")]
enum GraphWorkloadGraphConfig {
    #[orion(tag = "artifact")]
    Artifact {
        #[orion(path = "graph.artifact_id")]
        artifact_id: Option<String>,
    },
    #[orion(tag = "resource")]
    Resource {
        #[orion(path = "graph.resource_id")]
        resource_id: String,
    },
    #[orion(tag = "inline")]
    Inline {
        #[orion(path = "graph.inline")]
        inline: String,
    },
}

#[derive(OrionConfigDecode)]
struct GraphPluginConfig {
    name: String,
    version: Option<String>,
}

#[derive(OrionConfigDecode)]
struct GraphWorkloadPluginListConfig {
    #[orion(prefix = "plugin")]
    plugins: Vec<GraphPluginConfig>,
}

pub(super) async fn run(command: DescribeCommand) -> Result<(), String> {
    match command {
        DescribeCommand::Workload(args) => {
            let snapshot = args.source.fetch_snapshot().await?;
            let observability = fetch_observability_snapshot(&args.source).await?;
            let workload_id = WorkloadId::new(args.id.clone());
            let mut workload = snapshot
                .state
                .desired
                .workloads
                .get(&workload_id)
                .cloned()
                .ok_or_else(|| format!("workload `{}` not found", args.id))?;
            if let Some(observed) = snapshot.state.observed.workloads.get(&workload_id) {
                workload.observed_state = observed.observed_state;
                workload.resource_bindings = observed.resource_bindings.clone();
                if observed.assigned_node_id.is_some() {
                    workload.assigned_node_id = observed.assigned_node_id.clone();
                }
            }
            let blockers = compute_workload_blockers(&snapshot, &observability, &workload);
            match args.source.output {
                OutputFormat::Summary => {
                    print_workload_describe_summary(&workload, &blockers);
                    Ok(())
                }
                OutputFormat::Json | OutputFormat::Yaml | OutputFormat::Toml => print_structured(
                    &WorkloadDescribeReport { workload, blockers },
                    args.source.output,
                ),
            }
        }
        DescribeCommand::Node(args) => {
            let snapshot = args.source.fetch_snapshot().await?;
            let observability = fetch_observability_snapshot(&args.source).await?;
            let node_id = NodeId::new(args.id.clone());
            let node = snapshot
                .state
                .desired
                .nodes
                .get(&node_id)
                .cloned()
                .ok_or_else(|| format!("node `{}` not found", args.id))?;
            let observed = snapshot.state.observed.nodes.get(&node_id).cloned();
            let executors = snapshot
                .state
                .desired
                .executors
                .values()
                .filter(|executor| executor.node_id == node_id)
                .map(|executor| executor.executor_id.to_string())
                .collect::<Vec<_>>();
            let providers = snapshot
                .state
                .desired
                .providers
                .values()
                .filter(|provider| provider.node_id == node_id)
                .map(|provider| provider.provider_id.to_string())
                .collect::<Vec<_>>();
            let workloads = effective_workloads(&snapshot)
                .into_iter()
                .filter(|workload| workload.assigned_node_id.as_ref() == Some(&node_id))
                .map(|workload| workload.workload_id.to_string())
                .collect::<Vec<_>>();
            let resources = snapshot
                .state
                .observed
                .resources
                .values()
                .filter(|resource| {
                    snapshot
                        .state
                        .desired
                        .providers
                        .get(&resource.provider_id)
                        .map(|provider| provider.node_id == node_id)
                        .unwrap_or(false)
                })
                .map(|resource| resource.resource_id.to_string())
                .collect::<Vec<_>>();
            let report = NodeDescribeReport {
                node,
                observed,
                maintenance_mode: if observability.node_id == node_id {
                    observability.maintenance.mode
                } else {
                    MaintenanceMode::Normal
                },
                peer_sync_paused: observability.node_id == node_id
                    && observability.peer_sync_paused,
                remote_desired_state_blocked: observability.node_id == node_id
                    && observability.remote_desired_state_blocked,
                executors,
                providers,
                workloads,
                resources,
            };
            match args.source.output {
                OutputFormat::Summary => {
                    print_node_describe_summary(&report);
                    Ok(())
                }
                OutputFormat::Json | OutputFormat::Yaml | OutputFormat::Toml => {
                    print_structured(&report, args.source.output)
                }
            }
        }
        DescribeCommand::Artifact(args) => {
            let snapshot = args.source.fetch_snapshot().await?;
            let artifact_id = ArtifactId::new(args.id.clone());
            let artifact = snapshot
                .state
                .desired
                .artifacts
                .get(&artifact_id)
                .cloned()
                .ok_or_else(|| format!("artifact `{}` not found", args.id))?;
            let referenced_by_workloads = effective_workloads(&snapshot)
                .into_iter()
                .filter(|workload| workload.artifact_id == artifact_id)
                .map(|workload| workload.workload_id.to_string())
                .collect::<Vec<_>>();
            let report = ArtifactDescribeReport {
                artifact,
                referenced_by_workloads,
            };
            match args.source.output {
                OutputFormat::Summary => {
                    print_artifact_describe_summary(&report);
                    Ok(())
                }
                OutputFormat::Json | OutputFormat::Yaml | OutputFormat::Toml => {
                    print_structured(&report, args.source.output)
                }
            }
        }
        DescribeCommand::Resource(args) => {
            let snapshot = args.source.fetch_snapshot().await?;
            let resource_id = ResourceId::new(args.id.clone());
            let resource = snapshot
                .state
                .observed
                .resources
                .get(&resource_id)
                .cloned()
                .or_else(|| snapshot.state.desired.resources.get(&resource_id).cloned())
                .ok_or_else(|| format!("resource `{}` not found", args.id))?;
            let lease = snapshot.state.desired.leases.get(&resource_id);
            let report = ResourceDescribeReport {
                resource,
                lease_holder_node: lease
                    .and_then(|lease| lease.holder_node_id.as_ref().map(ToString::to_string)),
                lease_holder_workload: lease
                    .and_then(|lease| lease.holder_workload_id.as_ref().map(ToString::to_string)),
            };
            match args.source.output {
                OutputFormat::Summary => {
                    print_resource_describe_summary(&report);
                    Ok(())
                }
                OutputFormat::Json | OutputFormat::Yaml | OutputFormat::Toml => {
                    print_structured(&report, args.source.output)
                }
            }
        }
    }
}

pub(crate) fn compute_workload_blockers(
    snapshot: &StateSnapshot,
    observability: &orion_control_plane::NodeObservabilitySnapshot,
    workload: &WorkloadRecord,
) -> Vec<String> {
    let mut blockers = Vec::new();

    if workload.desired_state != orion_control_plane::DesiredState::Running
        || workload.observed_state == WorkloadObservedState::Running
    {
        return blockers;
    }

    if !snapshot
        .state
        .desired
        .artifacts
        .contains_key(&workload.artifact_id)
    {
        blockers.push(format!(
            "artifact {} is missing from desired state",
            workload.artifact_id
        ));
    }

    let Some(node_id) = workload.assigned_node_id.as_ref() else {
        blockers.push("no assigned node".to_owned());
        return blockers;
    };

    if !snapshot.state.desired.nodes.contains_key(node_id) {
        blockers.push(format!(
            "assigned node {} is not present in desired state",
            node_id
        ));
        return blockers;
    }

    if snapshot
        .state
        .desired
        .nodes
        .get(node_id)
        .map(|node| !node.schedulable)
        .unwrap_or(false)
    {
        blockers.push(format!("assigned node {} is unschedulable", node_id));
    }

    if observability.node_id == *node_id
        && observability.maintenance.mode != MaintenanceMode::Normal
    {
        blockers.push(format!(
            "assigned node {} is in {} mode",
            node_id, observability.maintenance.mode
        ));
        if !observability
            .maintenance
            .allow_runtime_types
            .contains(&workload.runtime_type)
            && !observability
                .maintenance
                .allow_workload_ids
                .contains(&workload.workload_id)
        {
            blockers.push(format!(
                "runtime {} is not allowlisted during maintenance",
                workload.runtime_type
            ));
        }
    }

    let runtime_available = snapshot.state.desired.executors.values().any(|executor| {
        executor.node_id == *node_id && executor.runtime_types.contains(&workload.runtime_type)
    });
    if !runtime_available {
        blockers.push(format!(
            "runtime {} is not registered on assigned node {}",
            workload.runtime_type, node_id
        ));
    }

    for requirement in &workload.requirements {
        let available_count = snapshot
            .state
            .observed
            .resources
            .values()
            .filter(|resource| {
                resource.resource_type == requirement.resource_type
                    && resource.availability == orion_control_plane::AvailabilityState::Available
                    && snapshot
                        .state
                        .desired
                        .providers
                        .get(&resource.provider_id)
                        .map(|provider| provider.node_id == *node_id)
                        .unwrap_or(false)
            })
            .count() as u32;
        if available_count < requirement.count {
            blockers.push(format!(
                "required resource type {} count={} is not available on {} (available={})",
                requirement.resource_type, requirement.count, node_id, available_count
            ));
        }
    }

    blockers
}

fn print_workload_describe_summary(workload: &WorkloadRecord, blockers: &[String]) {
    println!("workload: {}", workload.workload_id);
    println!("runtime: {}", workload.runtime_type);
    println!("artifact: {}", workload.artifact_id);
    println!("desired: {}", render_desired_state(workload.desired_state));
    println!(
        "observed: {}",
        render_observed_state(workload.observed_state)
    );
    println!(
        "assigned_node: {}",
        workload
            .assigned_node_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "-".to_owned())
    );
    println!(
        "restart_policy: {}",
        render_restart_policy(workload.restart_policy)
    );
    println!("requirements: {}", workload.requirements.len());
    println!("bindings: {}", workload.resource_bindings.len());
    if let Some(config) = workload.config.as_ref() {
        println!("config_schema: {}", config.schema_id);
        if let Some(summary) = typed_workload_config_summary(workload) {
            println!("config_summary: {summary}");
        }
        if config.payload.is_empty() {
            println!("config_fields: -");
        } else {
            println!("config_fields:");
            for (key, value) in &config.payload {
                println!("  {key}={value:?}");
            }
        }
    } else {
        println!("config_schema: -");
    }
    if blockers.is_empty() {
        println!("blockers: -");
    } else {
        println!("blockers:");
        for blocker in blockers {
            println!("  - {blocker}");
        }
    }
}

fn typed_workload_config_summary(workload: &WorkloadRecord) -> Option<String> {
    let config = workload.config.as_ref()?;
    if config.schema_id.as_str() != "graph.workload.config.v1" {
        return None;
    }

    let scalar = GraphWorkloadScalarConfig::try_from(config).ok()?;
    let graph = GraphWorkloadGraphConfig::try_from(config).ok()?;
    let plugins = GraphWorkloadPluginListConfig::try_from(config)
        .map(|decoded| decoded.plugins)
        .unwrap_or_default();

    let mut parts = vec![format!("graph_kind={}", scalar.graph_kind)];
    match graph {
        GraphWorkloadGraphConfig::Artifact { artifact_id } => {
            let artifact_id = artifact_id.unwrap_or_else(|| workload.artifact_id.to_string());
            parts.push(format!("graph_ref=artifact:{artifact_id}"));
        }
        GraphWorkloadGraphConfig::Resource { resource_id } => {
            parts.push(format!("graph_ref=resource:{resource_id}"));
        }
        GraphWorkloadGraphConfig::Inline { inline } => {
            parts.push(format!("graph_inline_bytes={}", inline.len()));
        }
    }
    if let Some(binding_count) = scalar.binding_count {
        parts.push(format!("binding_count={binding_count}"));
    }
    if !plugins.is_empty() {
        let plugins = plugins
            .into_iter()
            .map(|plugin| match plugin.version {
                Some(version) => format!("{}@{}", plugin.name, version),
                None => plugin.name,
            })
            .collect::<Vec<_>>()
            .join(",");
        parts.push(format!("plugins={plugins}"));
    }

    Some(parts.join(" "))
}

fn print_node_describe_summary(report: &NodeDescribeReport) {
    println!("node: {}", report.node.node_id);
    println!(
        "desired_health: {}",
        render_health_state(report.node.health)
    );
    println!("schedulable: {}", report.node.schedulable);
    println!(
        "observed_health: {}",
        report
            .observed
            .as_ref()
            .map(|node| render_health_state(node.health))
            .unwrap_or("-")
    );
    println!("labels: {}", join_or_dash(&report.node.labels));
    println!("maintenance_mode: {}", report.maintenance_mode);
    println!("peer_sync_paused: {}", report.peer_sync_paused);
    println!(
        "remote_desired_state_blocked: {}",
        report.remote_desired_state_blocked
    );
    println!("executors: {}", join_or_dash(&report.executors));
    println!("providers: {}", join_or_dash(&report.providers));
    println!("workloads: {}", join_or_dash(&report.workloads));
    println!("resources: {}", join_or_dash(&report.resources));
}

fn print_artifact_describe_summary(report: &ArtifactDescribeReport) {
    println!("artifact: {}", report.artifact.artifact_id);
    println!("labels: {}", join_or_dash(&report.artifact.labels));
    println!(
        "content_type: {}",
        report.artifact.content_type.as_deref().unwrap_or("-")
    );
    println!(
        "size_bytes: {}",
        report
            .artifact
            .size_bytes
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_owned())
    );
    println!(
        "referenced_by_workloads: {}",
        join_or_dash(&report.referenced_by_workloads)
    );
}

fn print_resource_describe_summary(report: &ResourceDescribeReport) {
    println!("resource: {}", report.resource.resource_id);
    println!("type: {}", report.resource.resource_type);
    println!("provider: {}", report.resource.provider_id);
    println!("health: {}", render_health_state(report.resource.health));
    println!(
        "availability: {}",
        render_availability_state(report.resource.availability)
    );
    println!(
        "lease_state: {}",
        render_lease_state(report.resource.lease_state)
    );
    println!("ownership_mode: {:?}", report.resource.ownership_mode);
    println!(
        "executor: {}",
        report
            .resource
            .realized_by_executor_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "-".to_owned())
    );
    println!(
        "owner_workload: {}",
        report
            .resource
            .realized_for_workload_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "-".to_owned())
    );
    println!(
        "source_workload: {}",
        report
            .resource
            .source_workload_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "-".to_owned())
    );
    println!(
        "source_resource: {}",
        report
            .resource
            .source_resource_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "-".to_owned())
    );
    println!(
        "lease_holder_node: {}",
        report.lease_holder_node.as_deref().unwrap_or("-")
    );
    println!(
        "lease_holder_workload: {}",
        report.lease_holder_workload.as_deref().unwrap_or("-")
    );
    println!(
        "capabilities: {}",
        join_display(
            &report
                .resource
                .capabilities
                .iter()
                .map(|capability| capability.capability_id.to_string())
                .collect::<Vec<_>>()
        )
    );
    println!("labels: {}", join_or_dash(&report.resource.labels));
    println!("endpoints: {}", join_or_dash(&report.resource.endpoints));
    if let Some(state) = report.resource.state.as_ref() {
        println!("state_observed_at_ms: {}", state.observed_at_ms);
    } else {
        println!("state_observed_at_ms: -");
    }
}

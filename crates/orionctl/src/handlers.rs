mod action;
mod apply;
mod delete;
mod describe;
mod discovered;
mod get;
mod get_communication;
mod get_memory;
mod get_status;
mod operators;
mod peers;
mod watch;

pub(crate) use action::{ActionCommand, ActionListArgs};
pub(crate) use discovered::PeerRemoveArgs;
pub(crate) use operators::OperatorCommand;

use clap::Parser;
use orion_control_plane::{NodeRecord, WorkloadRecord};
use std::collections::BTreeMap;

use crate::{
    cli::{Cli, Command},
    maintenance::run_maintenance,
};

fn effective_workloads(snapshot: &orion_control_plane::StateSnapshot) -> Vec<WorkloadRecord> {
    let observed_by_id: BTreeMap<_, _> = snapshot.state.observed.workloads.iter().collect();

    snapshot
        .state
        .desired
        .workloads
        .values()
        .cloned()
        .map(|mut workload| {
            if let Some(observed) = observed_by_id.get(&workload.workload_id) {
                workload.observed_state = observed.observed_state;
                workload.resource_bindings = observed.resource_bindings.clone();
                if observed.assigned_node_id.is_some() {
                    workload.assigned_node_id = observed.assigned_node_id.clone();
                }
            }
            workload
        })
        .collect()
}

/// Desired node records merged with observed ones: nodes only present in observed state (every
/// node publishes its own record with clock and host facts) are listed too, and the observed clock
/// and host facts replace the desired record's.
fn effective_nodes(snapshot: &orion_control_plane::StateSnapshot) -> Vec<NodeRecord> {
    let mut nodes = snapshot.state.desired.nodes.clone();
    for (node_id, observed) in &snapshot.state.observed.nodes {
        nodes
            .entry(node_id.clone())
            .and_modify(|node| {
                node.clock = observed.clock.clone();
                node.host = observed.host.clone();
            })
            .or_insert_with(|| observed.clone());
    }
    nodes.into_values().collect()
}

pub(crate) async fn run() -> Result<(), String> {
    let cli = Cli::parse();
    match cli.command {
        Command::Get { command } => get::run(command).await,
        Command::Describe { command } => describe::run(command).await,
        Command::Watch { command } => watch::run(command).await,
        Command::Apply { command } => apply::run(*command).await,
        Command::Delete { command } => delete::run(command).await,
        Command::Peers { command } => peers::run(command).await,
        Command::Operators { command } => operators::run(command).await,
        Command::Maintenance { command } => run_maintenance(command).await,
        Command::Action { command } => action::run(command).await,
    }
}

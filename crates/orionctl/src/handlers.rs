mod apply;
mod delete;
mod describe;
mod get;
mod peers;
mod watch;

use clap::Parser;
use orion_control_plane::WorkloadRecord;
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

pub(crate) async fn run() -> Result<(), String> {
    let cli = Cli::parse();
    match cli.command {
        Command::Get { command } => get::run(command).await,
        Command::Describe { command } => describe::run(command).await,
        Command::Watch { command } => watch::run(command).await,
        Command::Apply { command } => apply::run(*command).await,
        Command::Delete { command } => delete::run(command).await,
        Command::Peers { command } => peers::run(command).await,
        Command::Maintenance { command } => run_maintenance(command).await,
    }
}

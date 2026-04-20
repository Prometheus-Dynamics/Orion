use orion_control_plane::{MaintenanceAction, MaintenanceCommand as ControlMaintenanceCommand};

use crate::{
    cli::{LocalControlArgs, MaintenanceCommand as CliMaintenanceCommand, OutputFormat},
    render::{print_maintenance_summary, print_structured},
};

pub(crate) async fn run_maintenance(command: CliMaintenanceCommand) -> Result<(), String> {
    match command {
        CliMaintenanceCommand::Status(args) => {
            let status = args
                .client()?
                .query_maintenance()
                .await
                .map_err(|error| error.to_string())?;
            match args.output {
                OutputFormat::Summary => {
                    print_maintenance_summary(&status);
                    Ok(())
                }
                OutputFormat::Json | OutputFormat::Yaml | OutputFormat::Toml => {
                    print_structured(&status, args.output)
                }
            }
        }
        CliMaintenanceCommand::Cordon(args) => {
            update_maintenance(
                args,
                ControlMaintenanceCommand {
                    action: MaintenanceAction::Cordon,
                    allow_runtime_types: Vec::new(),
                    allow_workload_ids: Vec::new(),
                },
            )
            .await
        }
        CliMaintenanceCommand::Drain(args) => {
            update_maintenance(
                args,
                ControlMaintenanceCommand {
                    action: MaintenanceAction::Drain,
                    allow_runtime_types: Vec::new(),
                    allow_workload_ids: Vec::new(),
                },
            )
            .await
        }
        CliMaintenanceCommand::Enter(args) => {
            let allow_runtime_types = args.runtime_types();
            let allow_workload_ids = args.workload_ids();
            update_maintenance(
                args.local,
                ControlMaintenanceCommand {
                    action: MaintenanceAction::Enter,
                    allow_runtime_types,
                    allow_workload_ids,
                },
            )
            .await
        }
        CliMaintenanceCommand::Isolate(args) => {
            let allow_runtime_types = args.runtime_types();
            let allow_workload_ids = args.workload_ids();
            update_maintenance(
                args.local,
                ControlMaintenanceCommand {
                    action: MaintenanceAction::Isolate,
                    allow_runtime_types,
                    allow_workload_ids,
                },
            )
            .await
        }
        CliMaintenanceCommand::Exit(args) => {
            update_maintenance(
                args,
                ControlMaintenanceCommand {
                    action: MaintenanceAction::Exit,
                    allow_runtime_types: Vec::new(),
                    allow_workload_ids: Vec::new(),
                },
            )
            .await
        }
    }
}

async fn update_maintenance(
    args: LocalControlArgs,
    command: ControlMaintenanceCommand,
) -> Result<(), String> {
    let status = args
        .client()?
        .update_maintenance(command)
        .await
        .map_err(|error| error.to_string())?;
    match args.output {
        OutputFormat::Summary => {
            print_maintenance_summary(&status);
            Ok(())
        }
        OutputFormat::Json | OutputFormat::Yaml | OutputFormat::Toml => {
            print_structured(&status, args.output)
        }
    }
}

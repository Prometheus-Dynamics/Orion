use serde::Serialize;
use serde_json::json;

use orion_control_plane::{DesiredStateMutation, MutationBatch};
use orion_core::{ArtifactId, WorkloadId};

use crate::{
    cli::{DeleteCommand, OutputFormat},
    render::print_structured,
};

#[derive(Serialize)]
struct DeleteDryRunReport {
    accepted: bool,
    action: String,
    id: String,
    reasons: Vec<String>,
}

pub(super) async fn run(command: DeleteCommand) -> Result<(), String> {
    match command {
        DeleteCommand::Artifact(args) => {
            let client = args.local.client()?;
            let snapshot = client
                .fetch_state_snapshot()
                .await
                .map_err(|error| error.to_string())?;
            let artifact_id = ArtifactId::new(args.artifact_id);
            if args.dry_run {
                let exists = snapshot.state.desired.artifacts.contains_key(&artifact_id);
                return print_delete_dry_run(
                    DeleteDryRunReport {
                        accepted: exists,
                        action: "delete artifact".to_owned(),
                        id: artifact_id.to_string(),
                        reasons: if exists {
                            Vec::new()
                        } else {
                            vec![format!(
                                "artifact {} does not exist in desired state",
                                artifact_id
                            )]
                        },
                    },
                    args.local.output,
                );
            }
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
                OutputFormat::Json
                | OutputFormat::Yaml
                | OutputFormat::Toml
                | OutputFormat::Metrics => {
                    print_structured(&json!({ "artifact_id": artifact_id }), args.local.output)
                }
            }
        }
        DeleteCommand::Workload(args) => {
            let client = args.local.client()?;
            let snapshot = client
                .fetch_state_snapshot()
                .await
                .map_err(|error| error.to_string())?;
            let workload_id = WorkloadId::new(args.workload_id);
            if args.dry_run {
                let exists = snapshot.state.desired.workloads.contains_key(&workload_id);
                return print_delete_dry_run(
                    DeleteDryRunReport {
                        accepted: exists,
                        action: "delete workload".to_owned(),
                        id: workload_id.to_string(),
                        reasons: if exists {
                            Vec::new()
                        } else {
                            vec![format!(
                                "workload {} does not exist in desired state",
                                workload_id
                            )]
                        },
                    },
                    args.local.output,
                );
            }
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
                OutputFormat::Json
                | OutputFormat::Yaml
                | OutputFormat::Toml
                | OutputFormat::Metrics => {
                    print_structured(&json!({ "workload_id": workload_id }), args.local.output)
                }
            }
        }
    }
}

fn print_delete_dry_run(report: DeleteDryRunReport, output: OutputFormat) -> Result<(), String> {
    match output {
        OutputFormat::Summary => {
            println!(
                "dry-run: {}\nwould {} {}",
                if report.accepted {
                    "accepted"
                } else {
                    "rejected"
                },
                report.action,
                report.id,
            );
            if report.reasons.is_empty() {
                println!("reasons: -");
            } else {
                println!("reasons:");
                for reason in &report.reasons {
                    println!("  - {reason}");
                }
            }
            Ok(())
        }
        OutputFormat::Json | OutputFormat::Yaml | OutputFormat::Toml | OutputFormat::Metrics => {
            print_structured(&report, output)
        }
    }
}

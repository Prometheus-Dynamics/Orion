use orion_client::LocalControlPlaneClient;
use orion_control_plane::{
    ArtifactRecord, DesiredStateMutation, MutationBatch, TypedConfigValue, WorkloadConfig,
    WorkloadRecord, WorkloadRequirement,
};
use orion_core::{ArtifactId, ConfigSchemaId, RuntimeType};
use serde::Serialize;
use std::collections::BTreeMap;

use crate::{
    cli::{ApplyCommand, OutputFormat, StructuredFormat},
    render::print_structured,
};

use super::describe::compute_workload_blockers;

#[derive(Serialize)]
struct ArtifactDryRunReport {
    accepted: bool,
    action: String,
    artifact: ArtifactRecord,
}

#[derive(Serialize)]
struct WorkloadDryRunReport {
    accepted: bool,
    action: String,
    workload: WorkloadRecord,
    reasons: Vec<String>,
}

pub(super) async fn run(command: ApplyCommand) -> Result<(), String> {
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
            if args.dry_run {
                let action = if snapshot
                    .state
                    .desired
                    .artifacts
                    .contains_key(&artifact.artifact_id)
                {
                    "update artifact"
                } else {
                    "create artifact"
                };
                return print_artifact_dry_run(
                    ArtifactDryRunReport {
                        accepted: true,
                        action: action.to_owned(),
                        artifact,
                    },
                    args.local.output,
                );
            }
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
                OutputFormat::Json | OutputFormat::Yaml | OutputFormat::Toml => {
                    print_structured(&artifact, args.local.output)
                }
            }
        }
        ApplyCommand::Workload(args) => {
            let client = args.local.client()?;
            let dry_run = args.dry_run;
            let output = args.local.output;
            let snapshot = client
                .fetch_state_snapshot()
                .await
                .map_err(|error| error.to_string())?;
            let workload = workload_from_apply_args(*args)?;
            run_workload_apply(workload, snapshot, output, dry_run, &client).await
        }
    }
}

async fn run_workload_apply(
    workload: WorkloadRecord,
    snapshot: orion_control_plane::StateSnapshot,
    output: OutputFormat,
    dry_run: bool,
    client: &LocalControlPlaneClient,
) -> Result<(), String> {
    if dry_run {
        let observability = client
            .query_observability()
            .await
            .map_err(|error| error.to_string())?;
        let mut reasons = Vec::new();
        if !snapshot
            .state
            .desired
            .artifacts
            .contains_key(&workload.artifact_id)
        {
            reasons.push(format!(
                "artifact {} is missing from desired state",
                workload.artifact_id
            ));
        }
        reasons.extend(compute_workload_blockers(
            &snapshot,
            &observability,
            &workload,
        ));
        let action = if snapshot
            .state
            .desired
            .workloads
            .contains_key(&workload.workload_id)
        {
            "update workload"
        } else {
            "create workload"
        };
        return print_workload_dry_run(
            WorkloadDryRunReport {
                accepted: reasons.is_empty(),
                action: action.to_owned(),
                workload,
                reasons,
            },
            output,
        );
    }

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
        OutputFormat::Json | OutputFormat::Yaml | OutputFormat::Toml => {
            print_structured(&workload, output)
        }
    }
}

fn print_artifact_dry_run(
    report: ArtifactDryRunReport,
    output: OutputFormat,
) -> Result<(), String> {
    match output {
        OutputFormat::Summary => {
            println!(
                "dry-run: accepted\nwould {} {}\ncontent_type: {}\nsize_bytes: {}",
                report.action,
                report.artifact.artifact_id,
                report.artifact.content_type.as_deref().unwrap_or("-"),
                report
                    .artifact
                    .size_bytes
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_owned()),
            );
            Ok(())
        }
        OutputFormat::Json | OutputFormat::Yaml | OutputFormat::Toml => {
            print_structured(&report, output)
        }
    }
}

fn print_workload_dry_run(
    report: WorkloadDryRunReport,
    output: OutputFormat,
) -> Result<(), String> {
    match output {
        OutputFormat::Summary => {
            println!(
                "dry-run: {}\nwould {} {}\nassigned_node: {}\nruntime: {}\nartifact: {}",
                if report.accepted {
                    "accepted"
                } else {
                    "rejected"
                },
                report.action,
                report.workload.workload_id,
                report
                    .workload
                    .assigned_node_id
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "-".to_owned()),
                report.workload.runtime_type,
                report.workload.artifact_id,
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
        OutputFormat::Json | OutputFormat::Yaml | OutputFormat::Toml => {
            print_structured(&report, output)
        }
    }
}

fn workload_from_apply_args(args: crate::cli::ApplyWorkloadArgs) -> Result<WorkloadRecord, String> {
    if let Some(spec_path) = args.spec.as_ref() {
        reject_inline_workload_spec_conflicts(&args)?;
        return load_structured_file(spec_path, args.spec_format, "workload spec");
    }
    if args.spec_format.is_some() {
        return Err("--spec-format requires --spec".to_owned());
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
        || args.spec_format.is_some()
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

fn load_structured_file<T: serde::de::DeserializeOwned>(
    path: &std::path::Path,
    explicit_format: Option<StructuredFormat>,
    label: &str,
) -> Result<T, String> {
    let bytes = std::fs::read(path)
        .map_err(|error| format!("failed to read {label} {}: {error}", path.display()))?;
    let format = match explicit_format {
        Some(format) => format,
        None => infer_structured_format(path)?,
    };
    match format {
        StructuredFormat::Json => serde_json::from_slice(&bytes).map_err(|error| {
            format!(
                "failed to parse {label} {} as JSON: {error}",
                path.display()
            )
        }),
        StructuredFormat::Yaml => serde_yaml::from_slice(&bytes).map_err(|error| {
            format!(
                "failed to parse {label} {} as YAML: {error}",
                path.display()
            )
        }),
        StructuredFormat::Toml => {
            let text = std::str::from_utf8(&bytes).map_err(|error| {
                format!(
                    "failed to decode {label} {} as UTF-8: {error}",
                    path.display()
                )
            })?;
            toml::from_str(text).map_err(|error| {
                format!(
                    "failed to parse {label} {} as TOML: {error}",
                    path.display()
                )
            })
        }
    }
}

fn infer_structured_format(path: &std::path::Path) -> Result<StructuredFormat, String> {
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .ok_or_else(|| {
            format!(
                "could not infer file format for {}; use --spec-format json|yaml|toml",
                path.display()
            )
        })?;
    match extension {
        "json" => Ok(StructuredFormat::Json),
        "yaml" | "yml" => Ok(StructuredFormat::Yaml),
        "toml" => Ok(StructuredFormat::Toml),
        other => Err(format!(
            "unsupported file extension `.{other}` for {}; use --spec-format json|yaml|toml",
            path.display()
        )),
    }
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

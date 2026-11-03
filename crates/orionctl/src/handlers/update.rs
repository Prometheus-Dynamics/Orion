//! `orionctl get updates`, `apply update` and `delete update` (`docs/update-recovery.md`).

use orion_control_plane::{
    ActionTarget, DesiredStateMutation, DrainPolicy, MutationBatch, UpdateHandler,
    UpdateIntentRecord, UpdateIntentView, UpdatePhase, update_intent_id, update_intent_views,
};
use orion_core::{ArtifactId, NodeId, UpdateIntentId};
use serde::Serialize;

use crate::{
    cli::{ApplyUpdateArgs, DeleteUpdateArgs, ListArgs, OutputFormat},
    render::print_structured,
};

#[derive(Serialize)]
struct UpdateWriteReport {
    accepted: bool,
    action: String,
    intent: UpdateIntentRecord,
}

/// `orionctl get updates [--node <id>]`.
pub(super) async fn get_updates(args: ListArgs) -> Result<(), String> {
    let snapshot = args.source.fetch_snapshot().await?;
    let views: Vec<UpdateIntentView> =
        update_intent_views(&snapshot.state.desired, &snapshot.state.observed)
            .into_iter()
            .filter(|view| {
                args.node
                    .as_deref()
                    .is_none_or(|node| view.intent.node_id.as_str() == node)
            })
            .collect();
    match args.source.output {
        OutputFormat::Summary => {
            println!("updates count={}", views.len());
            for view in &views {
                println!("{}", render_update_line(view));
            }
            Ok(())
        }
        OutputFormat::Json | OutputFormat::Yaml | OutputFormat::Toml => {
            print_structured(&views, args.source.output)
        }
        OutputFormat::Metrics => {
            Err("metrics output is not supported for update records".to_owned())
        }
    }
}

/// `orionctl apply update`: writes the intent for `<node>/<component>`. The generation stays the
/// same for an unchanged version, moves on when the version changes, and moves on with `--retry`.
pub(super) async fn apply_update(args: ApplyUpdateArgs) -> Result<(), String> {
    let node_id = NodeId::try_new(args.node.clone()).map_err(|error| error.to_string())?;
    let client = args.local.client()?;
    let snapshot = client
        .fetch_state_snapshot()
        .await
        .map_err(|error| error.to_string())?;

    let mut intent = UpdateIntentRecord::new(
        node_id.clone(),
        args.component.clone(),
        args.version.clone(),
    );
    let intent_id: UpdateIntentId = intent.intent_id.clone();
    intent.generation = match snapshot.state.desired.update_intents.get(&intent_id) {
        None => 1,
        Some(current) if current.target_version != args.version || args.retry => {
            current.generation.saturating_add(1)
        }
        Some(current) => current.generation,
    };
    intent.artifact = args
        .artifact
        .map(ArtifactId::try_new)
        .transpose()
        .map_err(|error| error.to_string())?;
    intent.expected_digest = args.digest;
    intent.handler = UpdateHandler::Action {
        target: ActionTarget::Node(node_id),
        name: args.action,
    };
    intent.args = args.args;
    intent.drain = args.drain;
    intent.paused = args.paused;
    intent.validate()?;

    if args.dry_run {
        return print_write(
            UpdateWriteReport {
                accepted: true,
                action: "apply update".to_owned(),
                intent,
            },
            args.local.output,
        );
    }
    client
        .apply_mutations(MutationBatch {
            base_revision: snapshot.state.desired.revision,
            mutations: vec![DesiredStateMutation::PutUpdateIntent(intent.clone())],
            stamps: Vec::new(),
        })
        .await
        .map_err(|error| error.to_string())?;
    match args.local.output {
        OutputFormat::Summary => {
            println!(
                "apply update accepted intent={} version={} generation={}",
                intent.intent_id, intent.target_version, intent.generation
            );
            Ok(())
        }
        OutputFormat::Json | OutputFormat::Yaml | OutputFormat::Toml => print_write(
            UpdateWriteReport {
                accepted: true,
                action: "apply update".to_owned(),
                intent,
            },
            args.local.output,
        ),
        OutputFormat::Metrics => Err("metrics output is not supported here".to_owned()),
    }
}

/// `orionctl delete update`: removes the intent. The node keeps whatever it runs.
pub(super) async fn delete_update(args: DeleteUpdateArgs) -> Result<(), String> {
    let node_id = NodeId::try_new(args.node).map_err(|error| error.to_string())?;
    let intent_id = update_intent_id(&node_id, &args.component);
    let client = args.local.client()?;
    let snapshot = client
        .fetch_state_snapshot()
        .await
        .map_err(|error| error.to_string())?;
    if args.dry_run {
        println!("delete update dry run intent={intent_id}");
        return Ok(());
    }
    client
        .apply_mutations(MutationBatch {
            base_revision: snapshot.state.desired.revision,
            mutations: vec![DesiredStateMutation::RemoveUpdateIntent(intent_id.clone())],
            stamps: Vec::new(),
        })
        .await
        .map_err(|error| error.to_string())?;
    match args.local.output {
        OutputFormat::Summary => {
            println!("delete update accepted intent={intent_id}");
            Ok(())
        }
        OutputFormat::Json | OutputFormat::Yaml | OutputFormat::Toml | OutputFormat::Metrics => {
            Err("structured output is not supported for delete update".to_owned())
        }
    }
}

fn print_write(report: UpdateWriteReport, output: OutputFormat) -> Result<(), String> {
    match output {
        OutputFormat::Summary => {
            println!("{}", render_update_intent(&report.intent));
            Ok(())
        }
        OutputFormat::Json | OutputFormat::Yaml | OutputFormat::Toml => {
            print_structured(&report, output)
        }
        OutputFormat::Metrics => Err("metrics output is not supported here".to_owned()),
    }
}

/// One line per intent with its status, as `orionctl get updates` prints it.
pub(crate) fn render_update_line(view: &UpdateIntentView) -> String {
    let intent = &view.intent;
    let status = view.status.as_ref();
    format!(
        "update intent={} node={} component={} version={} generation={} handler={} drain={} paused={} phase={} acting_on={} installed={} previous={} error={} updated_at_ms={}",
        intent.intent_id,
        intent.node_id,
        intent.component,
        intent.target_version,
        intent.generation,
        render_handler(&intent.handler),
        render_drain(intent.drain),
        intent.paused,
        status.map_or("-".to_owned(), |status| render_phase(status.phase)),
        status.and_then(|status| status.acting_on.as_ref()).map_or(
            "-".to_owned(),
            |acting| format!("{}@{}", acting.target_version, acting.generation)
        ),
        status
            .and_then(|status| status.installed_version.clone())
            .unwrap_or_else(|| "-".to_owned()),
        status
            .and_then(|status| status.previous_version.clone())
            .unwrap_or_else(|| "-".to_owned()),
        status
            .and_then(|status| status.last_error.clone())
            .unwrap_or_else(|| "-".to_owned()),
        status.map_or(0, |status| status.updated_at_ms),
    )
}

fn render_update_intent(intent: &UpdateIntentRecord) -> String {
    format!(
        "intent={} version={} generation={} handler={} drain={} paused={}",
        intent.intent_id,
        intent.target_version,
        intent.generation,
        render_handler(&intent.handler),
        render_drain(intent.drain),
        intent.paused,
    )
}

fn render_handler(handler: &UpdateHandler) -> String {
    match handler {
        UpdateHandler::Action { target, name } => format!("action:{name}@{target}"),
    }
}

fn render_drain(drain: DrainPolicy) -> String {
    match drain {
        DrainPolicy::None => "none".to_owned(),
        DrainPolicy::Cordon => "cordon".to_owned(),
        DrainPolicy::Drain { timeout_ms } => format!("drain:{timeout_ms}"),
    }
}

fn render_phase(phase: UpdatePhase) -> String {
    format!("{phase:?}")
}

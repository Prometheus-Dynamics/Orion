//! `orionctl action run` and `orionctl get actions` (`docs/actions.md`).

use std::time::Duration;

use clap::{Args, Subcommand};
use orion_control_plane::{
    ActionQuery, ActionRequest, ActionResult, ActionTarget, TypedConfigValue,
};

use crate::cli::{LocalControlArgs, OutputFormat};
use crate::render::print_structured;

const ACTION_AFTER_HELP: &str = "Examples:\n  orionctl action run node/node-a reboot --arg delay_ms=5000 --wait\n  orionctl action run resource/camera.front locate --arg duration_ms=10000\n  orionctl action run provider/provider.camera restart-unit --arg unit=camera.service --wait -o json\n\nArguments are KEY=VALUE; values are typed as bool (true/false), int, uint, or string. Force a\ntype with KEY=TYPE:VALUE where TYPE is bool, int, uint, f64, string, or hex (bytes); floats\nare never inferred (f64:1.5).";

#[derive(Subcommand, Debug)]
#[command(after_help = ACTION_AFTER_HELP)]
pub(crate) enum ActionCommand {
    /// Submits an action to a node, provider, resource, or executor (local socket only).
    Run(ActionRunArgs),
}

#[derive(Args, Clone, Debug)]
pub(crate) struct ActionRunArgs {
    #[command(flatten)]
    pub(crate) local: LocalControlArgs,
    /// `node/<id>`, `provider/<id>`, `resource/<id>`, or `executor/<id>`.
    pub(crate) target: String,
    /// Action name, for example `reboot`, `restart-unit`, or `locate`.
    pub(crate) name: String,
    /// Action argument `KEY=VALUE` (repeatable).
    #[arg(long = "arg", value_parser = parse_action_arg)]
    pub(crate) args: Vec<(String, TypedConfigValue)>,
    /// Unique action id (default: generated).
    #[arg(long)]
    pub(crate) id: Option<String>,
    /// Time budget in milliseconds (0: the node default).
    #[arg(long, default_value_t = 0)]
    pub(crate) deadline_ms: u64,
    /// Waits until the action is final and fails when it did not succeed.
    #[arg(long)]
    pub(crate) wait: bool,
}

#[derive(Args, Clone, Debug)]
pub(crate) struct ActionListArgs {
    #[command(flatten)]
    pub(crate) local: LocalControlArgs,
    /// Only actions on this target (`node/<id>`, `provider/<id>`, ...).
    #[arg(long)]
    pub(crate) target: Option<String>,
    /// Only this action.
    #[arg(long)]
    pub(crate) id: Option<String>,
}

pub(crate) fn parse_action_arg(input: &str) -> Result<(String, TypedConfigValue), String> {
    let (key, value) = input
        .split_once('=')
        .ok_or_else(|| "expected KEY=VALUE".to_owned())?;
    if key.is_empty() {
        return Err("argument name must not be empty".to_owned());
    }
    let typed = |kind: &str, raw: &str| -> Result<TypedConfigValue, String> {
        Ok(match kind {
            "bool" => TypedConfigValue::Bool(
                raw.parse()
                    .map_err(|_| format!("invalid bool `{raw}`: expected true or false"))?,
            ),
            "int" => TypedConfigValue::Int(
                raw.parse()
                    .map_err(|error| format!("invalid int `{raw}`: {error}"))?,
            ),
            "uint" => TypedConfigValue::UInt(
                raw.parse()
                    .map_err(|error| format!("invalid uint `{raw}`: {error}"))?,
            ),
            "f64" => TypedConfigValue::F64(
                raw.parse()
                    .map_err(|error| format!("invalid f64 `{raw}`: {error}"))?,
            ),
            "string" => TypedConfigValue::String(raw.to_owned()),
            "hex" => TypedConfigValue::Bytes(decode_hex(raw)?),
            other => return Err(format!("unknown argument type `{other}`")),
        })
    };
    let value = match value.split_once(':') {
        Some((kind, raw)) if ["bool", "int", "uint", "f64", "string", "hex"].contains(&kind) => {
            typed(kind, raw)?
        }
        _ => match value {
            "true" => TypedConfigValue::Bool(true),
            "false" => TypedConfigValue::Bool(false),
            _ => value
                .parse::<u64>()
                .map(TypedConfigValue::UInt)
                .or_else(|_| value.parse::<i64>().map(TypedConfigValue::Int))
                .unwrap_or_else(|_| TypedConfigValue::String(value.to_owned())),
        },
    };
    Ok((key.to_owned(), value))
}

fn decode_hex(input: &str) -> Result<Vec<u8>, String> {
    if !input.len().is_multiple_of(2) {
        return Err("hex input must have an even number of characters".to_owned());
    }
    (0..input.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&input[index..index + 2], 16)
                .map_err(|_| format!("invalid hex `{input}`"))
        })
        .collect()
}

fn generated_action_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    format!("orionctl-{}-{nanos:x}", std::process::id())
}

pub(crate) async fn run(command: ActionCommand) -> Result<(), String> {
    match command {
        ActionCommand::Run(args) => run_action(args).await,
    }
}

async fn run_action(args: ActionRunArgs) -> Result<(), String> {
    let target: ActionTarget = args.target.parse().map_err(|error| format!("{error}"))?;
    let mut request = ActionRequest::new(
        args.id.clone().unwrap_or_else(generated_action_id),
        target,
        args.name.clone(),
    )
    .with_deadline_ms(args.deadline_ms);
    request.args = args.args.iter().cloned().collect();
    let client = args.local.client()?;
    let mut result = client
        .run_action(request)
        .await
        .map_err(|error| error.to_string())?;
    if args.wait && !result.state.is_terminal() {
        // The node times the action out at its deadline, so waiting a little longer than the
        // largest deadline the node accepts always ends.
        result = client
            .wait_for_action(
                &result.action_id,
                Duration::from_millis(200),
                Duration::from_secs(24 * 60 * 60),
            )
            .await
            .map_err(|error| error.to_string())?;
    }
    print_results(std::slice::from_ref(&result), args.local.output)?;
    if args.wait && result.state.is_terminal() && result.state.as_str() != "succeeded" {
        return Err(format!(
            "action {} {}{}",
            result.action_id,
            result.state,
            result
                .state
                .reason()
                .map(|reason| format!(": {reason}"))
                .unwrap_or_default()
        ));
    }
    Ok(())
}

pub(crate) async fn get_actions(args: ActionListArgs) -> Result<(), String> {
    let target = args
        .target
        .as_deref()
        .map(str::parse::<ActionTarget>)
        .transpose()
        .map_err(|error| error.to_string())?;
    let results = args
        .local
        .client()?
        .query_actions(ActionQuery {
            action_id: args.id.clone(),
            target,
        })
        .await
        .map_err(|error| error.to_string())?;
    print_results(&results, args.local.output)
}

fn print_results(results: &[ActionResult], output: OutputFormat) -> Result<(), String> {
    match output {
        OutputFormat::Summary => {
            print!("{}", render_actions_summary(results));
            Ok(())
        }
        OutputFormat::Json | OutputFormat::Yaml | OutputFormat::Toml => {
            print_structured(&results, output)
        }
        OutputFormat::Metrics => {
            Err("metrics output is supported only for observability views".to_owned())
        }
    }
}

fn render_value(value: &TypedConfigValue) -> String {
    match value {
        TypedConfigValue::Bool(value) => value.to_string(),
        TypedConfigValue::Int(value) => value.to_string(),
        TypedConfigValue::UInt(value) => value.to_string(),
        TypedConfigValue::F64(value) => value.to_string(),
        TypedConfigValue::String(value) if value.contains(char::is_whitespace) => {
            format!("{value:?}")
        }
        TypedConfigValue::String(value) => value.clone(),
        TypedConfigValue::Bytes(bytes) => {
            let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
            format!("0x{hex}")
        }
    }
}

fn render_actions_summary(results: &[ActionResult]) -> String {
    let mut out = format!("actions count={}\n", results.len());
    for result in results {
        let progress = match &result.state {
            orion_control_plane::ActionState::Running {
                progress: Some(progress),
            } => format!(" progress={progress}"),
            _ => String::new(),
        };
        let reason = result
            .state
            .reason()
            .map(|reason| format!(" reason={reason:?}"))
            .unwrap_or_default();
        let output = if result.output.is_empty() {
            "-".to_owned()
        } else {
            result
                .output
                .iter()
                .map(|(key, value)| format!("{key}={}", render_value(value)))
                .collect::<Vec<_>>()
                .join(",")
        };
        out.push_str(&format!(
            "action id={} target={} name={} state={}{progress}{reason} handled_by={} \
             requested_by={} output={} created_at_ms={} updated_at_ms={}\n",
            result.action_id,
            result.target,
            result.name,
            result.state,
            result.handled_by,
            result.requested_by,
            output,
            result.created_at_ms,
            result.updated_at_ms,
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use orion_control_plane::ActionState;
    use orion_core::NodeId;

    #[test]
    fn arguments_are_inferred_or_explicitly_typed() {
        assert_eq!(
            parse_action_arg("delay_ms=5000"),
            Ok(("delay_ms".into(), TypedConfigValue::UInt(5000)))
        );
        assert_eq!(
            parse_action_arg("offset=-3"),
            Ok(("offset".into(), TypedConfigValue::Int(-3)))
        );
        assert_eq!(
            parse_action_arg("enabled=false"),
            Ok(("enabled".into(), TypedConfigValue::Bool(false)))
        );
        assert_eq!(
            parse_action_arg("unit=camera.service"),
            Ok((
                "unit".into(),
                TypedConfigValue::String("camera.service".into())
            ))
        );
        assert_eq!(
            parse_action_arg("code=string:0042"),
            Ok(("code".into(), TypedConfigValue::String("0042".into())))
        );
        assert_eq!(
            parse_action_arg("fx=f64:912.25"),
            Ok(("fx".into(), TypedConfigValue::F64(912.25)))
        );
        // Floats are never inferred: version strings stay strings.
        assert_eq!(
            parse_action_arg("version=2.0"),
            Ok(("version".into(), TypedConfigValue::String("2.0".into())))
        );
        assert!(parse_action_arg("fx=f64:wide").is_err());
        assert_eq!(
            parse_action_arg("blob=hex:01ff"),
            Ok(("blob".into(), TypedConfigValue::Bytes(vec![1, 255])))
        );
        assert_eq!(
            parse_action_arg("url=http://x"),
            Ok(("url".into(), TypedConfigValue::String("http://x".into())))
        );
        assert!(parse_action_arg("novalue").is_err());
        assert!(parse_action_arg("n=uint:x").is_err());
    }

    #[test]
    fn summary_lists_state_reason_and_output() {
        let mut result = ActionResult::new(
            "a1",
            ActionTarget::Resource("camera.front".into()),
            "locate",
            NodeId::new("node-a"),
            ActionState::Failed {
                reason: "led broken".into(),
            },
        );
        result.requested_by = "local:orionctl".into();
        result = result.with_output("attempts", TypedConfigValue::UInt(2));
        assert_eq!(
            render_actions_summary(&[result]),
            "actions count=1\naction id=a1 target=resource/camera.front name=locate state=failed \
             reason=\"led broken\" handled_by=node-a requested_by=local:orionctl output=attempts=2 \
             created_at_ms=0 updated_at_ms=0\n"
        );
    }
}

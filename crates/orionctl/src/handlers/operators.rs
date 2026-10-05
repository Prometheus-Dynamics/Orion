//! `orionctl operators ...` and `orionctl get operators`: remote operator administration on the
//! local socket (`docs/remote-operator.md`).

use clap::{Args, Subcommand};
use orion_control_plane::{
    OperatorEnrollment, OperatorId, OperatorPolicy, OperatorTrustState, OperatorsSnapshot,
};
use orion_core::PublicKeyHex;

use crate::cli::{LocalControlArgs, OutputFormat};
use crate::render::print_structured;

const OPERATORS_AFTER_HELP: &str = "Examples:\n  orionctl get operators\n  orionctl operators enroll operator:alice --fingerprint sha256:91c2...\n  orionctl operators enroll alice --public-key <64 hex> --action locate --action 'self-*'\n  orionctl operators enroll alice --fingerprint sha256:91c2... --no-read --no-actions\n  orionctl operators remove operator:alice";

/// Remote operators: desktop and fleet tools that talk to nodes over `orion+tcp`.
#[derive(Subcommand, Debug)]
#[command(after_help = OPERATORS_AFTER_HELP)]
pub(crate) enum OperatorCommand {
    /// Enrolled, pending and revoked operators (same as `orionctl get operators`).
    List(LocalControlArgs),
    /// Approve an operator: pin its key with a policy (lifts a removal).
    Enroll(OperatorEnrollArgs),
    /// Revoke an operator's key (persisted).
    Remove(OperatorRemoveArgs),
}

#[derive(Args, Clone, Debug)]
pub(crate) struct OperatorEnrollArgs {
    #[command(flatten)]
    pub(crate) local: LocalControlArgs,
    /// `operator:<name>` (or just `<name>`).
    pub(crate) operator_id: String,
    /// Only enroll if the operator's key has this fingerprint (`sha256:...`, as the operator's
    /// tool shows it).
    #[arg(long)]
    pub(crate) fingerprint: Option<String>,
    /// The operator's public key (64 hex characters). Without it, the key of the operator's
    /// pending request is used (it must have connected once).
    #[arg(long)]
    pub(crate) public_key: Option<String>,
    /// Skip the fingerprint confirmation (lab setups only).
    #[arg(long)]
    pub(crate) yes: bool,
    /// No read access (node records, status, observability, other operators' actions).
    #[arg(long)]
    pub(crate) no_read: bool,
    /// Action-name pattern the operator may run (`*`, `prefix*`, or an exact name); repeatable.
    /// Without `--action` or `--no-actions` the node default applies
    /// (`ORION_NODE_OPERATOR_ACTIONS`).
    #[arg(long = "action", conflicts_with = "no_actions")]
    pub(crate) actions: Vec<String>,
    /// The operator may run no action at all.
    #[arg(long)]
    pub(crate) no_actions: bool,
}

#[derive(Args, Clone, Debug)]
pub(crate) struct OperatorRemoveArgs {
    #[command(flatten)]
    pub(crate) local: LocalControlArgs,
    pub(crate) operator_id: String,
}

pub(super) async fn run(command: OperatorCommand) -> Result<(), String> {
    match command {
        OperatorCommand::List(args) => get_operators(args).await,
        OperatorCommand::Enroll(args) => enroll(args).await,
        OperatorCommand::Remove(args) => {
            let operator_id = parse_id(&args.operator_id)?;
            args.local
                .client()?
                .remove_operator(operator_id.clone())
                .await
                .map_err(|error| error.to_string())?;
            println!("operators remove accepted: {operator_id} is revoked");
            Ok(())
        }
    }
}

fn parse_id(value: &str) -> Result<OperatorId, String> {
    OperatorId::try_new(value).map_err(|error| error.to_string())
}

pub(super) async fn get_operators(args: LocalControlArgs) -> Result<(), String> {
    let snapshot = args
        .client()?
        .query_operators()
        .await
        .map_err(|error| error.to_string())?;
    match args.output {
        OutputFormat::Summary => {
            print!("{}", render_operators(&snapshot));
            Ok(())
        }
        OutputFormat::Json | OutputFormat::Yaml | OutputFormat::Toml => {
            print_structured(&snapshot, args.output)
        }
        OutputFormat::Metrics => {
            Err("metrics output is supported only for observability views".to_owned())
        }
    }
}

async fn enroll(args: OperatorEnrollArgs) -> Result<(), String> {
    let operator_id = parse_id(&args.operator_id)?;
    let client = args.local.client()?;
    let fingerprint = match (&args.fingerprint, &args.public_key, args.yes) {
        (Some(fingerprint), _, _) => Some(fingerprint.clone()),
        (None, Some(_), _) | (None, None, true) => None,
        (None, None, false) => {
            let snapshot = client
                .query_operators()
                .await
                .map_err(|error| error.to_string())?;
            let pending = snapshot
                .operators
                .iter()
                .find(|record| {
                    record.operator_id == operator_id && record.state == OperatorTrustState::Pending
                })
                .ok_or_else(|| {
                    format!(
                        "operator {operator_id} has no pending request; let it connect first or \
                         pass --public-key"
                    )
                })?;
            let fingerprint = pending.key_fingerprint.clone().unwrap_or_default();
            println!("operator {operator_id}\n  key fingerprint {fingerprint}");
            super::discovered::confirm(
                "Compare the fingerprint with the one the operator's tool shows. Trust this key? \
                 [y/N] ",
            )?;
            Some(fingerprint)
        }
    };
    let actions = if args.no_actions {
        Some(Vec::new())
    } else if args.actions.is_empty() {
        None
    } else {
        Some(args.actions.clone())
    };
    client
        .enroll_operator(OperatorEnrollment {
            operator_id: operator_id.clone(),
            public_key_hex: args.public_key.map(PublicKeyHex::new),
            expected_key_fingerprint: fingerprint,
            policy: OperatorPolicy {
                read: !args.no_read,
                actions,
            },
        })
        .await
        .map_err(|error| error.to_string())?;
    println!("operators enroll accepted: {operator_id} is enrolled");
    Ok(())
}

fn render_operators(snapshot: &OperatorsSnapshot) -> String {
    let mut out = format!(
        "operators local_fingerprint={} enrollment_key={} default_actions={}\n",
        snapshot.local_key_fingerprint,
        snapshot.enrollment_key_configured,
        patterns(&snapshot.default_actions)
    );
    for record in &snapshot.operators {
        out.push_str(&format!(
            "operator id={} state={} fingerprint={} method={} read={} actions={} last_seen_ms={}\n",
            record.operator_id,
            record.state,
            record.key_fingerprint.as_deref().unwrap_or("-"),
            record
                .method
                .map(|method| format!("{method:?}").to_ascii_lowercase())
                .unwrap_or_else(|| "-".into()),
            record.policy.read,
            patterns(&record.effective_actions),
            record.last_seen_ms
        ));
    }
    out
}

fn patterns(patterns: &[String]) -> String {
    if patterns.is_empty() {
        "-".to_owned()
    } else {
        patterns.join(",")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orion_control_plane::{OperatorEnrollmentMethod, OperatorRecord};

    #[test]
    fn operators_render_one_line_each() {
        let snapshot = OperatorsSnapshot {
            local_key_fingerprint: "sha256:aa".into(),
            default_actions: vec!["locate".into()],
            enrollment_key_configured: true,
            operators: vec![OperatorRecord {
                operator_id: OperatorId::try_new("alice").expect("id"),
                state: OperatorTrustState::Enrolled,
                public_key_hex: None,
                key_fingerprint: Some("sha256:bb".into()),
                method: Some(OperatorEnrollmentMethod::EnrollmentKey),
                policy: OperatorPolicy::default(),
                effective_actions: vec!["locate".into()],
                since_ms: 1,
                last_seen_ms: 2,
            }],
        };
        let rendered = render_operators(&snapshot);
        assert!(rendered.contains("default_actions=locate"));
        assert!(rendered.contains(
            "operator id=operator:alice state=enrolled fingerprint=sha256:bb \
             method=enrollmentkey read=true actions=locate"
        ));
    }
}

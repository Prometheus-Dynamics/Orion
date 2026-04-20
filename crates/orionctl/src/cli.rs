use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use orion_client::{default_ipc_socket_path, default_ipc_stream_socket_path};
use orion_control_plane::{DesiredState, RestartPolicy, TypedConfigValue};
use orion_core::{NodeId, ResourceId, ResourceType};

const RUNTIME_IPC_SOCKET_PATH: &str = "/run/orion/control.sock";
const RUNTIME_IPC_STREAM_SOCKET_PATH: &str = "/run/orion/control-stream.sock";

#[derive(Parser, Debug)]
#[command(name = "orionctl")]
#[command(about = "Operator CLI for the Orion daemon.")]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Subcommand, Debug)]
pub(crate) enum Command {
    Get {
        #[command(subcommand)]
        command: GetCommand,
    },
    Watch {
        #[command(subcommand)]
        command: WatchCommand,
    },
    Apply {
        #[command(subcommand)]
        command: Box<ApplyCommand>,
    },
    Delete {
        #[command(subcommand)]
        command: DeleteCommand,
    },
    #[command(alias = "trust")]
    Peers {
        #[command(subcommand)]
        command: PeerCommand,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum GetCommand {
    Health(HttpTargetArgs),
    Readiness(HttpTargetArgs),
    Observability(StateQueryArgs),
    Snapshot(StateQueryArgs),
    Workloads(ListArgs),
    Resources(ListArgs),
    Providers(ListArgs),
    Executors(ListArgs),
    Leases(ListArgs),
}

#[derive(Subcommand, Debug)]
pub(crate) enum WatchCommand {
    State(WatchStateArgs),
}

#[derive(Subcommand, Debug)]
pub(crate) enum ApplyCommand {
    Artifact(ApplyArtifactArgs),
    Workload(Box<ApplyWorkloadArgs>),
}

#[derive(Subcommand, Debug)]
pub(crate) enum DeleteCommand {
    Artifact(DeleteArtifactArgs),
    Workload(DeleteWorkloadArgs),
}

#[derive(Subcommand, Debug)]
pub(crate) enum PeerCommand {
    List(PeerListArgs),
    Enroll(PeerEnrollArgs),
    Revoke(PeerNodeArgs),
    ReplaceKey(PeerReplaceKeyArgs),
    RotateHttpTls(LocalControlArgs),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum OutputFormat {
    Summary,
    Json,
    Yaml,
    Toml,
}

impl std::fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Summary => f.write_str("summary"),
            Self::Json => f.write_str("json"),
            Self::Yaml => f.write_str("yaml"),
            Self::Toml => f.write_str("toml"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum StructuredFormat {
    Json,
    Yaml,
    Toml,
}

impl std::fmt::Display for StructuredFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json => f.write_str("json"),
            Self::Yaml => f.write_str("yaml"),
            Self::Toml => f.write_str("toml"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum CliDesiredState {
    Running,
    Stopped,
}

impl From<CliDesiredState> for DesiredState {
    fn from(value: CliDesiredState) -> Self {
        match value {
            CliDesiredState::Running => Self::Running,
            CliDesiredState::Stopped => Self::Stopped,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum CliRestartPolicy {
    Never,
    Always,
    #[value(alias = "on_failure")]
    OnFailure,
}

impl From<CliRestartPolicy> for RestartPolicy {
    fn from(value: CliRestartPolicy) -> Self {
        match value {
            CliRestartPolicy::Never => Self::Never,
            CliRestartPolicy::Always => Self::Always,
            CliRestartPolicy::OnFailure => Self::OnFailure,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HttpTargetScheme {
    Http,
    Https,
}

#[derive(Args, Clone, Debug)]
pub(crate) struct HttpTargetArgs {
    #[arg(long)]
    pub(crate) http: String,
    #[arg(long)]
    pub(crate) ca_cert: Option<PathBuf>,
    #[arg(long)]
    pub(crate) client_cert: Option<PathBuf>,
    #[arg(long)]
    pub(crate) client_key: Option<PathBuf>,
    #[arg(long, default_value = "orionctl.get")]
    pub(crate) client_name: String,
    #[arg(short = 'o', long, default_value_t = OutputFormat::Summary)]
    pub(crate) output: OutputFormat,
}

#[derive(Args, Clone, Debug)]
pub(crate) struct StateQueryArgs {
    #[arg(long)]
    pub(crate) http: Option<String>,
    #[arg(long)]
    pub(crate) socket: Option<PathBuf>,
    #[arg(long)]
    pub(crate) ca_cert: Option<PathBuf>,
    #[arg(long)]
    pub(crate) client_cert: Option<PathBuf>,
    #[arg(long)]
    pub(crate) client_key: Option<PathBuf>,
    #[arg(long, default_value = "orionctl.get")]
    pub(crate) client_name: String,
    #[arg(short = 'o', long, default_value_t = OutputFormat::Summary)]
    pub(crate) output: OutputFormat,
}

#[derive(Args, Clone, Debug)]
pub(crate) struct ListArgs {
    #[command(flatten)]
    pub(crate) source: StateQueryArgs,
}

#[derive(Args, Clone, Debug)]
pub(crate) struct LocalControlArgs {
    #[arg(long, default_value_os_t = preferred_ipc_socket_path())]
    pub(crate) socket: PathBuf,
    #[arg(long, default_value = "orionctl")]
    pub(crate) client_name: String,
    #[arg(short = 'o', long, default_value_t = OutputFormat::Summary)]
    pub(crate) output: OutputFormat,
}

#[derive(Args, Clone, Debug)]
pub(crate) struct WatchStateArgs {
    #[arg(long, default_value_os_t = preferred_ipc_stream_socket_path())]
    pub(crate) stream_socket: PathBuf,
    #[arg(long, default_value = "orionctl.watch")]
    pub(crate) client_name: String,
    #[arg(long, default_value_t = 0)]
    pub(crate) desired_revision: u64,
    #[arg(long, default_value_t = 1)]
    pub(crate) batches: u32,
    #[arg(short = 'o', long, default_value_t = OutputFormat::Summary)]
    pub(crate) output: OutputFormat,
}

#[derive(Args, Clone, Debug)]
pub(crate) struct ApplyArtifactArgs {
    #[command(flatten)]
    pub(crate) local: LocalControlArgs,
    #[arg(long)]
    pub(crate) artifact_id: String,
    #[arg(long)]
    pub(crate) content_type: Option<String>,
    #[arg(long)]
    pub(crate) size_bytes: Option<u64>,
}

#[derive(Clone, Debug)]
pub(crate) struct RequirementSpec {
    pub(crate) resource_type: ResourceType,
    pub(crate) count: u32,
}

#[derive(Clone, Debug)]
pub(crate) struct BindingSpec {
    pub(crate) resource_id: ResourceId,
    pub(crate) node_id: NodeId,
}

#[derive(Clone, Debug)]
pub(crate) struct ConfigFieldSpec {
    pub(crate) key: String,
    pub(crate) value: TypedConfigValue,
}

#[derive(Args, Clone, Debug)]
pub(crate) struct ApplyWorkloadArgs {
    #[command(flatten)]
    pub(crate) local: LocalControlArgs,
    #[arg(long)]
    pub(crate) spec: Option<PathBuf>,
    #[arg(long, value_enum)]
    pub(crate) spec_format: Option<StructuredFormat>,
    #[arg(long)]
    pub(crate) workload_id: Option<String>,
    #[arg(long)]
    pub(crate) runtime_type: Option<String>,
    #[arg(long)]
    pub(crate) artifact_id: Option<String>,
    #[arg(long)]
    pub(crate) assigned_node: Option<String>,
    #[arg(long, value_enum, default_value_t = CliDesiredState::Stopped)]
    pub(crate) desired_state: CliDesiredState,
    #[arg(long, value_enum, default_value_t = CliRestartPolicy::Never)]
    pub(crate) restart_policy: CliRestartPolicy,
    #[arg(long = "require", value_parser = parse_requirement)]
    pub(crate) requirements: Vec<RequirementSpec>,
    #[arg(long = "bind", value_parser = parse_binding)]
    pub(crate) bindings: Vec<BindingSpec>,
    #[arg(long)]
    pub(crate) config_schema: Option<String>,
    #[arg(long = "config-bool", value_parser = parse_bool_config)]
    pub(crate) config_bools: Vec<ConfigFieldSpec>,
    #[arg(long = "config-int", value_parser = parse_int_config)]
    pub(crate) config_ints: Vec<ConfigFieldSpec>,
    #[arg(long = "config-uint", value_parser = parse_uint_config)]
    pub(crate) config_uints: Vec<ConfigFieldSpec>,
    #[arg(long = "config-string", value_parser = parse_string_config)]
    pub(crate) config_strings: Vec<ConfigFieldSpec>,
    #[arg(long = "config-bytes-hex", value_parser = parse_bytes_hex_config)]
    pub(crate) config_bytes_hex: Vec<ConfigFieldSpec>,
}

#[derive(Args, Clone, Debug)]
pub(crate) struct DeleteArtifactArgs {
    #[command(flatten)]
    pub(crate) local: LocalControlArgs,
    #[arg(long)]
    pub(crate) artifact_id: String,
}

#[derive(Args, Clone, Debug)]
pub(crate) struct DeleteWorkloadArgs {
    #[command(flatten)]
    pub(crate) local: LocalControlArgs,
    #[arg(long)]
    pub(crate) workload_id: String,
}

#[derive(Args, Clone, Debug)]
pub(crate) struct PeerListArgs {
    #[command(flatten)]
    pub(crate) local: LocalControlArgs,
}

#[derive(Args, Clone, Debug)]
pub(crate) struct PeerEnrollArgs {
    #[command(flatten)]
    pub(crate) local: LocalControlArgs,
    #[arg(long)]
    pub(crate) node_id: String,
    #[arg(long)]
    pub(crate) base_url: String,
    #[arg(long)]
    pub(crate) public_key: Option<String>,
    #[arg(long)]
    pub(crate) tls_root_cert: Option<PathBuf>,
}

#[derive(Args, Clone, Debug)]
pub(crate) struct PeerNodeArgs {
    #[command(flatten)]
    pub(crate) local: LocalControlArgs,
    #[arg(long)]
    pub(crate) node_id: String,
}

#[derive(Args, Clone, Debug)]
pub(crate) struct PeerReplaceKeyArgs {
    #[command(flatten)]
    pub(crate) local: LocalControlArgs,
    #[arg(long)]
    pub(crate) node_id: String,
    #[arg(long)]
    pub(crate) public_key: String,
}

pub(crate) fn parse_requirement(input: &str) -> Result<RequirementSpec, String> {
    let Some((resource_type, count)) = input.rsplit_once(':') else {
        return Err("expected RESOURCE_TYPE:COUNT".to_owned());
    };
    let count = count
        .parse::<u32>()
        .map_err(|error| format!("invalid requirement count `{count}`: {error}"))?;
    if count == 0 {
        return Err("requirement count must be greater than zero".to_owned());
    }
    Ok(RequirementSpec {
        resource_type: ResourceType::new(resource_type),
        count,
    })
}

pub(crate) fn parse_binding(input: &str) -> Result<BindingSpec, String> {
    let Some((resource_id, node_id)) = input.rsplit_once('@') else {
        return Err("expected RESOURCE_ID@NODE_ID".to_owned());
    };
    if resource_id.is_empty() || node_id.is_empty() {
        return Err("binding requires both resource id and node id".to_owned());
    }
    Ok(BindingSpec {
        resource_id: ResourceId::new(resource_id),
        node_id: NodeId::new(node_id),
    })
}

fn parse_bool_config(input: &str) -> Result<ConfigFieldSpec, String> {
    parse_config_field(input, |value| match value {
        "true" => Ok(TypedConfigValue::Bool(true)),
        "false" => Ok(TypedConfigValue::Bool(false)),
        _ => Err(format!(
            "invalid bool config value `{value}`: expected true or false"
        )),
    })
}

fn parse_int_config(input: &str) -> Result<ConfigFieldSpec, String> {
    parse_config_field(input, |value| {
        value
            .parse::<i64>()
            .map(TypedConfigValue::Int)
            .map_err(|error| format!("invalid int config value `{value}`: {error}"))
    })
}

fn parse_uint_config(input: &str) -> Result<ConfigFieldSpec, String> {
    parse_config_field(input, |value| {
        value
            .parse::<u64>()
            .map(TypedConfigValue::UInt)
            .map_err(|error| format!("invalid uint config value `{value}`: {error}"))
    })
}

fn parse_string_config(input: &str) -> Result<ConfigFieldSpec, String> {
    parse_config_field(input, |value| {
        Ok(TypedConfigValue::String(value.to_owned()))
    })
}

fn parse_bytes_hex_config(input: &str) -> Result<ConfigFieldSpec, String> {
    parse_config_field(input, |value| {
        decode_hex(value).map(TypedConfigValue::Bytes)
    })
}

fn parse_config_field(
    input: &str,
    value_parser: impl FnOnce(&str) -> Result<TypedConfigValue, String>,
) -> Result<ConfigFieldSpec, String> {
    let Some((key, value)) = input.split_once('=') else {
        return Err("expected KEY=VALUE".to_owned());
    };
    if key.is_empty() {
        return Err("config key must not be empty".to_owned());
    }
    Ok(ConfigFieldSpec {
        key: key.to_owned(),
        value: value_parser(value)?,
    })
}

fn decode_hex(input: &str) -> Result<Vec<u8>, String> {
    if !input.len().is_multiple_of(2) {
        return Err("hex input must have an even number of characters".to_owned());
    }
    let mut bytes = Vec::with_capacity(input.len() / 2);
    let mut chars = input.chars();
    while let (Some(high), Some(low)) = (chars.next(), chars.next()) {
        let high = high
            .to_digit(16)
            .ok_or_else(|| format!("invalid hex character `{high}`"))?;
        let low = low
            .to_digit(16)
            .ok_or_else(|| format!("invalid hex character `{low}`"))?;
        bytes.push(((high << 4) | low) as u8);
    }
    Ok(bytes)
}

pub(crate) fn preferred_ipc_socket_path() -> PathBuf {
    preferred_runtime_path(
        PathBuf::from(RUNTIME_IPC_SOCKET_PATH),
        default_ipc_socket_path,
    )
}

pub(crate) fn preferred_ipc_stream_socket_path() -> PathBuf {
    preferred_runtime_path(
        PathBuf::from(RUNTIME_IPC_STREAM_SOCKET_PATH),
        default_ipc_stream_socket_path,
    )
}

fn preferred_runtime_path(runtime_path: PathBuf, fallback: impl FnOnce() -> PathBuf) -> PathBuf {
    if runtime_path.exists() {
        runtime_path
    } else {
        fallback()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::{
        HttpTargetArgs, HttpTargetScheme, OutputFormat, StructuredFormat, TypedConfigValue,
        parse_binding, parse_bool_config, parse_bytes_hex_config, parse_requirement,
        parse_string_config, parse_uint_config, preferred_runtime_path,
    };
    use crate::transport::HttpTargetExt;

    fn target(http: &str) -> HttpTargetArgs {
        HttpTargetArgs {
            http: http.to_owned(),
            ca_cert: None,
            client_cert: None,
            client_key: None,
            client_name: "orionctl.test".to_owned(),
            output: OutputFormat::Summary,
        }
    }

    #[test]
    fn plain_http_targets_reject_tls_material_early() {
        let mut args = target("http://127.0.0.1:9100");
        args.ca_cert = Some("ca.pem".into());
        let error = args
            .validate_tls_args(HttpTargetScheme::Http)
            .expect_err("plain HTTP should reject TLS material");
        assert!(error.contains("plain http:// targets do not use TLS material"));
    }

    #[test]
    fn client_identity_requires_matching_key() {
        let mut args = target("https://127.0.0.1:9100");
        args.client_cert = Some("client-cert.pem".into());
        let error = args
            .validate_tls_args(HttpTargetScheme::Https)
            .expect_err("partial client identity should fail");
        assert!(error.contains("requires both --client-cert and --client-key"));
    }

    #[test]
    fn parse_requirement_accepts_type_and_count() {
        let requirement = parse_requirement("camera.stream:2").expect("requirement should parse");
        assert_eq!(requirement.resource_type.to_string(), "camera.stream");
        assert_eq!(requirement.count, 2);
    }

    #[test]
    fn parse_binding_accepts_resource_and_node() {
        let binding = parse_binding("resource.front@node-a").expect("binding should parse");
        assert_eq!(binding.resource_id.to_string(), "resource.front");
        assert_eq!(binding.node_id.to_string(), "node-a");
    }

    #[test]
    fn parse_typed_config_fields_accept_expected_shapes() {
        let enabled = parse_bool_config("enabled=true").expect("bool config should parse");
        assert_eq!(enabled.key, "enabled");
        assert_eq!(enabled.value, TypedConfigValue::Bool(true));

        let count = parse_uint_config("count=42").expect("uint config should parse");
        assert_eq!(count.key, "count");
        assert_eq!(count.value, TypedConfigValue::UInt(42));

        let label = parse_string_config("graph.kind=inline").expect("string config should parse");
        assert_eq!(label.key, "graph.kind");
        assert_eq!(label.value, TypedConfigValue::String("inline".to_owned()));

        let bytes = parse_bytes_hex_config("payload=6869").expect("hex config should parse");
        assert_eq!(bytes.key, "payload");
        assert_eq!(bytes.value, TypedConfigValue::Bytes(vec![0x68, 0x69]));
    }

    #[test]
    fn structured_format_display_matches_cli_values() {
        assert_eq!(StructuredFormat::Json.to_string(), "json");
        assert_eq!(StructuredFormat::Yaml.to_string(), "yaml");
        assert_eq!(StructuredFormat::Toml.to_string(), "toml");
    }

    #[test]
    fn preferred_runtime_path_uses_runtime_socket_when_present() {
        let runtime = unique_test_path("runtime.sock");
        fs::write(&runtime, b"socket").expect("test runtime socket placeholder should write");

        let selected =
            preferred_runtime_path(runtime.clone(), || unique_test_path("fallback.sock"));

        assert_eq!(selected, runtime);
        let _ = fs::remove_file(selected);
    }

    #[test]
    fn preferred_runtime_path_falls_back_when_runtime_socket_is_missing() {
        let runtime = unique_test_path("missing.sock");
        let fallback = unique_test_path("fallback.sock");

        let selected = preferred_runtime_path(runtime, || fallback.clone());

        assert_eq!(selected, fallback);
    }

    fn unique_test_path(name: &str) -> std::path::PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("orionctl-{stamp}-{name}"))
    }
}

mod runtime_tuning;

pub(crate) use runtime_tuning::normalize_runtime_tuning_duration;
#[cfg(test)]
pub(crate) use runtime_tuning::parse_config_value;
#[cfg(test)]
pub(crate) use runtime_tuning::runtime_tuning_doc_defaults;
pub use runtime_tuning::{AuditLogOverloadPolicy, NodeRuntimeTuning};
use runtime_tuning::{bool_env_or_false, duration_ms_env_or, parse_env_or};

use crate::NodeError;
use crate::PeerSyncExecution;
use crate::auth::PeerAuthenticationMode;
use crate::peer::PeerConfig;
use orion::NodeId;
use orion_transport_common::loopback_ephemeral_socket_addr;
use std::{
    env,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    time::Duration,
};

const DEFAULT_RECONCILE_INTERVAL_MS: u64 = 250;
const DEFAULT_PEER_SYNC_MAX_IN_FLIGHT: usize = 4;
#[cfg(test)]
const DEFAULT_IPC_STREAM_HEARTBEAT_INTERVAL_MS: u64 = 50;
#[cfg(not(test))]
const DEFAULT_IPC_STREAM_HEARTBEAT_INTERVAL_MS: u64 = 5_000;
#[cfg(test)]
const DEFAULT_IPC_STREAM_HEARTBEAT_TIMEOUT_MS: u64 = 125;
#[cfg(not(test))]
const DEFAULT_IPC_STREAM_HEARTBEAT_TIMEOUT_MS: u64 = 15_000;

pub(crate) fn parse_env_choice<T: Copy>(
    key: &str,
    default: &str,
    choices: &[(&str, T)],
) -> Result<T, NodeError> {
    let raw = match env::var(key) {
        Ok(value) => value,
        Err(env::VarError::NotPresent) => default.to_owned(),
        Err(env::VarError::NotUnicode(_)) => {
            return Err(NodeError::Config(format!("{key} must be valid unicode")));
        }
    };
    let normalized = raw.trim().to_ascii_lowercase();
    choices
        .iter()
        .find_map(|(name, value)| (*name == normalized).then_some(*value))
        .ok_or_else(|| {
            let expected = choices
                .iter()
                .map(|(name, _)| format!("`{name}`"))
                .collect::<Vec<_>>()
                .join(", ");
            NodeError::Config(format!(
                "invalid {key} `{normalized}`; expected one of {expected}"
            ))
        })
}

#[derive(Clone, Copy)]
enum PeerSyncMode {
    Serial,
    Parallel,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeConfig {
    pub node_id: NodeId,
    pub http_bind_addr: SocketAddr,
    pub ipc_socket_path: PathBuf,
    pub reconcile_interval: Duration,
    pub state_dir: Option<PathBuf>,
    pub peers: Vec<PeerConfig>,
    pub peer_authentication: PeerAuthenticationMode,
    pub peer_sync_execution: PeerSyncExecution,
    pub ipc_stream_heartbeat_interval: Duration,
    pub ipc_stream_heartbeat_timeout: Duration,
    pub runtime_tuning: NodeRuntimeTuning,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeProcessConfig {
    pub node: NodeConfig,
    pub ipc_stream_socket_path: PathBuf,
    pub http_probe_addr: Option<SocketAddr>,
    pub audit_log_path: Option<PathBuf>,
    pub http_tls_cert_path: Option<PathBuf>,
    pub http_tls_key_path: Option<PathBuf>,
    pub auto_http_tls: bool,
    pub shutdown_after_init: Option<Duration>,
}

impl NodeConfig {
    pub fn default_http_bind_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9100)
    }

    pub fn local_ephemeral_http_bind_addr() -> SocketAddr {
        loopback_ephemeral_socket_addr()
    }

    pub fn default_ipc_socket_path_for(node_id: impl AsRef<str>) -> PathBuf {
        std::env::temp_dir().join(format!("orion-{}-control.sock", node_id.as_ref()))
    }

    pub fn default_ipc_stream_socket_path_for(node_id: impl AsRef<str>) -> PathBuf {
        std::env::temp_dir().join(format!("orion-{}-control-stream.sock", node_id.as_ref()))
    }

    /// Builds a local-development node config with deterministic defaults.
    pub fn for_local_node(node_id: impl Into<NodeId>) -> Self {
        let node_id = node_id.into();
        let node_name = node_id.as_str().to_owned();

        Self {
            node_id,
            http_bind_addr: Self::default_http_bind_addr(),
            ipc_socket_path: Self::default_ipc_socket_path_for(&node_name),
            reconcile_interval: Duration::from_millis(DEFAULT_RECONCILE_INTERVAL_MS),
            state_dir: None,
            peers: Vec::new(),
            peer_authentication: PeerAuthenticationMode::Optional,
            peer_sync_execution: PeerSyncExecution::Parallel {
                max_in_flight: DEFAULT_PEER_SYNC_MAX_IN_FLIGHT,
            },
            ipc_stream_heartbeat_interval: Self::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: Self::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeRuntimeTuning::default(),
        }
    }

    pub fn with_http_bind_addr(mut self, http_bind_addr: SocketAddr) -> Self {
        self.http_bind_addr = http_bind_addr;
        self
    }

    pub fn with_ipc_socket_path(mut self, ipc_socket_path: impl Into<PathBuf>) -> Self {
        self.ipc_socket_path = ipc_socket_path.into();
        self
    }

    pub fn with_reconcile_interval(mut self, reconcile_interval: Duration) -> Self {
        self.reconcile_interval = reconcile_interval;
        self
    }

    pub fn with_state_dir(mut self, state_dir: impl Into<PathBuf>) -> Self {
        self.state_dir = Some(state_dir.into());
        self
    }

    pub fn with_peers(mut self, peers: Vec<PeerConfig>) -> Self {
        self.peers = peers;
        self
    }

    pub fn with_peer_authentication(mut self, peer_authentication: PeerAuthenticationMode) -> Self {
        self.peer_authentication = peer_authentication;
        self
    }

    pub fn with_peer_sync_execution(mut self, peer_sync_execution: PeerSyncExecution) -> Self {
        self.peer_sync_execution = peer_sync_execution;
        self
    }

    pub fn with_runtime_tuning(mut self, runtime_tuning: NodeRuntimeTuning) -> Self {
        self.runtime_tuning = runtime_tuning;
        self
    }

    pub fn with_runtime_tuning_mut(
        mut self,
        mutate: impl FnOnce(NodeRuntimeTuning) -> NodeRuntimeTuning,
    ) -> Self {
        self.runtime_tuning = mutate(self.runtime_tuning);
        self
    }

    /// Loads node config from the current process environment with typed validation errors.
    pub fn try_from_env() -> Result<Self, NodeError> {
        let node_id_raw = env::var("ORION_NODE_ID").unwrap_or_else(|_| "node.local".to_owned());
        let node_id = NodeId::try_new(node_id_raw.clone()).map_err(|err| {
            NodeError::Config(format!("ORION_NODE_ID must be a non-empty node id: {err}"))
        })?;
        let http_bind_addr_raw = env::var("ORION_NODE_HTTP_ADDR")
            .unwrap_or_else(|_| Self::default_http_bind_addr().to_string());
        let http_bind_addr = http_bind_addr_raw.parse().map_err(|err| {
            NodeError::Config(format!(
                "ORION_NODE_HTTP_ADDR must be a valid socket address: {http_bind_addr_raw} ({err})"
            ))
        })?;
        let ipc_socket_path = env::var("ORION_NODE_IPC_SOCKET")
            .map(PathBuf::from)
            .unwrap_or_else(|_| Self::default_ipc_socket_path_for(&node_id_raw));
        let reconcile_interval =
            duration_ms_env_or("ORION_NODE_RECONCILE_MS", DEFAULT_RECONCILE_INTERVAL_MS)?;
        let state_dir = env::var("ORION_NODE_STATE_DIR").ok().map(PathBuf::from);
        let peers = match env::var("ORION_NODE_PEERS") {
            Ok(value) => parse_peer_configs_checked(&value)?,
            Err(_) => Vec::new(),
        };
        Ok(Self {
            node_id,
            http_bind_addr,
            ipc_socket_path,
            reconcile_interval,
            state_dir,
            peers,
            peer_authentication: PeerAuthenticationMode::try_from_env()?,
            peer_sync_execution: Self::try_peer_sync_execution_from_env()?,
            ipc_stream_heartbeat_interval: Self::try_ipc_stream_heartbeat_interval_from_env()?,
            ipc_stream_heartbeat_timeout: Self::try_ipc_stream_heartbeat_timeout_from_env()?,
            runtime_tuning: Self::try_runtime_tuning_from_env()?,
        })
    }

    pub fn try_runtime_tuning_from_env() -> Result<NodeRuntimeTuning, NodeError> {
        NodeRuntimeTuning::try_from_env()
    }

    pub fn try_peer_sync_execution_from_env() -> Result<PeerSyncExecution, NodeError> {
        match parse_env_choice(
            "ORION_NODE_PEER_SYNC_MODE",
            "parallel",
            &[
                ("serial", PeerSyncMode::Serial),
                ("parallel", PeerSyncMode::Parallel),
            ],
        ) {
            Ok(PeerSyncMode::Serial) => Ok(PeerSyncExecution::Serial),
            Ok(PeerSyncMode::Parallel) => Ok(PeerSyncExecution::Parallel {
                max_in_flight: parse_env_or(
                    "ORION_NODE_PEER_SYNC_MAX_IN_FLIGHT",
                    DEFAULT_PEER_SYNC_MAX_IN_FLIGHT,
                )?,
            }),
            Err(err) => Err(err),
        }
    }

    pub fn default_ipc_stream_heartbeat_interval() -> Duration {
        Duration::from_millis(DEFAULT_IPC_STREAM_HEARTBEAT_INTERVAL_MS)
    }

    pub fn default_ipc_stream_heartbeat_timeout() -> Duration {
        Duration::from_millis(DEFAULT_IPC_STREAM_HEARTBEAT_TIMEOUT_MS)
    }

    pub fn try_ipc_stream_heartbeat_interval_from_env() -> Result<Duration, NodeError> {
        duration_ms_env_or(
            "ORION_NODE_IPC_STREAM_HEARTBEAT_INTERVAL_MS",
            DEFAULT_IPC_STREAM_HEARTBEAT_INTERVAL_MS,
        )
    }

    pub fn try_ipc_stream_heartbeat_timeout_from_env() -> Result<Duration, NodeError> {
        duration_ms_env_or(
            "ORION_NODE_IPC_STREAM_HEARTBEAT_TIMEOUT_MS",
            DEFAULT_IPC_STREAM_HEARTBEAT_TIMEOUT_MS,
        )
    }

    pub fn try_http_tls_auto_from_env() -> Result<bool, NodeError> {
        bool_env_or_false("ORION_NODE_HTTP_TLS_AUTO")
    }

    pub fn try_http_probe_addr_from_env() -> Result<Option<SocketAddr>, NodeError> {
        env::var("ORION_NODE_HTTP_PROBE_ADDR")
            .ok()
            .map(|value| {
                value.parse().map_err(|err| {
                    NodeError::Config(format!(
                        "ORION_NODE_HTTP_PROBE_ADDR must be a valid socket address: {value} ({err})"
                    ))
                })
            })
            .transpose()
    }

    pub fn audit_log_path_from_env() -> Option<PathBuf> {
        env::var("ORION_NODE_AUDIT_LOG").ok().map(PathBuf::from)
    }

    pub fn try_shutdown_after_init_from_env() -> Result<Option<Duration>, NodeError> {
        env::var("ORION_NODE_SHUTDOWN_AFTER_INIT_MS")
            .ok()
            .map(|value| {
                value.parse::<u64>().map(Duration::from_millis).map_err(|err| {
                    NodeError::Config(format!(
                        "ORION_NODE_SHUTDOWN_AFTER_INIT_MS must be an integer millisecond value: {value} ({err})"
                    ))
                })
            })
            .transpose()
    }
}

impl NodeProcessConfig {
    /// Loads the full process startup config from environment with typed validation errors.
    pub fn try_from_env() -> Result<Self, NodeError> {
        let node = NodeConfig::try_from_env()?;
        let ipc_stream_socket_path = env::var("ORION_NODE_IPC_STREAM_SOCKET")
            .map(PathBuf::from)
            .unwrap_or_else(|_| NodeConfig::default_ipc_stream_socket_path_for(&node.node_id));
        let http_probe_addr = NodeConfig::try_http_probe_addr_from_env()?;
        let audit_log_path = NodeConfig::audit_log_path_from_env();
        let http_tls_cert_path = env::var("ORION_NODE_HTTP_TLS_CERT").ok().map(PathBuf::from);
        let http_tls_key_path = env::var("ORION_NODE_HTTP_TLS_KEY").ok().map(PathBuf::from);
        let auto_http_tls = NodeConfig::try_http_tls_auto_from_env()?;
        let shutdown_after_init = NodeConfig::try_shutdown_after_init_from_env()?;

        match (&http_tls_cert_path, &http_tls_key_path) {
            (Some(_), Some(_)) | (None, None) => Ok(Self {
                node,
                ipc_stream_socket_path,
                http_probe_addr,
                audit_log_path,
                http_tls_cert_path,
                http_tls_key_path,
                auto_http_tls,
                shutdown_after_init,
            }),
            _ => Err(NodeError::Config(
                "ORION_NODE_HTTP_TLS_CERT and ORION_NODE_HTTP_TLS_KEY must either both be set or both be unset".into(),
            )),
        }
    }
}

fn parse_peer_configs_checked(value: &str) -> Result<Vec<PeerConfig>, NodeError> {
    value
        .split(',')
        .filter_map(|entry| {
            let entry = entry.trim();
            if entry.is_empty() {
                return None;
            }

            Some((|| {
                let (node_id, base_url) = entry.split_once('=').ok_or_else(|| {
                    NodeError::Config(format!(
                        "invalid ORION_NODE_PEERS entry `{entry}`; expected node-id=http://host:port"
                    ))
                })?;
                let mut segments = base_url.trim().split('|');
                let base_url = segments.next().ok_or_else(|| {
                    NodeError::Config(format!(
                        "invalid ORION_NODE_PEERS entry `{entry}`; peer base URL segment is missing"
                    ))
                })?;
                let mut peer = PeerConfig::try_new(node_id.trim(), base_url.trim())
                    .map_err(|err| NodeError::Config(format!("invalid ORION_NODE_PEERS entry `{entry}`: {err}")))?;
                for segment in segments {
                    let segment = segment.trim();
                    if segment.is_empty() {
                        continue;
                    }
                    if let Some(path) = segment.strip_prefix("ca=") {
                        peer = peer.with_tls_root_cert_path(path);
                        continue;
                    }
                    peer = peer.try_with_trusted_public_key_hex(segment).map_err(|err| {
                        NodeError::Config(format!(
                            "invalid ORION_NODE_PEERS trust key in entry `{entry}`: {err}"
                        ))
                    })?;
                }
                Ok(peer)
            })())
        })
        .collect()
}

#[cfg(test)]
mod tests;

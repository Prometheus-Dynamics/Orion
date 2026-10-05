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
const HTTP_ADDR_DISABLED_VALUES: &[&str] = &["off", "disabled", "none"];
/// Suffix for configuration errors raised when an HTTP-only setting is used in a build without
/// the `transport-http` feature (IPC-only appliance builds).
#[cfg(not(feature = "transport-http"))]
const HTTP_FEATURE_DISABLED: &str =
    "orion-node was built without the `transport-http` feature (IPC-only build)";
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
    /// Whether the HTTP control listener is started. Disabled with `ORION_NODE_HTTP_ADDR=off`
    /// for single-node appliances that only use local IPC.
    pub http_enabled: bool,
    pub runtime_threads: NodeRuntimeThreads,
    /// Listener for `orion+tcp` peers (`ORION_NODE_PEER_ADDR`, feature `peer-tcp`).
    pub peer_tcp_addr: Option<SocketAddr>,
    /// Microcontroller links to serve (`ORION_NODE_LINKS`, feature `link-gateway`).
    #[cfg(feature = "link-gateway")]
    pub links: Vec<crate::link_gateway::LinkConfig>,
    /// Peer discovery (`ORION_NODE_DISCOVERY=mdns`, feature `discovery-mdns`).
    #[cfg(feature = "discovery-mdns")]
    pub discovery: Option<crate::discovery::DiscoveryConfig>,
}

/// Tokio runtime sizing for the node binary.
///
/// `None` keeps Tokio's defaults (one worker per core, 512 blocking threads).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NodeRuntimeThreads {
    pub worker_threads: Option<usize>,
    pub max_blocking_threads: Option<usize>,
}

impl NodeRuntimeThreads {
    pub fn try_from_env() -> Result<Self, NodeError> {
        Ok(Self {
            worker_threads: positive_usize_env("ORION_NODE_RUNTIME_WORKER_THREADS")?,
            max_blocking_threads: positive_usize_env("ORION_NODE_RUNTIME_MAX_BLOCKING_THREADS")?,
        })
    }

    /// Builds the multi-threaded Tokio runtime used by the node binary.
    pub fn build_runtime(&self) -> std::io::Result<tokio::runtime::Runtime> {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.enable_all();
        if let Some(worker_threads) = self.worker_threads {
            builder.worker_threads(worker_threads);
        }
        if let Some(max_blocking_threads) = self.max_blocking_threads {
            builder.max_blocking_threads(max_blocking_threads);
        }
        builder.build()
    }
}

fn positive_usize_env(key: &str) -> Result<Option<usize>, NodeError> {
    let Ok(raw) = env::var(key) else {
        return Ok(None);
    };
    match raw.trim().parse::<usize>() {
        Ok(0) | Err(_) => Err(NodeError::Config(format!(
            "{key} must be a positive integer: {raw}"
        ))),
        Ok(value) => Ok(Some(value)),
    }
}

fn is_http_addr_disabled(raw: &str) -> bool {
    HTTP_ADDR_DISABLED_VALUES.contains(&raw.trim().to_ascii_lowercase().as_str())
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
        let http_bind_addr = match env::var("ORION_NODE_HTTP_ADDR") {
            Ok(raw) if is_http_addr_disabled(&raw) => Self::default_http_bind_addr(),
            #[cfg(not(feature = "transport-http"))]
            Ok(raw) => {
                return Err(NodeError::Config(format!(
                    "ORION_NODE_HTTP_ADDR={raw} is not supported: {HTTP_FEATURE_DISABLED}; unset it or set it to `off`"
                )));
            }
            #[cfg(feature = "transport-http")]
            Ok(raw) => raw.parse().map_err(|err| {
                NodeError::Config(format!(
                    "ORION_NODE_HTTP_ADDR must be a valid socket address or `off`: {raw} ({err})"
                ))
            })?,
            Err(_) => Self::default_http_bind_addr(),
        };
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
        for peer in &peers {
            crate::peer::PeerTransportKind::check_supported(peer.base_url.as_str()).map_err(
                |err| {
                    NodeError::Config(format!(
                        "invalid ORION_NODE_PEERS entry for {}: {err}",
                        peer.node_id
                    ))
                },
            )?;
        }
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

    /// Returns `false` when `ORION_NODE_HTTP_ADDR` is `off`, `disabled`, or `none`.
    /// Whether the peer HTTP listener should be started. Always `false` in builds without the
    /// `transport-http` feature.
    pub fn http_enabled_from_env() -> bool {
        cfg!(feature = "transport-http")
            && env::var("ORION_NODE_HTTP_ADDR").map_or(true, |raw| !is_http_addr_disabled(&raw))
    }

    pub fn try_http_probe_addr_from_env() -> Result<Option<SocketAddr>, NodeError> {
        #[cfg(not(feature = "transport-http"))]
        if let Ok(value) = env::var("ORION_NODE_HTTP_PROBE_ADDR") {
            return Err(NodeError::Config(format!(
                "ORION_NODE_HTTP_PROBE_ADDR={value} is not supported: {HTTP_FEATURE_DISABLED}"
            )));
        }
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

    /// Parses `ORION_NODE_PEER_ADDR`, the `orion+tcp` peer listener. Unset means no listener (the
    /// node can still sync outbound with `orion+tcp://` peers).
    pub fn try_peer_tcp_addr_from_env() -> Result<Option<SocketAddr>, NodeError> {
        let Ok(raw) = env::var("ORION_NODE_PEER_ADDR") else {
            return Ok(None);
        };
        if is_http_addr_disabled(&raw) || raw.trim().is_empty() {
            return Ok(None);
        }
        if !cfg!(feature = "peer-tcp") {
            return Err(NodeError::Config(format!(
                "ORION_NODE_PEER_ADDR={raw} is not supported: orion-node was built without the `peer-tcp` feature"
            )));
        }
        raw.trim().parse().map(Some).map_err(|err| {
            NodeError::Config(format!(
                "ORION_NODE_PEER_ADDR must be a valid socket address or `off`: {raw} ({err})"
            ))
        })
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
        let http_enabled = NodeConfig::http_enabled_from_env();
        let runtime_threads = NodeRuntimeThreads::try_from_env()?;
        let peer_tcp_addr = NodeConfig::try_peer_tcp_addr_from_env()?;
        #[cfg(feature = "link-gateway")]
        let links = crate::link_gateway::LinkConfig::try_from_env()?;
        #[cfg(all(feature = "link-gateway", not(target_os = "linux")))]
        if !links.is_empty() {
            return Err(NodeError::Config(
                "ORION_NODE_LINKS is not supported: serial and SocketCAN links are only available on Linux".into(),
            ));
        }
        #[cfg(not(feature = "link-gateway"))]
        if env::var_os("ORION_NODE_LINKS").is_some_and(|value| !value.is_empty()) {
            return Err(NodeError::Config(
                "ORION_NODE_LINKS is not supported: orion-node was built without the `link-gateway` feature; rebuild with `--features link-gateway` or unset it".into(),
            ));
        }

        #[cfg(feature = "discovery-mdns")]
        let discovery = crate::discovery::DiscoveryConfig::try_from_env()?;
        #[cfg(feature = "discovery-mdns")]
        if discovery.is_some() {
            if node.peer_authentication != PeerAuthenticationMode::Required {
                return Err(NodeError::Config(
                    "ORION_NODE_DISCOVERY=mdns requires ORION_NODE_PEER_AUTH=required (discovered peers must never be trusted on first contact)".into(),
                ));
            }
            if peer_tcp_addr.is_none() {
                return Err(NodeError::Config(
                    "ORION_NODE_DISCOVERY=mdns requires ORION_NODE_PEER_ADDR (the orion+tcp listener that is advertised)".into(),
                ));
            }
        }
        #[cfg(not(feature = "discovery-mdns"))]
        if env::var("ORION_NODE_DISCOVERY").is_ok_and(|value| {
            !matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "" | "off" | "none" | "disabled"
            )
        }) || env::var_os("ORION_NODE_ENROLLMENT_KEY").is_some()
            || env::var_os("ORION_NODE_ENROLLMENT_KEY_FILE").is_some()
        {
            return Err(NodeError::Config(
                "ORION_NODE_DISCOVERY and ORION_NODE_ENROLLMENT_KEY(_FILE) are not supported: orion-node was built without the `discovery-mdns` feature".into(),
            ));
        }

        #[cfg(not(feature = "transport-http"))]
        if http_tls_cert_path.is_some() || http_tls_key_path.is_some() || auto_http_tls {
            return Err(NodeError::Config(format!(
                "ORION_NODE_HTTP_TLS_CERT, ORION_NODE_HTTP_TLS_KEY and ORION_NODE_HTTP_TLS_AUTO are not supported: {HTTP_FEATURE_DISABLED}"
            )));
        }

        if !http_enabled {
            let http_peer = node.peers.iter().any(|peer| {
                matches!(
                    crate::peer::PeerTransportKind::from_base_url(peer.base_url.as_str()),
                    Ok(crate::peer::PeerTransportKind::Http
                        | crate::peer::PeerTransportKind::Https)
                )
            });
            if http_peer {
                return Err(NodeError::Config(
                    "ORION_NODE_HTTP_ADDR=off cannot be combined with http:// or https:// entries in ORION_NODE_PEERS; HTTP peer sync requires the HTTP listener (use orion+tcp:// peers instead)".into(),
                ));
            }
            if http_tls_cert_path.is_some() || http_tls_key_path.is_some() || auto_http_tls {
                return Err(NodeError::Config(
                    "ORION_NODE_HTTP_ADDR=off cannot be combined with HTTP TLS settings".into(),
                ));
            }
        }

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
                http_enabled,
                runtime_threads,
                peer_tcp_addr,
                #[cfg(feature = "link-gateway")]
                links,
                #[cfg(feature = "discovery-mdns")]
                discovery,
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

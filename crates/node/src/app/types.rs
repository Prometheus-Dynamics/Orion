use crate::service::ControlOperation;
#[cfg(feature = "transport-quic")]
use orion::transport::quic::QuicTransportError;
#[cfg(feature = "transport-tcp")]
use orion::transport::tcp::TcpTransportError;
use orion::{
    ExecutorId, NodeId, ProviderId, Revision,
    control_plane::{ClientRole, MutationApplyError},
    runtime::{ExecutorCommand, RuntimeError},
    transport::{
        http::HttpTransportError,
        ipc::{IpcTransportError, LocalAddress},
    },
};
use std::{path::PathBuf, sync::Arc};
use thiserror::Error;
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HttpMutualTlsMode {
    Disabled,
    Optional,
    Required,
}

impl HttpMutualTlsMode {
    /// Parses `ORION_NODE_HTTP_MTLS` without panicking on invalid values.
    pub fn try_from_env() -> Result<Self, NodeError> {
        crate::config::parse_env_choice(
            "ORION_NODE_HTTP_MTLS",
            "disabled",
            &[
                ("disabled", Self::Disabled),
                ("optional", Self::Optional),
                ("required", Self::Required),
            ],
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeSnapshot {
    pub node_id: NodeId,
    pub desired_revision: Revision,
    pub observed_revision: Revision,
    pub applied_revision: Revision,
    pub registered_peers: usize,
    pub registered_providers: usize,
    pub registered_executors: usize,
    pub pending_commands: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeTickReport {
    pub local_node_id: NodeId,
    pub desired_revision: Revision,
    pub applied_revision: Revision,
    pub commands: Vec<ExecutorCommand>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerSyncExecution {
    Serial,
    Parallel { max_in_flight: usize },
}

#[derive(Debug, Error)]
pub enum NodeError {
    #[error("invalid node configuration: {0}")]
    Config(String),
    #[error("failed to acquire node state lock for {0}")]
    LockPoisoned(&'static str),
    #[error("provider {0} is already registered")]
    DuplicateProvider(ProviderId),
    #[error("executor {0} is already registered")]
    DuplicateExecutor(ExecutorId),
    #[error("peer {0} is already registered")]
    DuplicatePeer(NodeId),
    #[error("peer {0} is not registered")]
    UnknownPeer(NodeId),
    #[error("provider {provider_id} belongs to node {found}, not local node {expected}")]
    ProviderNodeMismatch {
        provider_id: ProviderId,
        expected: NodeId,
        found: NodeId,
    },
    #[error("executor {executor_id} belongs to node {found}, not local node {expected}")]
    ExecutorNodeMismatch {
        executor_id: ExecutorId,
        expected: NodeId,
        found: NodeId,
    },
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error(transparent)]
    MutationApply(#[from] MutationApplyError),
    #[error(transparent)]
    HttpTransport(#[from] HttpTransportError),
    #[error(transparent)]
    IpcTransport(#[from] IpcTransportError),
    #[cfg(feature = "transport-tcp")]
    #[error(transparent)]
    TcpTransport(#[from] TcpTransportError),
    #[cfg(feature = "transport-quic")]
    #[error(transparent)]
    QuicTransport(#[from] QuicTransportError),
    #[error("persistence worker is not available")]
    PersistenceWorkerUnavailable,
    #[error("persistence worker terminated")]
    PersistenceWorkerTerminated,
    #[error("auth state worker is not available")]
    AuthStateWorkerUnavailable,
    #[error("auth state worker terminated")]
    AuthStateWorkerTerminated,
    #[error("storage operation failed: {action} `{path}`: {message}")]
    StoragePath {
        action: &'static str,
        path: PathBuf,
        message: String,
    },
    #[error("storage operation failed: {0}")]
    Storage(String),
    #[error("startup failed: failed to spawn {context}: {message}")]
    StartupSpawn {
        context: &'static str,
        message: String,
    },
    #[error("startup failed: failed to install ctrl-c listener: {message}")]
    StartupSignalListener { message: String },
    #[error("startup failed: {0}")]
    Startup(String),
    #[error("local client {0} is not registered")]
    UnknownClient(LocalAddress),
    #[error("local client {client} role mismatch: expected {expected:?}, found {found:?}")]
    ClientRoleMismatch {
        client: LocalAddress,
        expected: ClientRole,
        found: ClientRole,
    },
    #[error("local client {client} exceeded local message rate limit")]
    RateLimitedClient { client: LocalAddress },
    #[error("authentication failed: invalid public key length")]
    InvalidPublicKeyLength,
    #[error("authentication failed: invalid signature length")]
    InvalidSignatureLength,
    #[error("storage operation failed: invalid hex-encoded public key")]
    InvalidHexPublicKey,
    #[error("authentication failed: {0}")]
    Authentication(String),
    #[error("authorization failed: authenticated peer identity is required for {operation:?}")]
    AuthenticatedPeerRequired { operation: ControlOperation },
    #[error("authorization failed: configured peer identity is required for {operation:?}")]
    ConfiguredPeerRequired { operation: ControlOperation },
    #[error("authorization failed: {0}")]
    Authorization(String),
}

#[derive(Clone)]
pub struct ReconcileLoopHandle {
    pub(super) shutdown_tx: watch::Sender<bool>,
    pub(super) task: Arc<std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl ReconcileLoopHandle {
    pub fn stop(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    pub async fn shutdown(self) {
        self.stop();
        let handle = self.task.lock().ok().and_then(|mut task| task.take());
        if let Some(handle) = handle {
            let _ = handle.await;
        }
    }
}

pub(super) fn is_https_base_url(base_url: &str) -> bool {
    base_url
        .get(..8)
        .map(|scheme| scheme.eq_ignore_ascii_case("https://"))
        .unwrap_or(false)
}

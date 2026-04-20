mod builder;
mod desired_state;
mod desired_sync;
mod local_clients;
mod local_control;
mod maintenance_admin;
mod observability;
mod peer_sync;
mod peer_sync_client;
mod peer_sync_parallel;
mod peer_sync_response;
mod peer_sync_state;
mod persistence;
mod reconcile;
mod startup;
mod startup_loops;
mod state_access;
mod status_admin;
mod task_handle;
mod tls_bootstrap;
mod types;
#[cfg(test)]
pub(crate) use observability::{clear_test_audit_append_delay, set_test_audit_append_delay};
#[cfg(test)]
pub(crate) use persistence::{clear_test_persist_delay, set_test_persist_delay};
pub(crate) use task_handle::GracefulTaskHandle;
pub use types::{
    HttpMutualTlsMode, NodeError, NodeSnapshot, NodeTickReport, PeerSyncExecution,
    ReconcileLoopHandle,
};

use crate::auth::{LocalAuthenticationMode, NodeSecurity};
use crate::config::NodeConfig;
use crate::peer::{PeerConfig, PeerState};
use crate::service::ControlMiddlewareHandle;
use crate::storage::NodeStorage;
use crate::transport_security::{
    ManagedClientTransportSecurity, ManagedNodeTransportSurface, ManagedServerTransportSecurity,
    ManagedTransportProtocol, NodeTransportSecurityManager,
};
use desired_sync::{
    all_desired_sections, changed_sections, diff_desired_against_summary_sections,
    empty_summary_for_sections,
};
use local_clients::{ClientRegistryTxn, LocalClientState};
use observability::{
    AuditEventKind, AuditLogSink, LifecycleSnapshot, LifecycleState, ObservabilityState,
    ObservabilityTxn, classify_node_error, classify_peer_sync_error, classify_peer_sync_error_kind,
    is_client_auth_tls_error, peer_sync_troubleshooting_hint, push_observability_event,
    write_audit_record,
};
#[cfg(feature = "transport-quic")]
use orion::transport::quic::{QuicClientTlsConfig, QuicServerTlsConfig, QuicTransport};
#[cfg(feature = "transport-tcp")]
use orion::transport::tcp::{TcpClientTlsConfig, TcpServerTlsConfig, TcpTransport};
use orion::{
    ArchiveEncode, ArtifactId, ExecutorId, NodeId, ProviderId, ResourceId, Revision, WorkloadId,
    control_plane::{
        AppliedClusterState, DesiredClusterState, DesiredStateMutation, DesiredStateObjectSelector,
        DesiredStateSection, DesiredStateSectionFingerprints, DesiredStateSummary, MutationBatch,
        ObservabilityEventKind, ObservedClusterState, WorkloadRecord,
    },
    encode_to_vec,
    runtime::{ExecutorIntegration, ProviderIntegration, Runtime},
    transport::{
        http::{HttpTransport, HttpTransportError},
        ipc::{IpcTransport, IpcTransportError, LocalAddress},
    },
};
use orion_core::PeerBaseUrl;
use state_access::NodeState;
use std::{
    collections::BTreeMap,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tls_bootstrap::stable_fingerprint;
use tracing::{error, info, warn};
use types::is_https_base_url;

type ProviderRegistry = BTreeMap<ProviderId, Arc<dyn ProviderIntegration + Send + Sync>>;
type ExecutorRegistry = BTreeMap<ExecutorId, Arc<dyn ExecutorIntegration + Send + Sync>>;
type PeerRegistry = BTreeMap<NodeId, PeerState>;
type ClientRegistry = BTreeMap<LocalAddress, LocalClientState>;
type PeerClientRegistry = BTreeMap<NodeId, CachedPeerClient>;

#[derive(Clone)]
struct CachedPeerClient {
    base_url: PeerBaseUrl,
    tls_root_cert_path: Option<PathBuf>,
    tls_root_cert_fingerprint: Option<u64>,
    uses_client_identity: bool,
    client: orion::transport::http::HttpClient,
}

#[derive(Clone, Debug)]
pub(crate) struct DesiredStateMetadataCache {
    revision: Revision,
    fingerprint: u64,
    section_fingerprints: DesiredStateSectionFingerprints,
}

#[derive(Clone, Debug)]
pub(crate) struct DesiredStateSummaryCache {
    revision: Revision,
    summaries_by_mask: BTreeMap<u8, DesiredStateSummary>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ClusterRevisionState {
    desired: Revision,
    observed: Revision,
    applied: Revision,
}

/// Configured Orion node runtime.
///
/// Preferred construction:
///
/// - [`NodeApp::try_new`] for a direct config-to-runtime path.
/// - [`NodeApp::builder`] when transport/runtime overrides are needed.
#[derive(Clone)]
pub struct NodeApp {
    pub config: NodeConfig,
    pub runtime: Runtime,
    state: Arc<NodeState>,
    storage: Option<NodeStorage>,
    persistence_worker: Option<Arc<persistence::PersistenceWorker>>,
    http_tls_cert_path: Option<PathBuf>,
    http_tls_key_path: Option<PathBuf>,
    auto_http_tls: bool,
    http_mutual_tls_mode: HttpMutualTlsMode,
    transport_security: Arc<NodeTransportSecurityManager>,
    audit_log: Option<Arc<AuditLogSink>>,
    pub(crate) security: Arc<NodeSecurity>,
    pub(crate) control_middlewares: Arc<[ControlMiddlewareHandle]>,
    pub http: HttpTransport,
    pub ipc: IpcTransport,
    #[cfg(feature = "transport-tcp")]
    pub tcp: TcpTransport,
    #[cfg(feature = "transport-quic")]
    pub quic: QuicTransport,
}

impl NodeApp {
    /// Builds a node app from an explicit config without panicking on startup/config errors.
    pub fn try_new(config: NodeConfig) -> Result<Self, NodeError> {
        Self::builder().config(config).try_build()
    }

    /// Returns the preferred customization surface for embedded/tests-only startup flows.
    pub fn builder() -> NodeAppBuilder {
        NodeAppBuilder::default()
    }

    pub fn http_tls_cert_path(&self) -> Option<&Path> {
        self.http_tls_cert_path.as_deref()
    }

    pub fn http_tls_key_path(&self) -> Option<&Path> {
        self.http_tls_key_path.as_deref()
    }

    pub fn transport_security(&self) -> &NodeTransportSecurityManager {
        &self.transport_security
    }

    pub fn managed_server_transport_security(
        &self,
        protocol: ManagedTransportProtocol,
    ) -> Result<Option<ManagedServerTransportSecurity>, NodeError> {
        self.transport_security.server_security(protocol)
    }

    pub fn managed_client_transport_security(
        &self,
        protocol: ManagedTransportProtocol,
        node_id: &NodeId,
        tls_root_cert_path: Option<&str>,
        server_name: Option<&str>,
        endpoint_hint: Option<&str>,
    ) -> Result<Option<ManagedClientTransportSecurity>, NodeError> {
        self.transport_security.client_security(
            protocol,
            node_id,
            tls_root_cert_path,
            server_name,
            endpoint_hint,
        )
    }

    pub fn managed_surface_server_transport_security(
        &self,
        surface: ManagedNodeTransportSurface,
    ) -> Result<Option<ManagedServerTransportSecurity>, NodeError> {
        self.transport_security.surface_server_security(surface)
    }

    pub fn managed_surface_client_transport_security(
        &self,
        surface: ManagedNodeTransportSurface,
        node_id: &NodeId,
        tls_root_cert_path: Option<&str>,
        server_name: Option<&str>,
        endpoint_hint: Option<&str>,
    ) -> Result<Option<ManagedClientTransportSecurity>, NodeError> {
        self.transport_security.surface_client_security(
            surface,
            node_id,
            tls_root_cert_path,
            server_name,
            endpoint_hint,
        )
    }

    #[cfg(feature = "transport-tcp")]
    pub fn tcp_server_tls_config(&self) -> Result<Option<TcpServerTlsConfig>, NodeError> {
        self.transport_security.tcp_server_config()
    }

    #[cfg(feature = "transport-tcp")]
    pub fn tcp_client_tls_config(
        &self,
        node_id: &NodeId,
        tls_root_cert_path: Option<&str>,
        server_name: &str,
    ) -> Result<Option<TcpClientTlsConfig>, NodeError> {
        self.transport_security
            .tcp_client_config(node_id, tls_root_cert_path, server_name)
    }

    #[cfg(feature = "transport-quic")]
    pub fn quic_server_tls_config(&self) -> Result<Option<QuicServerTlsConfig>, NodeError> {
        self.transport_security.quic_server_config()
    }

    #[cfg(feature = "transport-quic")]
    pub fn quic_client_tls_config(
        &self,
        node_id: &NodeId,
        tls_root_cert_path: Option<&str>,
    ) -> Result<Option<QuicClientTlsConfig>, NodeError> {
        self.transport_security
            .quic_client_config(node_id, tls_root_cert_path)
    }

    pub fn register_provider<P>(&self, provider: P) -> Result<ProviderId, NodeError>
    where
        P: ProviderIntegration + Send + Sync + 'static,
    {
        let provider = Arc::new(provider);
        let record = provider.provider_record();
        if record.node_id != self.config.node_id {
            return Err(NodeError::ProviderNodeMismatch {
                provider_id: record.provider_id,
                expected: self.config.node_id.clone(),
                found: record.node_id,
            });
        }

        {
            let mut providers = self.providers_lock();
            if providers.contains_key(&record.provider_id) {
                return Err(NodeError::DuplicateProvider(record.provider_id));
            }
            providers.insert(record.provider_id.clone(), provider);
        }

        self.put_provider_record_tracked(record.clone())?;
        Ok(record.provider_id)
    }

    pub fn register_executor<E>(&self, executor: E) -> Result<ExecutorId, NodeError>
    where
        E: ExecutorIntegration + Send + Sync + 'static,
    {
        let executor = Arc::new(executor);
        let record = executor.executor_record();
        if record.node_id != self.config.node_id {
            return Err(NodeError::ExecutorNodeMismatch {
                executor_id: record.executor_id,
                expected: self.config.node_id.clone(),
                found: record.node_id,
            });
        }

        {
            let mut executors = self.executors_lock();
            if executors.contains_key(&record.executor_id) {
                return Err(NodeError::DuplicateExecutor(record.executor_id));
            }
            executors.insert(record.executor_id.clone(), executor);
        }

        self.put_executor_record_tracked(record.clone())?;
        Ok(record.executor_id)
    }

    fn record_replay_success(&self, duration: Duration) {
        self.state.lifecycle.set_replay_state(true);
        self.with_observability_txn(|txn| {
            txn.state_mut().replay.record_success(duration);
            txn.push_event(
                ObservabilityEventKind::Replay,
                true,
                Some(duration),
                "replay succeeded".into(),
            );
        });
        info!(node = %self.config.node_id, duration_ms = duration.as_millis() as u64, "replay succeeded");
    }

    fn record_replay_failure(&self, duration: Duration, error: &NodeError) {
        let message = error.to_string();
        let category = classify_node_error(error);
        self.state.lifecycle.set_replay_state(false);
        self.with_observability_txn(|txn| {
            txn.state_mut()
                .replay
                .record_failure(duration, category, message.clone());
            txn.push_event(
                ObservabilityEventKind::Replay,
                false,
                Some(duration),
                message,
            );
        });
        error!(
            node = %self.config.node_id,
            duration_ms = duration.as_millis() as u64,
            failure_category = ?category,
            error = %error,
            "replay failed"
        );
    }

    fn record_peer_sync_success(
        &self,
        node_id: &NodeId,
        duration: Duration,
    ) -> Result<(), NodeError> {
        self.with_peer_mut(node_id, |peer| {
            peer.note_sync_success();
        })?;
        self.with_observability_txn(|txn| {
            txn.state_mut().peer_sync.record_success(duration);
            txn.push_event_with_context(
                ObservabilityEventKind::PeerSync,
                true,
                Some(duration),
                Some(format!("peer-sync:{node_id}")),
                Some(node_id.to_string()),
                format!("peer sync succeeded node={node_id}"),
            );
        });
        info!(node = %self.config.node_id, peer = %node_id, duration_ms = duration.as_millis() as u64, "peer sync succeeded");
        Ok(())
    }

    fn record_peer_sync_failure(
        &self,
        node_id: Option<&NodeId>,
        duration: Duration,
        error: &NodeError,
    ) {
        let message = error.to_string();
        let category = classify_node_error(error);
        let error_kind = classify_peer_sync_error(error);
        if let Some(node_id) = node_id {
            let _ = self.with_peer_mut(node_id, |peer| {
                peer.set_last_error_kind(Some(error_kind));
            });
        }
        self.with_observability_txn(|txn| {
            txn.state_mut()
                .peer_sync
                .record_failure(duration, category, message.clone());
            txn.push_event_with_context(
                ObservabilityEventKind::PeerSync,
                false,
                Some(duration),
                node_id.map(|value| format!("peer-sync:{value}")),
                node_id.map(ToString::to_string),
                message,
            );
        });
        warn!(
            node = %self.config.node_id,
            peer = node_id.map(ToString::to_string).unwrap_or_else(|| "unknown".into()),
            duration_ms = duration.as_millis() as u64,
            failure_category = ?category,
            error = %error,
            "peer sync failed"
        );
    }

    fn record_reconcile_success(&self, duration: Duration) {
        let mut observability = self.observability_lock();
        observability.reconcile.record_success(duration);
        push_observability_event(
            &mut observability,
            ObservabilityEventKind::Reconcile,
            true,
            Some(duration),
            "reconcile succeeded".into(),
        );
        info!(node = %self.config.node_id, duration_ms = duration.as_millis() as u64, "reconcile succeeded");
    }

    fn record_reconcile_failure(&self, duration: Duration, error: &NodeError) {
        let message = error.to_string();
        let category = classify_node_error(error);
        let mut observability = self.observability_lock();
        observability
            .reconcile
            .record_failure(duration, category, message.clone());
        push_observability_event(
            &mut observability,
            ObservabilityEventKind::Reconcile,
            false,
            Some(duration),
            message,
        );
        warn!(
            node = %self.config.node_id,
            duration_ms = duration.as_millis() as u64,
            failure_category = ?category,
            error = %error,
            "reconcile failed"
        );
    }

    fn record_mutation_apply_success(&self, duration: Duration) {
        let mut observability = self.observability_lock();
        observability.mutation_apply.record_success(duration);
        push_observability_event(
            &mut observability,
            ObservabilityEventKind::MutationApply,
            true,
            Some(duration),
            "mutation apply succeeded".into(),
        );
        info!(node = %self.config.node_id, duration_ms = duration.as_millis() as u64, "mutation apply succeeded");
    }

    fn record_mutation_apply_failure(&self, duration: Duration, error: &NodeError) {
        let message = error.to_string();
        let category = classify_node_error(error);
        let mut observability = self.observability_lock();
        observability
            .mutation_apply
            .record_failure(duration, category, message.clone());
        push_observability_event(
            &mut observability,
            ObservabilityEventKind::MutationApply,
            false,
            Some(duration),
            message,
        );
        warn!(
            node = %self.config.node_id,
            duration_ms = duration.as_millis() as u64,
            failure_category = ?category,
            error = %error,
            "mutation apply failed"
        );
    }

    fn record_persistence_success(&self, duration: Duration) {
        let mut observability = self.observability_lock();
        observability.persistence.record_success(duration);
        push_observability_event(
            &mut observability,
            ObservabilityEventKind::Persistence,
            true,
            Some(duration),
            "state persistence succeeded".into(),
        );
        info!(node = %self.config.node_id, duration_ms = duration.as_millis() as u64, "state persistence succeeded");
    }

    fn record_persistence_failure(&self, duration: Duration, error: &NodeError) {
        let message = error.to_string();
        let category = classify_node_error(error);
        let mut observability = self.observability_lock();
        observability
            .persistence
            .record_failure(duration, category, message.clone());
        push_observability_event(
            &mut observability,
            ObservabilityEventKind::Persistence,
            false,
            Some(duration),
            message,
        );
        warn!(
            node = %self.config.node_id,
            duration_ms = duration.as_millis() as u64,
            failure_category = ?category,
            error = %error,
            "state persistence failed"
        );
    }

    fn record_artifact_write_success(&self, duration: Duration) {
        let mut observability = self.observability_lock();
        observability.artifact_write.record_success(duration);
        push_observability_event(
            &mut observability,
            ObservabilityEventKind::Persistence,
            true,
            Some(duration),
            "artifact write succeeded".into(),
        );
        info!(node = %self.config.node_id, duration_ms = duration.as_millis() as u64, "artifact write succeeded");
    }

    fn record_artifact_write_failure(&self, duration: Duration, error: &NodeError) {
        let message = error.to_string();
        let category = classify_node_error(error);
        let mut observability = self.observability_lock();
        observability
            .artifact_write
            .record_failure(duration, category, message.clone());
        push_observability_event(
            &mut observability,
            ObservabilityEventKind::Persistence,
            false,
            Some(duration),
            message,
        );
        warn!(
            node = %self.config.node_id,
            duration_ms = duration.as_millis() as u64,
            failure_category = ?category,
            error = %error,
            "artifact write failed"
        );
    }

    pub(crate) fn record_http_transport_error(&self, error: &HttpTransportError) {
        let mut observability = self.observability_lock();
        match error {
            HttpTransportError::DecodeRequest(_)
            | HttpTransportError::UnsupportedMethod(_)
            | HttpTransportError::UnsupportedPath(_)
            | HttpTransportError::UnsupportedControlMessage => {
                observability.http_malformed_input_count =
                    observability.http_malformed_input_count.saturating_add(1);
            }
            HttpTransportError::Tls(message) => {
                observability.http_tls_failures = observability.http_tls_failures.saturating_add(1);
                if is_client_auth_tls_error(message) {
                    observability.http_tls_client_auth_failures = observability
                        .http_tls_client_auth_failures
                        .saturating_add(1);
                }
                push_observability_event(
                    &mut observability,
                    ObservabilityEventKind::TransportSecurity,
                    false,
                    None,
                    format!("http tls failure: {message}"),
                );
                warn!(node = %self.config.node_id, error = %message, "http tls failure");
                self.record_audit_event(
                    AuditEventKind::TransportSecurityFailure,
                    None,
                    format!("http tls failure: {message}"),
                );
            }
            _ => {
                observability.http_request_failures =
                    observability.http_request_failures.saturating_add(1);
            }
        }
    }

    pub(crate) fn record_ipc_transport_error(&self, error: &IpcTransportError) {
        let mut observability = self.observability_lock();
        observability.ipc_frame_failures = observability.ipc_frame_failures.saturating_add(1);
        if matches!(error, IpcTransportError::DecodeFailed(_)) {
            observability.ipc_malformed_input_count =
                observability.ipc_malformed_input_count.saturating_add(1);
        }
    }
}

/// Builder for advanced node startup customization.
///
/// Prefer [`NodeApp::try_new`] when an explicit [`crate::NodeConfig`] is already available.
/// Reach for this builder when a caller needs to override transports, runtime tuning,
/// startup replay behavior, or middleware composition.
pub struct NodeAppBuilder {
    config: Option<NodeConfig>,
    desired: Option<DesiredClusterState>,
    observed: Option<ObservedClusterState>,
    applied: Option<AppliedClusterState>,
    peers: Option<Vec<PeerConfig>>,
    http: Option<HttpTransport>,
    ipc: Option<IpcTransport>,
    #[cfg(feature = "transport-tcp")]
    tcp: Option<TcpTransport>,
    #[cfg(feature = "transport-quic")]
    quic: Option<QuicTransport>,
    runtime_tuning: Option<crate::config::NodeRuntimeTuning>,
    http_tls_cert_path: Option<PathBuf>,
    http_tls_key_path: Option<PathBuf>,
    auto_http_tls: bool,
    http_mutual_tls_mode: Option<HttpMutualTlsMode>,
    audit_log_path: Option<PathBuf>,
    local_authentication_mode: Option<LocalAuthenticationMode>,
    control_middlewares: Vec<ControlMiddlewareHandle>,
    auto_startup_replay: bool,
}

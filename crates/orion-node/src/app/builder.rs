use super::{
    HttpMutualTlsMode, LifecycleState, NodeApp, NodeAppBuilder, ObservabilityState,
    persistence::PersistenceWorker,
    state_access::{
        AppLocalSessionLookup, ClientRegistryState, NodeState, PeerRegistryState, PersistedState,
        RuntimeRegistryState,
    },
};
use crate::app::tls_bootstrap::resolve_http_server_tls_paths;
use crate::auth::{LocalAuthenticationMode, NodeSecurity, PeerSecurityMiddleware};
use crate::config::{NodeConfig, NodeRuntimeTuning, normalize_runtime_tuning_duration};
use crate::peer::{PeerConfig, PeerState};
use crate::service::{ControlMiddleware, ControlMiddlewareHandle};
use crate::storage::NodeStorage;
use crate::transport_security::{NodeTransportSecurityManager, PeerTransportSecurityMode};
#[cfg(feature = "transport-quic")]
use orion::transport::quic::QuicTransport;
#[cfg(feature = "transport-tcp")]
use orion::transport::tcp::TcpTransport;
use orion::{
    control_plane::{
        AppliedClusterState, DesiredClusterState, MaintenanceState, ObservedClusterState,
    },
    runtime::{LocalRuntimeStore, Runtime},
    transport::{http::HttpTransport, ipc::IpcTransport},
};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, RwLock},
    time::Duration,
};
use tracing::warn;

impl Default for NodeAppBuilder {
    fn default() -> Self {
        Self {
            config: None,
            desired: None,
            observed: None,
            applied: None,
            peers: None,
            http: None,
            ipc: None,
            #[cfg(feature = "transport-tcp")]
            tcp: None,
            #[cfg(feature = "transport-quic")]
            quic: None,
            runtime_tuning: None,
            http_tls_cert_path: None,
            http_tls_key_path: None,
            auto_http_tls: false,
            http_mutual_tls_mode: None,
            audit_log_path: None,
            local_authentication_mode: None,
            control_middlewares: Vec::new(),
            auto_startup_replay: true,
        }
    }
}

impl NodeAppBuilder {
    fn runtime_tuning_mut(&mut self) -> &mut NodeRuntimeTuning {
        self.runtime_tuning
            .get_or_insert_with(NodeRuntimeTuning::default)
    }

    pub fn config(mut self, config: NodeConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Loads the base [`NodeConfig`] from environment while keeping the builder available
    /// for additional overrides before construction.
    pub fn try_from_env(self) -> Result<Self, super::NodeError> {
        Ok(self.config(NodeConfig::try_from_env()?))
    }

    pub fn desired(mut self, desired: DesiredClusterState) -> Self {
        self.desired = Some(desired);
        self
    }

    pub fn observed(mut self, observed: ObservedClusterState) -> Self {
        self.observed = Some(observed);
        self
    }

    pub fn applied(mut self, applied: AppliedClusterState) -> Self {
        self.applied = Some(applied);
        self
    }

    pub fn peers<I>(mut self, peers: I) -> Self
    where
        I: IntoIterator<Item = PeerConfig>,
    {
        self.peers = Some(peers.into_iter().collect());
        self
    }

    pub fn with_http(mut self, http: HttpTransport) -> Self {
        self.http = Some(http);
        self
    }

    pub fn with_ipc(mut self, ipc: IpcTransport) -> Self {
        self.ipc = Some(ipc);
        self
    }

    #[cfg(feature = "transport-tcp")]
    pub fn with_tcp(mut self, tcp: TcpTransport) -> Self {
        self.tcp = Some(tcp);
        self
    }

    #[cfg(feature = "transport-quic")]
    pub fn with_quic(mut self, quic: QuicTransport) -> Self {
        self.quic = Some(quic);
        self
    }

    pub fn with_startup_replay(mut self, enabled: bool) -> Self {
        self.auto_startup_replay = enabled;
        self
    }

    pub fn with_max_mutation_history_batches(mut self, max_batches: usize) -> Self {
        self.runtime_tuning_mut().max_mutation_history_batches = max_batches;
        self
    }

    pub fn with_max_mutation_history_bytes(mut self, max_bytes: usize) -> Self {
        self.runtime_tuning_mut().max_mutation_history_bytes = max_bytes;
        self
    }

    pub fn with_snapshot_rewrite_cadence(mut self, cadence: u64) -> Self {
        self.runtime_tuning_mut().snapshot_rewrite_cadence = cadence;
        self
    }

    pub fn with_peer_sync_backoff_base(mut self, duration: Duration) -> Self {
        let runtime_tuning = self.runtime_tuning_mut();
        runtime_tuning.peer_sync_backoff_base = normalize_runtime_tuning_duration(duration);
        runtime_tuning.peer_sync_backoff_max = runtime_tuning
            .peer_sync_backoff_max
            .max(runtime_tuning.peer_sync_backoff_base);
        self
    }

    pub fn with_peer_sync_backoff_max(mut self, duration: Duration) -> Self {
        let runtime_tuning = self.runtime_tuning_mut();
        runtime_tuning.peer_sync_backoff_max =
            normalize_runtime_tuning_duration(duration).max(runtime_tuning.peer_sync_backoff_base);
        self
    }

    pub fn with_peer_sync_backoff_jitter_ms(mut self, jitter_ms: u64) -> Self {
        self.runtime_tuning_mut().peer_sync_backoff_jitter_ms = jitter_ms;
        self
    }

    pub fn with_local_rate_limit_window(mut self, duration: Duration) -> Self {
        self.runtime_tuning_mut().local_rate_limit_window = duration;
        self
    }

    pub fn with_local_rate_limit_max_messages(mut self, max_messages: u32) -> Self {
        self.runtime_tuning_mut().local_rate_limit_max_messages = max_messages;
        self
    }

    pub fn with_local_session_ttl(mut self, duration: Duration) -> Self {
        self.runtime_tuning_mut().local_session_ttl = duration;
        self
    }

    pub fn with_local_stream_send_queue_capacity(mut self, capacity: usize) -> Self {
        self.runtime_tuning_mut().local_stream_send_queue_capacity = capacity;
        self
    }

    pub fn with_local_client_event_queue_limit(mut self, limit: usize) -> Self {
        self.runtime_tuning_mut().local_client_event_queue_limit = limit;
        self
    }

    pub fn with_persistence_worker_queue_capacity(mut self, capacity: usize) -> Self {
        self.runtime_tuning_mut().persistence_worker_queue_capacity = capacity;
        self
    }

    pub fn with_auth_state_worker_queue_capacity(mut self, capacity: usize) -> Self {
        self.runtime_tuning_mut().auth_state_worker_queue_capacity = capacity;
        self
    }

    pub fn with_audit_log_queue_capacity(mut self, capacity: usize) -> Self {
        self.runtime_tuning_mut().audit_log_queue_capacity = capacity;
        self
    }

    pub fn with_local_authentication_mode(mut self, mode: LocalAuthenticationMode) -> Self {
        self.local_authentication_mode = Some(mode);
        self
    }

    pub fn with_http_tls_files(
        mut self,
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
    ) -> Self {
        self.http_tls_cert_path = Some(cert_path.into());
        self.http_tls_key_path = Some(key_path.into());
        self
    }

    pub fn with_auto_http_tls(mut self, enabled: bool) -> Self {
        self.auto_http_tls = enabled;
        self
    }

    pub fn with_http_mutual_tls_mode(mut self, mode: HttpMutualTlsMode) -> Self {
        self.http_mutual_tls_mode = Some(mode);
        self
    }

    pub fn with_audit_log_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.audit_log_path = Some(path.into());
        self
    }

    pub fn with_control_middleware<M>(mut self, middleware: M) -> Self
    where
        M: ControlMiddleware,
    {
        self.control_middlewares.push(Arc::new(middleware));
        self
    }

    pub fn with_control_middleware_arc(mut self, middleware: ControlMiddlewareHandle) -> Self {
        self.control_middlewares.push(middleware);
        self
    }

    /// Installs the default HTTP, IPC, TCP, and QUIC transport implementations explicitly.
    ///
    /// Most callers do not need this because [`Self::try_build`] will already fall back to
    /// these defaults when no custom transports are provided.
    pub fn with_default_transports(self) -> Self {
        let builder = self
            .with_http(HttpTransport::new())
            .with_ipc(IpcTransport::new());
        #[cfg(feature = "transport-tcp")]
        let builder = builder.with_tcp(TcpTransport::new());
        #[cfg(feature = "transport-quic")]
        let builder = builder.with_quic(QuicTransport::new());
        builder
    }

    /// Builds the node app using either the provided config or [`NodeConfig::try_from_env`].
    ///
    /// This is the preferred builder finalizer for public use because it preserves typed
    /// startup/configuration errors.
    pub fn try_build(self) -> Result<NodeApp, super::NodeError> {
        let mut config = match self.config {
            Some(config) => config,
            None => NodeConfig::try_from_env()?,
        };
        let runtime = Runtime::new(config.node_id.clone());
        let mut store = LocalRuntimeStore::new(config.node_id.clone());
        store.replace_desired(self.desired.unwrap_or_default());
        store.observed = self.observed.unwrap_or_default();
        store.applied = self.applied.unwrap_or_default();
        let storage = config.state_dir.clone().map(NodeStorage::new);
        let maintenance_state = storage
            .as_ref()
            .map(NodeStorage::load_maintenance_state)
            .transpose()?
            .flatten()
            .unwrap_or_else(MaintenanceState::normal);
        store.set_maintenance(maintenance_state.clone());
        let (http_tls_cert_path, http_tls_key_path) = resolve_http_server_tls_paths(
            self.http_tls_cert_path,
            self.http_tls_key_path,
            self.auto_http_tls,
            storage.as_ref(),
            &config.node_id,
            config.http_bind_addr,
        )
        .map_err(|err| super::NodeError::Config(format!("failed to initialize HTTP TLS: {err}")))?;
        let peer_configs = self.peers.unwrap_or_else(|| config.peers.clone());
        if let Some(mut runtime_tuning) = self.runtime_tuning {
            runtime_tuning.normalize();
            config.runtime_tuning = runtime_tuning;
        }
        let local_authentication_mode = self
            .local_authentication_mode
            .map(Ok)
            .unwrap_or_else(LocalAuthenticationMode::try_from_env)?;
        let http_mutual_tls_mode = self
            .http_mutual_tls_mode
            .map(Ok)
            .unwrap_or_else(HttpMutualTlsMode::try_from_env)?;
        let security = Arc::new(
            NodeSecurity::load_or_create(
                config.node_id.clone(),
                config.peer_authentication,
                &peer_configs,
                storage.clone(),
                config.runtime_tuning.auth_state_worker_queue_capacity,
            )
            .map_err(|err| {
                super::NodeError::Config(format!("failed to initialize node security: {err}"))
            })?,
        );
        let transport_security = Arc::new(NodeTransportSecurityManager::new(
            security.clone(),
            http_tls_cert_path.clone(),
            http_tls_key_path.clone(),
            PeerTransportSecurityMode::from(http_mutual_tls_mode),
        ));
        let peers = peer_configs
            .clone()
            .into_iter()
            .map(|peer| (peer.node_id.clone(), PeerState::from_config(peer)))
            .collect();
        let observability_event_limit = config.runtime_tuning.observability_event_limit;
        let persistence_worker_queue_capacity =
            config.runtime_tuning.persistence_worker_queue_capacity;
        let audit_log_queue_capacity = config.runtime_tuning.audit_log_queue_capacity;
        let audit_log_overload_policy = config.runtime_tuning.audit_log_overload_policy;
        let state = Arc::new(NodeState {
            persisted: PersistedState {
                store: RwLock::new(store),
                mutation_history: RwLock::new(Vec::new()),
                mutation_history_baseline: RwLock::new(DesiredClusterState::default()),
                maintenance_state: RwLock::new(maintenance_state),
                desired_metadata_cache: RwLock::new(None),
                desired_summary_cache: RwLock::new(None),
            },
            peers: PeerRegistryState {
                peers: RwLock::new(peers),
                peer_clients: RwLock::new(BTreeMap::new()),
            },
            runtime: RuntimeRegistryState {
                providers: RwLock::new(BTreeMap::new()),
                executors: RwLock::new(BTreeMap::new()),
            },
            clients: ClientRegistryState {
                clients: RwLock::new(BTreeMap::new()),
            },
            observability: RwLock::new(ObservabilityState::with_event_limit(
                observability_event_limit,
            )),
            lifecycle: LifecycleState::default(),
        });
        let persistence_worker = storage
            .as_ref()
            .map(|storage| {
                PersistenceWorker::new(storage.clone(), persistence_worker_queue_capacity)
            })
            .transpose()?
            .map(Arc::new);
        let local_sessions = Arc::new(AppLocalSessionLookup {
            state: state.clone(),
        });

        let mut app = NodeApp {
            config,
            runtime,
            state,
            storage,
            persistence_worker,
            http_tls_cert_path,
            http_tls_key_path,
            auto_http_tls: self.auto_http_tls,
            http_mutual_tls_mode,
            transport_security,
            audit_log: self
                .audit_log_path
                .map(|path| {
                    super::AuditLogSink::new(
                        path,
                        audit_log_queue_capacity,
                        audit_log_overload_policy,
                    )
                })
                .transpose()?
                .map(Arc::new),
            security,
            control_middlewares: Arc::from(Vec::<ControlMiddlewareHandle>::new()),
            http: self.http.unwrap_or_default(),
            ipc: self.ipc.unwrap_or_default(),
            #[cfg(feature = "transport-tcp")]
            tcp: self.tcp.unwrap_or_default(),
            #[cfg(feature = "transport-quic")]
            quic: self.quic.unwrap_or_default(),
        };
        let mut control_middlewares = vec![Arc::new(PeerSecurityMiddleware::new(
            &app.security,
            local_authentication_mode,
            local_sessions,
        )) as ControlMiddlewareHandle];
        control_middlewares.extend(self.control_middlewares);
        app.control_middlewares = Arc::from(control_middlewares);

        if self.auto_startup_replay && app.storage.is_some() {
            if let Err(err) = app.replay_state() {
                warn!(
                    node = %app.config.node_id,
                    error = %err,
                    "startup replay returned an error during node build"
                );
            }
        } else if app.storage.is_none() {
            app.state.lifecycle.set_replay_state(true);
        }

        Ok(app)
    }
}

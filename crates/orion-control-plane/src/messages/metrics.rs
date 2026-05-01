use super::maintenance::MaintenanceState;
use orion_core::{NodeId, PeerBaseUrl, PublicKeyHex, Revision};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt};

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum PeerSyncStatus {
    Configured,
    Negotiating,
    Ready,
    BackingOff,
    Syncing,
    Synced,
    Error,
}

impl fmt::Display for PeerSyncStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Configured => "configured",
            Self::Negotiating => "negotiating",
            Self::Ready => "ready",
            Self::BackingOff => "backing_off",
            Self::Syncing => "syncing",
            Self::Synced => "synced",
            Self::Error => "error",
        };
        f.write_str(value)
    }
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
#[serde(rename_all = "kebab-case")]
pub enum PeerSyncErrorKind {
    TlsTrust,
    TlsHandshake,
    AuthPolicy,
    TransportConnectivity,
    PeerSync,
}

impl fmt::Display for PeerSyncErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::TlsTrust => "tls-trust",
            Self::TlsHandshake => "tls-handshake",
            Self::AuthPolicy => "auth-policy",
            Self::TransportConnectivity => "transport-connectivity",
            Self::PeerSync => "peer-sync",
        };
        f.write_str(value)
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum HttpMutualTlsMode {
    Disabled,
    Optional,
    Required,
}

impl fmt::Display for HttpMutualTlsMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Disabled => "disabled",
            Self::Optional => "optional",
            Self::Required => "required",
        };
        f.write_str(value)
    }
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct PeerTrustRecord {
    pub node_id: NodeId,
    pub base_url: Option<PeerBaseUrl>,
    pub configured_public_key_hex: Option<PublicKeyHex>,
    pub trusted_public_key_hex: Option<PublicKeyHex>,
    pub configured_tls_root_cert_path: Option<String>,
    pub learned_tls_root_cert_fingerprint: Option<String>,
    pub revoked: bool,
    pub sync_status: Option<PeerSyncStatus>,
    pub last_error: Option<String>,
    pub last_error_kind: Option<PeerSyncErrorKind>,
    pub troubleshooting_hint: Option<String>,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct PeerTrustSnapshot {
    pub http_mutual_tls_mode: HttpMutualTlsMode,
    pub peers: Vec<PeerTrustRecord>,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct OperationMetricsSnapshot {
    pub success_count: u64,
    pub failure_count: u64,
    pub total_duration_ms: u64,
    pub last_duration_ms: Option<u64>,
    pub max_duration_ms: u64,
    pub latency: LatencyMetricsSnapshot,
    pub last_error_category: Option<OperationFailureCategory>,
    pub last_error: Option<String>,
}

#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct LatencyMetricsSnapshot {
    pub samples_total: u64,
    pub total_duration_ms: u64,
    pub last_duration_ms: Option<u64>,
    pub max_duration_ms: u64,
    pub bucket_le_1_ms: u64,
    pub bucket_le_5_ms: u64,
    pub bucket_le_10_ms: u64,
    pub bucket_le_50_ms: u64,
    pub bucket_le_100_ms: u64,
    pub bucket_le_500_ms: u64,
    pub bucket_gt_500_ms: u64,
}

#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct CommunicationStageMetricsSnapshot {
    pub queue_wait: LatencyMetricsSnapshot,
    pub encode: LatencyMetricsSnapshot,
    pub decode: LatencyMetricsSnapshot,
    pub socket_read: LatencyMetricsSnapshot,
    pub socket_write: LatencyMetricsSnapshot,
    pub retry_delay: LatencyMetricsSnapshot,
    pub backpressure: LatencyMetricsSnapshot,
}

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum CommunicationFailureKind {
    Timeout,
    Refused,
    Tls,
    Protocol,
    Decode,
    UnavailablePeer,
    Canceled,
    Transport,
    Unknown,
}

impl fmt::Display for CommunicationFailureKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Timeout => "timeout",
            Self::Refused => "refused",
            Self::Tls => "tls",
            Self::Protocol => "protocol",
            Self::Decode => "decode",
            Self::UnavailablePeer => "unavailable_peer",
            Self::Canceled => "canceled",
            Self::Transport => "transport",
            Self::Unknown => "unknown",
        };
        f.write_str(value)
    }
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CommunicationFailureCountSnapshot {
    pub kind: CommunicationFailureKind,
    pub count: u64,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CommunicationRecentMetricsSnapshot {
    pub window_ms: u64,
    pub successes: u64,
    pub failures: u64,
    pub avg_latency_ms: Option<u64>,
    pub max_latency_ms: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
}

impl Default for CommunicationRecentMetricsSnapshot {
    fn default() -> Self {
        Self {
            window_ms: 300_000,
            successes: 0,
            failures: 0,
            avg_latency_ms: None,
            max_latency_ms: 0,
            bytes_sent: 0,
            bytes_received: 0,
        }
    }
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CommunicationMetricsSnapshot {
    pub messages_sent_total: u64,
    pub messages_received_total: u64,
    pub bytes_sent_total: u64,
    pub bytes_received_total: u64,
    pub estimated_wire_bytes_sent_total: u64,
    pub estimated_wire_bytes_received_total: u64,
    pub failures_total: u64,
    pub failures_by_kind: Vec<CommunicationFailureCountSnapshot>,
    pub reconnects_total: u64,
    pub last_success_at_ms: Option<u64>,
    pub last_failure_at_ms: Option<u64>,
    pub last_error: Option<String>,
    pub latency: LatencyMetricsSnapshot,
    pub stages: CommunicationStageMetricsSnapshot,
    pub recent: CommunicationRecentMetricsSnapshot,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum CommunicationTransportKind {
    Http,
    Ipc,
    Tcp,
    Quic,
    Other(String),
}

impl CommunicationTransportKind {
    pub fn from_label(value: impl Into<String>) -> Self {
        match value.into().as_str() {
            "http" => Self::Http,
            "ipc" => Self::Ipc,
            "tcp" => Self::Tcp,
            "quic" => Self::Quic,
            other => Self::Other(other.to_owned()),
        }
    }

    pub fn as_label(&self) -> &str {
        match self {
            Self::Http => "http",
            Self::Ipc => "ipc",
            Self::Tcp => "tcp",
            Self::Quic => "quic",
            Self::Other(value) => value,
        }
    }
}

impl fmt::Display for CommunicationTransportKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

impl PartialEq<&str> for CommunicationTransportKind {
    fn eq(&self, other: &&str) -> bool {
        self.as_label() == *other
    }
}

impl PartialEq<String> for CommunicationTransportKind {
    fn eq(&self, other: &String) -> bool {
        self.as_label() == other
    }
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum CommunicationEndpointScope {
    Control,
    Health,
    Readiness,
    Metrics,
    Probe,
    PeerSync,
    LocalUnary,
    LocalStream,
    ClientLocalUnary,
    ClientLocalStream,
    DataPlane,
    Other(String),
}

impl CommunicationEndpointScope {
    pub fn from_label(value: impl Into<String>) -> Self {
        match value.into().as_str() {
            "control" => Self::Control,
            "health" => Self::Health,
            "readiness" => Self::Readiness,
            "metrics" => Self::Metrics,
            "probe" => Self::Probe,
            "peer_sync" => Self::PeerSync,
            "local_unary" => Self::LocalUnary,
            "local_stream" => Self::LocalStream,
            "client_local_unary" => Self::ClientLocalUnary,
            "client_local_stream" => Self::ClientLocalStream,
            "data_plane" => Self::DataPlane,
            other => Self::Other(other.to_owned()),
        }
    }

    pub fn as_label(&self) -> &str {
        match self {
            Self::Control => "control",
            Self::Health => "health",
            Self::Readiness => "readiness",
            Self::Metrics => "metrics",
            Self::Probe => "probe",
            Self::PeerSync => "peer_sync",
            Self::LocalUnary => "local_unary",
            Self::LocalStream => "local_stream",
            Self::ClientLocalUnary => "client_local_unary",
            Self::ClientLocalStream => "client_local_stream",
            Self::DataPlane => "data_plane",
            Self::Other(value) => value,
        }
    }

    pub fn is_local_stream(&self) -> bool {
        matches!(self, Self::LocalStream | Self::ClientLocalStream)
    }
}

impl fmt::Display for CommunicationEndpointScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

impl PartialEq<&str> for CommunicationEndpointScope {
    fn eq(&self, other: &&str) -> bool {
        self.as_label() == *other
    }
}

impl PartialEq<String> for CommunicationEndpointScope {
    fn eq(&self, other: &String) -> bool {
        self.as_label() == other
    }
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CommunicationEndpointSnapshot {
    pub id: String,
    pub transport: CommunicationTransportKind,
    pub scope: CommunicationEndpointScope,
    pub local: Option<String>,
    pub remote: Option<String>,
    pub labels: BTreeMap<String, String>,
    pub connected: bool,
    pub queued: Option<u64>,
    pub metrics: CommunicationMetricsSnapshot,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct HostMetricsSnapshot {
    pub hostname: Option<String>,
    pub os_name: String,
    pub os_version: Option<String>,
    pub kernel_version: Option<String>,
    pub architecture: String,
    pub uptime_seconds: Option<u64>,
    pub load_1_milli: Option<u64>,
    pub load_5_milli: Option<u64>,
    pub load_15_milli: Option<u64>,
    pub memory_total_bytes: Option<u64>,
    pub memory_available_bytes: Option<u64>,
    pub swap_total_bytes: Option<u64>,
    pub swap_free_bytes: Option<u64>,
    pub process_id: u32,
    pub process_rss_bytes: Option<u64>,
    pub process_pss_bytes: Option<u64>,
    pub process_private_dirty_bytes: Option<u64>,
    pub process_anonymous_bytes: Option<u64>,
    pub process_vm_size_bytes: Option<u64>,
    pub process_vm_data_bytes: Option<u64>,
    pub process_vm_hwm_bytes: Option<u64>,
    pub process_threads: Option<u64>,
    pub process_fd_count: Option<u64>,
}

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum OperationFailureCategory {
    Concurrency,
    Validation,
    Authentication,
    Authorization,
    RateLimit,
    Transport,
    Storage,
    Runtime,
    UnknownPeer,
    StateConflict,
    Internal,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ClientSessionMetricsSnapshot {
    pub registered_clients: u64,
    pub live_stream_clients: u64,
    pub state_watch_clients: u64,
    pub executor_watch_clients: u64,
    pub provider_watch_clients: u64,
    pub queued_client_events: u64,
    pub registrations_total: u64,
    pub stream_attaches_total: u64,
    pub stream_detaches_total: u64,
    pub stale_evictions_total: u64,
    pub rate_limited_total: u64,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct TransportMetricsSnapshot {
    pub http_request_failures: u64,
    pub http_malformed_input_count: u64,
    pub http_tls_failures: u64,
    pub http_tls_client_auth_failures: u64,
    pub ipc_frame_failures: u64,
    pub ipc_malformed_input_count: u64,
    pub reconnect_count: u64,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct PersistenceMetricsSnapshot {
    pub state_persist: OperationMetricsSnapshot,
    pub artifact_write: OperationMetricsSnapshot,
    pub worker_queue_capacity: u64,
    pub worker_operation_count: u64,
    pub worker_queue_wait_count: u64,
    pub worker_queue_wait_ms_total: u64,
    pub worker_queue_wait_ms_max: u64,
    pub worker_reply_wait_count: u64,
    pub worker_reply_wait_ms_total: u64,
    pub worker_reply_wait_ms_max: u64,
    pub worker_operation_ms_total: u64,
    pub worker_operation_ms_max: u64,
    pub worker_enqueue_count: u64,
    pub worker_enqueue_wait_count: u64,
    pub worker_enqueue_wait_ms_total: u64,
    pub worker_enqueue_wait_ms_max: u64,
}

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum NodeHealthStatus {
    Healthy,
    Degraded,
}

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum NodeReadinessStatus {
    Ready,
    NotReady,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct NodeHealthSnapshot {
    pub node_id: NodeId,
    pub alive: bool,
    pub replay_completed: bool,
    pub replay_successful: bool,
    pub http_bound: bool,
    pub ipc_bound: bool,
    pub ipc_stream_bound: bool,
    pub degraded_peer_count: u64,
    pub status: NodeHealthStatus,
    pub reasons: Vec<String>,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct NodeReadinessSnapshot {
    pub node_id: NodeId,
    pub ready: bool,
    pub replay_completed: bool,
    pub replay_successful: bool,
    pub http_bound: bool,
    pub ipc_bound: bool,
    pub ipc_stream_bound: bool,
    pub initial_sync_complete: bool,
    pub ready_peer_count: u64,
    pub pending_peer_count: u64,
    pub degraded_peer_count: u64,
    pub status: NodeReadinessStatus,
    pub reasons: Vec<String>,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum ObservabilityEventKind {
    Replay,
    PeerSync,
    Reconcile,
    MutationApply,
    Persistence,
    TransportSecurity,
    ClientRegistration,
    ClientStreamAttach,
    ClientStreamDetach,
    ClientStaleEviction,
    ClientRateLimited,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ObservabilityEvent {
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub kind: ObservabilityEventKind,
    pub success: bool,
    pub duration_ms: Option<u64>,
    pub correlation_id: Option<String>,
    pub subject: Option<String>,
    pub detail: String,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum AuditLogBackpressureMode {
    Block,
    DropNewest,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct NodeObservabilitySnapshot {
    pub node_id: NodeId,
    pub desired_revision: Revision,
    pub observed_revision: Revision,
    pub applied_revision: Revision,
    pub maintenance: MaintenanceState,
    pub peer_sync_paused: bool,
    pub remote_desired_state_blocked: bool,
    pub host: HostMetricsSnapshot,
    pub configured_peer_count: u64,
    pub ready_peer_count: u64,
    pub pending_peer_count: u64,
    pub degraded_peer_count: u64,
    pub peer_sync_parallel_in_flight_cap: u64,
    pub replay: OperationMetricsSnapshot,
    pub peer_sync: OperationMetricsSnapshot,
    pub reconcile: OperationMetricsSnapshot,
    pub mutation_apply: OperationMetricsSnapshot,
    pub persistence: PersistenceMetricsSnapshot,
    pub audit_log_queue_capacity: u64,
    pub audit_log_backpressure_mode: AuditLogBackpressureMode,
    pub audit_log_dropped_records: u64,
    pub client_sessions: ClientSessionMetricsSnapshot,
    pub transport: TransportMetricsSnapshot,
    pub communication: Vec<CommunicationEndpointSnapshot>,
    pub recent_events: Vec<ObservabilityEvent>,
}

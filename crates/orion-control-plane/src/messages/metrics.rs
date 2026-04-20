use super::maintenance::MaintenanceState;
use orion_core::{NodeId, PeerBaseUrl, PublicKeyHex, Revision};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

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
    pub last_error_category: Option<OperationFailureCategory>,
    pub last_error: Option<String>,
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
    pub recent_events: Vec<ObservabilityEvent>,
}

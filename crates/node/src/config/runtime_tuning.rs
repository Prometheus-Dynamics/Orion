use crate::NodeError;
use orion::control_plane::ClockSourceKind;
use orion_transport_common::{
    DEFAULT_MAX_TRANSPORT_PAYLOAD_BYTES, DEFAULT_TRANSPORT_IO_TIMEOUT,
    DEFAULT_TRANSPORT_MAX_CONCURRENT_CONNECTIONS,
};
use std::{env, time::Duration};

const DEFAULT_MUTATION_HISTORY_BATCHES: usize = 256;
const DEFAULT_MUTATION_HISTORY_BYTES: usize = 1024 * 1024;
const DEFAULT_SNAPSHOT_REWRITE_CADENCE: u64 = 1;
const DEFAULT_PEER_SYNC_BACKOFF_BASE_MS: u64 = 250;
const DEFAULT_PEER_SYNC_BACKOFF_MAX_MS: u64 = 5_000;
const DEFAULT_PEER_SYNC_BACKOFF_JITTER_MS: u64 = 150;
const DEFAULT_PEER_SYNC_SMALL_CLUSTER_THRESHOLD: usize = 4;
const DEFAULT_PEER_SYNC_SMALL_CLUSTER_CAP: usize = 4;
const DEFAULT_PEER_SYNC_LARGE_CLUSTER_CAP: usize = 3;
const DEFAULT_PEER_SYNC_NO_STAGGER_THRESHOLD: usize = 4;
const DEFAULT_PEER_SYNC_SPAWN_STAGGER_STEP_MS: u64 = 5;
const DEFAULT_PEER_SYNC_SPAWN_STAGGER_MAX_MS: u64 = 20;
const DEFAULT_PEER_SYNC_FOLLOWUP_STAGGER_MS: u64 = 5;
const DEFAULT_LOCAL_RATE_LIMIT_WINDOW_MS: u64 = 1_000;
const DEFAULT_LOCAL_RATE_LIMIT_MAX_MESSAGES: u32 = 256;
const DEFAULT_LOCAL_SESSION_TTL_MS: u64 = 300_000;
const DEFAULT_LOCAL_STREAM_SEND_QUEUE_CAPACITY: usize = 64;
const DEFAULT_LOCAL_CLIENT_EVENT_QUEUE_LIMIT: usize = 256;
const DEFAULT_OBSERVABILITY_EVENT_LIMIT: usize = 128;
const DEFAULT_PERSISTENCE_WORKER_QUEUE_CAPACITY: usize = 64;
const DEFAULT_AUTH_STATE_WORKER_QUEUE_CAPACITY: usize = 128;
const DEFAULT_AUDIT_LOG_QUEUE_CAPACITY: usize = 1024;
const DEFAULT_RECONCILE_BACKSTOP_MS: u64 = 5_000;
const DEFAULT_HLC_MAX_DRIFT_MS: u64 = 300_000;
const DEFAULT_TOMBSTONE_RETENTION_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
const DEFAULT_CLOCK_REFRESH_MS: u64 = 10_000;
const DEFAULT_OBSERVED_PERSIST_INTERVAL_MS: u64 = 2_000;
const DEFAULT_STATUS_MAX_ENTRIES: usize = 4_096;
const DEFAULT_STATUS_MAX_ENTRIES_PER_PUBLISHER: usize = 256;
const DEFAULT_STATUS_MAX_TTL_MS: u64 = 300_000;
const MIN_RUNTIME_TUNING_DURATION_MS: u64 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuditLogOverloadPolicy {
    Block,
    DropNewest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeRuntimeTuning {
    pub max_mutation_history_batches: usize,
    pub max_mutation_history_bytes: usize,
    pub snapshot_rewrite_cadence: u64,
    pub peer_sync_backoff_base: Duration,
    pub peer_sync_backoff_max: Duration,
    pub peer_sync_backoff_jitter_ms: u64,
    pub peer_sync_parallel_small_cluster_peer_count_threshold: usize,
    pub peer_sync_parallel_small_cluster_cap: usize,
    pub peer_sync_parallel_large_cluster_cap: usize,
    pub peer_sync_parallel_no_stagger_peer_count_threshold: usize,
    pub peer_sync_parallel_spawn_stagger_step_ms: u64,
    pub peer_sync_parallel_spawn_stagger_max_ms: u64,
    pub peer_sync_parallel_followup_stagger_ms: u64,
    pub local_rate_limit_window: Duration,
    pub local_rate_limit_max_messages: u32,
    pub local_session_ttl: Duration,
    pub local_stream_send_queue_capacity: usize,
    pub local_client_event_queue_limit: usize,
    pub observability_event_limit: usize,
    pub transport_max_payload_bytes: usize,
    pub transport_io_timeout: Duration,
    pub transport_max_concurrent_connections: usize,
    pub persistence_worker_queue_capacity: usize,
    pub auth_state_worker_queue_capacity: usize,
    pub audit_log_queue_capacity: usize,
    pub audit_log_overload_policy: AuditLogOverloadPolicy,
    /// Longest the reconcile loop stays idle without a wake-up before it runs a periodic
    /// backstop pass. Values at or below the reconcile interval (`ORION_NODE_RECONCILE_MS`)
    /// restore fixed-interval polling.
    pub reconcile_backstop_interval: Duration,
    /// Largest distance a remote HLC timestamp may be ahead of the local wall clock before the
    /// object version carrying it is rejected (`ORION_NODE_HLC_MAX_DRIFT_MS`).
    pub hlc_max_drift: Duration,
    /// How long desired-state tombstones are kept before they are collected
    /// (`ORION_NODE_TOMBSTONE_RETENTION_MS`).
    pub tombstone_retention: Duration,
    /// How often the node re-reads its clock state (`ORION_NODE_CLOCK_REFRESH_MS`). The observed
    /// node record is only republished on meaningful change.
    pub clock_refresh_interval: Duration,
    /// Operator-declared clock source (`ORION_NODE_CLOCK_SOURCE`), for example PTP or chrony,
    /// which the kernel cannot report. `None` reports `system` on Linux and `unknown` elsewhere.
    pub clock_source: Option<ClockSourceKind>,
    /// Timebase producers on this node stamp in (`ORION_NODE_TIMEBASE`), for example `TAI`.
    pub clock_timebase: Option<String>,
    /// Shortest spacing between coalesced observed/applied state writes. `Duration::ZERO` writes
    /// every change immediately (the pre-coalescing behaviour). Desired-state commits are always
    /// written immediately.
    pub observed_persist_interval: Duration,
    /// Node-wide cap on volatile status lane entries.
    pub status_max_entries: usize,
    /// Cap on status lane entries held for one publisher (local client or link device).
    pub status_max_entries_per_publisher: usize,
    /// Longest time-to-live of a status entry (also the TTL of entries published with `ttl_ms = 0`).
    pub status_max_ttl: Duration,
}

impl NodeRuntimeTuning {
    pub fn with_max_mutation_history_batches(mut self, max_batches: usize) -> Self {
        self.max_mutation_history_batches = max_batches;
        self.normalize();
        self
    }

    pub fn with_max_mutation_history_bytes(mut self, max_bytes: usize) -> Self {
        self.max_mutation_history_bytes = max_bytes;
        self.normalize();
        self
    }

    pub fn with_snapshot_rewrite_cadence(mut self, cadence: u64) -> Self {
        self.snapshot_rewrite_cadence = cadence;
        self.normalize();
        self
    }

    pub fn with_peer_sync_parallel_small_cluster_threshold(mut self, threshold: usize) -> Self {
        self.peer_sync_parallel_small_cluster_peer_count_threshold = threshold;
        self.normalize();
        self
    }

    pub fn with_peer_sync_parallel_small_cluster_cap(mut self, cap: usize) -> Self {
        self.peer_sync_parallel_small_cluster_cap = cap;
        self.normalize();
        self
    }

    pub fn with_peer_sync_parallel_large_cluster_cap(mut self, cap: usize) -> Self {
        self.peer_sync_parallel_large_cluster_cap = cap;
        self.normalize();
        self
    }

    pub fn with_peer_sync_parallel_no_stagger_threshold(mut self, threshold: usize) -> Self {
        self.peer_sync_parallel_no_stagger_peer_count_threshold = threshold;
        self.normalize();
        self
    }

    pub fn with_peer_sync_parallel_spawn_stagger_step_ms(mut self, step_ms: u64) -> Self {
        self.peer_sync_parallel_spawn_stagger_step_ms = step_ms;
        self.normalize();
        self
    }

    pub fn with_peer_sync_parallel_spawn_stagger_max_ms(mut self, max_ms: u64) -> Self {
        self.peer_sync_parallel_spawn_stagger_max_ms = max_ms;
        self.normalize();
        self
    }

    pub fn with_peer_sync_parallel_followup_stagger_ms(mut self, stagger_ms: u64) -> Self {
        self.peer_sync_parallel_followup_stagger_ms = stagger_ms;
        self.normalize();
        self
    }

    pub fn with_local_rate_limit_window(mut self, window: Duration) -> Self {
        self.local_rate_limit_window = window;
        self.normalize();
        self
    }

    pub fn with_local_rate_limit_max_messages(mut self, max_messages: u32) -> Self {
        self.local_rate_limit_max_messages = max_messages;
        self.normalize();
        self
    }

    pub fn with_local_session_ttl(mut self, ttl: Duration) -> Self {
        self.local_session_ttl = ttl;
        self.normalize();
        self
    }

    pub fn with_local_stream_send_queue_capacity(mut self, capacity: usize) -> Self {
        self.local_stream_send_queue_capacity = capacity;
        self.normalize();
        self
    }

    pub fn with_local_client_event_queue_limit(mut self, limit: usize) -> Self {
        self.local_client_event_queue_limit = limit;
        self.normalize();
        self
    }

    pub fn with_observability_event_limit(mut self, limit: usize) -> Self {
        self.observability_event_limit = limit;
        self.normalize();
        self
    }

    pub fn with_transport_max_payload_bytes(mut self, max_payload_bytes: usize) -> Self {
        self.transport_max_payload_bytes = max_payload_bytes;
        self.normalize();
        self
    }

    pub fn with_transport_io_timeout(mut self, timeout: Duration) -> Self {
        self.transport_io_timeout = timeout;
        self.normalize();
        self
    }

    pub fn with_transport_max_concurrent_connections(mut self, max_connections: usize) -> Self {
        self.transport_max_concurrent_connections = max_connections;
        self.normalize();
        self
    }

    pub fn with_persistence_worker_queue_capacity(mut self, capacity: usize) -> Self {
        self.persistence_worker_queue_capacity = capacity;
        self.normalize();
        self
    }

    pub fn with_auth_state_worker_queue_capacity(mut self, capacity: usize) -> Self {
        self.auth_state_worker_queue_capacity = capacity;
        self.normalize();
        self
    }

    pub fn with_audit_log_queue_capacity(mut self, capacity: usize) -> Self {
        self.audit_log_queue_capacity = capacity;
        self.normalize();
        self
    }

    pub fn with_audit_log_overload_policy(mut self, policy: AuditLogOverloadPolicy) -> Self {
        self.audit_log_overload_policy = policy;
        self.normalize();
        self
    }

    pub fn with_reconcile_backstop_interval(mut self, interval: Duration) -> Self {
        self.reconcile_backstop_interval = interval;
        self.normalize();
        self
    }

    pub fn with_hlc_max_drift(mut self, max_drift: Duration) -> Self {
        self.hlc_max_drift = max_drift;
        self.normalize();
        self
    }

    pub fn with_clock_refresh_interval(mut self, interval: Duration) -> Self {
        self.clock_refresh_interval = interval;
        self.normalize();
        self
    }

    pub fn with_tombstone_retention(mut self, retention: Duration) -> Self {
        self.tombstone_retention = retention;
        self.normalize();
        self
    }

    pub fn with_observed_persist_interval(mut self, interval: Duration) -> Self {
        self.observed_persist_interval = interval;
        self.normalize();
        self
    }

    pub fn with_clock_source(mut self, source: Option<ClockSourceKind>) -> Self {
        self.clock_source = source;
        self
    }

    pub fn with_clock_timebase(mut self, timebase: Option<String>) -> Self {
        self.clock_timebase = timebase;
        self
    }

    pub fn with_status_limits(
        mut self,
        max_entries: usize,
        max_entries_per_publisher: usize,
        max_ttl: Duration,
    ) -> Self {
        self.status_max_entries = max_entries;
        self.status_max_entries_per_publisher = max_entries_per_publisher;
        self.status_max_ttl = max_ttl;
        self.normalize();
        self
    }

    pub fn try_from_env() -> Result<Self, NodeError> {
        let mut tuning = Self {
            max_mutation_history_batches: parse_env_or(
                "ORION_NODE_MAX_MUTATION_HISTORY",
                DEFAULT_MUTATION_HISTORY_BATCHES,
            )?,
            max_mutation_history_bytes: parse_env_or(
                "ORION_NODE_MAX_MUTATION_HISTORY_BYTES",
                DEFAULT_MUTATION_HISTORY_BYTES,
            )?,
            snapshot_rewrite_cadence: parse_env_or(
                "ORION_NODE_SNAPSHOT_REWRITE_CADENCE",
                DEFAULT_SNAPSHOT_REWRITE_CADENCE,
            )?,
            peer_sync_backoff_base: duration_ms_env_or(
                "ORION_NODE_PEER_SYNC_BACKOFF_BASE_MS",
                DEFAULT_PEER_SYNC_BACKOFF_BASE_MS,
            )?,
            peer_sync_backoff_max: duration_ms_env_or(
                "ORION_NODE_PEER_SYNC_BACKOFF_MAX_MS",
                DEFAULT_PEER_SYNC_BACKOFF_MAX_MS,
            )?,
            peer_sync_backoff_jitter_ms: parse_env_or(
                "ORION_NODE_PEER_SYNC_BACKOFF_JITTER_MS",
                DEFAULT_PEER_SYNC_BACKOFF_JITTER_MS,
            )?,
            peer_sync_parallel_small_cluster_peer_count_threshold: parse_env_or(
                "ORION_NODE_PEER_SYNC_SMALL_CLUSTER_THRESHOLD",
                DEFAULT_PEER_SYNC_SMALL_CLUSTER_THRESHOLD,
            )?,
            peer_sync_parallel_small_cluster_cap: parse_env_or(
                "ORION_NODE_PEER_SYNC_SMALL_CLUSTER_CAP",
                DEFAULT_PEER_SYNC_SMALL_CLUSTER_CAP,
            )?,
            peer_sync_parallel_large_cluster_cap: parse_env_or(
                "ORION_NODE_PEER_SYNC_LARGE_CLUSTER_CAP",
                DEFAULT_PEER_SYNC_LARGE_CLUSTER_CAP,
            )?,
            peer_sync_parallel_no_stagger_peer_count_threshold: parse_env_or(
                "ORION_NODE_PEER_SYNC_NO_STAGGER_THRESHOLD",
                DEFAULT_PEER_SYNC_NO_STAGGER_THRESHOLD,
            )?,
            peer_sync_parallel_spawn_stagger_step_ms: parse_env_or(
                "ORION_NODE_PEER_SYNC_SPAWN_STAGGER_STEP_MS",
                DEFAULT_PEER_SYNC_SPAWN_STAGGER_STEP_MS,
            )?,
            peer_sync_parallel_spawn_stagger_max_ms: parse_env_or(
                "ORION_NODE_PEER_SYNC_SPAWN_STAGGER_MAX_MS",
                DEFAULT_PEER_SYNC_SPAWN_STAGGER_MAX_MS,
            )?,
            peer_sync_parallel_followup_stagger_ms: parse_env_or(
                "ORION_NODE_PEER_SYNC_FOLLOWUP_STAGGER_MS",
                DEFAULT_PEER_SYNC_FOLLOWUP_STAGGER_MS,
            )?,
            local_rate_limit_window: duration_ms_env_or(
                "ORION_NODE_LOCAL_RATE_LIMIT_WINDOW_MS",
                DEFAULT_LOCAL_RATE_LIMIT_WINDOW_MS,
            )?,
            local_rate_limit_max_messages: parse_env_or(
                "ORION_NODE_LOCAL_RATE_LIMIT_MAX_MESSAGES",
                DEFAULT_LOCAL_RATE_LIMIT_MAX_MESSAGES,
            )?,
            local_session_ttl: duration_ms_env_or(
                "ORION_NODE_LOCAL_SESSION_TTL_MS",
                DEFAULT_LOCAL_SESSION_TTL_MS,
            )?,
            local_stream_send_queue_capacity: parse_env_or(
                "ORION_NODE_LOCAL_STREAM_SEND_QUEUE_CAPACITY",
                DEFAULT_LOCAL_STREAM_SEND_QUEUE_CAPACITY,
            )?,
            local_client_event_queue_limit: parse_env_or(
                "ORION_NODE_LOCAL_CLIENT_EVENT_QUEUE_LIMIT",
                DEFAULT_LOCAL_CLIENT_EVENT_QUEUE_LIMIT,
            )?,
            observability_event_limit: parse_env_or(
                "ORION_NODE_OBSERVABILITY_EVENT_LIMIT",
                DEFAULT_OBSERVABILITY_EVENT_LIMIT,
            )?,
            transport_max_payload_bytes: parse_env_or(
                "ORION_NODE_TRANSPORT_MAX_PAYLOAD_BYTES",
                DEFAULT_MAX_TRANSPORT_PAYLOAD_BYTES,
            )?,
            transport_io_timeout: duration_ms_env_or(
                "ORION_NODE_TRANSPORT_IO_TIMEOUT_MS",
                DEFAULT_TRANSPORT_IO_TIMEOUT
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64,
            )?,
            transport_max_concurrent_connections: parse_env_or(
                "ORION_NODE_TRANSPORT_MAX_CONCURRENT_CONNECTIONS",
                DEFAULT_TRANSPORT_MAX_CONCURRENT_CONNECTIONS,
            )?,
            persistence_worker_queue_capacity: parse_env_or(
                "ORION_NODE_PERSISTENCE_WORKER_QUEUE_CAPACITY",
                DEFAULT_PERSISTENCE_WORKER_QUEUE_CAPACITY,
            )?,
            auth_state_worker_queue_capacity: parse_env_or(
                "ORION_NODE_AUTH_STATE_WORKER_QUEUE_CAPACITY",
                DEFAULT_AUTH_STATE_WORKER_QUEUE_CAPACITY,
            )?,
            audit_log_queue_capacity: parse_env_or(
                "ORION_NODE_AUDIT_LOG_QUEUE_CAPACITY",
                DEFAULT_AUDIT_LOG_QUEUE_CAPACITY,
            )?,
            audit_log_overload_policy: parse_audit_log_overload_policy(
                "ORION_NODE_AUDIT_LOG_OVERLOAD_POLICY",
                AuditLogOverloadPolicy::DropNewest,
            )?,
            reconcile_backstop_interval: duration_ms_env_or(
                "ORION_NODE_RECONCILE_BACKSTOP_MS",
                DEFAULT_RECONCILE_BACKSTOP_MS,
            )?,
            hlc_max_drift: duration_ms_env_or(
                "ORION_NODE_HLC_MAX_DRIFT_MS",
                DEFAULT_HLC_MAX_DRIFT_MS,
            )?,
            tombstone_retention: duration_ms_env_or(
                "ORION_NODE_TOMBSTONE_RETENTION_MS",
                DEFAULT_TOMBSTONE_RETENTION_MS,
            )?,
            clock_refresh_interval: duration_ms_env_or(
                "ORION_NODE_CLOCK_REFRESH_MS",
                DEFAULT_CLOCK_REFRESH_MS,
            )?,
            clock_source: optional_label_env("ORION_NODE_CLOCK_SOURCE")?
                .as_deref()
                .and_then(ClockSourceKind::from_label),
            clock_timebase: optional_label_env("ORION_NODE_TIMEBASE")?,
            observed_persist_interval: duration_ms_env_or(
                "ORION_NODE_OBSERVED_PERSIST_INTERVAL_MS",
                DEFAULT_OBSERVED_PERSIST_INTERVAL_MS,
            )?,
            status_max_entries: parse_env_or(
                "ORION_NODE_STATUS_MAX_ENTRIES",
                DEFAULT_STATUS_MAX_ENTRIES,
            )?,
            status_max_entries_per_publisher: parse_env_or(
                "ORION_NODE_STATUS_MAX_ENTRIES_PER_PUBLISHER",
                DEFAULT_STATUS_MAX_ENTRIES_PER_PUBLISHER,
            )?,
            status_max_ttl: duration_ms_env_or(
                "ORION_NODE_STATUS_MAX_TTL_MS",
                DEFAULT_STATUS_MAX_TTL_MS,
            )?,
        };
        tuning.normalize();
        Ok(tuning)
    }

    pub fn normalize(&mut self) {
        self.max_mutation_history_batches = self.max_mutation_history_batches.max(1);
        self.max_mutation_history_bytes = self.max_mutation_history_bytes.max(1);
        self.snapshot_rewrite_cadence = self.snapshot_rewrite_cadence.max(1);
        self.peer_sync_backoff_base =
            normalize_runtime_tuning_duration(self.peer_sync_backoff_base);
        self.peer_sync_backoff_max = self
            .peer_sync_backoff_max
            .max(min_runtime_tuning_duration())
            .max(self.peer_sync_backoff_base);
        self.peer_sync_parallel_small_cluster_cap =
            self.peer_sync_parallel_small_cluster_cap.max(1);
        self.peer_sync_parallel_large_cluster_cap =
            self.peer_sync_parallel_large_cluster_cap.max(1);
        self.local_rate_limit_window =
            normalize_runtime_tuning_duration(self.local_rate_limit_window);
        self.local_rate_limit_max_messages = self.local_rate_limit_max_messages.max(1);
        self.local_session_ttl = normalize_runtime_tuning_duration(self.local_session_ttl);
        self.local_stream_send_queue_capacity = self.local_stream_send_queue_capacity.max(1);
        self.local_client_event_queue_limit = self.local_client_event_queue_limit.max(1);
        self.observability_event_limit = self.observability_event_limit.max(1);
        self.transport_max_payload_bytes = self.transport_max_payload_bytes.max(1);
        self.transport_io_timeout = normalize_runtime_tuning_duration(self.transport_io_timeout);
        self.transport_max_concurrent_connections =
            self.transport_max_concurrent_connections.max(1);
        self.persistence_worker_queue_capacity = self.persistence_worker_queue_capacity.max(1);
        self.auth_state_worker_queue_capacity = self.auth_state_worker_queue_capacity.max(1);
        self.audit_log_queue_capacity = self.audit_log_queue_capacity.max(1);
        self.reconcile_backstop_interval =
            normalize_runtime_tuning_duration(self.reconcile_backstop_interval);
        self.hlc_max_drift = normalize_runtime_tuning_duration(self.hlc_max_drift);
        self.tombstone_retention = normalize_runtime_tuning_duration(self.tombstone_retention);
        self.clock_refresh_interval =
            normalize_runtime_tuning_duration(self.clock_refresh_interval);
        self.status_max_entries = self.status_max_entries.max(1);
        self.status_max_entries_per_publisher = self
            .status_max_entries_per_publisher
            .clamp(1, self.status_max_entries);
        self.status_max_ttl = normalize_runtime_tuning_duration(self.status_max_ttl);
    }
}

impl Default for NodeRuntimeTuning {
    fn default() -> Self {
        Self {
            max_mutation_history_batches: DEFAULT_MUTATION_HISTORY_BATCHES,
            max_mutation_history_bytes: DEFAULT_MUTATION_HISTORY_BYTES,
            snapshot_rewrite_cadence: DEFAULT_SNAPSHOT_REWRITE_CADENCE,
            peer_sync_backoff_base: Duration::from_millis(DEFAULT_PEER_SYNC_BACKOFF_BASE_MS),
            peer_sync_backoff_max: Duration::from_millis(DEFAULT_PEER_SYNC_BACKOFF_MAX_MS),
            peer_sync_backoff_jitter_ms: DEFAULT_PEER_SYNC_BACKOFF_JITTER_MS,
            peer_sync_parallel_small_cluster_peer_count_threshold:
                DEFAULT_PEER_SYNC_SMALL_CLUSTER_THRESHOLD,
            peer_sync_parallel_small_cluster_cap: DEFAULT_PEER_SYNC_SMALL_CLUSTER_CAP,
            peer_sync_parallel_large_cluster_cap: DEFAULT_PEER_SYNC_LARGE_CLUSTER_CAP,
            peer_sync_parallel_no_stagger_peer_count_threshold:
                DEFAULT_PEER_SYNC_NO_STAGGER_THRESHOLD,
            peer_sync_parallel_spawn_stagger_step_ms: DEFAULT_PEER_SYNC_SPAWN_STAGGER_STEP_MS,
            peer_sync_parallel_spawn_stagger_max_ms: DEFAULT_PEER_SYNC_SPAWN_STAGGER_MAX_MS,
            peer_sync_parallel_followup_stagger_ms: DEFAULT_PEER_SYNC_FOLLOWUP_STAGGER_MS,
            local_rate_limit_window: Duration::from_millis(DEFAULT_LOCAL_RATE_LIMIT_WINDOW_MS),
            local_rate_limit_max_messages: DEFAULT_LOCAL_RATE_LIMIT_MAX_MESSAGES,
            local_session_ttl: Duration::from_millis(DEFAULT_LOCAL_SESSION_TTL_MS),
            local_stream_send_queue_capacity: DEFAULT_LOCAL_STREAM_SEND_QUEUE_CAPACITY,
            local_client_event_queue_limit: DEFAULT_LOCAL_CLIENT_EVENT_QUEUE_LIMIT,
            observability_event_limit: DEFAULT_OBSERVABILITY_EVENT_LIMIT,
            transport_max_payload_bytes: DEFAULT_MAX_TRANSPORT_PAYLOAD_BYTES,
            transport_io_timeout: DEFAULT_TRANSPORT_IO_TIMEOUT,
            transport_max_concurrent_connections: DEFAULT_TRANSPORT_MAX_CONCURRENT_CONNECTIONS,
            persistence_worker_queue_capacity: DEFAULT_PERSISTENCE_WORKER_QUEUE_CAPACITY,
            auth_state_worker_queue_capacity: DEFAULT_AUTH_STATE_WORKER_QUEUE_CAPACITY,
            audit_log_queue_capacity: DEFAULT_AUDIT_LOG_QUEUE_CAPACITY,
            audit_log_overload_policy: AuditLogOverloadPolicy::DropNewest,
            reconcile_backstop_interval: Duration::from_millis(DEFAULT_RECONCILE_BACKSTOP_MS),
            hlc_max_drift: Duration::from_millis(DEFAULT_HLC_MAX_DRIFT_MS),
            tombstone_retention: Duration::from_millis(DEFAULT_TOMBSTONE_RETENTION_MS),
            clock_refresh_interval: Duration::from_millis(DEFAULT_CLOCK_REFRESH_MS),
            clock_source: None,
            clock_timebase: None,
            observed_persist_interval: Duration::from_millis(DEFAULT_OBSERVED_PERSIST_INTERVAL_MS),
            status_max_entries: DEFAULT_STATUS_MAX_ENTRIES,
            status_max_entries_per_publisher: DEFAULT_STATUS_MAX_ENTRIES_PER_PUBLISHER,
            status_max_ttl: Duration::from_millis(DEFAULT_STATUS_MAX_TTL_MS),
        }
    }
}

#[cfg(test)]
pub(crate) fn runtime_tuning_doc_defaults() -> Vec<(&'static str, String)> {
    let tuning = NodeRuntimeTuning::default();
    vec![
        (
            "ORION_NODE_MAX_MUTATION_HISTORY",
            tuning.max_mutation_history_batches.to_string(),
        ),
        (
            "ORION_NODE_MAX_MUTATION_HISTORY_BYTES",
            tuning.max_mutation_history_bytes.to_string(),
        ),
        (
            "ORION_NODE_SNAPSHOT_REWRITE_CADENCE",
            tuning.snapshot_rewrite_cadence.to_string(),
        ),
        (
            "ORION_NODE_PEER_SYNC_BACKOFF_BASE_MS",
            tuning.peer_sync_backoff_base.as_millis().to_string(),
        ),
        (
            "ORION_NODE_PEER_SYNC_BACKOFF_MAX_MS",
            tuning.peer_sync_backoff_max.as_millis().to_string(),
        ),
        (
            "ORION_NODE_PEER_SYNC_BACKOFF_JITTER_MS",
            tuning.peer_sync_backoff_jitter_ms.to_string(),
        ),
        (
            "ORION_NODE_PEER_SYNC_SMALL_CLUSTER_THRESHOLD",
            tuning
                .peer_sync_parallel_small_cluster_peer_count_threshold
                .to_string(),
        ),
        (
            "ORION_NODE_PEER_SYNC_SMALL_CLUSTER_CAP",
            tuning.peer_sync_parallel_small_cluster_cap.to_string(),
        ),
        (
            "ORION_NODE_PEER_SYNC_LARGE_CLUSTER_CAP",
            tuning.peer_sync_parallel_large_cluster_cap.to_string(),
        ),
        (
            "ORION_NODE_PEER_SYNC_NO_STAGGER_THRESHOLD",
            tuning
                .peer_sync_parallel_no_stagger_peer_count_threshold
                .to_string(),
        ),
        (
            "ORION_NODE_PEER_SYNC_SPAWN_STAGGER_STEP_MS",
            tuning.peer_sync_parallel_spawn_stagger_step_ms.to_string(),
        ),
        (
            "ORION_NODE_PEER_SYNC_SPAWN_STAGGER_MAX_MS",
            tuning.peer_sync_parallel_spawn_stagger_max_ms.to_string(),
        ),
        (
            "ORION_NODE_PEER_SYNC_FOLLOWUP_STAGGER_MS",
            tuning.peer_sync_parallel_followup_stagger_ms.to_string(),
        ),
        (
            "ORION_NODE_LOCAL_RATE_LIMIT_WINDOW_MS",
            tuning.local_rate_limit_window.as_millis().to_string(),
        ),
        (
            "ORION_NODE_LOCAL_RATE_LIMIT_MAX_MESSAGES",
            tuning.local_rate_limit_max_messages.to_string(),
        ),
        (
            "ORION_NODE_LOCAL_SESSION_TTL_MS",
            tuning.local_session_ttl.as_millis().to_string(),
        ),
        (
            "ORION_NODE_LOCAL_STREAM_SEND_QUEUE_CAPACITY",
            tuning.local_stream_send_queue_capacity.to_string(),
        ),
        (
            "ORION_NODE_LOCAL_CLIENT_EVENT_QUEUE_LIMIT",
            tuning.local_client_event_queue_limit.to_string(),
        ),
        (
            "ORION_NODE_OBSERVABILITY_EVENT_LIMIT",
            tuning.observability_event_limit.to_string(),
        ),
        (
            "ORION_NODE_TRANSPORT_MAX_PAYLOAD_BYTES",
            tuning.transport_max_payload_bytes.to_string(),
        ),
        (
            "ORION_NODE_TRANSPORT_IO_TIMEOUT_MS",
            tuning.transport_io_timeout.as_millis().to_string(),
        ),
        (
            "ORION_NODE_TRANSPORT_MAX_CONCURRENT_CONNECTIONS",
            tuning.transport_max_concurrent_connections.to_string(),
        ),
        (
            "ORION_NODE_PERSISTENCE_WORKER_QUEUE_CAPACITY",
            tuning.persistence_worker_queue_capacity.to_string(),
        ),
        (
            "ORION_NODE_AUTH_STATE_WORKER_QUEUE_CAPACITY",
            tuning.auth_state_worker_queue_capacity.to_string(),
        ),
        (
            "ORION_NODE_AUDIT_LOG_QUEUE_CAPACITY",
            tuning.audit_log_queue_capacity.to_string(),
        ),
        (
            "ORION_NODE_RECONCILE_BACKSTOP_MS",
            tuning.reconcile_backstop_interval.as_millis().to_string(),
        ),
        (
            "ORION_NODE_HLC_MAX_DRIFT_MS",
            tuning.hlc_max_drift.as_millis().to_string(),
        ),
        (
            "ORION_NODE_TOMBSTONE_RETENTION_MS",
            tuning.tombstone_retention.as_millis().to_string(),
        ),
        (
            "ORION_NODE_CLOCK_REFRESH_MS",
            tuning.clock_refresh_interval.as_millis().to_string(),
        ),
        (
            "ORION_NODE_OBSERVED_PERSIST_INTERVAL_MS",
            tuning.observed_persist_interval.as_millis().to_string(),
        ),
        (
            "ORION_NODE_STATUS_MAX_ENTRIES",
            tuning.status_max_entries.to_string(),
        ),
        (
            "ORION_NODE_STATUS_MAX_ENTRIES_PER_PUBLISHER",
            tuning.status_max_entries_per_publisher.to_string(),
        ),
        (
            "ORION_NODE_STATUS_MAX_TTL_MS",
            tuning.status_max_ttl.as_millis().to_string(),
        ),
    ]
}

pub(crate) fn min_runtime_tuning_duration() -> Duration {
    Duration::from_millis(MIN_RUNTIME_TUNING_DURATION_MS)
}

pub(crate) fn normalize_runtime_tuning_duration(duration: Duration) -> Duration {
    duration.max(min_runtime_tuning_duration())
}

pub(crate) fn parse_env_or<T>(key: &str, default: T) -> Result<T, NodeError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match env::var(key) {
        Ok(value) => parse_config_value(key, &value),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(_)) => {
            Err(NodeError::Config(format!("{key} must be valid unicode")))
        }
    }
}

pub(crate) fn parse_config_value<T>(key: &str, value: &str) -> Result<T, NodeError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|err| NodeError::Config(format!("{key} has invalid value `{value}`: {err}")))
}

pub(crate) fn duration_ms_env_or(key: &str, default_ms: u64) -> Result<Duration, NodeError> {
    Ok(Duration::from_millis(parse_env_or(key, default_ms)?))
}

pub(crate) fn bool_env_or_false(key: &str) -> Result<bool, NodeError> {
    match env::var(key) {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            other => Err(NodeError::Config(format!(
                "{key} has invalid boolean value `{other}`; expected one of `1`, `0`, `true`, `false`, `yes`, `no`, `on`, or `off`"
            ))),
        },
        Err(env::VarError::NotPresent) => Ok(false),
        Err(env::VarError::NotUnicode(_)) => {
            Err(NodeError::Config(format!("{key} must be valid unicode")))
        }
    }
}

/// A trimmed, non-empty string variable; unset or blank is `None`.
fn optional_label_env(key: &str) -> Result<Option<String>, NodeError> {
    match env::var(key) {
        Ok(value) => {
            let value = value.trim();
            Ok((!value.is_empty()).then(|| value.to_owned()))
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => {
            Err(NodeError::Config(format!("{key} must be valid unicode")))
        }
    }
}

fn parse_audit_log_overload_policy(
    key: &str,
    default: AuditLogOverloadPolicy,
) -> Result<AuditLogOverloadPolicy, NodeError> {
    match env::var(key) {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "block" => Ok(AuditLogOverloadPolicy::Block),
            "drop_newest" | "drop-newest" | "drop" => Ok(AuditLogOverloadPolicy::DropNewest),
            other => Err(NodeError::Config(format!(
                "{key} has invalid value `{other}`; expected `block` or `drop_newest`"
            ))),
        },
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(_)) => {
            Err(NodeError::Config(format!("{key} must be valid unicode")))
        }
    }
}

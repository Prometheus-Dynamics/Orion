use crate::NodeError;
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
    pub persistence_worker_queue_capacity: usize,
    pub auth_state_worker_queue_capacity: usize,
    pub audit_log_queue_capacity: usize,
    pub audit_log_overload_policy: AuditLogOverloadPolicy,
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
        self.persistence_worker_queue_capacity = self.persistence_worker_queue_capacity.max(1);
        self.auth_state_worker_queue_capacity = self.auth_state_worker_queue_capacity.max(1);
        self.audit_log_queue_capacity = self.audit_log_queue_capacity.max(1);
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
            persistence_worker_queue_capacity: DEFAULT_PERSISTENCE_WORKER_QUEUE_CAPACITY,
            auth_state_worker_queue_capacity: DEFAULT_AUTH_STATE_WORKER_QUEUE_CAPACITY,
            audit_log_queue_capacity: DEFAULT_AUDIT_LOG_QUEUE_CAPACITY,
            audit_log_overload_policy: AuditLogOverloadPolicy::DropNewest,
        }
    }
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

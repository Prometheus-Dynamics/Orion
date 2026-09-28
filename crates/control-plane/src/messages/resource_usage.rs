//! Node memory and backlog diagnostics carried inside the observability snapshot.
//!
//! Every section is cheap to compute on demand and derives `Default` plus `#[serde(default)]`,
//! so structured (JSON/YAML/TOML) consumers written against older snapshots keep decoding when
//! fields are added. The rkyv wire encoding is layout-exact, so node and `orionctl` versions still
//! need to match for the binary control protocol.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

/// Aggregate memory, state-size, and backlog diagnostics for one node.
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
#[serde(default)]
pub struct NodeResourceUsageSnapshot {
    pub process: ProcessMemorySnapshot,
    pub state: StateSizeSnapshot,
    pub mutation_history: MutationHistoryUsageSnapshot,
    pub local_streams: LocalStreamUsageSnapshot,
    pub worker_queues: Vec<WorkerQueueUsageSnapshot>,
    pub registries: RegistryUsageSnapshot,
}

/// Process memory counters read from `/proc/self/status` and `/proc/self/smaps_rollup`.
///
/// Every field is `None` when the host does not expose the corresponding counter.
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
#[serde(default)]
pub struct ProcessMemorySnapshot {
    pub vm_rss_bytes: Option<u64>,
    pub vm_hwm_bytes: Option<u64>,
    pub rss_anon_bytes: Option<u64>,
    pub rss_file_bytes: Option<u64>,
    pub rss_shmem_bytes: Option<u64>,
    pub vm_data_bytes: Option<u64>,
    pub pss_bytes: Option<u64>,
    pub pss_anon_bytes: Option<u64>,
    pub pss_file_bytes: Option<u64>,
    pub private_dirty_bytes: Option<u64>,
    pub threads: Option<u64>,
}

/// Record counts for one cluster-state view.
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
#[serde(default)]
pub struct StateSectionCounts {
    pub nodes: u64,
    pub artifacts: u64,
    pub workloads: u64,
    pub workload_tombstones: u64,
    pub resources: u64,
    pub providers: u64,
    pub executors: u64,
    pub leases: u64,
}

impl StateSectionCounts {
    pub fn total(&self) -> u64 {
        self.nodes
            .saturating_add(self.artifacts)
            .saturating_add(self.workloads)
            .saturating_add(self.workload_tombstones)
            .saturating_add(self.resources)
            .saturating_add(self.providers)
            .saturating_add(self.executors)
            .saturating_add(self.leases)
    }
}

/// In-memory state record counts plus on-disk persisted sizes when storage is configured.
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
#[serde(default)]
pub struct StateSizeSnapshot {
    pub desired: StateSectionCounts,
    pub observed: StateSectionCounts,
    /// Sum of the persisted snapshot manifest, desired, observed, and applied section files.
    pub persisted_snapshot_bytes: Option<u64>,
    /// Size of the persisted mutation-history file.
    pub persisted_mutation_history_bytes: Option<u64>,
}

/// Retained mutation history compared with its configured caps.
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
#[serde(default)]
pub struct MutationHistoryUsageSnapshot {
    pub batches: u64,
    pub max_batches: u64,
    pub mutations: u64,
    /// Encoded size of the retained history, measured with the same encoding used for the
    /// `max_bytes` cap. Recomputed only when the history changes.
    pub encoded_bytes: Option<u64>,
    pub max_bytes: u64,
}

/// Local IPC stream subscribers and their backlog.
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
#[serde(default)]
pub struct LocalStreamUsageSnapshot {
    pub registered_clients: u64,
    pub attached_streams: u64,
    pub state_watchers: u64,
    pub executor_watchers: u64,
    pub provider_watchers: u64,
    /// Configured per-stream send queue capacity (`ORION_NODE_LOCAL_STREAM_SEND_QUEUE_CAPACITY`).
    pub send_queue_capacity: u64,
    pub send_queue_depth_total: u64,
    pub send_queue_depth_max: u64,
    /// Configured per-client pending event limit (`ORION_NODE_LOCAL_CLIENT_EVENT_QUEUE_LIMIT`).
    pub client_event_queue_limit: u64,
    pub queued_client_events_total: u64,
    pub queued_client_events_max: u64,
    /// Events discarded because a registered client's pending queue was full.
    pub dropped_client_events_total: u64,
}

/// Queue depth for a bounded background worker.
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct WorkerQueueUsageSnapshot {
    pub name: String,
    pub capacity: u64,
    pub depth: u64,
    #[serde(default)]
    pub dropped_total: Option<u64>,
}

/// Sizes of in-memory registries that grow with peers, clients, and traffic.
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
#[serde(default)]
pub struct RegistryUsageSnapshot {
    pub peers: u64,
    pub local_clients: u64,
    pub local_providers: u64,
    pub local_executors: u64,
    pub communication_endpoints: u64,
    pub communication_endpoint_limit: u64,
    pub recent_events: u64,
    pub recent_event_limit: u64,
    pub auth_nonce_peers: u64,
    pub auth_seen_nonces: u64,
}

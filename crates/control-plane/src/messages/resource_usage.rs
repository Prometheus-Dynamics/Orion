//! Node memory and backlog diagnostics carried inside the observability snapshot.
//!
//! Every section is cheap to compute on demand and derives `Default` plus `#[serde(default)]`,
//! so structured (JSON/YAML/TOML) consumers written against older snapshots keep decoding when
//! fields are added. The rkyv wire encoding is layout-exact, so node and `orionctl` versions still
//! need to match for the binary control protocol.

use alloc::{string::String, vec::Vec};
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
    pub observed_persistence: ObservedPersistenceUsageSnapshot,
    pub status_lane: StatusLaneUsageSnapshot,
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
    pub tombstones: u64,
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
            .saturating_add(self.tombstones)
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

/// Coalesced observed/applied state persistence (`ORION_NODE_OBSERVED_PERSIST_INTERVAL_MS`).
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
pub struct ObservedPersistenceUsageSnapshot {
    /// Configured coalescing interval; `0` writes every change immediately.
    pub interval_ms: u64,
    /// Whether the coalescer is running (`false` until the node's maintenance loop starts, and
    /// after it stops; changes are then written immediately).
    pub coalescing: bool,
    /// Whether observed or applied changes are waiting for the next flush.
    pub pending: bool,
    /// Observed or applied changes that were deferred instead of written immediately.
    pub coalesced_changes_total: u64,
    /// Deferred writes performed by the coalescer (flushes).
    pub flushes_total: u64,
    /// Pending changes that a durable write (such as a desired-state commit) carried along.
    pub absorbed_flushes_total: u64,
}

/// In-memory volatile status lane (never persisted, not replicated).
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
pub struct StatusLaneUsageSnapshot {
    pub entries: u64,
    /// Node-wide entry cap (`ORION_NODE_STATUS_MAX_ENTRIES`).
    pub max_entries: u64,
    /// Per-publisher entry cap (`ORION_NODE_STATUS_MAX_ENTRIES_PER_PUBLISHER`).
    pub max_entries_per_publisher: u64,
    /// Longest TTL an entry may have (`ORION_NODE_STATUS_MAX_TTL_MS`).
    pub max_ttl_ms: u64,
    pub publishers: u64,
    pub watchers: u64,
    /// Entries accepted (new or updated values).
    pub published_total: u64,
    /// Entries dropped because their TTL ran out.
    pub expired_total: u64,
    /// Entries refused because a cap was reached or the entry was invalid.
    pub dropped_total: u64,
    /// Publish batches refused because the publisher does not own a subject.
    pub unauthorized_total: u64,
}

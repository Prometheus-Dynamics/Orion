//! Peer liveness and the node facts placement reads (`docs/placement.md`, "Liveness").
//!
//! A peer is *live* while this node has heard from it within
//! `ORION_NODE_LIVENESS_TIMEOUT_MS`: a successful outbound sync round (its signed `Hello`
//! answer), or any signed request it sent (sync rounds it initiates, observed-slice pushes).
//! Liveness is a local judgement, not replicated state; placement only acts on it after the grace
//! period, and only for the node doing the acting, so a brief disagreement between nodes cannot
//! make two nodes run the same workload for long (the HLC merge keeps one assignment).

use super::NodeApp;
use orion::{
    NodeId, ResourceId,
    cluster::ClusterCoordinator,
    control_plane::{MaintenanceMode, NodeRecord},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Mutex, MutexGuard, PoisonError},
};

#[derive(Debug, Default)]
pub(super) struct ClusterRuntimeState {
    inner: Mutex<ClusterRuntimeInner>,
}

#[derive(Debug, Default)]
pub(super) struct ClusterRuntimeInner {
    /// Wall-clock milliseconds when each peer was last heard from.
    pub(super) last_heard_ms: BTreeMap<NodeId, u64>,
    /// Placement hysteresis bookkeeping (created on first use with the node's id and grace).
    pub(super) coordinator: Option<ClusterCoordinator>,
    /// When the owner of a resource this node leases was first seen unreachable.
    pub(super) owner_gone_since: BTreeMap<ResourceId, u64>,
}

impl ClusterRuntimeState {
    pub(super) fn lock(&self) -> MutexGuard<'_, ClusterRuntimeInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl NodeApp {
    fn liveness_timeout_ms(&self) -> u64 {
        u64::try_from(
            self.config
                .runtime_tuning
                .placement
                .liveness_timeout
                .as_millis(),
        )
        .unwrap_or(u64::MAX)
    }

    pub(super) fn placement_grace_ms(&self) -> u64 {
        u64::try_from(self.config.runtime_tuning.placement.grace.as_millis()).unwrap_or(u64::MAX)
    }

    /// Records that `node_id` was heard from now. Hearing from a peer that was considered gone
    /// requests a reconcile pass, so bindings to its resources become available again promptly.
    pub(crate) fn note_peer_heard(&self, node_id: &NodeId) {
        if node_id == &self.config.node_id {
            return;
        }
        let now_ms = Self::current_time_ms();
        let was_live = {
            let mut cluster = self.state.cluster.lock();
            let was_live = cluster
                .last_heard_ms
                .get(node_id)
                .is_some_and(|heard| now_ms.saturating_sub(*heard) < self.liveness_timeout_ms());
            cluster.last_heard_ms.insert(node_id.clone(), now_ms);
            was_live
        };
        if !was_live {
            self.request_reconcile();
        }
    }

    /// Whether `node_id` is live at `now_ms` (the local node always is).
    pub(crate) fn is_node_live(&self, node_id: &NodeId, now_ms: u64) -> bool {
        node_id == &self.config.node_id
            || self
                .state
                .cluster
                .lock()
                .last_heard_ms
                .get(node_id)
                .is_some_and(|heard| now_ms.saturating_sub(*heard) < self.liveness_timeout_ms())
    }

    /// Peers this node currently considers gone, with when it last heard from each (`None`:
    /// never).
    pub fn unreachable_peers(&self) -> BTreeMap<NodeId, Option<u64>> {
        let now_ms = Self::current_time_ms();
        self.known_peer_ids()
            .into_iter()
            .filter(|node_id| !self.is_node_live(node_id, now_ms))
            .map(|node_id| {
                let heard = self
                    .state
                    .cluster
                    .lock()
                    .last_heard_ms
                    .get(&node_id)
                    .copied();
                (node_id, heard)
            })
            .collect()
    }

    /// Every other node this node knows of: configured peers and nodes named by desired or
    /// observed state.
    fn known_peer_ids(&self) -> BTreeSet<NodeId> {
        let mut ids: BTreeSet<NodeId> = self.peers_read().keys().cloned().collect();
        {
            let store = self.store_read();
            ids.extend(store.desired.nodes.keys().cloned());
            ids.extend(store.observed.nodes.keys().cloned());
            ids.extend(store.desired.providers.values().map(|p| p.node_id.clone()));
            ids.extend(store.desired.executors.values().map(|e| e.node_id.clone()));
        }
        ids.remove(&self.config.node_id);
        ids
    }

    /// Recomputes the unreachable-peer set the runtime planner uses for cross-node bindings.
    /// Returns `true` when it changed (executor watchers are then notified, since bindings'
    /// availability changed without a desired-state write).
    pub(super) fn refresh_unreachable_nodes(&self) -> bool {
        let unreachable: BTreeSet<NodeId> = self.unreachable_peers().into_keys().collect();
        let changed = self.with_store_mut(|store| {
            if store.unreachable_nodes == unreachable {
                return false;
            }
            store.unreachable_nodes = unreachable;
            true
        });
        if changed {
            self.notify_client_watchers();
        }
        changed
    }

    /// Publishes this node's labels (`ORION_NODE_LABELS`) and schedulability (maintenance mode) in
    /// its observed node record, which travels to peers in the observed slice. A record is
    /// created only when there is something to say (labels, or not schedulable). Returns whether
    /// the observed record changed.
    pub(super) fn publish_local_node_facts(&self) -> bool {
        let labels = &self.config.runtime_tuning.placement.labels;
        self.with_store_mut(|store| {
            let schedulable = store.maintenance().mode == MaintenanceMode::Normal;
            let node_id = store.local_node_id.clone();
            match store.observed.nodes.get_mut(&node_id) {
                Some(record) => {
                    if &record.labels == labels && record.schedulable == schedulable {
                        return false;
                    }
                    record.labels.clone_from(labels);
                    record.schedulable = schedulable;
                    true
                }
                None if !labels.is_empty() || !schedulable => {
                    let mut record = NodeRecord::builder(node_id.clone())
                        .schedulable(schedulable)
                        .build();
                    record.labels.clone_from(labels);
                    store.observed.put_node(record);
                    true
                }
                None => false,
            }
        })
    }
}

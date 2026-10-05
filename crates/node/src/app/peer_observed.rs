//! Replication of each node's own observed state to its peers (`docs/peer-sync.md`, "Observed
//! state").
//!
//! Every node is the only writer of its observed slice: its own `NodeRecord` (health, clock
//! facts), the workloads assigned to it, and the resources and leases of its providers and
//! executors. At the end of a sync round the initiator pushes that slice as an `ObservedUpdate`
//! when it changed since the last push to that peer, or at least every
//! [`OBSERVED_REFRESH_INTERVAL_MS`]. The receiver replaces exactly that origin's slice
//! (`merge_peer_observed_state`) and never touches other nodes' records.

use super::{NodeApp, desired_state::entry_fingerprint, peer_transport::PeerSyncTransport};
use orion::{
    NodeId,
    control_plane::{AppliedClusterState, ObservedClusterState, ObservedStateUpdate},
    transport::http::{HttpRequestPayload, HttpResponsePayload},
};
use std::collections::BTreeSet;
use tracing::debug;

/// Unchanged slices are re-sent this often, so a peer that restarted catches up.
pub(crate) const OBSERVED_REFRESH_INTERVAL_MS: u64 = 30_000;

impl NodeApp {
    /// This node's observed slice: its node record, its assigned workloads, and the resources
    /// (and their leases) of its providers and executors.
    pub(crate) fn local_observed_slice(&self) -> ObservedClusterState {
        let store = self.store_read();
        let local = &self.config.node_id;
        let desired = &store.desired;
        let observed = &store.observed;
        let providers: BTreeSet<_> = desired
            .providers
            .values()
            .filter(|provider| &provider.node_id == local)
            .map(|provider| provider.provider_id.clone())
            .collect();
        let executors: BTreeSet<_> = desired
            .executors
            .values()
            .filter(|executor| &executor.node_id == local)
            .map(|executor| executor.executor_id.clone())
            .collect();
        // Every resource of a local provider or executor, whether or not it is also in the
        // desired state, so peers can resolve cross-node bindings to it (`docs/placement.md`).
        let local_resources: BTreeSet<_> = desired
            .resources
            .values()
            .chain(observed.resources.values())
            .filter(|resource| {
                providers.contains(&resource.provider_id)
                    || resource
                        .realized_by_executor_id
                        .as_ref()
                        .is_some_and(|executor| executors.contains(executor))
            })
            .map(|resource| resource.resource_id.clone())
            .collect();
        ObservedClusterState {
            revision: observed.revision,
            nodes: observed
                .nodes
                .iter()
                .filter(|(node_id, _)| *node_id == local)
                .map(|(node_id, record)| (node_id.clone(), record.clone()))
                .collect(),
            workloads: observed
                .workloads
                .iter()
                .filter(|(_, workload)| workload.assigned_node_id.as_ref() == Some(local))
                .map(|(id, record)| (id.clone(), record.clone()))
                .collect(),
            resources: observed
                .resources
                .iter()
                .filter(|(id, _)| local_resources.contains(*id))
                .map(|(id, record)| (id.clone(), record.clone()))
                .collect(),
            leases: observed
                .leases
                .iter()
                .filter(|(id, _)| local_resources.contains(*id))
                .map(|(id, record)| (id.clone(), record.clone()))
                .collect(),
        }
    }

    /// Pushes this node's observed slice to `node_id` when it changed or the refresh interval
    /// passed. Best effort: a failed push does not fail the sync round and is retried next round.
    pub(super) async fn push_observed_slice<T: PeerSyncTransport>(
        &self,
        node_id: &NodeId,
        transport: &T,
    ) {
        let slice = self.local_observed_slice();
        let Ok(fingerprint) = entry_fingerprint(&ObservedClusterState {
            revision: orion::Revision::ZERO,
            ..slice.clone()
        }) else {
            return;
        };
        let now_ms = Self::current_time_ms();
        let due = self
            .with_peer_mut(node_id, |peer| {
                peer.observed_push_due(fingerprint, now_ms, OBSERVED_REFRESH_INTERVAL_MS)
            })
            .unwrap_or(false);
        if !due {
            return;
        }
        let update = HttpRequestPayload::ObservedUpdate(ObservedStateUpdate {
            observed: slice,
            applied: AppliedClusterState::default(),
        });
        match transport.exchange(self, node_id, update).await {
            Ok(HttpResponsePayload::Accepted) => {
                let _ = self.with_peer_mut(node_id, |peer| {
                    peer.note_observed_pushed(fingerprint, now_ms);
                });
            }
            Ok(_) => {
                debug!(node = %self.config.node_id, peer = %node_id, "unexpected response to observed state push");
            }
            Err(err) => {
                debug!(node = %self.config.node_id, peer = %node_id, error = %err, "observed state push failed; retrying next round");
            }
        }
    }
}

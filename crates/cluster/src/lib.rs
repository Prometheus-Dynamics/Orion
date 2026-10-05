//! Cluster membership, admission, replication, leaderless placement and cross-node lease
//! helpers for Orion (`docs/placement.md`).

#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

mod assignment;
pub mod leases;
mod membership;
mod placement;
#[cfg(test)]
mod placement_tests;

pub use assignment::{ClusterCoordinator, PlacementStatus};
pub use leases::LeaseEdit;
pub use membership::{
    AdmissionDecision, AdmissionRejection, ClusterMembership, ClusterPeer, ClusterRole,
    ReplicationState, ensure_node_present, negotiation_error_kind,
};
pub use placement::{
    ClusterView, Ineligibility, NodeCandidate, choose_node, eligibility, eligible_nodes,
    rendezvous_choice, rendezvous_score, resource_host,
};

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::{vec, vec::Vec};
    use orion_control_plane::{DesiredClusterState, HealthState, NodeRecord};
    use orion_core::{CompatibilityState, NodeId, ProtocolVersion};
    use orion_data_plane::{LinkType, PeerCapabilities, TransportType};

    fn peer(node_id: &str) -> PeerCapabilities {
        PeerCapabilities {
            node_id: NodeId::new(node_id),
            control_versions: vec![ProtocolVersion::new(1, 0)],
            data_versions: vec![ProtocolVersion::new(1, 0)],
            transports: vec![TransportType::Http, TransportType::TcpStream],
            link_types: vec![LinkType::ReliableOrdered],
            features: Vec::new(),
        }
    }

    #[test]
    fn membership_admits_compatible_peer() {
        let local = peer("node-a");
        let remote = peer("node-b");
        let mut membership = ClusterMembership::new(NodeId::new("node-a"));

        let decision = membership.admit(&local, &remote, ClusterRole::Follower);

        assert_eq!(
            decision,
            AdmissionDecision::Accepted {
                compatibility: CompatibilityState::Downgraded,
            }
        );
        assert!(membership.peers.contains_key(&NodeId::new("node-b")));
    }

    #[test]
    fn membership_rejects_duplicate_node() {
        let local = peer("node-a");
        let remote = peer("node-b");
        let mut membership = ClusterMembership::new(NodeId::new("node-a"));
        let _ = membership.admit(&local, &remote, ClusterRole::Follower);

        let decision = membership.admit(&local, &remote, ClusterRole::Follower);

        assert_eq!(
            decision,
            AdmissionDecision::Rejected {
                reason: AdmissionRejection::DuplicateNodeId(NodeId::new("node-b")),
            }
        );
    }

    #[test]
    fn replication_state_tracks_desired_revision_from_snapshot() {
        let mut state = DesiredClusterState::default();
        ensure_node_present(
            &mut state,
            NodeRecord {
                node_id: NodeId::new("node-a"),
                health: HealthState::Healthy,
                schedulable: true,
                labels: Vec::new(),
                clock: None,
                host: None,
            },
        );
        let mut replication = ReplicationState::new(orion_core::Revision::ZERO);

        replication.apply_snapshot(&state);

        assert_eq!(replication.last_desired_revision, state.revision);
    }
}

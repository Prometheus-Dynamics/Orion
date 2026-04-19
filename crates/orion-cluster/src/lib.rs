//! Cluster membership, admission, replication, and assignment helpers for
//! Orion.

mod assignment;
mod membership;

pub use assignment::ClusterCoordinator;
pub use membership::{
    AdmissionDecision, AdmissionRejection, ClusterMembership, ClusterPeer, ClusterRole,
    ReplicationState, ensure_node_present, negotiation_error_kind,
};

#[cfg(test)]
mod tests {
    use super::*;
    use orion_control_plane::{DesiredClusterState, DesiredState, NodeRecord, WorkloadRecord};
    use orion_control_plane::{HealthState, RestartPolicy, WorkloadObservedState};
    use orion_core::{
        ArtifactId, CompatibilityState, NodeId, ProtocolVersion, ResourceType, RuntimeType,
        WorkloadId,
    };
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
    fn coordinator_assigns_workload_inside_desired_state() {
        let coordinator = ClusterCoordinator;
        let workload_id = WorkloadId::new("workload.pose");
        let mut state = DesiredClusterState::default();
        state.put_workload(WorkloadRecord {
            workload_id: workload_id.clone(),
            runtime_type: RuntimeType::new("graph.exec.v1"),
            artifact_id: ArtifactId::new("artifact.pose"),
            config: None,
            desired_state: DesiredState::Running,
            observed_state: WorkloadObservedState::Pending,
            assigned_node_id: None,
            requirements: vec![orion_control_plane::WorkloadRequirement {
                resource_type: ResourceType::new("imu.sample_source"),
                count: 1,
                ownership_mode: None,
                required_capabilities: Vec::new(),
            }],
            resource_bindings: Vec::new(),
            restart_policy: RestartPolicy::OnFailure,
        });
        let previous_revision = state.revision;

        let assigned = coordinator.assign(&mut state, &workload_id, NodeId::new("node-a"));

        assert!(assigned);
        assert_eq!(
            state.workloads[&workload_id].assigned_node_id,
            Some(NodeId::new("node-a"))
        );
        assert_eq!(
            state.workloads[&workload_id].observed_state,
            WorkloadObservedState::Pending
        );
        assert!(state.revision > previous_revision);
    }

    #[test]
    fn replication_state_tracks_desired_revision_from_snapshot() {
        let mut state = DesiredClusterState::default();
        ensure_node_present(
            &mut state,
            NodeRecord {
                node_id: NodeId::new("node-a"),
                health: HealthState::Healthy,
                labels: Vec::new(),
            },
        );
        let mut replication = ReplicationState::new(orion_core::Revision::ZERO);

        replication.apply_snapshot(&state);

        assert_eq!(replication.last_desired_revision, state.revision);
    }
}

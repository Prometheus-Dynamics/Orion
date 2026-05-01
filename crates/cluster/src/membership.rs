use orion_control_plane::{DesiredClusterState, NodeRecord};
use orion_core::{CompatibilityState, NodeId, Revision};
use orion_data_plane::{NegotiationError, PeerCapabilities, PeerLink};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClusterRole {
    Coordinator,
    Follower,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdmissionDecision {
    Accepted { compatibility: CompatibilityState },
    Rejected { reason: AdmissionRejection },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdmissionRejection {
    DuplicateNodeId(NodeId),
    IncompatiblePeer,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterPeer {
    pub node_id: NodeId,
    pub role: ClusterRole,
    pub link: PeerLink,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterMembership {
    pub coordinator_id: NodeId,
    pub peers: BTreeMap<NodeId, ClusterPeer>,
}

impl ClusterMembership {
    pub fn new(coordinator_id: NodeId) -> Self {
        Self {
            coordinator_id,
            peers: BTreeMap::new(),
        }
    }

    pub fn admit(
        &mut self,
        local: &PeerCapabilities,
        remote: &PeerCapabilities,
        role: ClusterRole,
    ) -> AdmissionDecision {
        if self.peers.contains_key(&remote.node_id) || self.coordinator_id == remote.node_id {
            return AdmissionDecision::Rejected {
                reason: AdmissionRejection::DuplicateNodeId(remote.node_id.clone()),
            };
        }

        match local.negotiate_with(remote) {
            Ok(link) => {
                let compatibility = link.compatibility;
                let peer = ClusterPeer {
                    node_id: remote.node_id.clone(),
                    role,
                    link,
                };
                self.peers.insert(remote.node_id.clone(), peer);
                AdmissionDecision::Accepted { compatibility }
            }
            Err(_err) => AdmissionDecision::Rejected {
                reason: AdmissionRejection::IncompatiblePeer,
            },
        }
    }

    pub fn remove(&mut self, node_id: &NodeId) -> Option<ClusterPeer> {
        self.peers.remove(node_id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationState {
    pub last_desired_revision: Revision,
}

impl ReplicationState {
    pub fn new(last_desired_revision: Revision) -> Self {
        Self {
            last_desired_revision,
        }
    }

    pub fn apply_snapshot(&mut self, state: &DesiredClusterState) {
        self.last_desired_revision = state.revision;
    }
}

pub fn ensure_node_present(state: &mut DesiredClusterState, record: NodeRecord) {
    state.put_node(record);
}

pub fn negotiation_error_kind(err: &NegotiationError) -> &'static str {
    match err {
        NegotiationError::NoControlVersion => "no_control_version",
        NegotiationError::NoDataVersion => "no_data_version",
        NegotiationError::NoTransport => "no_transport",
        NegotiationError::NoLinkType => "no_link_type",
    }
}

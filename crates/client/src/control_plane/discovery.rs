//! Peer discovery and enrollment requests (`docs/discovery.md`).

use super::LocalControlPlaneClient;
use crate::{error::ClientError, response::expect_accepted};
use orion_control_plane::{ControlMessage, DiscoveredPeerEnrollment, DiscoverySnapshot};
use orion_core::NodeId;

impl LocalControlPlaneClient {
    /// Discovered peers and discovery/enrollment counters. Empty when discovery is off.
    pub async fn query_discovery(&self) -> Result<DiscoverySnapshot, ClientError> {
        self.send_request_with(ControlMessage::QueryDiscovery, |message| match message {
            ControlMessage::Discovery(snapshot) => Ok(*snapshot),
            ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
            _ => Err(ClientError::NoMessageAvailable),
        })
        .await
    }

    /// Operator approval of a discovered peer: pins its advertised key (optionally only if it
    /// still has `expected_key_fingerprint`) and registers it for sync.
    pub async fn enroll_discovered_peer(
        &self,
        enrollment: DiscoveredPeerEnrollment,
    ) -> Result<(), ClientError> {
        self.send_request_with(
            ControlMessage::EnrollDiscoveredPeer(enrollment),
            expect_accepted,
        )
        .await
    }

    /// Revokes a peer's key, stops syncing with it and forgets its discovery enrollment.
    pub async fn remove_peer(&self, node_id: NodeId) -> Result<(), ClientError> {
        self.send_request_with(ControlMessage::RemovePeer(node_id), expect_accepted)
            .await
    }
}

//! Robot-link peers in open mode (`docs/discovery.md`, "Robot links").
//!
//! A robot-link hello is signed by its sender and verified by the authenticator, which leaves a
//! [`RobotLinkProof`] on the request. This module applies the robot-link policy to it: the key is
//! pinned on first use from a robot link, and the peer is then registered for sync at the address
//! the connection came from, on the port the sender announced.

use super::{AuditEventKind, NodeApp};
use crate::auth::RobotLinkProof;
use crate::peer::{PeerConfig, PeerState};
#[cfg(peer_sync)]
use orion::NodeId;
use orion::control_plane::{ControlMessage, RobotLinkListener};
use std::net::{IpAddr, SocketAddr};
#[cfg(peer_sync)]
use tracing::debug;
use tracing::warn;

impl NodeApp {
    /// Applies the robot-link policy to a request whose signature verified. A refused trust
    /// decision is logged and the request is still answered as usual: refusing it would fail sync
    /// with a peer that merely lacks the policy, and the peer is then simply not registered.
    pub(crate) fn note_robot_link_message(
        &self,
        proof: &RobotLinkProof,
        remote: Option<IpAddr>,
        message: &ControlMessage,
    ) {
        let announced_port = match message {
            ControlMessage::Hello(hello) if hello.node_id == proof.node_id => None,
            ControlMessage::RobotLinkListener(RobotLinkListener { node_id, port })
                if node_id == &proof.node_id =>
            {
                Some(*port)
            }
            _ => return,
        };
        if let Err(err) =
            self.security
                .trust_robot_link_peer(&proof.node_id, remote, proof.public_key)
        {
            warn!(peer = %proof.node_id, error = %err, "robot-link peer not trusted");
            return;
        }
        let (Some(port), Some(remote)) = (announced_port, remote) else {
            return;
        };
        let base_url = format!(
            "orion+tcp://{}",
            SocketAddr::new(remote.to_canonical(), port)
        );
        let registered = self.with_peers_mut(|peers| {
            if peers.contains_key(&proof.node_id) {
                return false;
            }
            let config = PeerConfig::new(
                proof.node_id.clone(),
                orion_core::PeerBaseUrl::new(base_url.clone()),
            );
            peers.insert(proof.node_id.clone(), PeerState::from_config(config));
            true
        });
        if registered {
            self.record_audit_event(
                AuditEventKind::PeerEnrolled,
                Some(proof.node_id.as_str().to_owned()),
                format!(
                    "registered robot-link peer `{}` at `{base_url}`",
                    proof.node_id
                ),
            );
        }
    }

    /// Tells a robot-link peer which port this node listens on, right after a successful hello.
    /// Only a node with robot-link trust on and a running listener announces; anything else is
    /// best effort and never fails the sync round.
    #[cfg(peer_sync)]
    pub(crate) async fn announce_robot_link_listener<
        T: super::peer_transport::PeerSyncTransport,
    >(
        &self,
        node_id: &NodeId,
        transport: &T,
    ) {
        if self.security.mode() != crate::PeerAuthenticationMode::Disabled
            || !self.security.signs_robot_link_messages()
        {
            return;
        }
        let Some(listener) = self.peer_tcp_bound.get() else {
            return;
        };
        let announcement = ControlMessage::RobotLinkListener(RobotLinkListener {
            node_id: self.config.node_id.clone(),
            port: listener.port(),
        });
        if let Err(err) = transport.send_control(self, node_id, announcement).await {
            debug!(peer = %node_id, error = %err, "robot-link listener announcement failed");
        }
    }
}

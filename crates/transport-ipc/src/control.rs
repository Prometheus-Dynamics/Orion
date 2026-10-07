use crate::LocalAddress;
use orion_control_plane::ControlMessage;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct UnixPeerIdentity {
    pub pid: Option<u32>,
    pub uid: u32,
    /// Primary group of the peer (`SO_PEERCRED`).
    pub gid: u32,
    /// Supplementary groups of the peer when the platform reports them (`SO_PEERGROUPS` on
    /// Linux 4.13 and newer); empty otherwise.
    #[serde(default)]
    pub groups: Vec<u32>,
}

impl UnixPeerIdentity {
    /// Whether the peer's primary group or one of its supplementary groups is `gid`.
    pub fn is_member_of(&self, gid: u32) -> bool {
        self.gid == gid || self.groups.contains(&gid)
    }
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ControlEnvelope {
    pub source: LocalAddress,
    pub destination: LocalAddress,
    pub message: ControlMessage,
}

pub trait LocalControlTransport {
    fn register_control_endpoint(&self, address: LocalAddress) -> bool;

    fn send_control(&self, envelope: ControlEnvelope) -> bool;

    fn recv_control(&self, address: &LocalAddress) -> Option<ControlEnvelope>;
}

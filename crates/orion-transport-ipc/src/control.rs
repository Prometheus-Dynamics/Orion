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
    pub gid: u32,
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

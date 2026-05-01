use crate::LocalAddress;
use orion_data_plane::{PeerLink, RemoteBinding};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DataEnvelope {
    pub source: LocalAddress,
    pub destination: LocalAddress,
    pub link: PeerLink,
    pub binding: RemoteBinding,
    pub payload: Vec<u8>,
}

pub trait LocalDataTransport {
    fn register_data_endpoint(&self, address: LocalAddress) -> bool;

    fn send_data(&self, envelope: DataEnvelope) -> bool;

    fn recv_data(&self, address: &LocalAddress) -> Option<DataEnvelope>;
}

use crate::QuicEndpoint;
use orion_data_plane::{PeerLink, RemoteBinding};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum QuicChannel {
    Stream(u64),
    Datagram,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct QuicFrame {
    pub source: QuicEndpoint,
    pub destination: QuicEndpoint,
    pub connection_id: u64,
    pub channel: QuicChannel,
    pub link: PeerLink,
    pub binding: RemoteBinding,
    pub payload: Vec<u8>,
}

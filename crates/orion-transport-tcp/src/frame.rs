use crate::TcpEndpoint;
use orion_data_plane::{PeerLink, RemoteBinding};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct TcpFrame {
    pub source: TcpEndpoint,
    pub destination: TcpEndpoint,
    pub link: PeerLink,
    pub binding: RemoteBinding,
    pub payload: Vec<u8>,
}

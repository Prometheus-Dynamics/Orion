use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct QuicEndpoint {
    pub host: String,
    pub port: u16,
    pub server_name: Option<String>,
}

impl QuicEndpoint {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        let host = host.into();
        assert!(
            !host.trim().is_empty(),
            "QUIC endpoint host must not be empty"
        );
        Self {
            host,
            port,
            server_name: None,
        }
    }

    pub fn with_server_name(mut self, server_name: impl Into<String>) -> Self {
        let server_name = server_name.into();
        assert!(
            !server_name.trim().is_empty(),
            "QUIC server name must not be empty"
        );
        self.server_name = Some(server_name);
        self
    }
}

impl fmt::Display for QuicEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

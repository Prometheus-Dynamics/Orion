//! The `_orion._tcp` advertisement and its TXT record. The layout lives in
//! `orion_auth::discovery` so the remote operator client browses with the same parser.

pub use orion_auth::crypto::key_fingerprint;
pub use orion_auth::discovery::{Advertisement, SERVICE_TYPE};

pub(crate) fn hex(bytes: &[u8]) -> String {
    orion_auth::hex::encode_hex(bytes)
}

pub(crate) fn parse_key_hex(value: &str) -> Result<[u8; 32], String> {
    orion_auth::hex::parse_key_hex(value)
}

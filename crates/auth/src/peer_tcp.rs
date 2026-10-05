//! Payload layout of `orion+tcp` frames, shared by `orion-node` and the remote operator client.
//!
//! Every frame travels inside the fixed control frame header used by local IPC streams
//! (`[b"OC"][CONTROL_PROTOCOL_VERSION u16 LE][payload_len u32 LE][payload]`). The payloads are:
//!
//! ```text
//! request  = [kind u8][rkyv archive]   (the HTTP codec's request body, usually an
//!                                       AuthenticatedPeerRequest)
//! response = [status u8][sig_len u8][signature][key_len u8][public key][body]
//!            status 0: body is an rkyv HttpResponsePayload; status 1: body is a UTF-8 error
//! ```
//!
//! The response signature covers [`crate::canonical_peer_response_bytes`]: the responder's id and
//! key, the exact request payload, the status and the body.

use alloc::{
    format,
    string::{String, ToString},
    vec::Vec,
};
use core::fmt;

/// URL scheme of the `orion+tcp` transport.
pub const PEER_TCP_SCHEME: &str = "orion+tcp";
/// Response status: the body is an rkyv `HttpResponsePayload`.
pub const PEER_TCP_STATUS_OK: u8 = 0;
/// Response status: the body is a UTF-8 error message.
pub const PEER_TCP_STATUS_ERROR: u8 = 1;
/// Largest response header: status, signature length, signature, key length, key.
pub const PEER_TCP_RESPONSE_HEADER_MAX_BYTES: usize = 3 + 64 + 32;

/// A malformed `orion+tcp` response frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerTcpFrameError(pub String);

impl fmt::Display for PeerTcpFrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl core::error::Error for PeerTcpFrameError {}

/// Signature and public key attached to a signed response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerResponseSignature {
    pub signature: Vec<u8>,
    pub public_key: Vec<u8>,
}

/// One `orion+tcp` response frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerTcpResponseFrame {
    pub status: u8,
    pub signature: Option<PeerResponseSignature>,
    pub body: Vec<u8>,
}

impl PeerTcpResponseFrame {
    pub fn encode(&self) -> Result<Vec<u8>, PeerTcpFrameError> {
        let (signature, public_key) = match &self.signature {
            Some(signature) => (
                signature.signature.as_slice(),
                signature.public_key.as_slice(),
            ),
            None => (&[][..], &[][..]),
        };
        let signature_len = u8::try_from(signature.len())
            .map_err(|_| PeerTcpFrameError("response signature is too long".into()))?;
        let key_len = u8::try_from(public_key.len())
            .map_err(|_| PeerTcpFrameError("response public key is too long".into()))?;
        let mut bytes =
            Vec::with_capacity(3 + signature.len() + public_key.len() + self.body.len());
        bytes.push(self.status);
        bytes.push(signature_len);
        bytes.extend_from_slice(signature);
        bytes.push(key_len);
        bytes.extend_from_slice(public_key);
        bytes.extend_from_slice(&self.body);
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, PeerTcpFrameError> {
        let malformed = || PeerTcpFrameError("truncated response frame".into());
        let (&status, rest) = bytes.split_first().ok_or_else(malformed)?;
        if status != PEER_TCP_STATUS_OK && status != PEER_TCP_STATUS_ERROR {
            return Err(PeerTcpFrameError(format!(
                "unknown response status {status}"
            )));
        }
        let (&signature_len, rest) = rest.split_first().ok_or_else(malformed)?;
        let (signature, rest) = rest
            .split_at_checked(usize::from(signature_len))
            .ok_or_else(malformed)?;
        let (&key_len, rest) = rest.split_first().ok_or_else(malformed)?;
        let (public_key, body) = rest
            .split_at_checked(usize::from(key_len))
            .ok_or_else(malformed)?;
        let signature = match (signature.is_empty(), public_key.is_empty()) {
            (true, true) => None,
            (false, false) => Some(PeerResponseSignature {
                signature: signature.to_vec(),
                public_key: public_key.to_vec(),
            }),
            _ => {
                return Err(PeerTcpFrameError(
                    "response carries a signature without a key or a key without a signature"
                        .to_string(),
                ));
            }
        };
        Ok(Self {
            status,
            signature,
            body: body.to_vec(),
        })
    }
}

/// The `host:port` of an `orion+tcp://host:port` URL.
pub fn peer_tcp_authority(base_url: &str) -> Result<&str, String> {
    let rest = base_url
        .split_once("://")
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case(PEER_TCP_SCHEME))
        .map(|(_, rest)| rest)
        .ok_or_else(|| format!("`{base_url}` is not an {PEER_TCP_SCHEME}:// URL"))?;
    let authority = rest.trim_end_matches('/');
    if authority.is_empty() || authority.contains('/') || !authority.contains(':') {
        return Err(format!(
            "`{base_url}` must have the form {PEER_TCP_SCHEME}://host:port"
        ));
    }
    Ok(authority)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn response_frames_roundtrip_and_reject_truncation() {
        let signed = PeerTcpResponseFrame {
            status: PEER_TCP_STATUS_OK,
            signature: Some(PeerResponseSignature {
                signature: vec![1; 64],
                public_key: vec![2; 32],
            }),
            body: b"body".to_vec(),
        };
        let bytes = signed.encode().expect("frame should encode");
        assert_eq!(PeerTcpResponseFrame::decode(&bytes), Ok(signed));

        let unsigned = PeerTcpResponseFrame {
            status: PEER_TCP_STATUS_ERROR,
            signature: None,
            body: b"boom".to_vec(),
        };
        let bytes = unsigned.encode().expect("frame should encode");
        assert_eq!(bytes[..3], [PEER_TCP_STATUS_ERROR, 0, 0]);
        assert_eq!(PeerTcpResponseFrame::decode(&bytes), Ok(unsigned));

        assert!(PeerTcpResponseFrame::decode(&[]).is_err());
        assert!(PeerTcpResponseFrame::decode(&[PEER_TCP_STATUS_OK, 64, 1, 2]).is_err());
        assert!(PeerTcpResponseFrame::decode(&[7, 0, 0]).is_err());
        assert!(PeerTcpResponseFrame::decode(&[PEER_TCP_STATUS_OK, 1, 9, 0]).is_err());
    }

    #[test]
    fn peer_tcp_urls_parse_to_authorities() {
        assert_eq!(
            peer_tcp_authority("orion+tcp://10.0.0.2:9200/"),
            Ok("10.0.0.2:9200")
        );
        assert!(peer_tcp_authority("orion+tcp://host").is_err());
        assert!(peer_tcp_authority("http://host:1").is_err());
    }
}

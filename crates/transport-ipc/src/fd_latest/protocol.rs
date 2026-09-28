//! Wire format for the latest-value fd channel.
//!
//! Every message is carried in a single [`UnixFdFrame`](crate::UnixFdFrame). Requests are a fixed
//! 16-byte payload without descriptors:
//!
//! ```text
//! [version u8][kind u8][reserved u16][after_sequence u64 LE][wait_ms u32 LE]
//! ```
//!
//! Responses prefix the opaque producer payload with a fixed 20-byte header and carry the dup'd
//! descriptors only for [`ResponseStatus::Frame`]:
//!
//! ```text
//! [version u8][status u8][reserved u16][sequence u64 LE][published_at_unix_nanos u64 LE][payload]
//! ```

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::IpcTransportError;

pub(super) const PROTOCOL_VERSION: u8 = 1;
pub(super) const REQUEST_BYTES: usize = 16;
pub(super) const RESPONSE_HEADER_BYTES: usize = 20;

const REQUEST_KIND_LATEST: u8 = 0;
const REQUEST_KIND_NEXT_AFTER: u8 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum LatestRequest {
    /// Reply immediately with whatever is currently published.
    Latest,
    /// Reply once a frame with a sequence greater than `sequence` exists, or after `wait`.
    NextAfter { sequence: u64, wait: Duration },
}

impl LatestRequest {
    pub(super) fn encode(self) -> Vec<u8> {
        let (kind, sequence, wait_ms) = match self {
            Self::Latest => (REQUEST_KIND_LATEST, 0, 0),
            Self::NextAfter { sequence, wait } => (
                REQUEST_KIND_NEXT_AFTER,
                sequence,
                u32::try_from(wait.as_millis()).unwrap_or(u32::MAX),
            ),
        };
        let mut bytes = Vec::with_capacity(REQUEST_BYTES);
        bytes.extend_from_slice(&[PROTOCOL_VERSION, kind, 0, 0]);
        bytes.extend_from_slice(&sequence.to_le_bytes());
        bytes.extend_from_slice(&wait_ms.to_le_bytes());
        bytes
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self, IpcTransportError> {
        if bytes.len() != REQUEST_BYTES {
            return Err(IpcTransportError::DecodeFailed(
                "fd latest request has invalid length".into(),
            ));
        }
        if bytes[0] != PROTOCOL_VERSION {
            return Err(IpcTransportError::DecodeFailed(
                "fd latest request has unsupported protocol version".into(),
            ));
        }
        let sequence = u64::from_le_bytes(bytes[4..12].try_into().expect("fixed slice length"));
        let wait_ms = u32::from_le_bytes(bytes[12..16].try_into().expect("fixed slice length"));
        match bytes[1] {
            REQUEST_KIND_LATEST => Ok(Self::Latest),
            REQUEST_KIND_NEXT_AFTER => Ok(Self::NextAfter {
                sequence,
                wait: Duration::from_millis(u64::from(wait_ms)),
            }),
            _ => Err(IpcTransportError::DecodeFailed(
                "fd latest request has unknown kind".into(),
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(super) enum ResponseStatus {
    Frame = 0,
    Empty = 1,
    Stale = 2,
    Timeout = 3,
    Busy = 4,
}

impl ResponseStatus {
    fn from_u8(value: u8) -> Result<Self, IpcTransportError> {
        match value {
            0 => Ok(Self::Frame),
            1 => Ok(Self::Empty),
            2 => Ok(Self::Stale),
            3 => Ok(Self::Timeout),
            4 => Ok(Self::Busy),
            _ => Err(IpcTransportError::DecodeFailed(
                "fd latest response has unknown status".into(),
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ResponseHeader {
    pub(super) status: ResponseStatus,
    /// Frame sequence for `Frame`/`Stale`, latest known sequence (or 0) for `Timeout`.
    pub(super) sequence: u64,
    pub(super) published_at_unix_nanos: u64,
}

impl ResponseHeader {
    pub(super) fn status_only(status: ResponseStatus, sequence: u64) -> Self {
        Self {
            status,
            sequence,
            published_at_unix_nanos: 0,
        }
    }

    pub(super) fn encode_with_payload(self, payload: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(RESPONSE_HEADER_BYTES + payload.len());
        bytes.extend_from_slice(&[PROTOCOL_VERSION, self.status as u8, 0, 0]);
        bytes.extend_from_slice(&self.sequence.to_le_bytes());
        bytes.extend_from_slice(&self.published_at_unix_nanos.to_le_bytes());
        bytes.extend_from_slice(payload);
        bytes
    }

    /// Splits a response payload into its header and the opaque producer payload (in place).
    pub(super) fn decode_from(mut bytes: Vec<u8>) -> Result<(Self, Vec<u8>), IpcTransportError> {
        if bytes.len() < RESPONSE_HEADER_BYTES {
            return Err(IpcTransportError::DecodeFailed(
                "fd latest response missing header".into(),
            ));
        }
        if bytes[0] != PROTOCOL_VERSION {
            return Err(IpcTransportError::DecodeFailed(
                "fd latest response has unsupported protocol version".into(),
            ));
        }
        let header = Self {
            status: ResponseStatus::from_u8(bytes[1])?,
            sequence: u64::from_le_bytes(bytes[4..12].try_into().expect("fixed slice length")),
            published_at_unix_nanos: u64::from_le_bytes(
                bytes[12..20].try_into().expect("fixed slice length"),
            ),
        };
        bytes.drain(..RESPONSE_HEADER_BYTES);
        Ok((header, bytes))
    }
}

pub(super) fn system_time_to_unix_nanos(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

pub(super) fn unix_nanos_to_system_time(nanos: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_nanos(nanos)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_roundtrip_and_reject_malformed_bytes() {
        for request in [
            LatestRequest::Latest,
            LatestRequest::NextAfter {
                sequence: 42,
                wait: Duration::from_millis(1_500),
            },
        ] {
            let bytes = request.encode();
            assert_eq!(bytes.len(), REQUEST_BYTES);
            assert_eq!(LatestRequest::decode(&bytes), Ok(request));
        }

        let mut bad_version = LatestRequest::Latest.encode();
        bad_version[0] = PROTOCOL_VERSION + 1;
        assert!(LatestRequest::decode(&bad_version).is_err());
        let mut bad_kind = LatestRequest::Latest.encode();
        bad_kind[1] = 9;
        assert!(LatestRequest::decode(&bad_kind).is_err());
        assert!(LatestRequest::decode(&[PROTOCOL_VERSION]).is_err());
    }

    #[test]
    fn response_header_roundtrips_and_keeps_payload_opaque() {
        let header = ResponseHeader {
            status: ResponseStatus::Frame,
            sequence: 7,
            published_at_unix_nanos: 1_234,
        };
        let bytes = header.encode_with_payload(b"opaque");
        let (decoded, payload) =
            ResponseHeader::decode_from(bytes).expect("response header should decode");
        assert_eq!(decoded, header);
        assert_eq!(payload, b"opaque");

        assert!(ResponseHeader::decode_from(vec![PROTOCOL_VERSION, 0]).is_err());
    }
}

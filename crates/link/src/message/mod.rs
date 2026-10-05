//! Typed link messages (feature `alloc`): postcard bodies carried in [`crate::frame`]s.
//!
//! Each message kind has a stable number in [`kind`]. The body of a frame is the postcard encoding
//! of the kind's body type, written straight into the frame buffer's payload area (no second
//! buffer). Receivers decode with [`Message::decode`]; kinds they do not know (including the
//! reserved ones) decode to [`Message::Unknown`] and sessions ignore them, which allows additive
//! extensions without a version bump.
//!
//! ```
//! use orion_link::message::{Message, kind};
//! let mut buf = [0u8; 32];
//! let len = Message::Ping { now_ms: 1234 }.encode(7, &mut buf).unwrap();
//! let frame = orion_link::frame::decode(&buf[..len]).unwrap();
//! assert_eq!((frame.kind(), frame.seq()), (kind::PING, 7));
//! assert_eq!(Message::decode(&frame).unwrap(), Message::Ping { now_ms: 1234 });
//! ```

mod codec;

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;

// The model types a device needs, re-exported so a port depends on `orion-link` alone.
pub use orion_control_plane::{
    AvailabilityState, HealthState, LeaseRecord, LeaseState, ProviderRecord, ResourceCapability,
    ResourceRecord, TypedConfigValue,
};
pub use orion_core::{CapabilityId, NodeId, ProviderId, ResourceId, ResourceType, WorkloadId};
use serde::{Deserialize, Serialize};

pub(crate) use codec::decode_leases;
#[cfg(feature = "std")]
pub(crate) use codec::{decode_status, encode_with};
pub use codec::{encode_leases, encode_provider_state, encode_status};

// Kind numbers, roles, and reject reasons are shared with the minimal device path.
pub use crate::wire::{RejectReason, Roles, kind};

use crate::frame::FrameError;

/// Opens (or reopens) a session. Device → host, kind [`kind::HELLO`].
///
/// The protocol version travels in the frame header, not in the body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    /// Stable device name; the host may restrict accepted names.
    pub device_name: String,
    /// Announced roles.
    pub roles: Roles,
    /// Largest frame (header + payload + CRC) the device can receive and send.
    pub max_frame: u32,
}

/// Accepts a session. Host → device, kind [`kind::WELCOME`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Welcome {
    /// The node the device is attached to.
    pub node_id: NodeId,
    /// Identifies this session; a different value means the host started a new session.
    pub session_id: u32,
    /// Interval at which the device pings; liveness is judged in multiples of it.
    pub heartbeat_ms: u32,
    /// Negotiated maximum frame length: the minimum of both sides. Neither side sends larger frames.
    pub max_frame: u32,
}

/// Full provider snapshot. Device → host, kind [`kind::PROVIDER_STATE`]. Idempotent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderState {
    /// The provider record. The gateway owns `node_id` and overwrites it.
    pub provider: ProviderRecord,
    /// Every resource the provider currently offers.
    #[serde(deserialize_with = "codec::bounded_vec")]
    pub resources: Vec<ResourceRecord>,
}

/// One volatile status value. Device → host inside a [`kind::STATUS`] frame; the gateway files it
/// in the node's status lane under the device's provider.
///
/// Status is fire-and-forget: it is never acknowledged or retransmitted, and only the newest
/// value per key matters. The node keeps it in memory only, for `ttl_ms` (`0` means the node's
/// maximum, which also caps larger values).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusEntry {
    /// Key, unique per device (at most 128 bytes on the node).
    pub key: String,
    /// The value (strings and bytes at most 1024 bytes on the node).
    pub value: TypedConfigValue,
    /// Requested time-to-live in milliseconds.
    pub ttl_ms: u32,
}

impl StatusEntry {
    /// A status entry with the node's maximum TTL.
    pub fn new(key: impl Into<String>, value: TypedConfigValue) -> Self {
        Self {
            key: key.into(),
            value,
            ttl_ms: 0,
        }
    }

    /// Requests a time-to-live.
    #[must_use]
    pub fn with_ttl_ms(mut self, ttl_ms: u32) -> Self {
        self.ttl_ms = ttl_ms;
        self
    }
}

/// A decoded link message.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Message {
    /// [`kind::HELLO`].
    Hello(Hello),
    /// [`kind::WELCOME`].
    Welcome(Welcome),
    /// [`kind::REJECT`].
    Reject(RejectReason),
    /// [`kind::PROVIDER_STATE`].
    ProviderState(ProviderState),
    /// [`kind::ACK`]: acknowledges the state message sent with `seq`.
    Ack {
        /// Sequence number of the acknowledged frame.
        seq: u16,
    },
    /// [`kind::LEASES`]: the full lease set for the device's provider.
    Leases(Vec<LeaseRecord>),
    /// [`kind::PING`].
    Ping {
        /// Sender's millisecond clock.
        now_ms: u64,
    },
    /// [`kind::PONG`].
    Pong {
        /// The `now_ms` of the ping being answered.
        now_ms: u64,
    },
    /// [`kind::STATUS`]: volatile status values of the device's provider.
    Status(Vec<StatusEntry>),
    /// A kind this build does not handle (including the reserved kinds). Ignored by sessions.
    Unknown(u8),
}

/// Message encode/decode failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MessageError {
    /// The frame buffer cannot hold the encoded message.
    BufferTooSmall {
        /// Size of the buffer that was too small.
        available: usize,
    },
    /// Postcard could not encode the body.
    Encode,
    /// The payload is not a valid body for the frame's kind.
    Decode {
        /// The frame's kind.
        kind: u8,
    },
    /// Framing failed.
    Frame(FrameError),
}

impl fmt::Display for MessageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BufferTooSmall { available } => {
                write!(f, "message does not fit in {available} bytes")
            }
            Self::Encode => f.write_str("message body could not be encoded"),
            Self::Decode { kind } => write!(f, "invalid body for message kind {kind:#04x}"),
            Self::Frame(err) => write!(f, "message frame: {err}"),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for MessageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Frame(err) => Some(err),
            _ => None,
        }
    }
}

impl From<FrameError> for MessageError {
    fn from(err: FrameError) -> Self {
        Self::Frame(err)
    }
}

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
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[cfg(feature = "std")]
pub(crate) use codec::decode_status;
pub(crate) use codec::{
    HelloRef, decode_body, decode_leases, encode_with, provider_state_payload_len,
    status_payload_len, write_provider_state_payload,
};
pub use codec::{encode_leases, encode_provider_state, encode_status};

use crate::frame::FrameError;

/// Stable message kind numbers (the frame header's `kind` byte).
///
/// Numbers are never reused. `HELLO` and `REJECT` (and their bodies) are frozen across protocol
/// versions so that a version mismatch can always be reported.
pub mod kind {
    /// [`super::Hello`], device → host.
    pub const HELLO: u8 = 0x01;
    /// [`super::Welcome`], host → device.
    pub const WELCOME: u8 = 0x02;
    /// [`super::RejectReason`], host → device.
    pub const REJECT: u8 = 0x03;
    /// `Ping { now_ms }`, either direction (sent by the device in v1).
    pub const PING: u8 = 0x04;
    /// `Pong { now_ms }`: answers a ping, echoing its `now_ms`.
    pub const PONG: u8 = 0x05;
    /// `Ack { seq }`, host → device: acknowledges a state message.
    pub const ACK: u8 = 0x06;
    /// [`super::ProviderState`], device → host.
    pub const PROVIDER_STATE: u8 = 0x10;
    /// `Leases(Vec<LeaseRecord>)`, host → device.
    pub const LEASES: u8 = 0x11;
    /// Reserved for the executor role (device → host executor snapshot).
    pub const EXECUTOR_STATE: u8 = 0x12;
    /// Reserved for the executor role (host → device workload assignments).
    pub const WORKLOADS: u8 = 0x13;
    /// `Status(Vec<StatusEntry>)`, device → host: volatile status values, fire-and-forget.
    pub const STATUS: u8 = 0x14;
}

/// Roles a device announces in [`Hello`] (bit set).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct Roles(pub u8);

impl Roles {
    /// The device publishes provider and resource state and receives leases.
    pub const PROVIDER: Self = Self(0x01);
    /// The device runs workloads (reserved; not served by v1 hosts).
    pub const EXECUTOR: Self = Self(0x02);

    /// Whether every role in `other` is set.
    #[must_use]
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    /// Union of two role sets.
    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

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

/// Why the host refused a device. Host → device, kind [`kind::REJECT`].
///
/// Encoded as a single byte (frozen across versions); unknown codes decode to
/// [`RejectReason::Other`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RejectReason {
    /// The device speaks a different `LINK_PROTOCOL_VERSION`.
    VersionMismatch,
    /// The device name is not accepted on this link.
    UnknownDevice,
    /// None of the announced roles is served by this host.
    UnsupportedRoles,
    /// The device's `max_frame` is below the host's minimum.
    FrameTooSmall,
    /// The device sent session traffic but the host has no session for it (for example after a
    /// host restart). The device reconnects immediately, without backoff.
    NoSession,
    /// A reason this build does not know.
    Other(u8),
}

impl RejectReason {
    /// Wire code.
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::VersionMismatch => 1,
            Self::UnknownDevice => 2,
            Self::UnsupportedRoles => 3,
            Self::FrameTooSmall => 4,
            Self::NoSession => 5,
            Self::Other(code) => code,
        }
    }

    /// Parses a wire code.
    #[must_use]
    pub const fn from_code(code: u8) -> Self {
        match code {
            1 => Self::VersionMismatch,
            2 => Self::UnknownDevice,
            3 => Self::UnsupportedRoles,
            4 => Self::FrameTooSmall,
            5 => Self::NoSession,
            other => Self::Other(other),
        }
    }
}

impl fmt::Display for RejectReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::VersionMismatch => f.write_str("link protocol version mismatch"),
            Self::UnknownDevice => f.write_str("device name not accepted"),
            Self::UnsupportedRoles => f.write_str("no supported role announced"),
            Self::FrameTooSmall => f.write_str("device max_frame too small"),
            Self::NoSession => f.write_str("no session"),
            Self::Other(code) => write!(f, "reject reason {code}"),
        }
    }
}

impl Serialize for RejectReason {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u8(self.code())
    }
}

impl<'de> Deserialize<'de> for RejectReason {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        u8::deserialize(deserializer).map(Self::from_code)
    }
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

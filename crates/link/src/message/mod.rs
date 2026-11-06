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

pub(crate) mod body;
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
pub use codec::{
    encode_action_cancel, encode_action_request, encode_leases, encode_provider_state,
    encode_status,
};

/// Encodes action arguments or an action's output: the postcard encoding of the map (a count, then
/// `(key, value)` pairs in key order). See `docs/link-protocol.md`, "Action payloads".
///
/// # Errors
///
/// [`MessageError::Encode`] if serialization fails (it does not for these types).
pub fn encode_action_map(
    map: &alloc::collections::BTreeMap<String, TypedConfigValue>,
) -> Result<Vec<u8>, MessageError> {
    let wire: alloc::collections::BTreeMap<&String, body::ValueWire> =
        map.iter().map(|(key, value)| (key, value.into())).collect();
    postcard::to_allocvec(&wire).map_err(|_| MessageError::Encode)
}

/// Decodes the map [`encode_action_map`] writes. An empty payload is an empty map.
///
/// # Errors
///
/// [`MessageError::Decode`] (kind 0) if the payload is not such a map.
pub fn decode_action_map(
    payload: &[u8],
) -> Result<alloc::collections::BTreeMap<String, TypedConfigValue>, MessageError> {
    if payload.is_empty() {
        return Ok(alloc::collections::BTreeMap::new());
    }
    let wire: alloc::collections::BTreeMap<String, body::ValueWire> =
        postcard::from_bytes(payload).map_err(|_| MessageError::Decode { kind: 0 })?;
    Ok(wire
        .into_iter()
        .map(|(key, value)| (key, value.into()))
        .collect())
}

// Kind numbers, roles, and reject reasons are shared with the minimal device path.
pub use crate::wire::{RejectReason, Roles, kind};

use crate::frame::FrameError;

/// Opens (or reopens) a session. Device → host, kind [`kind::HELLO`].
///
/// The protocol version travels in the frame header, not in the body. The body's first three
/// fields are frozen; `revision` is appended, and left off the wire when it is 0, so a revision-0
/// `Hello` is byte for byte what devices sent before revisions existed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hello {
    /// Stable device name; the host may restrict accepted names.
    pub device_name: String,
    /// Announced roles.
    pub roles: Roles,
    /// Largest frame (header + payload + CRC) the device can receive and send.
    pub max_frame: u32,
    /// The highest interface revision the device speaks (`docs/design/stable-interface.md`).
    /// 0, and absent on the wire, for devices that predate revisions.
    pub revision: u16,
}

/// Accepts a session. Host → device, kind [`kind::WELCOME`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Welcome {
    /// The node the device is attached to.
    pub node_id: NodeId,
    /// Identifies this session; a different value means the host started a new session.
    pub session_id: u32,
    /// Interval at which the device pings; liveness is judged in multiples of it.
    pub heartbeat_ms: u32,
    /// Negotiated maximum frame length: the minimum of both sides. Neither side sends larger frames.
    pub max_frame: u32,
    /// The highest interface revision the host speaks; the session uses the lower of both sides'.
    /// Appended, and absent on the wire when it is 0.
    pub revision: u16,
}

/// Full provider snapshot. Device → host, kind [`kind::PROVIDER_STATE`]. Idempotent.
///
/// The records are encoded through the link's own body types (`body.rs`), so their wire does not
/// follow changes to the `orion-control-plane` records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderState {
    /// The provider record. The gateway owns `node_id` and overwrites it.
    pub provider: ProviderRecord,
    /// Every resource the provider currently offers.
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
    #[serde(with = "body::value")]
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

/// A named action the host asks a device to run. Host → device, kind [`kind::ACTION_REQUEST`].
///
/// The names and the meaning of `payload` belong to the handler, as with node actions
/// (`docs/actions.md`). The link layer only carries them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionRequest {
    /// Host-chosen correlation id, unique among the device's outstanding actions. The device
    /// echoes it in every [`ActionResult`] for this request.
    pub id: u32,
    /// Action name, free-form (see the well-known names in `docs/actions.md`).
    pub name: String,
    /// Arguments, encoded by the sender.
    #[serde(deserialize_with = "codec::bounded_vec")]
    pub payload: Vec<u8>,
}

/// State of an action on the device, in an [`ActionResult`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LinkActionState {
    /// Accepted and in progress. A later result for the same id carries the outcome.
    Running,
    /// Finished successfully; the payload is the output.
    Succeeded,
    /// Finished unsuccessfully; the payload is the reason.
    Failed,
    /// Refused by the device without running; the payload is the reason.
    Rejected,
    /// Stopped at the host's [`ActionCancel`]; the payload is empty. Final.
    Cancelled,
}

/// The names of the actions a device handles, declared with `DeviceSession::declare_actions`.
/// Device → host, kind [`kind::ACTION_CATALOG`]. The body is a postcard sequence of strings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionCatalog {
    /// Action names, in the order the device declared them.
    #[serde(deserialize_with = "codec::bounded_vec")]
    pub names: Vec<String>,
}

/// The device's report on an [`ActionRequest`]. Device → host, kind [`kind::ACTION_RESULT`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionResult {
    /// The `id` of the request this answers.
    pub id: u32,
    /// The action's state.
    pub state: LinkActionState,
    /// Output (an action map, see [`encode_action_map`]) for `Succeeded`, a UTF-8 reason for
    /// `Failed` and `Rejected`, and an optional progress (a `u16` varint, per-mille) for `Running`.
    #[serde(deserialize_with = "codec::bounded_vec")]
    pub payload: Vec<u8>,
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
    /// [`kind::ACTION_REQUEST`]: run a named action on the device.
    ActionRequest(ActionRequest),
    /// [`kind::ACTION_RESULT`]: the state of an action the device runs.
    ActionResult(ActionResult),
    /// [`kind::ACTION_CANCEL`]: stop the action with this `id` (accepted while the device is busy).
    ActionCancel {
        /// The `id` of the request to stop.
        id: u32,
    },
    /// [`kind::ACTION_CATALOG`]: the action names the device handles.
    ActionCatalog(ActionCatalog),
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

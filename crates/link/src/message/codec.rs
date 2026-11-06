//! Postcard encoding of message bodies into frame buffers, and decoding from frame views.

use alloc::vec::Vec;
use core::fmt;
use core::marker::PhantomData;

use serde::de::{SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use super::body::{LeaseWire, ProviderWire, ResourceWire};
use super::{
    ActionCatalog, ActionRequest, ActionResult, Hello, LeaseRecord, Message, MessageError,
    ProviderRecord, ProviderState, RejectReason, ResourceRecord, Roles, StatusEntry, Welcome, kind,
};
use crate::frame::{self, FrameHeader, FrameView};
use orion_core::NodeId;

/// Elements preallocated for a decoded sequence, whatever length the (untrusted) payload claims.
/// Longer sequences grow as elements actually decode, so memory stays bounded by the frame size.
const MAX_PREALLOC: usize = 8;

/// Deserializes a `Vec<T>` without trusting the encoded length for preallocation.
pub(super) fn bounded_vec<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct BoundedVisitor<T>(PhantomData<T>);

    impl<'de, T: Deserialize<'de>> Visitor<'de> for BoundedVisitor<T> {
        type Value = Vec<T>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a sequence")
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<T>, A::Error> {
            let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(MAX_PREALLOC));
            while let Some(value) = seq.next_element()? {
                out.push(value);
            }
            Ok(out)
        }
    }

    deserializer.deserialize_seq(BoundedVisitor(PhantomData))
}

/// A lease set: a sequence of [`LeaseWire`]s, decoded with [`bounded_vec`].
#[derive(Deserialize)]
struct LeasesBody {
    #[serde(deserialize_with = "bounded_vec")]
    leases: Vec<LeaseWire>,
}

/// Same wire format as `Vec<StatusEntry>`, decoded with [`bounded_vec`].
#[derive(Deserialize)]
struct StatusBody {
    #[serde(deserialize_with = "bounded_vec")]
    entries: Vec<StatusEntry>,
}

/// Borrowed [`ActionRequest`] (same wire format).
#[derive(Serialize)]
struct ActionRequestRef<'a> {
    id: u32,
    name: &'a str,
    payload: &'a [u8],
}

/// A [`ProviderState`] body.
#[derive(Serialize, Deserialize)]
struct ProviderStateWire {
    provider: ProviderWire,
    #[serde(deserialize_with = "bounded_vec")]
    resources: Vec<ResourceWire>,
}

impl From<ProviderStateWire> for ProviderState {
    fn from(wire: ProviderStateWire) -> Self {
        Self {
            provider: wire.provider.into(),
            resources: wire.resources.into_iter().map(Into::into).collect(),
        }
    }
}

/// The frozen prefix of a [`Hello`] body; `revision` follows it when it is not 0.
#[derive(Serialize, Deserialize)]
struct HelloPrefix {
    device_name: alloc::string::String,
    roles: Roles,
    max_frame: u32,
}

/// The [`Welcome`] fields before `revision`.
#[derive(Serialize, Deserialize)]
struct WelcomePrefix {
    node_id: NodeId,
    session_id: u32,
    heartbeat_ms: u32,
    max_frame: u32,
}

/// Serializes `prefix`, then `revision` unless it is 0, so a revision-0 body keeps the bytes it had
/// before revisions were appended.
fn serialize_with_revision<S: serde::Serializer, P: Serialize>(
    prefix: &P,
    revision: u16,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    use serde::ser::SerializeTuple;
    let mut tuple = serializer.serialize_tuple(if revision == 0 { 1 } else { 2 })?;
    tuple.serialize_element(prefix)?;
    if revision != 0 {
        tuple.serialize_element(&revision)?;
    }
    tuple.end()
}

/// Decodes `P`, then the `revision` appended after it: 0 when absent (a sender that predates
/// revisions) or unreadable. Bytes after the revision are ignored, as after any body.
fn decode_with_revision<'a, P: Deserialize<'a>>(
    kind: u8,
    payload: &'a [u8],
) -> Result<(P, u16), MessageError> {
    let (prefix, rest) =
        postcard::take_from_bytes::<P>(payload).map_err(|_| MessageError::Decode { kind })?;
    let revision = postcard::take_from_bytes::<u16>(rest).map_or(0, |(revision, _)| revision);
    Ok((prefix, revision))
}

impl Serialize for Hello {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let prefix = HelloPrefix {
            device_name: self.device_name.clone(),
            roles: self.roles,
            max_frame: self.max_frame,
        };
        serialize_with_revision(&prefix, self.revision, serializer)
    }
}

impl Serialize for Welcome {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let prefix = WelcomePrefix {
            node_id: self.node_id.clone(),
            session_id: self.session_id,
            heartbeat_ms: self.heartbeat_ms,
            max_frame: self.max_frame,
        };
        serialize_with_revision(&prefix, self.revision, serializer)
    }
}

fn decode_hello(payload: &[u8]) -> Result<Hello, MessageError> {
    let (prefix, revision) = decode_with_revision::<HelloPrefix>(kind::HELLO, payload)?;
    Ok(Hello {
        device_name: prefix.device_name,
        roles: prefix.roles,
        max_frame: prefix.max_frame,
        revision,
    })
}

fn decode_welcome(payload: &[u8]) -> Result<Welcome, MessageError> {
    let (prefix, revision) = decode_with_revision::<WelcomePrefix>(kind::WELCOME, payload)?;
    Ok(Welcome {
        node_id: prefix.node_id,
        session_id: prefix.session_id,
        heartbeat_ms: prefix.heartbeat_ms,
        max_frame: prefix.max_frame,
        revision,
    })
}

fn map_encode_error(err: postcard::Error, available: usize) -> MessageError {
    match err {
        postcard::Error::SerializeBufferFull => MessageError::BufferTooSmall { available },
        _ => MessageError::Encode,
    }
}

/// Serializes `body` into the payload area of `buf` and returns the payload length. Finish the
/// frame later with [`frame::encode_in_place`].
pub(crate) fn encode_payload<T: Serialize + ?Sized>(
    body: &T,
    buf: &mut [u8],
) -> Result<usize, MessageError> {
    let available = buf.len();
    postcard::to_slice(body, frame::payload_area(buf))
        .map(|payload| payload.len())
        .map_err(|err| map_encode_error(err, available))
}

/// Encodes a complete frame of `kind` with `body` into `buf`, returning the frame length.
pub(crate) fn encode_with<T: Serialize + ?Sized>(
    kind: u8,
    seq: u16,
    body: &T,
    buf: &mut [u8],
) -> Result<usize, MessageError> {
    let payload_len = encode_payload(body, buf)?;
    Ok(frame::encode_in_place(
        FrameHeader::new(kind, seq),
        payload_len,
        buf,
    )?)
}

/// Encodes a [`kind::PROVIDER_STATE`] frame from borrowed records, returning the frame length.
///
/// # Errors
///
/// [`MessageError::BufferTooSmall`] if the frame does not fit in `buf`.
pub fn encode_provider_state(
    provider: &ProviderRecord,
    resources: &[ResourceRecord],
    seq: u16,
    buf: &mut [u8],
) -> Result<usize, MessageError> {
    encode_with(
        kind::PROVIDER_STATE,
        seq,
        &ProviderStateWire {
            provider: provider.into(),
            resources: resources.iter().map(ResourceWire::from).collect(),
        },
        buf,
    )
}

/// Encodes a [`kind::LEASES`] frame from a borrowed lease set, returning the frame length.
///
/// # Errors
///
/// [`MessageError::BufferTooSmall`] if the frame does not fit in `buf`.
pub fn encode_leases(
    leases: &[LeaseRecord],
    seq: u16,
    buf: &mut [u8],
) -> Result<usize, MessageError> {
    let leases: Vec<LeaseWire> = leases.iter().map(LeaseWire::from).collect();
    encode_with(kind::LEASES, seq, &leases, buf)
}

/// Encodes a [`kind::ACTION_REQUEST`] frame from borrowed fields, returning the frame length.
///
/// # Errors
///
/// [`MessageError::BufferTooSmall`] if the frame does not fit in `buf`.
pub fn encode_action_request(
    id: u32,
    name: &str,
    payload: &[u8],
    seq: u16,
    buf: &mut [u8],
) -> Result<usize, MessageError> {
    encode_with(
        kind::ACTION_REQUEST,
        seq,
        &ActionRequestRef { id, name, payload },
        buf,
    )
}

/// Encodes a [`kind::ACTION_CANCEL`] frame for the request `id`, returning the frame length.
///
/// # Errors
///
/// [`MessageError::BufferTooSmall`] if the frame does not fit in `buf`.
pub fn encode_action_cancel(id: u32, seq: u16, buf: &mut [u8]) -> Result<usize, MessageError> {
    encode_with(kind::ACTION_CANCEL, seq, &id, buf)
}

/// Encodes a [`kind::STATUS`] frame from borrowed entries, returning the frame length.
///
/// # Errors
///
/// [`MessageError::BufferTooSmall`] if the frame does not fit in `buf`.
pub fn encode_status(
    entries: &[StatusEntry],
    seq: u16,
    buf: &mut [u8],
) -> Result<usize, MessageError> {
    encode_with(kind::STATUS, seq, entries, buf)
}

/// Decodes a [`kind::STATUS`] body.
pub(crate) fn decode_status(payload: &[u8]) -> Result<Vec<StatusEntry>, MessageError> {
    decode_body::<StatusBody>(kind::STATUS, payload).map(|body| body.entries)
}

/// Decodes one body type directly. Sessions use this instead of [`Message::decode`] so a device
/// only links the decoders for the kinds it actually receives.
pub(crate) fn decode_body<'a, T: Deserialize<'a>>(
    kind: u8,
    payload: &'a [u8],
) -> Result<T, MessageError> {
    postcard::from_bytes(payload).map_err(|_| MessageError::Decode { kind })
}

/// Decodes a [`kind::LEASES`] body.
pub(crate) fn decode_leases(payload: &[u8]) -> Result<Vec<LeaseRecord>, MessageError> {
    decode_body::<LeasesBody>(kind::LEASES, payload)
        .map(|body| body.leases.into_iter().map(Into::into).collect())
}

impl Message {
    /// The frame kind for this message.
    #[must_use]
    pub const fn kind(&self) -> u8 {
        match self {
            Self::Hello(_) => kind::HELLO,
            Self::Welcome(_) => kind::WELCOME,
            Self::Reject(_) => kind::REJECT,
            Self::ProviderState(_) => kind::PROVIDER_STATE,
            Self::Ack { .. } => kind::ACK,
            Self::Leases(_) => kind::LEASES,
            Self::Ping { .. } => kind::PING,
            Self::Pong { .. } => kind::PONG,
            Self::Status(_) => kind::STATUS,
            Self::ActionRequest(_) => kind::ACTION_REQUEST,
            Self::ActionResult(_) => kind::ACTION_RESULT,
            Self::ActionCancel { .. } => kind::ACTION_CANCEL,
            Self::ActionCatalog(_) => kind::ACTION_CATALOG,
            Self::RequestError(_) => kind::REQUEST_ERROR,
            Self::RequestEnd { .. } => kind::REQUEST_END,
            Self::StatusQuery(_) => kind::STATUS_QUERY,
            Self::StatusWatch(_) => kind::STATUS_WATCH,
            Self::StatusChanges(_) => kind::STATUS_CHANGES,
            Self::ActionRun(_) => kind::ACTION_RUN,
            Self::ActionQuery(_) => kind::ACTION_QUERY,
            Self::ActionWatch(_) => kind::ACTION_WATCH,
            Self::ActionResults(_) => kind::ACTION_RESULTS,
            Self::StreamDeclare(_) => kind::STREAM_DECLARE,
            Self::StreamPublish(_) => kind::STREAM_PUBLISH,
            Self::StreamDemand(_) => kind::STREAM_DEMAND,
            Self::StreamSubscribe(_) => kind::STREAM_SUBSCRIBE,
            Self::StreamCatalog(_) => kind::STREAM_CATALOG,
            Self::StreamData(_) => kind::STREAM_DATA,
            Self::StreamReset(_) => kind::STREAM_RESET,
            Self::Unknown(kind) => *kind,
        }
    }

    /// Encodes the complete frame (header, postcard body, CRC) with sequence number `seq` into
    /// `buf`, serializing directly into the payload area. Returns the frame length.
    ///
    /// # Errors
    ///
    /// [`MessageError::BufferTooSmall`] if the frame does not fit; [`MessageError::Encode`] for
    /// [`Message::Unknown`], which has no body.
    pub fn encode(&self, seq: u16, buf: &mut [u8]) -> Result<usize, MessageError> {
        let kind = self.kind();
        if let Some(encoded) = super::client::encode_client(self, seq, buf) {
            return encoded;
        }
        if let Some(encoded) = super::streams::encode_stream(self, seq, buf) {
            return encoded;
        }
        match self {
            Self::Hello(body) => encode_with(kind, seq, body, buf),
            Self::Welcome(body) => encode_with(kind, seq, body, buf),
            Self::Reject(reason) => encode_with(kind, seq, reason, buf),
            Self::ProviderState(body) => {
                encode_provider_state(&body.provider, &body.resources, seq, buf)
            }
            Self::Ack { seq: acked } => encode_with(kind, seq, acked, buf),
            Self::Leases(leases) => encode_leases(leases, seq, buf),
            Self::Ping { now_ms } | Self::Pong { now_ms } => encode_with(kind, seq, now_ms, buf),
            Self::Status(entries) => encode_status(entries, seq, buf),
            Self::ActionRequest(body) => {
                encode_action_request(body.id, &body.name, &body.payload, seq, buf)
            }
            Self::ActionResult(body) => encode_with(kind, seq, body, buf),
            Self::ActionCancel { id } => encode_with(kind, seq, id, buf),
            Self::ActionCatalog(body) => encode_with(kind, seq, body, buf),
            Self::Unknown(_) => Err(MessageError::Encode),
            // Encoded by `client::encode_client` above.
            Self::RequestError(_)
            | Self::RequestEnd { .. }
            | Self::StatusQuery(_)
            | Self::StatusWatch(_)
            | Self::StatusChanges(_)
            | Self::ActionRun(_)
            | Self::ActionQuery(_)
            | Self::ActionWatch(_)
            | Self::ActionResults(_)
            // Encoded by `streams::encode_stream` above.
            | Self::StreamDeclare(_)
            | Self::StreamPublish(_)
            | Self::StreamDemand(_)
            | Self::StreamSubscribe(_)
            | Self::StreamCatalog(_)
            | Self::StreamData(_)
            | Self::StreamReset(_) => Err(MessageError::Encode),
        }
    }

    /// Decodes the body of a validated frame. The frame's version is not checked here (see
    /// [`crate::frame::FrameHeader::is_current_version`]). Unknown and reserved kinds return
    /// [`Message::Unknown`]. Trailing payload bytes after the body are ignored.
    ///
    /// # Errors
    ///
    /// [`MessageError::Decode`] if the payload is not a valid body for the frame's kind.
    pub fn decode(frame: &FrameView<'_>) -> Result<Self, MessageError> {
        Self::decode_payload(frame.kind(), frame.payload())
    }

    /// Decodes a body of `kind` from `payload`; see [`Message::decode`].
    ///
    /// # Errors
    ///
    /// [`MessageError::Decode`] if the payload is not a valid body for `kind`.
    pub fn decode_payload(kind: u8, payload: &[u8]) -> Result<Self, MessageError> {
        if let Some(decoded) = super::client::decode_client(kind, payload) {
            return decoded;
        }
        if let Some(decoded) = super::streams::decode_stream(kind, payload) {
            return decoded;
        }
        Ok(match kind {
            kind::HELLO => Self::Hello(decode_hello(payload)?),
            kind::WELCOME => Self::Welcome(decode_welcome(payload)?),
            kind::REJECT => Self::Reject(decode_body::<RejectReason>(kind, payload)?),
            kind::PROVIDER_STATE => {
                Self::ProviderState(decode_body::<ProviderStateWire>(kind, payload)?.into())
            }
            kind::ACK => Self::Ack {
                seq: decode_body(kind, payload)?,
            },
            kind::LEASES => Self::Leases(decode_leases(payload)?),
            kind::PING => Self::Ping {
                now_ms: decode_body(kind, payload)?,
            },
            kind::PONG => Self::Pong {
                now_ms: decode_body(kind, payload)?,
            },
            kind::STATUS => Self::Status(decode_status(payload)?),
            kind::ACTION_REQUEST => {
                Self::ActionRequest(decode_body::<ActionRequest>(kind, payload)?)
            }
            kind::ACTION_RESULT => Self::ActionResult(decode_body::<ActionResult>(kind, payload)?),
            kind::ACTION_CANCEL => Self::ActionCancel {
                id: decode_body(kind, payload)?,
            },
            kind::ACTION_CATALOG => {
                Self::ActionCatalog(decode_body::<ActionCatalog>(kind, payload)?)
            }
            // Firmware-transfer kinds are reserved; they decode as unknown like any other kind.
            other => Self::Unknown(other),
        })
    }

    /// Validates `bytes` as a frame and decodes its header and message.
    ///
    /// # Errors
    ///
    /// [`MessageError::Frame`] for a bad frame, [`MessageError::Decode`] for a bad body.
    pub fn decode_frame(bytes: &[u8]) -> Result<(FrameHeader, Self), MessageError> {
        let view = frame::decode(bytes)?;
        Ok((view.header(), Self::decode(&view)?))
    }
}

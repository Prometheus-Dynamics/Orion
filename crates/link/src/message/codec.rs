//! Postcard encoding of message bodies into frame buffers, and decoding from frame views.

use alloc::vec::Vec;
use core::fmt;
use core::marker::PhantomData;

use serde::de::{SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use super::{
    Hello, LeaseRecord, Message, MessageError, ProviderRecord, ProviderState, RejectReason,
    ResourceRecord, Roles, StatusEntry, Welcome, kind,
};
use crate::frame::{self, FrameHeader, FrameView};

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

/// Same wire format as `Vec<LeaseRecord>`, decoded with [`bounded_vec`].
#[derive(Deserialize)]
struct LeasesBody {
    #[serde(deserialize_with = "bounded_vec")]
    leases: Vec<LeaseRecord>,
}

/// Same wire format as `Vec<StatusEntry>`, decoded with [`bounded_vec`].
#[derive(Deserialize)]
struct StatusBody {
    #[serde(deserialize_with = "bounded_vec")]
    entries: Vec<StatusEntry>,
}

/// Borrowed [`Hello`] (same wire format).
#[derive(Serialize)]
pub(crate) struct HelloRef<'a> {
    pub(crate) device_name: &'a str,
    pub(crate) roles: Roles,
    pub(crate) max_frame: u32,
}

/// Borrowed [`ProviderState`] (same wire format).
#[derive(Serialize)]
struct ProviderStateRef<'a> {
    provider: &'a ProviderRecord,
    resources: &'a [ResourceRecord],
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

/// Encodes a [`kind::PROVIDER_STATE`] frame from borrowed records (no clone), returning the frame
/// length.
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
        &ProviderStateRef {
            provider,
            resources,
        },
        buf,
    )
}

/// Postcard length of a provider-state body, computed without writing it.
pub(crate) fn provider_state_payload_len(
    provider: &ProviderRecord,
    resources: &[ResourceRecord],
) -> Result<usize, MessageError> {
    postcard::serialize_with_flavor(
        &ProviderStateRef {
            provider,
            resources,
        },
        postcard::ser_flavors::Size::default(),
    )
    .map_err(|_| MessageError::Encode)
}

/// Writes a provider-state body into the payload area of `buf`, returning the payload length.
pub(crate) fn write_provider_state_payload(
    provider: &ProviderRecord,
    resources: &[ResourceRecord],
    buf: &mut [u8],
) -> Result<usize, MessageError> {
    encode_payload(
        &ProviderStateRef {
            provider,
            resources,
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
    encode_with(kind::LEASES, seq, leases, buf)
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

/// Postcard length of a status body, computed without writing it.
pub(crate) fn status_payload_len(entries: &[StatusEntry]) -> Result<usize, MessageError> {
    postcard::serialize_with_flavor(entries, postcard::ser_flavors::Size::default())
        .map_err(|_| MessageError::Encode)
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
    decode_body::<LeasesBody>(kind::LEASES, payload).map(|body| body.leases)
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
            Self::Unknown(_) => Err(MessageError::Encode),
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
        Ok(match kind {
            kind::HELLO => Self::Hello(decode_body::<Hello>(kind, payload)?),
            kind::WELCOME => Self::Welcome(decode_body::<Welcome>(kind, payload)?),
            kind::REJECT => Self::Reject(decode_body::<RejectReason>(kind, payload)?),
            kind::PROVIDER_STATE => {
                Self::ProviderState(decode_body::<ProviderState>(kind, payload)?)
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

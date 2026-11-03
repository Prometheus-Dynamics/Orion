//! Action messages of protocol 1 (feature `device`): no allocator.
//!
//! `ActionRequest { id: u32, name: String, payload: Vec<u8> }` (host → device) and
//! `ActionResult { id: u32, state, payload: Vec<u8> }` (device → host) with the same postcard
//! encodings as the `alloc` messages in [`crate::message`]. The request is read as a borrowed
//! [`ActionRequestView`]; the result is written from borrowed parts by the session.

use super::WireError;
use super::codec::Reader;
#[cfg(feature = "actions")]
use super::codec::{Encode, Writer};

/// State of an action as the device reports it (`ActionResult.state`, a postcard enum index).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ActionState {
    /// Accepted and in progress. The payload is an optional per-mille progress (a `u16` varint),
    /// empty when unknown.
    Running,
    /// Finished successfully; the payload is the output.
    Succeeded,
    /// Finished unsuccessfully; the payload is the reason (UTF-8).
    Failed,
    /// Refused without running; the payload is the reason (UTF-8).
    Rejected,
}

impl ActionState {
    /// The postcard variant index.
    #[must_use]
    pub const fn index(self) -> u8 {
        match self {
            Self::Running => 0,
            Self::Succeeded => 1,
            Self::Failed => 2,
            Self::Rejected => 3,
        }
    }

    /// Parses a variant index.
    #[must_use]
    pub const fn from_index(index: u32) -> Option<Self> {
        Some(match index {
            0 => Self::Running,
            1 => Self::Succeeded,
            2 => Self::Failed,
            3 => Self::Rejected,
            _ => return None,
        })
    }

    /// Whether this is a final state (the device is done with the request).
    #[must_use]
    pub const fn is_final(self) -> bool {
        !matches!(self, Self::Running)
    }
}

/// An `ActionRequest` body, borrowed from the receive buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActionRequestView<'a> {
    /// Host-chosen correlation id; echoed in every result.
    pub id: u32,
    /// Action name (free-form; the handler decides what it runs).
    pub name: &'a str,
    /// Arguments, encoded by the host (see `docs/link-protocol.md`).
    pub payload: &'a [u8],
}

impl<'a> ActionRequestView<'a> {
    /// Decodes an `ActionRequest` body.
    ///
    /// # Errors
    ///
    /// [`WireError::Decode`] if the payload is not a valid body.
    pub fn decode(payload: &'a [u8]) -> Result<Self, WireError> {
        let mut r = Reader::new(payload);
        Ok(Self {
            id: r.varint_u32()?,
            name: r.str()?,
            payload: r.bytes()?,
        })
    }
}

/// An `ActionResult` body, written from borrowed parts.
#[cfg(feature = "actions")]
pub(crate) struct ActionResultBody<'a> {
    pub(crate) id: u32,
    pub(crate) state: ActionState,
    pub(crate) payload: &'a [u8],
}

#[cfg(feature = "actions")]
impl Encode for ActionResultBody<'_> {
    fn encode(&self, w: &mut Writer<'_>) {
        self.id.encode(w);
        w.varint(u64::from(self.state.index()));
        w.bytes(self.payload);
    }
}

/// Decodes a `Running` progress payload (a `u16` varint, or empty when unknown).
///
/// # Errors
///
/// [`WireError::Decode`] if the payload is not empty and not a valid `u16`.
pub fn decode_progress(payload: &[u8]) -> Result<Option<u16>, WireError> {
    if payload.is_empty() {
        return Ok(None);
    }
    Reader::new(payload).varint_u16().map(Some)
}

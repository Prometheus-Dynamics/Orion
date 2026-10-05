//! Host → device bodies, decoded without allocation. Strings borrow from the payload; trailing
//! payload bytes after a body are ignored (later versions may append fields).

use super::codec::Reader;
use super::views::LeaseState;
use super::{RejectReason, WireError};

/// Accepts a session. Host → device, kind [`super::kind::WELCOME`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WelcomeView<'a> {
    /// The node the device is attached to.
    pub node_id: &'a str,
    /// Identifies this session; a different value means the host started a new session.
    pub session_id: u32,
    /// Interval at which the device pings; liveness is judged in multiples of it.
    pub heartbeat_ms: u32,
    /// Negotiated maximum frame length: the minimum of both sides.
    pub max_frame: u32,
}

impl<'a> WelcomeView<'a> {
    /// Decodes a `Welcome` body.
    ///
    /// # Errors
    ///
    /// [`WireError::Decode`] if the payload is not a valid body.
    pub fn decode(payload: &'a [u8]) -> Result<Self, WireError> {
        let mut r = Reader::new(payload);
        Ok(Self {
            node_id: r.str()?,
            session_id: r.varint_u32()?,
            heartbeat_ms: r.varint_u32()?,
            max_frame: r.varint_u32()?,
        })
    }
}

/// Decodes a `Reject` body (one byte; unknown codes become [`RejectReason::Other`]).
///
/// # Errors
///
/// [`WireError::Decode`] for an empty payload.
pub fn decode_reject(payload: &[u8]) -> Result<RejectReason, WireError> {
    Reader::new(payload).byte().map(RejectReason::from_code)
}

/// Decodes an `Ack` body: the acknowledged sequence number.
///
/// # Errors
///
/// [`WireError::Decode`] if the payload is not a valid body.
pub fn decode_ack(payload: &[u8]) -> Result<u16, WireError> {
    Reader::new(payload).varint_u16()
}

/// Decodes a `Ping` or `Pong` body: `now_ms`.
///
/// # Errors
///
/// [`WireError::Decode`] if the payload is not a valid body.
pub fn decode_u64(payload: &[u8]) -> Result<u64, WireError> {
    Reader::new(payload).varint()
}

/// One `LeaseRecord`, borrowed from a `Leases` payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseView<'a> {
    /// The leased resource.
    pub resource_id: &'a str,
    /// Lease state.
    pub lease_state: LeaseState,
    /// Node of the (first) holder.
    pub holder_node_id: Option<&'a str>,
    /// Workload of the (first) holder.
    pub holder_workload_id: Option<&'a str>,
}

impl<'a> LeaseView<'a> {
    fn read(r: &mut Reader<'a>) -> Result<Self, WireError> {
        Ok(Self {
            resource_id: r.str()?,
            lease_state: LeaseState::from_index(r.varint_u32()?).ok_or(WireError::Decode)?,
            holder_node_id: r.option_str()?,
            holder_workload_id: r.option_str()?,
        })
    }
}

/// The lease set of a `Leases` body: an iterator of [`LeaseView`]s over a payload that was
/// validated completely by [`Leases::decode`], so iteration cannot fail.
#[derive(Debug, Clone)]
pub struct Leases<'a> {
    reader: Reader<'a>,
    remaining: usize,
}

impl<'a> Leases<'a> {
    /// No leases.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            reader: Reader::new(&[]),
            remaining: 0,
        }
    }

    /// Validates a `Leases` body (every entry, including UTF-8) and returns an iterator over it.
    ///
    /// # Errors
    ///
    /// [`WireError::Decode`] if any entry is malformed.
    pub fn decode(payload: &'a [u8]) -> Result<Self, WireError> {
        let mut reader = Reader::new(payload);
        let count = reader.varint_u32()? as usize;
        let leases = Self {
            reader: reader.clone(),
            remaining: count,
        };
        for _ in 0..count {
            LeaseView::read(&mut reader)?;
        }
        Ok(leases)
    }

    /// Number of leases not yet iterated.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.remaining
    }

    /// Whether no leases remain.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.remaining == 0
    }
}

impl<'a> Iterator for Leases<'a> {
    type Item = LeaseView<'a>;

    fn next(&mut self) -> Option<LeaseView<'a>> {
        self.remaining = self.remaining.checked_sub(1)?;
        let lease = LeaseView::read(&mut self.reader).ok();
        if lease.is_none() {
            self.remaining = 0;
        }
        lease
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for Leases<'_> {}

//! The minimal device-side message layer (feature `device`): no allocator, no serde, no
//! `core::fmt`.
//!
//! Message bodies are postcard encodings (see `docs/link-protocol.md`). This module implements the
//! subset the device needs by hand ([`Writer`], [`Reader`], [`Encode`]) together with borrowed
//! views of the bodies:
//!
//! - device → host: [`HelloView`], [`ProviderView`] and [`ResourceView`] (a `ProviderState`
//!   snapshot), [`StatusView`]. They encode to exactly the bytes postcard produces for the
//!   corresponding `orion-control-plane` records, so hosts cannot tell the difference. They are
//!   `const`-constructible, so a fixed snapshot can live in flash.
//! - host → device: [`WelcomeView`], [`RejectReason`], `Ack`, `Ping` / `Pong` (`u64`), and
//!   [`Leases`], an iterator of [`LeaseView`]s over a validated payload. Strings borrow from the
//!   receive buffer.
//!
//! ```
//! use orion_link::wire::{self, HelloView, Roles, kind};
//!
//! let mut buf = [0u8; 32];
//! let hello = HelloView { device_name: "imu-board", roles: Roles::PROVIDER, max_frame: 512 };
//! let len = wire::encode_frame(kind::HELLO, 0x0100, &hello, &mut buf).unwrap();
//! assert_eq!(&buf[4..len - 4], b"\x09imu-board\x01\x80\x04");
//! ```

mod codec;
mod decode;
mod views;

#[cfg(feature = "alloc")]
mod records;

pub use codec::{Encode, RawVarint, Reader, Writer, encoded_len, str_from_utf8};
pub use decode::{LeaseView, Leases, WelcomeView, decode_ack, decode_reject, decode_u64};
pub use views::{
    ActionResultView, ActionStatus, Availability, CapabilityView, ConfigField, Health, HelloView,
    LeaseState, Ownership, ProviderBody, ProviderStateView, ProviderView, ResourceBody,
    ResourceStateView, ResourceView, StateBody, StatusBody, StatusView, Value,
};

use crate::frame::{self, FrameHeader};

mod sealed {
    /// Restricts the body marker traits to encodings that match the records.
    pub trait Sealed {}
}

/// Stable message kind numbers (the frame header's `kind` byte).
///
/// Numbers are never reused. `HELLO` and `REJECT` (and their bodies) are frozen across protocol
/// versions so that a version mismatch can always be reported.
pub mod kind {
    /// `Hello`, device → host.
    pub const HELLO: u8 = 0x01;
    /// `Welcome`, host → device.
    pub const WELCOME: u8 = 0x02;
    /// `Reject`, host → device.
    pub const REJECT: u8 = 0x03;
    /// `Ping { now_ms }`, either direction (sent by the device in v1).
    pub const PING: u8 = 0x04;
    /// `Pong { now_ms }`: answers a ping, echoing its `now_ms`.
    pub const PONG: u8 = 0x05;
    /// `Ack { seq }`, host → device: acknowledges a state message.
    pub const ACK: u8 = 0x06;
    /// `ProviderState`, device → host.
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

/// Roles a device announces in `Hello` (bit set, encoded as one byte).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
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

impl Encode for Roles {
    fn encode(&self, w: &mut Writer<'_>) {
        w.byte(self.0);
    }
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

impl Encode for RejectReason {
    fn encode(&self, w: &mut Writer<'_>) {
        w.byte(self.code());
    }
}

#[cfg(feature = "std")]
impl core::fmt::Display for RejectReason {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
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

/// Wire codec failures. No `Display` outside `std`, so the device path links no formatting code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WireError {
    /// The frame does not fit the buffer.
    BufferTooSmall {
        /// Frame length that would have been needed.
        needed: usize,
    },
    /// The payload is not a valid body.
    Decode,
}

#[cfg(feature = "std")]
impl core::fmt::Display for WireError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::BufferTooSmall { needed } => write!(f, "frame needs {needed} bytes"),
            Self::Decode => f.write_str("invalid message body"),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for WireError {}

/// Encodes a complete frame of `kind` with `body` into `buf` (body written straight into the
/// payload area), returning the frame length.
///
/// # Errors
///
/// [`WireError::BufferTooSmall`] if the frame does not fit; `buf` may then be partly written.
#[inline]
pub fn encode_frame<B: Encode + ?Sized>(
    kind: u8,
    seq: u16,
    body: &B,
    buf: &mut [u8],
) -> Result<usize, WireError> {
    encode_frame_dyn(kind, seq, &body, buf)
}

/// [`encode_frame`] behind one non-generic function, so firmware links a single copy.
pub(crate) fn encode_frame_dyn(
    kind: u8,
    seq: u16,
    body: &dyn Encode,
    buf: &mut [u8],
) -> Result<usize, WireError> {
    let payload_len = write_payload(body, buf);
    let needed = frame::frame_len(payload_len);
    if needed > buf.len() {
        return Err(WireError::BufferTooSmall { needed });
    }
    frame::encode_in_place(FrameHeader::new(kind, seq), payload_len, buf)
        .map_err(|_| WireError::BufferTooSmall { needed })
}

/// Writes `body` into the payload area of `buf` (whatever fits) and returns its full length.
pub(crate) fn write_payload(body: &dyn Encode, buf: &mut [u8]) -> usize {
    let mut w = Writer::new(frame::payload_area(buf));
    body.encode(&mut w);
    w.len()
}

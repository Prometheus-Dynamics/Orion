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
//! let hello = HelloView {
//!     device_name: "imu-board",
//!     roles: Roles::PROVIDER,
//!     max_frame: 512,
//!     revision: 0,
//! };
//! let len = wire::encode_frame(kind::HELLO, 0x0100, &hello, &mut buf).unwrap();
//! assert_eq!(&buf[4..len - 4], b"\x09imu-board\x01\x80\x04");
//! ```

mod action;
mod codec;
mod decode;
pub mod firmware;
mod views;

#[cfg(feature = "alloc")]
mod records;

#[cfg(feature = "actions")]
pub(crate) use action::ActionCatalogBody;
#[cfg(feature = "actions")]
pub(crate) use action::ActionResultBody;
pub use action::{ActionRequestView, ActionState, decode_action_cancel, decode_progress};
pub use codec::{Encode, RawVarint, Reader, Writer, encoded_len, str_from_utf8};
pub use decode::{LeaseView, Leases, WelcomeView, decode_ack, decode_reject, decode_u64};
pub use firmware::{
    ActivateMode, ActivateView, ConfirmView, DeviceInfoView, FirmwareInfo, FirmwareSink,
    MAX_ROLLBACK_NOTE_BYTES, MAX_ROLLBACK_VERSION_BYTES, NoFirmware, RollbackReason, TrialRollback,
    XferAbortView, XferAckView, XferBeginView, XferChunkView, XferEndView, XferResultView,
    XferStatus, max_chunk_for_frame,
};
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
    /// `ExecutorState { executor, workloads, resources }`, executor → host. Revision 3 (reserved
    /// before it).
    pub const EXECUTOR_STATE: u8 = 0x12;
    /// `Workloads { bootstrap, more, workloads, removed }`, host → executor: the workloads
    /// assigned to the session's executor. Revision 3 (reserved before it).
    pub const WORKLOADS: u8 = 0x13;
    /// `Status(Vec<StatusEntry>)`, device → host: volatile status values, fire-and-forget.
    pub const STATUS: u8 = 0x14;
    /// `DeviceInfo`, device → host: firmware state, sent until acknowledged. Additive, `xfer`.
    pub const DEVICE_INFO: u8 = 0x20;
    /// `XferBegin`, host → device: start or resume a transfer. Additive, `xfer`.
    pub const XFER_BEGIN: u8 = 0x21;
    /// `XferChunk`, host → device: one chunk of the image at an offset. Additive, `xfer`.
    pub const XFER_CHUNK: u8 = 0x22;
    /// `XferAck`, device → host: cumulative acknowledgement (the resume point). Additive, `xfer`.
    pub const XFER_ACK: u8 = 0x23;
    /// `XferEnd`, host → device: verify the whole image. Additive, `xfer`.
    pub const XFER_END: u8 = 0x24;
    /// `XferResult`, device → host: verdict on the whole image. Additive, `xfer`.
    pub const XFER_RESULT: u8 = 0x25;
    /// `XferAbort`, either direction: stop a transfer or report an error. Additive, `xfer`.
    pub const XFER_ABORT: u8 = 0x26;
    /// `Activate`, host → device: run a verified image on trial or hand over to the bootloader.
    pub const ACTIVATE: u8 = 0x27;
    /// `Confirm`, host → device: keep the trial slot. Additive, `xfer`.
    pub const CONFIRM: u8 = 0x28;
    /// First kind of the range reserved for firmware transfer (`0x20..=0x2F`, see
    /// [`is_firmware`]). `0x29..=0x2F` are reserved; a session ignores them as unknown kinds.
    pub const FIRMWARE_FIRST: u8 = 0x20;
    /// Last kind of the range reserved for firmware transfer.
    pub const FIRMWARE_LAST: u8 = 0x2F;
    /// `ActionRequest { id, name, payload }`, host → device: run a named action. The device answers
    /// with one or more `ActionResult`s carrying the same `id`. Additive in protocol 1.
    pub const ACTION_REQUEST: u8 = 0x30;
    /// `ActionResult { id, state, payload }`, device → host: the state of the action with that
    /// `id`. Additive in protocol 1.
    pub const ACTION_RESULT: u8 = 0x31;
    /// `ActionCancel { id }`, host → device: asks the device to stop the action with that `id`. It
    /// is accepted while the device is busy with that action (the device answers with a final
    /// `ActionResult`, normally `Cancelled`). Additive in protocol 1.
    pub const ACTION_CANCEL: u8 = 0x32;
    /// `ActionCatalog { names }`, device → host: the action names the device handles, declared with
    /// `DeviceSession::declare_actions` (feature `actions`). Sent after every `Welcome` until the
    /// host acknowledges it. Additive in protocol 1.
    pub const ACTION_CATALOG: u8 = 0x33;

    /// `RequestError { req, code, reason }`, host → client: a client request was refused and
    /// ends. Revision 1.
    pub const REQUEST_ERROR: u8 = 0x40;
    /// `RequestEnd { req }`, client → host: ends the watch `req`; nothing answers it. Revision 1.
    pub const REQUEST_END: u8 = 0x41;
    /// `StatusQuery { req, filter }`, client → host. Revision 1.
    pub const STATUS_QUERY: u8 = 0x50;
    /// `StatusWatch { req, filter }`, client → host. Revision 1.
    pub const STATUS_WATCH: u8 = 0x51;
    /// `StatusChanges { req, bootstrap, more, updated, expired }`, host → client. Revision 1.
    pub const STATUS_CHANGES: u8 = 0x52;
    /// `ActionRun { req, action_id, target, name, args, deadline_ms }`, client → host. Revision 1.
    pub const ACTION_RUN: u8 = 0x60;
    /// `ActionQuery { req, filter }`, client → host. Revision 1.
    pub const ACTION_QUERY: u8 = 0x61;
    /// `ActionWatch { req, filter }`, client → host. Revision 1.
    pub const ACTION_WATCH: u8 = 0x62;
    /// `ActionResults { req, more, results }`, host → client. Revision 1.
    pub const ACTION_RESULTS: u8 = 0x63;

    /// `StreamDeclare { req, id, series }`, provider → host. Revision 2.
    pub const STREAM_DECLARE: u8 = 0x70;
    /// `StreamPublish { id, chunks }`, provider → host; not answered. Revision 2.
    pub const STREAM_PUBLISH: u8 = 0x71;
    /// `StreamDemand { id, subscribers, series }`, host → provider. Revision 2.
    pub const STREAM_DEMAND: u8 = 0x72;
    /// `StreamSubscribe { req, owner, id, series, min_interval_us, backfill }`, client → host.
    /// Revision 2.
    pub const STREAM_SUBSCRIBE: u8 = 0x73;
    /// `StreamCatalog { req, epoch, series, live }`, host → client. Revision 2.
    pub const STREAM_CATALOG: u8 = 0x74;
    /// `StreamData { req, epoch, seq, prev_seq, backfill, more, chunks }`, host → client.
    /// Revision 2.
    pub const STREAM_DATA: u8 = 0x75;
    /// `StreamReset { req, reason }`, host → client. Revision 2.
    pub const STREAM_RESET: u8 = 0x76;

    /// `RecordQuery { req, filter }`, client → host. Revision 3.
    pub const RECORD_QUERY: u8 = 0x80;
    /// `RecordWatch { req, filter }`, client → host. Revision 3.
    pub const RECORD_WATCH: u8 = 0x81;
    /// `Records { req, bootstrap, more, updated, removed }`, host → client. Revision 3.
    pub const RECORDS: u8 = 0x82;

    /// Whether `kind` is a request a client sends (revision 1 and later).
    #[must_use]
    pub const fn is_client_request(kind: u8) -> bool {
        matches!(
            kind,
            REQUEST_END
                | STATUS_QUERY
                | STATUS_WATCH
                | ACTION_RUN
                | ACTION_QUERY
                | ACTION_WATCH
                | STREAM_SUBSCRIBE
                | RECORD_QUERY
                | RECORD_WATCH
        )
    }

    /// Whether `kind` is a stream kind a provider sends (revision 2).
    #[must_use]
    pub const fn is_provider_stream(kind: u8) -> bool {
        matches!(kind, STREAM_DECLARE | STREAM_PUBLISH)
    }

    /// The interface revision that introduced `kind` (0 for the kinds before revisions).
    #[must_use]
    pub const fn revision(kind: u8) -> u16 {
        match kind {
            0x40..=0x6F => 1,
            0x70..=0x7F => 2,
            EXECUTOR_STATE | WORKLOADS | 0x80..=0x8F => 3,
            _ => 0,
        }
    }

    /// Whether `kind` lies in the firmware-transfer range reserved by [`FIRMWARE_FIRST`] and
    /// [`FIRMWARE_LAST`].
    #[must_use]
    pub const fn is_firmware(kind: u8) -> bool {
        matches!(kind, FIRMWARE_FIRST..=FIRMWARE_LAST)
    }
}

/// Roles a device announces in `Hello` (bit set, encoded as one byte).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Roles(pub u8);

impl Roles {
    /// The device publishes provider and resource state and receives leases.
    pub const PROVIDER: Self = Self(0x01);
    /// The session runs workloads: it sends `ExecutorState` and gets `Workloads` (revision 3,
    /// hosts that serve executors).
    pub const EXECUTOR: Self = Self(0x02);
    /// The session queries and watches status and runs, queries and watches actions (revision 1).
    pub const CLIENT: Self = Self(0x04);

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

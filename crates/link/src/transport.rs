//! Transport selection for the sessions (feature `alloc`): [`Stream`] (COBS byte streams) or
//! [`Packet`] (CAN / CAN FD segments).
//!
//! The device and host sessions are generic over a [`Transport`]. Everything above the transport
//! (handshake, state, leases, liveness) is shared; only how bytes enter and leave differs:
//!
//! | Transport | Feed received data | Pull data to send |
//! | --- | --- | --- |
//! | [`Stream`] | `receive(&[u8])` (any chunking) | `transmit(&mut [u8]) -> usize` |
//! | [`Packet`] | `receive_segment(&[u8])` (one CAN frame's data) | `next_segment() -> Option<Segment>` |

use crate::packet::{Reassembler, Segment, SegmentMtu, Segmenter, SegmenterState};
use crate::stream::{EncoderState, StreamDecoder, StreamEncoder};

mod sealed {
    pub trait Sealed {}
}

/// A link transport: [`Stream`] or [`Packet`]. Sealed.
pub trait Transport: sealed::Sealed {
    /// Receive-side decoder holding up to `N` frame bytes.
    #[doc(hidden)]
    type Decoder<const N: usize>: Default;
    /// Progress through the frame currently being transmitted.
    #[doc(hidden)]
    type Cursor: Copy + Default;
}

/// Byte-stream transport (UART, RS-485, USB-CDC, TCP): COBS frames delimited by `0x00`.
///
/// Every frame is sent with a leading `0x00`, so line noise or an aborted frame never corrupts
/// the next one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stream;

/// Packet transport (classic CAN, CAN FD): each frame is split into segments of at most
/// [`Packet::mtu`] bytes, one per CAN frame. CAN identifiers are chosen by the caller (see
/// [`crate::CanLinkIds`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Packet {
    /// Largest CAN data length used per segment.
    pub mtu: SegmentMtu,
}

impl Packet {
    /// Classic CAN (8-byte frames).
    pub const CLASSIC: Self = Self {
        mtu: SegmentMtu::CLASSIC,
    };
    /// CAN FD (64-byte frames).
    pub const FD: Self = Self {
        mtu: SegmentMtu::FD,
    };

    /// A packet transport with a custom MTU.
    #[must_use]
    pub const fn new(mtu: SegmentMtu) -> Self {
        Self { mtu }
    }
}

impl sealed::Sealed for Stream {}
impl sealed::Sealed for Packet {}

impl Transport for Stream {
    type Decoder<const N: usize> = StreamDecoder<N>;
    type Cursor = Option<EncoderState>;
}

impl Transport for Packet {
    type Decoder<const N: usize> = Reassembler<N>;
    type Cursor = SegmenterState;
}

/// Writes the next stream bytes of `frame` into `out`, continuing from `cursor`. Returns the
/// bytes written and whether the frame is complete (the cursor is then reset).
pub(crate) fn stream_fill(
    frame: &[u8],
    cursor: &mut Option<EncoderState>,
    out: &mut [u8],
) -> (usize, bool) {
    let mut encoder = match *cursor {
        Some(saved) => StreamEncoder::resume(frame, saved),
        None => StreamEncoder::for_frame(frame).with_leading_delimiter(),
    };
    let written = encoder.fill(out);
    if encoder.is_done() {
        *cursor = None;
        (written, true)
    } else {
        *cursor = Some(encoder.save());
        (written, false)
    }
}

/// The next segment of `frame` from `cursor`. With `commit`, advances the cursor (resetting it
/// when the frame is complete). Returns the segment and whether it was the frame's last.
pub(crate) fn segment_step(
    frame: &[u8],
    mtu: SegmentMtu,
    cursor: &mut SegmenterState,
    commit: bool,
) -> Option<(Segment, bool)> {
    let mut segmenter = Segmenter::resume(frame, mtu, *cursor);
    let segment = segmenter.next()?;
    let done = segmenter.is_done();
    if commit {
        *cursor = if done {
            SegmenterState::default()
        } else {
            segmenter.save()
        };
    }
    Some((segment, done))
}

/// Whether `seq` is newer than `last` in wrapping sequence order (within half the space).
/// Frames that are not newer are duplicates or stale and are ignored by the sessions.
pub(crate) const fn seq_newer(seq: u16, last: u16) -> bool {
    let diff = seq.wrapping_sub(last);
    diff != 0 && diff < 0x8000
}

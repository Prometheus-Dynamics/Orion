//! Packet-link transport (classic CAN, CAN FD): a frame is split into segments, one per CAN frame.
//!
//! Each segment starts with one header byte: bit 7 = start of message, bit 6 = end of message,
//! bits 5..0 = segment counter (mod 64). The start segment carries counter 0 and every following
//! segment increments it. Segments are never padded: every segment length (header included) is a
//! valid CAN FD data length (0..=8, 12, 16, 20, 24, 32, 48, 64), so the received CAN data length
//! is always the exact segment length and the reassembler never has to strip padding.

mod ids;
mod reassemble;
mod segment;

use core::fmt;

pub use ids::CanLinkIds;
pub use reassemble::{PacketStats, Reassembler};
#[cfg(feature = "device")]
pub(crate) use segment::SegmenterState;
pub use segment::{Segment, Segmenter};

use crate::frame::FrameError;

/// Segment header bit: first segment of a message.
pub const SEGMENT_START: u8 = 0x80;
/// Segment header bit: last segment of a message.
pub const SEGMENT_END: u8 = 0x40;
/// Segment header bits holding the counter.
pub const SEGMENT_COUNTER_MASK: u8 = 0x3F;

/// Data length of a classic CAN frame.
pub const CLASSIC_CAN_MAX_LEN: usize = 8;
/// Largest CAN FD data length.
pub const CAN_FD_MAX_LEN: usize = 64;

/// Data lengths a CAN FD frame can carry (its DLC values 0..=15).
pub const CAN_FD_LENGTHS: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 12, 16, 20, 24, 32, 48, 64];

/// Whether `len` is a CAN FD data length (and therefore also any classic CAN length `0..=8`).
#[must_use]
pub const fn is_can_fd_len(len: usize) -> bool {
    let mut rest: &[u8] = &CAN_FD_LENGTHS;
    while let [candidate, tail @ ..] = rest {
        if *candidate as usize == len {
            return true;
        }
        rest = tail;
    }
    false
}

/// Smallest CAN FD data length that holds `len` bytes, or `None` above 64.
#[must_use]
pub const fn can_fd_len_at_least(len: usize) -> Option<usize> {
    let mut rest: &[u8] = &CAN_FD_LENGTHS;
    while let [candidate, tail @ ..] = rest {
        if *candidate as usize >= len {
            return Some(*candidate as usize);
        }
        rest = tail;
    }
    None
}

/// Largest CAN FD data length that is at most `len`.
#[must_use]
pub const fn can_fd_len_at_most(len: usize) -> usize {
    let mut best = 0;
    let mut rest: &[u8] = &CAN_FD_LENGTHS;
    while let [candidate, tail @ ..] = rest {
        if *candidate as usize <= len {
            best = *candidate as usize;
        }
        rest = tail;
    }
    best
}

/// Maximum CAN data length used for segments (header byte included).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SegmentMtu(u8);

impl SegmentMtu {
    /// Classic CAN: 8-byte frames, 7 payload bytes per segment.
    pub const CLASSIC: Self = Self(8);
    /// CAN FD: 64-byte frames, 63 payload bytes per segment.
    pub const FD: Self = Self(64);

    /// A custom MTU, for example a CAN FD bus limited to 32-byte frames. `len` must be a CAN FD
    /// data length of at least 2.
    #[must_use]
    pub const fn new(len: usize) -> Option<Self> {
        if len >= 2 && is_can_fd_len(len) {
            // len <= 64 here.
            Some(Self(len as u8))
        } else {
            None
        }
    }

    /// CAN data length, header byte included.
    #[must_use]
    pub const fn frame_len(self) -> usize {
        self.0 as usize
    }

    /// Payload bytes per full segment.
    #[must_use]
    pub const fn payload_len(self) -> usize {
        self.frame_len() - 1
    }
}

/// Segmentation and reassembly failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PacketError {
    /// The output buffer cannot hold the next segment.
    BufferTooSmall {
        /// Bytes required.
        needed: usize,
        /// Bytes available.
        available: usize,
    },
    /// A zero-length CAN frame, which has no segment header.
    EmptySegment,
    /// A continuation segment arrived with no message in progress.
    Orphan,
    /// A segment counter skipped ahead or went backwards; the partial message was discarded.
    OutOfOrder {
        /// Counter expected next.
        expected: u8,
        /// Counter received.
        received: u8,
    },
    /// A start segment arrived before the previous message ended; the partial message was
    /// discarded and the new segment started a new message. (When the new segment is also an
    /// end segment, its frame is returned instead and only [`PacketStats::interrupted`] counts.)
    Interrupted,
    /// The message exceeded the reassembly buffer and was discarded.
    Overflow,
    /// The reassembled message is not a valid frame (too short or bad CRC).
    Frame(FrameError),
}

impl fmt::Display for PacketError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BufferTooSmall { needed, available } => {
                write!(
                    f,
                    "segment buffer too small: need {needed} bytes, have {available}"
                )
            }
            Self::EmptySegment => f.write_str("empty segment"),
            Self::Orphan => f.write_str("continuation segment without a start segment"),
            Self::OutOfOrder { expected, received } => {
                write!(
                    f,
                    "segment out of order: expected {expected}, received {received}"
                )
            }
            Self::Interrupted => f.write_str("message interrupted by a new start segment"),
            Self::Overflow => f.write_str("message exceeds reassembly buffer"),
            Self::Frame(err) => write!(f, "invalid reassembled frame: {err}"),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for PacketError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Frame(err) => Some(err),
            _ => None,
        }
    }
}

impl From<FrameError> for PacketError {
    fn from(err: FrameError) -> Self {
        Self::Frame(err)
    }
}

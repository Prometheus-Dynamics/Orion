//! Splitting a frame into CAN / CAN FD segments.

use super::{
    CAN_FD_MAX_LEN, PacketError, SEGMENT_COUNTER_MASK, SEGMENT_END, SEGMENT_START, SegmentMtu,
    can_fd_len_at_most,
};
use crate::frame::FrameHeader;
use crate::source::FrameSource;

/// One segment, ready to be sent as the data of a single CAN frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Segment {
    bytes: [u8; CAN_FD_MAX_LEN],
    len: u8,
}

impl Segment {
    /// The CAN frame data: header byte followed by payload.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.bytes.get(..usize::from(self.len)).unwrap_or_default()
    }

    /// Header byte.
    #[must_use]
    pub const fn header(&self) -> u8 {
        self.bytes[0]
    }

    /// First segment of a message.
    #[must_use]
    pub const fn is_start(&self) -> bool {
        self.header() & SEGMENT_START != 0
    }

    /// Last segment of a message.
    #[must_use]
    pub const fn is_end(&self) -> bool {
        self.header() & SEGMENT_END != 0
    }

    /// Segment counter (mod 64).
    #[must_use]
    pub const fn counter(&self) -> u8 {
        self.header() & SEGMENT_COUNTER_MASK
    }
}

/// Progress of a [`Segmenter`] without its borrowed frame (see [`Segmenter::save`]).
#[cfg(feature = "device")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[doc(hidden)]
pub struct SegmenterState {
    pos: usize,
    counter: u8,
    done: bool,
}

/// Splits one frame into segments for a given [`SegmentMtu`].
///
/// Full segments use the whole MTU. The tail is split into as few segments as possible whose
/// lengths are each a valid CAN FD data length, so no segment is ever padded.
///
/// ```
/// use orion_link::{FrameHeader, Reassembler, SegmentMtu, Segmenter};
/// let mut rx = Reassembler::<128>::new();
/// let mut delivered = None;
/// for segment in Segmenter::for_message(FrameHeader::new(1, 2), b"hello can", SegmentMtu::CLASSIC) {
///     if let Some(frame) = rx.push(segment.as_bytes()).unwrap() {
///         delivered = Some((frame.kind(), frame.payload().to_vec()));
///     }
/// }
/// assert_eq!(delivered, Some((1, b"hello can".to_vec())));
/// ```
#[derive(Debug, Clone)]
pub struct Segmenter<'a> {
    src: FrameSource<'a>,
    mtu: SegmentMtu,
    pos: usize,
    counter: u8,
    done: bool,
}

impl<'a> Segmenter<'a> {
    fn new(src: FrameSource<'a>, mtu: SegmentMtu) -> Self {
        Self {
            src,
            mtu,
            pos: 0,
            counter: 0,
            done: false,
        }
    }

    /// Segments bytes that already form a complete frame.
    #[must_use]
    pub fn for_frame(frame: &'a [u8], mtu: SegmentMtu) -> Self {
        Self::new(FrameSource::raw(frame), mtu)
    }

    /// Segments the frame for `header` and `payload` without assembling it first.
    #[must_use]
    pub fn for_message(header: FrameHeader, payload: &'a [u8], mtu: SegmentMtu) -> Self {
        Self::new(FrameSource::message(header, payload), mtu)
    }

    /// Progress so far, to continue later with [`Segmenter::resume`].
    #[cfg(feature = "device")]
    pub(crate) fn save(&self) -> SegmenterState {
        SegmenterState {
            pos: self.pos,
            counter: self.counter,
            done: self.done,
        }
    }

    /// Continues segmenting the raw `frame` from a saved state.
    #[cfg(feature = "device")]
    pub(crate) fn resume(frame: &'a [u8], mtu: SegmentMtu, saved: SegmenterState) -> Self {
        Self {
            src: FrameSource::raw(frame),
            mtu,
            pos: saved.pos,
            counter: saved.counter,
            done: saved.done,
        }
    }

    /// Whether every segment has been produced.
    #[must_use]
    pub const fn is_done(&self) -> bool {
        self.done
    }

    /// CAN data length of the next segment, header included, or `None` when done.
    #[must_use]
    pub fn next_len(&self) -> Option<usize> {
        if self.done {
            return None;
        }
        Some(self.chunk_len().saturating_add(1))
    }

    /// Number of segments still to be produced.
    #[must_use]
    pub fn remaining_segments(&self) -> usize {
        let mut probe = self.clone();
        let mut count = 0usize;
        while probe.advance(&mut []).is_some() {
            count = count.saturating_add(1);
        }
        count
    }

    /// Writes the next segment into `out` and returns its length, or `Ok(None)` when done.
    ///
    /// # Errors
    ///
    /// [`PacketError::BufferTooSmall`] if `out` is shorter than [`Segmenter::next_len`]; the
    /// segmenter does not advance. A buffer of [`SegmentMtu::frame_len`] bytes always suffices.
    pub fn next_into(&mut self, out: &mut [u8]) -> Result<Option<usize>, PacketError> {
        let Some(needed) = self.next_len() else {
            return Ok(None);
        };
        if out.len() < needed {
            return Err(PacketError::BufferTooSmall {
                needed,
                available: out.len(),
            });
        }
        Ok(self.advance(out))
    }

    /// Payload bytes in the next segment.
    fn chunk_len(&self) -> usize {
        let left = self.src.len().saturating_sub(self.pos);
        let full = self.mtu.payload_len();
        if left >= full {
            full
        } else {
            // Largest valid CAN data length that fits the remainder plus the header byte. Every
            // length 1..=8 is valid, so this always makes progress.
            can_fd_len_at_most(left.saturating_add(1)).saturating_sub(1)
        }
    }

    /// Produces the next segment into `out` (or just advances if `out` is empty, for counting).
    fn advance(&mut self, out: &mut [u8]) -> Option<usize> {
        if self.done {
            return None;
        }
        let chunk = self.chunk_len();
        let start = self.pos == 0;
        let end = self.pos.saturating_add(chunk) >= self.src.len();
        let mut header = self.counter & SEGMENT_COUNTER_MASK;
        if start {
            header |= SEGMENT_START;
        }
        if end {
            header |= SEGMENT_END;
            self.done = true;
        }
        if let Some(slot) = out.first_mut() {
            *slot = header;
            let data = out.get_mut(1..).unwrap_or_default();
            for (offset, slot) in data.iter_mut().take(chunk).enumerate() {
                *slot = self.src.get(self.pos.saturating_add(offset)).unwrap_or(0);
            }
        }
        self.pos = self.pos.saturating_add(chunk);
        self.counter = self.counter.wrapping_add(1) & SEGMENT_COUNTER_MASK;
        Some(chunk.saturating_add(1))
    }
}

impl Iterator for Segmenter<'_> {
    type Item = Segment;

    fn next(&mut self) -> Option<Segment> {
        let mut bytes = [0u8; CAN_FD_MAX_LEN];
        let len = self.advance(&mut bytes)?;
        // len <= 64 because the MTU is at most 64.
        Some(Segment {
            bytes,
            len: u8::try_from(len).unwrap_or(u8::MAX),
        })
    }
}

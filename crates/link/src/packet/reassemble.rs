//! Rebuilding frames from CAN / CAN FD segments.

use super::{PacketError, SEGMENT_COUNTER_MASK, SEGMENT_END, SEGMENT_START};
use crate::frame::{self, FrameError, FrameView};

/// Counters kept by a [`Reassembler`]. They wrap on overflow.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PacketStats {
    /// Valid frames delivered.
    pub frames: u32,
    /// Reassembled messages dropped because the frame CRC did not match.
    pub crc_errors: u32,
    /// Reassembled messages dropped because they were shorter than a frame, plus empty segments.
    pub framing_errors: u32,
    /// Partial messages dropped because a segment was missing or out of order.
    pub sequence_errors: u32,
    /// Partial messages dropped because a new start segment arrived first.
    pub interrupted: u32,
    /// Messages dropped because they exceeded the reassembly buffer.
    pub overflows: u32,
    /// Messages whose start segment was lost (a continuation arrived with no message in
    /// progress). The remaining segments of such a message are dropped without counting.
    pub orphans: u32,
    /// Repeated segments (same counter and bytes as the previous one) that were ignored.
    pub duplicates: u32,
}

impl PacketStats {
    /// Total messages (or stray segments) dropped for any reason. Ignored duplicates are not
    /// counted, because they lose nothing.
    #[must_use]
    pub const fn dropped(&self) -> u32 {
        self.crc_errors
            .wrapping_add(self.framing_errors)
            .wrapping_add(self.sequence_errors)
            .wrapping_add(self.interrupted)
            .wrapping_add(self.overflows)
            .wrapping_add(self.orphans)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Idle,
    /// Receiving a message; `last` is the counter of the most recent segment.
    Active {
        last: u8,
        segments: u32,
    },
    /// The current message is broken; drop its remaining segments silently until an end or start.
    Discarding,
}

/// Rebuilds frames from segments of one sender (one CAN identifier).
///
/// `N` bounds the frame length. A missing or out-of-order segment discards the partial message;
/// an immediately repeated, byte-identical segment (a CAN-level duplicate) is ignored; the frame
/// CRC guards the result. Never allocates and never panics on any input.
#[derive(Debug, Clone)]
pub struct Reassembler<const N: usize> {
    buf: [u8; N],
    len: usize,
    state: State,
    /// The previous push completed a frame; clear the buffer before the next segment.
    finished: bool,
    stats: PacketStats,
}

impl<const N: usize> Default for Reassembler<N> {
    #[inline(always)]
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> Reassembler<N> {
    /// An idle reassembler.
    #[must_use]
    #[inline(always)]
    pub const fn new() -> Self {
        Self {
            buf: [0; N],
            len: 0,
            state: State::Idle,
            finished: false,
            stats: PacketStats {
                frames: 0,
                crc_errors: 0,
                framing_errors: 0,
                sequence_errors: 0,
                interrupted: 0,
                overflows: 0,
                orphans: 0,
                duplicates: 0,
            },
        }
    }

    /// Largest frame this reassembler can deliver.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        N
    }

    /// Counters since construction (or [`Reassembler::reset_stats`]).
    #[must_use]
    pub const fn stats(&self) -> PacketStats {
        self.stats
    }

    /// Clears the counters.
    pub fn reset_stats(&mut self) {
        self.stats = PacketStats::default();
    }

    /// Drops any partial message.
    pub fn reset(&mut self) {
        self.len = 0;
        self.state = State::Idle;
        self.finished = false;
    }

    /// Feeds the data of one received CAN frame. Returns the frame when this segment completes a
    /// valid one, `Ok(None)` while a message is in progress or a duplicate was ignored.
    ///
    /// # Errors
    ///
    /// Each broken message is reported once: [`PacketError::EmptySegment`],
    /// [`PacketError::Orphan`], [`PacketError::OutOfOrder`], [`PacketError::Interrupted`],
    /// [`PacketError::Overflow`], or [`PacketError::Frame`]. The reassembler stays usable.
    pub fn push(&mut self, segment: &[u8]) -> Result<Option<FrameView<'_>>, PacketError> {
        self.push_status(segment)?;
        if self.finished {
            Ok(frame::view_unchecked(
                self.buf.get(..self.len).unwrap_or_default(),
            ))
        } else {
            Ok(None)
        }
    }

    /// Like [`Reassembler::push`] without borrowing the result; `Ok(true)` means a frame is ready.
    fn push_status(&mut self, segment: &[u8]) -> Result<bool, PacketError> {
        if self.finished {
            self.reset();
        }
        let Some((&header, data)) = segment.split_first() else {
            self.stats.framing_errors = self.stats.framing_errors.wrapping_add(1);
            return Err(PacketError::EmptySegment);
        };
        let start = header & SEGMENT_START != 0;
        let end = header & SEGMENT_END != 0;
        let counter = header & SEGMENT_COUNTER_MASK;

        let mut interrupted = false;
        if start {
            if let State::Active { last, segments } = self.state {
                if segments == 1
                    && last == counter
                    && self.len == data.len()
                    && self.ends_with(data)
                {
                    // The same start segment again, with nothing after it yet: a duplicate. (A new
                    // message after a lost tail differs at least in its sequence number.)
                    self.stats.duplicates = self.stats.duplicates.wrapping_add(1);
                    return Ok(false);
                }
                self.stats.interrupted = self.stats.interrupted.wrapping_add(1);
                interrupted = true;
            }
            self.len = 0;
            self.state = State::Active {
                last: counter,
                segments: 1,
            };
        } else {
            match self.state {
                State::Idle => {
                    // The rest of this message is dropped silently, so it is reported once.
                    self.stats.orphans = self.stats.orphans.wrapping_add(1);
                    self.discard(end);
                    return Err(PacketError::Orphan);
                }
                State::Discarding => {
                    if end {
                        self.state = State::Idle;
                    }
                    return Ok(false);
                }
                State::Active { last, segments } => {
                    if counter == last && self.ends_with(data) {
                        self.stats.duplicates = self.stats.duplicates.wrapping_add(1);
                        return Ok(false);
                    }
                    let expected = last.wrapping_add(1) & SEGMENT_COUNTER_MASK;
                    if counter != expected {
                        self.stats.sequence_errors = self.stats.sequence_errors.wrapping_add(1);
                        self.discard(end);
                        return Err(PacketError::OutOfOrder {
                            expected,
                            received: counter,
                        });
                    }
                    self.state = State::Active {
                        last: counter,
                        segments: segments.saturating_add(1),
                    };
                }
            }
        }

        if !self.append(data) {
            self.stats.overflows = self.stats.overflows.wrapping_add(1);
            self.discard(end);
            return Err(PacketError::Overflow);
        }
        if !end {
            return if interrupted {
                Err(PacketError::Interrupted)
            } else {
                Ok(false)
            };
        }
        self.state = State::Idle;
        self.finish()
    }

    fn discard(&mut self, end: bool) {
        self.len = 0;
        self.state = if end { State::Idle } else { State::Discarding };
    }

    /// Whether the buffer ends with `data`, i.e. `data` repeats the previous segment.
    fn ends_with(&self, data: &[u8]) -> bool {
        self.buf
            .get(..self.len)
            .is_some_and(|received| received.ends_with(data))
    }

    fn append(&mut self, data: &[u8]) -> bool {
        let Some(new_len) = self.len.checked_add(data.len()) else {
            return false;
        };
        match self.buf.get_mut(self.len..new_len) {
            Some(dst) => {
                frame::copy_prefix(dst, data);
                self.len = new_len;
                true
            }
            None => false,
        }
    }

    fn finish(&mut self) -> Result<bool, PacketError> {
        let bytes = self.buf.get(..self.len).unwrap_or_default();
        match frame::decode(bytes) {
            Ok(_) => {
                self.stats.frames = self.stats.frames.wrapping_add(1);
                self.finished = true;
                Ok(true)
            }
            Err(err) => {
                if matches!(err, FrameError::CrcMismatch { .. }) {
                    self.stats.crc_errors = self.stats.crc_errors.wrapping_add(1);
                } else {
                    self.stats.framing_errors = self.stats.framing_errors.wrapping_add(1);
                }
                self.len = 0;
                Err(PacketError::Frame(err))
            }
        }
    }
}

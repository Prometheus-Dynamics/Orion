//! Streaming COBS decoder with frame validation and resynchronization.

use super::StreamError;
use crate::frame::{self, FrameError, FrameView};

/// Counters kept by a [`StreamDecoder`]. They wrap on overflow.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StreamStats {
    /// Valid frames delivered.
    pub frames: u32,
    /// Packets dropped because the frame CRC did not match.
    pub crc_errors: u32,
    /// Packets dropped because they were malformed COBS or shorter than a frame.
    pub framing_errors: u32,
    /// Packets dropped because they exceeded the decoder buffer.
    pub overflows: u32,
}

impl StreamStats {
    /// Total packets dropped for any reason.
    #[must_use]
    pub const fn dropped(&self) -> u32 {
        self.crc_errors
            .wrapping_add(self.framing_errors)
            .wrapping_add(self.overflows)
    }
}

/// Outcome of feeding one byte, without borrowing the buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Status {
    Pending,
    Frame,
    Error(StreamError),
}

/// Receives `0x00`-delimited COBS packets one byte or slice at a time and yields validated frames.
///
/// `N` bounds the decoded frame length (header + payload + CRC); larger packets are discarded up
/// to the next delimiter. The decoder never allocates and never panics on any input.
///
/// ```
/// use orion_link::{FrameHeader, StreamDecoder, encode_message};
/// let mut wire = [0u8; 64];
/// let n = encode_message(FrameHeader::new(3, 9), b"payload", &mut wire).unwrap();
/// let mut decoder = StreamDecoder::<64>::new();
/// let (used, frame) = decoder.push_slice(&wire[..n]);
/// let frame = frame.unwrap().unwrap();
/// assert_eq!((used, frame.kind(), frame.seq(), frame.payload()), (n, 3, 9, &b"payload"[..]));
/// ```
#[derive(Debug, Clone)]
pub struct StreamDecoder<const N: usize> {
    buf: [u8; N],
    len: usize,
    /// Data bytes still expected in the current COBS block.
    block_left: u8,
    /// Code byte of the current block; 0 before the first block of a packet.
    code: u8,
    discarding: bool,
    /// The previous byte completed a packet; clear the buffer before the next byte.
    finished: bool,
    stats: StreamStats,
}

impl<const N: usize> Default for StreamDecoder<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> StreamDecoder<N> {
    /// An idle decoder.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            buf: [0; N],
            len: 0,
            block_left: 0,
            code: 0,
            discarding: false,
            finished: false,
            stats: StreamStats {
                frames: 0,
                crc_errors: 0,
                framing_errors: 0,
                overflows: 0,
            },
        }
    }

    /// Largest frame this decoder can deliver.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        N
    }

    /// Counters since construction (or [`StreamDecoder::reset_stats`]).
    #[must_use]
    pub const fn stats(&self) -> StreamStats {
        self.stats
    }

    /// Clears the counters.
    pub fn reset_stats(&mut self) {
        self.stats = StreamStats::default();
    }

    /// Drops any partial packet. Bytes up to the next `0x00` are then treated as a new packet, so
    /// call this only at a known packet boundary (for example after reopening the port).
    pub fn reset(&mut self) {
        self.len = 0;
        self.block_left = 0;
        self.code = 0;
        self.discarding = false;
        self.finished = false;
    }

    /// Feeds one byte. Returns a frame when `byte` is the delimiter that completes a valid frame,
    /// an error when it completes a corrupt packet, and `Ok(None)` otherwise. Empty packets
    /// (repeated delimiters) are ignored.
    ///
    /// # Errors
    ///
    /// [`StreamError::Overflow`], [`StreamError::Cobs`], or [`StreamError::Frame`]; the decoder has
    /// already resynchronized and the next byte starts a new packet.
    pub fn push(&mut self, byte: u8) -> Result<Option<FrameView<'_>>, StreamError> {
        let status = self.push_status(byte);
        self.resolve(status)
    }

    /// Feeds bytes until one completes a packet. Returns how many bytes were consumed and the
    /// [`StreamDecoder::push`] result of the last one; feed the rest of `bytes` afterwards.
    pub fn push_slice(
        &mut self,
        bytes: &[u8],
    ) -> (usize, Result<Option<FrameView<'_>>, StreamError>) {
        let (used, status) = self.push_slice_status(bytes);
        (used, self.resolve(status))
    }

    pub(crate) fn push_slice_status(&mut self, bytes: &[u8]) -> (usize, Status) {
        for (index, &byte) in bytes.iter().enumerate() {
            let status = self.push_status(byte);
            if status != Status::Pending {
                return (index.saturating_add(1), status);
            }
        }
        (bytes.len(), Status::Pending)
    }

    pub(crate) fn resolve(&self, status: Status) -> Result<Option<FrameView<'_>>, StreamError> {
        match status {
            Status::Pending => Ok(None),
            Status::Frame => Ok(self.frame()),
            Status::Error(err) => Err(err),
        }
    }

    /// The frame completed by the last byte (already validated).
    pub(crate) fn frame(&self) -> Option<FrameView<'_>> {
        frame::view_unchecked(self.buf.get(..self.len)?)
    }

    pub(crate) fn push_status(&mut self, byte: u8) -> Status {
        if self.finished {
            self.reset();
        }
        if byte == 0 {
            return self.end_packet();
        }
        if self.discarding {
            return Status::Pending;
        }
        if self.block_left == 0 {
            // Code byte. Every block except a full one (0xFF) implies a zero after its data, which
            // is only materialized once another block follows.
            if self.code != 0 && self.code != 0xFF {
                self.append(0);
            }
            self.code = byte;
            self.block_left = byte - 1;
        } else {
            self.block_left -= 1;
            self.append(byte);
        }
        Status::Pending
    }

    fn append(&mut self, byte: u8) {
        if self.discarding {
            return;
        }
        match self.buf.get_mut(self.len) {
            Some(slot) => {
                *slot = byte;
                self.len += 1;
            }
            None => self.discarding = true,
        }
    }

    fn end_packet(&mut self) -> Status {
        self.finished = true;
        if self.discarding {
            self.stats.overflows = self.stats.overflows.wrapping_add(1);
            return Status::Error(StreamError::Overflow);
        }
        if self.code == 0 {
            // Empty packet: idle or resync delimiters.
            return Status::Pending;
        }
        if self.block_left != 0 {
            self.stats.framing_errors = self.stats.framing_errors.wrapping_add(1);
            return Status::Error(StreamError::Cobs);
        }
        let Some(bytes) = self.buf.get(..self.len) else {
            return Status::Error(StreamError::Overflow);
        };
        match frame::decode(bytes) {
            Ok(_) => {
                self.stats.frames = self.stats.frames.wrapping_add(1);
                Status::Frame
            }
            Err(err) => {
                if matches!(err, FrameError::CrcMismatch { .. }) {
                    self.stats.crc_errors = self.stats.crc_errors.wrapping_add(1);
                } else {
                    self.stats.framing_errors = self.stats.framing_errors.wrapping_add(1);
                }
                Status::Error(StreamError::Frame(err))
            }
        }
    }
}

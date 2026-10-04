//! COBS encoding of frames into `0x00`-terminated byte streams.

use super::StreamError;
use crate::frame::FrameHeader;
use crate::source::FrameSource;

/// Longest COBS run: 254 data bytes behind a `0xFF` code byte.
const MAX_RUN: usize = 254;

/// Upper bound on the stream bytes for a frame of `frame_len` bytes, including the code bytes and
/// the trailing `0x00` delimiter (but not an optional leading delimiter). Saturating.
#[must_use]
pub const fn max_encoded_len(frame_len: usize) -> usize {
    frame_len
        .saturating_add(frame_len / MAX_RUN)
        .saturating_add(2)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Leading,
    Code,
    Data,
    Delimiter,
    Done,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockEnd {
    /// The block stands for a zero byte in the input, which is skipped.
    Zero,
    /// A full 254-byte block; no implied zero.
    Full,
    /// The block reaches the end of the input.
    End,
}

/// Progress of a [`StreamEncoder`] without its borrowed frame, so a session can store it and resume
/// the encoding on a later call (see [`StreamEncoder::save`] / [`StreamEncoder::resume`]).
#[cfg(feature = "alloc")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct EncoderState {
    pos: usize,
    block_left: usize,
    block_end: BlockEnd,
    state: State,
}

/// Incremental COBS encoder: yields the stream bytes for one frame, ending with `0x00`.
///
/// It holds only a borrowed payload and a few counters, so a UART driver can pull bytes one at a
/// time (or in chunks via [`StreamEncoder::fill`]) without a second, encoded copy of the frame.
///
/// ```
/// use orion_link::{FrameHeader, StreamEncoder};
/// let mut wire = [0u8; 32];
/// let n = StreamEncoder::for_message(FrameHeader::new(7, 1), b"hi").fill(&mut wire);
/// assert_eq!(wire[n - 1], 0x00);
/// assert!(!wire[..n - 1].contains(&0x00));
/// ```
#[derive(Debug, Clone)]
pub struct StreamEncoder<'a> {
    src: FrameSource<'a>,
    pos: usize,
    block_left: usize,
    block_end: BlockEnd,
    state: State,
}

impl<'a> StreamEncoder<'a> {
    fn new(src: FrameSource<'a>) -> Self {
        Self {
            src,
            pos: 0,
            block_left: 0,
            block_end: BlockEnd::End,
            state: State::Code,
        }
    }

    /// Encodes bytes that already form a complete frame (see [`crate::frame::encode`]).
    #[must_use]
    pub fn for_frame(frame: &'a [u8]) -> Self {
        Self::new(FrameSource::raw(frame))
    }

    /// Encodes the frame for `header` and `payload` directly, computing the CRC up front, so no
    /// frame buffer is needed at all.
    #[must_use]
    pub fn for_message(header: FrameHeader, payload: &'a [u8]) -> Self {
        Self::new(FrameSource::message(header, payload))
    }

    /// Also emits a `0x00` before the frame. Receivers ignore empty packets, so this is always
    /// compatible; it terminates any line noise received since the previous frame so that noise
    /// cannot corrupt this frame. Recommended after idle periods or on noisy links.
    #[must_use]
    pub fn with_leading_delimiter(mut self) -> Self {
        if self.state == State::Code && self.pos == 0 {
            self.state = State::Leading;
        }
        self
    }

    /// Progress so far, to continue later over the same frame with [`StreamEncoder::resume`].
    #[cfg(feature = "alloc")]
    pub(crate) fn save(&self) -> EncoderState {
        EncoderState {
            pos: self.pos,
            block_left: self.block_left,
            block_end: self.block_end,
            state: self.state,
        }
    }

    /// Continues encoding the raw `frame` from a saved state. `frame` must be the same bytes the
    /// state was saved from.
    #[cfg(feature = "alloc")]
    pub(crate) fn resume(frame: &'a [u8], saved: EncoderState) -> Self {
        Self {
            src: FrameSource::raw(frame),
            pos: saved.pos,
            block_left: saved.block_left,
            block_end: saved.block_end,
            state: saved.state,
        }
    }

    /// Whether every byte has been produced.
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.state == State::Done
    }

    /// Writes as many of the remaining stream bytes as fit into `out` and returns how many were
    /// written. Returns 0 once the frame is complete.
    pub fn fill(&mut self, out: &mut [u8]) -> usize {
        let mut written = 0;
        for slot in out.iter_mut() {
            match self.next() {
                Some(byte) => {
                    *slot = byte;
                    written += 1;
                }
                None => break,
            }
        }
        written
    }

    /// Starts the next block at `self.pos` and returns its code byte.
    fn start_block(&mut self) -> u8 {
        let len = self.src.len();
        let mut run = 0;
        let mut zero = false;
        while run < MAX_RUN {
            match self.src.get(self.pos.saturating_add(run)) {
                Some(0) => {
                    zero = true;
                    break;
                }
                Some(_) => run += 1,
                None => break,
            }
        }
        self.block_left = run;
        self.block_end = if zero {
            BlockEnd::Zero
        } else if run == MAX_RUN && self.pos.saturating_add(run) < len {
            BlockEnd::Full
        } else {
            BlockEnd::End
        };
        // run <= 254, so the code byte is at most 0xFF.
        (run as u8).wrapping_add(1)
    }
}

impl Iterator for StreamEncoder<'_> {
    type Item = u8;

    fn next(&mut self) -> Option<u8> {
        loop {
            match self.state {
                State::Leading => {
                    self.state = State::Code;
                    return Some(0);
                }
                State::Code => {
                    self.state = State::Data;
                    return Some(self.start_block());
                }
                State::Data => {
                    if self.block_left > 0 {
                        self.block_left -= 1;
                        if let Some(byte) = self.src.get(self.pos) {
                            self.pos = self.pos.saturating_add(1);
                            return Some(byte);
                        }
                        // Unreachable: blocks never extend past the source. End the frame
                        // rather than emitting a stray zero.
                        self.state = State::Delimiter;
                        continue;
                    }
                    self.state = match self.block_end {
                        BlockEnd::Zero => {
                            self.pos = self.pos.saturating_add(1);
                            State::Code
                        }
                        BlockEnd::Full => State::Code,
                        BlockEnd::End => State::Delimiter,
                    };
                }
                State::Delimiter => {
                    self.state = State::Done;
                    return Some(0);
                }
                State::Done => return None,
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.state == State::Done {
            return (0, Some(0));
        }
        let left = self.src.len().saturating_sub(self.pos);
        (1, Some(max_encoded_len(left).saturating_add(1)))
    }
}

/// COBS-encodes an already-built frame into `out`, returning the stream length (including the
/// trailing `0x00`).
///
/// # Errors
///
/// [`StreamError::BufferTooSmall`] if `out` cannot hold the encoding; `out` is then partially
/// written. [`max_encoded_len`] always suffices.
pub fn encode_frame(frame: &[u8], out: &mut [u8]) -> Result<usize, StreamError> {
    encode_all(StreamEncoder::for_frame(frame), frame.len(), out)
}

/// Builds and COBS-encodes the frame for `header` and `payload` into `out` in one pass, returning
/// the stream length.
///
/// # Errors
///
/// [`StreamError::BufferTooSmall`] if `out` cannot hold the encoding.
pub fn encode_message(
    header: FrameHeader,
    payload: &[u8],
    out: &mut [u8],
) -> Result<usize, StreamError> {
    let frame_len = crate::frame::frame_len(payload.len());
    encode_all(StreamEncoder::for_message(header, payload), frame_len, out)
}

fn encode_all(
    mut encoder: StreamEncoder<'_>,
    frame_len: usize,
    out: &mut [u8],
) -> Result<usize, StreamError> {
    let written = encoder.fill(out);
    if encoder.is_done() {
        Ok(written)
    } else {
        Err(StreamError::BufferTooSmall {
            needed: max_encoded_len(frame_len),
            available: out.len(),
        })
    }
}

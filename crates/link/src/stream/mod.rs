//! Byte-stream transport (UART, RS-485, USB-CDC, TCP for testing): frames are COBS-encoded and
//! terminated with `0x00`; receivers resynchronize at the next `0x00` after any error.

mod decode;
mod encode;

use core::fmt;

#[cfg(any(feature = "embedded-io", feature = "embedded-io-async"))]
pub(crate) use decode::Status;
pub use decode::{StreamDecoder, StreamStats};
pub use encode::{StreamEncoder, encode_frame, encode_message, max_encoded_len};

use crate::frame::FrameError;

/// Byte-stream encode/decode failures. Each corrupt packet is reported once, at its delimiter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StreamError {
    /// The output buffer cannot hold the encoded stream bytes.
    BufferTooSmall {
        /// Bytes that always suffice ([`max_encoded_len`]).
        needed: usize,
        /// Bytes available.
        available: usize,
    },
    /// The packet decoded to more bytes than the decoder's buffer holds; it was discarded.
    Overflow,
    /// The packet ended inside a COBS block, so it was truncated or corrupted.
    Cobs,
    /// The decoded packet is not a valid frame (too short or bad CRC).
    Frame(FrameError),
}

impl fmt::Display for StreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BufferTooSmall { needed, available } => {
                write!(
                    f,
                    "stream buffer too small: need {needed} bytes, have {available}"
                )
            }
            Self::Overflow => f.write_str("stream packet exceeds decoder buffer"),
            Self::Cobs => f.write_str("malformed cobs packet"),
            Self::Frame(err) => write!(f, "invalid stream frame: {err}"),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for StreamError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Frame(err) => Some(err),
            _ => None,
        }
    }
}

impl From<FrameError> for StreamError {
    fn from(err: FrameError) -> Self {
        Self::Frame(err)
    }
}

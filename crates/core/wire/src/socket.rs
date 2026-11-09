//! Sockets: a plain little-endian `u32` length in front of each frame. The same prefix file
//! descriptor passing uses, so a frame with descriptors attached is still an ordinary frame.

use core::fmt;

use crate::frame::{self, FrameError, FrameHeader, FrameView, frame_len};

/// Bytes of the length prefix.
pub const LENGTH_LEN: usize = 4;

/// Socket framing failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SocketError {
    /// The prefix claims more than the receiver allows. The stream can't be resynchronized.
    TooLarge {
        /// The claimed frame length.
        len: usize,
        /// The receiver's limit.
        max: usize,
    },
    /// The frame itself is bad. The stream is still in step, past this frame.
    Frame {
        /// What was wrong.
        error: FrameError,
        /// Bytes to skip.
        consumed: usize,
    },
    /// The output buffer is too small.
    BufferTooSmall,
}

impl fmt::Display for SocketError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLarge { len, max } => {
                write!(f, "frame of {len} bytes is over the {max} limit")
            }
            Self::Frame { error, .. } => write!(f, "{error}"),
            Self::BufferTooSmall => f.write_str("socket buffer too small"),
        }
    }
}

impl core::error::Error for SocketError {}

/// The outcome of [`parse`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Parsed<'a> {
    /// Not enough bytes yet. At least this many are needed in total.
    Incomplete {
        /// Total bytes the buffer must hold.
        needed: usize,
    },
    /// One whole frame.
    Frame {
        /// The frame.
        frame: FrameView<'a>,
        /// Bytes it took, prefix included.
        consumed: usize,
    },
}

/// Encodes a prefixed frame into `out`, returning the bytes written.
///
/// # Errors
///
/// [`SocketError::BufferTooSmall`] or [`SocketError::TooLarge`].
pub fn encode(header: FrameHeader, payload: &[u8], out: &mut [u8]) -> Result<usize, SocketError> {
    let len = frame_len(payload.len());
    let prefix = u32::try_from(len)
        .map_err(|_| SocketError::TooLarge {
            len,
            max: u32::MAX as usize,
        })?
        .to_le_bytes();
    let (head, rest) = out
        .split_at_mut_checked(LENGTH_LEN)
        .ok_or(SocketError::BufferTooSmall)?;
    head.copy_from_slice(&prefix);
    frame::encode(header, payload, rest).map_err(|_| SocketError::BufferTooSmall)?;
    Ok(LENGTH_LEN + len)
}

/// Finds the first frame at the start of `bytes`. Frames over `max` are refused without waiting
/// for them, so a peer can't make the receiver buffer an unbounded amount.
///
/// # Errors
///
/// [`SocketError::TooLarge`] or [`SocketError::Frame`].
pub fn parse(bytes: &[u8], max: usize) -> Result<Parsed<'_>, SocketError> {
    let Some(prefix) = bytes.first_chunk::<LENGTH_LEN>() else {
        return Ok(Parsed::Incomplete { needed: LENGTH_LEN });
    };
    let len = u32::from_le_bytes(*prefix) as usize;
    if len > max {
        return Err(SocketError::TooLarge { len, max });
    }
    let consumed = LENGTH_LEN + len;
    let Some(body) = bytes.get(LENGTH_LEN..consumed) else {
        return Ok(Parsed::Incomplete { needed: consumed });
    };
    match frame::decode(body) {
        Ok(frame) => Ok(Parsed::Frame { frame, consumed }),
        Err(error) => Err(SocketError::Frame { error, consumed }),
    }
}

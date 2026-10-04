//! Message frames: `[version u8][kind u8][seq u16 LE][payload][crc32c u32 LE]`.
//!
//! The CRC covers `version..payload`. Frames are encoded into caller-provided buffers and decoded
//! into borrowed [`FrameView`]s; nothing here allocates.

use core::fmt;

use crate::crc::{Crc32c, crc32c};

/// Version of the link wire format carried in every frame header.
///
/// Independent of the node's control-protocol version.
pub const LINK_PROTOCOL_VERSION: u8 = 1;

/// Header length: version, kind, and the little-endian sequence number.
pub const HEADER_LEN: usize = 4;

/// Trailer length: the little-endian CRC-32C.
pub const CRC_LEN: usize = 4;

/// Bytes a frame adds around its payload. This is also the minimum frame length.
pub const FRAME_OVERHEAD: usize = HEADER_LEN + CRC_LEN;

/// Encoded frame length for a payload of `payload_len` bytes (saturating).
#[must_use]
pub const fn frame_len(payload_len: usize) -> usize {
    payload_len.saturating_add(FRAME_OVERHEAD)
}

/// The fixed frame header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FrameHeader {
    /// Wire-format version, normally [`LINK_PROTOCOL_VERSION`].
    pub version: u8,
    /// Message kind. Unknown kinds are ignored by receivers.
    pub kind: u8,
    /// Sender sequence number.
    pub seq: u16,
}

impl FrameHeader {
    /// Header for the current [`LINK_PROTOCOL_VERSION`].
    #[must_use]
    pub const fn new(kind: u8, seq: u16) -> Self {
        Self {
            version: LINK_PROTOCOL_VERSION,
            kind,
            seq,
        }
    }

    /// Wire representation.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; HEADER_LEN] {
        let seq = self.seq.to_le_bytes();
        [self.version, self.kind, seq[0], seq[1]]
    }

    /// Parses the wire representation.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; HEADER_LEN]) -> Self {
        Self {
            version: bytes[0],
            kind: bytes[1],
            seq: u16::from_le_bytes([bytes[2], bytes[3]]),
        }
    }

    /// Whether this header carries the version this crate speaks.
    #[must_use]
    pub const fn is_current_version(&self) -> bool {
        self.version == LINK_PROTOCOL_VERSION
    }
}

/// A validated frame borrowed from a receive buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameView<'a> {
    header: FrameHeader,
    payload: &'a [u8],
}

impl<'a> FrameView<'a> {
    /// The decoded header.
    #[must_use]
    pub const fn header(&self) -> FrameHeader {
        self.header
    }

    /// Wire-format version. Not checked by [`decode`]; see [`FrameHeader::is_current_version`].
    #[must_use]
    pub const fn version(&self) -> u8 {
        self.header.version
    }

    /// Message kind.
    #[must_use]
    pub const fn kind(&self) -> u8 {
        self.header.kind
    }

    /// Sender sequence number.
    #[must_use]
    pub const fn seq(&self) -> u16 {
        self.header.seq
    }

    /// Message body.
    #[must_use]
    pub const fn payload(&self) -> &'a [u8] {
        self.payload
    }
}

/// Frame encode/decode failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FrameError {
    /// The output buffer cannot hold the encoded frame.
    BufferTooSmall {
        /// Bytes required.
        needed: usize,
        /// Bytes available.
        available: usize,
    },
    /// Fewer than [`FRAME_OVERHEAD`] bytes, so there is no header and CRC.
    TooShort {
        /// Length received.
        len: usize,
    },
    /// The trailer does not match the CRC-32C of the frame body.
    CrcMismatch {
        /// CRC carried in the frame.
        received: u32,
        /// CRC computed over the received bytes.
        computed: u32,
    },
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BufferTooSmall { needed, available } => {
                write!(
                    f,
                    "frame buffer too small: need {needed} bytes, have {available}"
                )
            }
            Self::TooShort { len } => write!(f, "frame too short: {len} bytes"),
            Self::CrcMismatch { received, computed } => write!(
                f,
                "frame crc mismatch: received {received:#010x}, computed {computed:#010x}"
            ),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for FrameError {}

/// Encodes a frame for `header` and `payload` into `out`, returning the frame length.
///
/// # Errors
///
/// [`FrameError::BufferTooSmall`] if `out` is shorter than [`frame_len`]`(payload.len())`.
pub fn encode(header: FrameHeader, payload: &[u8], out: &mut [u8]) -> Result<usize, FrameError> {
    let len = frame_len(payload.len());
    let available = out.len();
    let body =
        out.get_mut(HEADER_LEN..len.saturating_sub(CRC_LEN))
            .ok_or(FrameError::BufferTooSmall {
                needed: len,
                available,
            })?;
    body.copy_from_slice(payload);
    encode_in_place(header, payload.len(), out)
}

/// Finishes a frame whose payload the caller already wrote into
/// [`payload_area`]`(buf)[..payload_len]`: writes the header and CRC trailer and returns the frame
/// length. This lets a serializer write straight into the transmit buffer.
///
/// # Errors
///
/// [`FrameError::BufferTooSmall`] if `buf` cannot hold a frame with `payload_len` payload bytes.
pub fn encode_in_place(
    header: FrameHeader,
    payload_len: usize,
    buf: &mut [u8],
) -> Result<usize, FrameError> {
    let len = frame_len(payload_len);
    let available = buf.len();
    let too_small = FrameError::BufferTooSmall {
        needed: len,
        available,
    };
    if payload_len > usize::MAX - FRAME_OVERHEAD || len > available {
        return Err(too_small);
    }
    let (body, rest) = buf.split_at_mut(len - CRC_LEN);
    let (head, _) = body.split_at_mut(HEADER_LEN);
    head.copy_from_slice(&header.to_bytes());
    let crc = crc32c(body).to_le_bytes();
    let trailer = rest.get_mut(..CRC_LEN).ok_or(too_small)?;
    trailer.copy_from_slice(&crc);
    Ok(len)
}

/// The region of `buf` where a payload goes before [`encode_in_place`]: everything after the
/// header, minus room for the CRC. Empty if `buf` is shorter than [`FRAME_OVERHEAD`].
pub fn payload_area(buf: &mut [u8]) -> &mut [u8] {
    let end = buf.len().saturating_sub(CRC_LEN);
    buf.get_mut(HEADER_LEN..end).unwrap_or_default()
}

/// Validates the length and CRC of `bytes` and returns a borrowed view.
///
/// The version byte is reported, not checked, so the caller can answer a mismatch (for example
/// with `Reject { VersionMismatch }`) instead of silently dropping the frame.
///
/// # Errors
///
/// [`FrameError::TooShort`] or [`FrameError::CrcMismatch`].
pub fn decode(bytes: &[u8]) -> Result<FrameView<'_>, FrameError> {
    let len = bytes.len();
    if len < FRAME_OVERHEAD {
        return Err(FrameError::TooShort { len });
    }
    let (body, trailer) = bytes.split_at(len - CRC_LEN);
    let received = u32::from_le_bytes(take_array(trailer).ok_or(FrameError::TooShort { len })?);
    let mut crc = Crc32c::new();
    crc.update(body);
    let computed = crc.finish();
    if received != computed {
        return Err(FrameError::CrcMismatch { received, computed });
    }
    view_unchecked(bytes).ok_or(FrameError::TooShort { len })
}

/// Splits an already-validated frame into a view without recomputing the CRC.
pub(crate) fn view_unchecked(bytes: &[u8]) -> Option<FrameView<'_>> {
    let header = FrameHeader::from_bytes(take_array(bytes)?);
    let payload = bytes.get(HEADER_LEN..bytes.len().checked_sub(CRC_LEN)?)?;
    Some(FrameView { header, payload })
}

fn take_array<const K: usize>(bytes: &[u8]) -> Option<[u8; K]> {
    bytes.get(..K)?.try_into().ok()
}

//! The one wire. Every carrier sends the same frames:
//!
//! `[version u8][kind u8][seq u16 LE][payload][crc32c u32 LE]`
//!
//! Byte streams wrap a frame in COBS ([`stream`]), CAN splits it into segments ([`packet`]), and
//! sockets put a plain length in front of it ([`socket`]). Bodies are postcard, follow the
//! append-only rules in `docs/wire.md`, and are declared with [`body!`]. The whole kind table is in
//! [`kind`], so no two crates can claim the same number.
//!
//! `no_std`, no allocator. Bodies borrow from the receive buffer.

#![cfg_attr(not(feature = "std"), no_std)]

pub mod body;
mod crc;
pub mod frame;
pub mod kind;
pub mod packet;
pub mod socket;
mod source;
pub mod stream;

pub use body::{Body, Bytes, Decode, List, decode_body, encode_body};
pub use crc::{Crc32c, crc32c};
pub use frame::{
    CRC_LEN, FRAME_OVERHEAD, FrameError, FrameHeader, FrameView, HEADER_LEN, INTERFACE_REVISION,
    PROTOCOL_VERSION, frame_len, seq_newer,
};
pub use orion_types as types;
pub use packet::{CanLinkIds, Reassembler, Segment, SegmentMtu, Segmenter};
pub use stream::{StreamDecoder, StreamEncoder, encode_frame, encode_message, max_encoded_len};

#[doc(hidden)]
pub use serde as __serde;

//! The Orion link protocol (see `docs/link-protocol.md`).
//!
//! The link protocol connects microcontrollers to an `orion-node` over UART, RS-485, USB-CDC,
//! classic CAN, and CAN FD, without assuming any chip, HAL, RTOS, or async runtime. Layers:
//!
//! - [`frame`]: message frames `[version][kind][seq u16 LE][payload][crc32c LE]`.
//! - [`stream`]: COBS encoding with `0x00` delimiters for byte streams, plus a resynchronizing
//!   streaming decoder.
//! - [`packet`]: segmentation into classic CAN / CAN FD frames and reassembly.
//! - `wire` (feature `device`): the minimal device-side message layer: a hand-written
//!   postcard-compatible codec and borrowed views of every body a device sends or receives. No
//!   allocator, no serde, no `core::fmt`.
//! - `device` (feature `device`): the sans-IO device session, generic over `Stream` or `Packet`
//!   transports, with fixed const-generic buffers and no allocator.
//! - `message` (feature `alloc`): typed messages over the shared Orion records (postcard + serde),
//!   used by the host; with `alloc` the device session also accepts the records directly.
//! - `host` (feature `std`): the sans-IO host session and a multi-device CAN bus, for the node
//!   gateway.
//!
//! The default build is only the framing layers: `no_std`, allocation-free, and panic-free on any
//! input, with caller-provided or const-generic buffers. Optional features add thin adapters for
//! `embedded-io`, `embedded-io-async`, and `embedded-can`.
//!
//! ```
//! use orion_link::{FrameHeader, StreamDecoder, StreamEncoder};
//!
//! // Device side: emit stream bytes one at a time, no frame buffer needed.
//! let wire: Vec<u8> = StreamEncoder::for_message(FrameHeader::new(0x10, 1), b"state").collect();
//!
//! // Host side: feed bytes as they arrive.
//! let mut decoder = StreamDecoder::<256>::new();
//! let mut got = None;
//! for &byte in &wire {
//!     if let Ok(Some(frame)) = decoder.push(byte) {
//!         got = Some((frame.kind(), frame.seq(), frame.payload().to_vec()));
//!     }
//! }
//! assert_eq!(got, Some((0x10, 1, b"state".to_vec())));
//! ```

#![no_std]
#![cfg_attr(
    not(test),
    deny(
        clippy::indexing_slicing,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]
#![warn(missing_docs)]

#[cfg(feature = "alloc")]
extern crate alloc;
#[cfg(feature = "std")]
extern crate std;

mod crc;
pub mod frame;
pub mod packet;
mod source;
pub mod stream;

#[cfg(feature = "device")]
pub mod device;
#[cfg(feature = "std")]
pub mod host;
#[cfg(feature = "alloc")]
pub mod message;
#[cfg(feature = "device")]
pub mod transport;
#[cfg(feature = "device")]
pub mod wire;

#[cfg(feature = "embedded-can")]
mod can;
#[cfg(feature = "embedded-io")]
pub mod io;
#[cfg(feature = "embedded-io-async")]
pub mod io_async;
#[cfg(any(feature = "embedded-io", feature = "embedded-io-async"))]
mod read_error;

pub use crc::{Crc32c, crc32c};
pub use frame::{
    CRC_LEN, FRAME_OVERHEAD, FrameError, FrameHeader, FrameView, HEADER_LEN, LINK_PROTOCOL_VERSION,
};
pub use packet::{
    CanLinkIds, PacketError, PacketStats, Reassembler, Segment, SegmentMtu, Segmenter,
};
#[cfg(any(feature = "embedded-io", feature = "embedded-io-async"))]
pub use read_error::ReadFrameError;
pub use stream::{
    StreamDecoder, StreamEncoder, StreamError, StreamStats, encode_frame, encode_message,
    max_encoded_len,
};
#[cfg(feature = "device")]
pub use transport::{Packet, Stream, Transport};

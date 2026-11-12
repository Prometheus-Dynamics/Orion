//! Buffer topics: data too large to copy, passed as buffers in shared memory.
//!
//! A frame is too large to copy into a ring, so a buffer topic's ring carries *which buffers*
//! hold the frame, and the buffers themselves are shared memory handed over once.
//!
//! - A [`Pool`] is the publisher's set of buffers: slices of a few descriptors (dma-bufs from a
//!   camera, or memfds). It is passed to the comm core and on to each subscriber once, as those
//!   descriptors plus a small [pool page](page) the publisher writes.
//! - A [`PoolView`] is a subscriber's read-only mapping of the same buffers.
//! - Each frame is one ring [entry](entry): the pool's generation, one or more buffers (an image
//!   and its pyramid levels travel together), the sample time and opaque metadata.
//! - A [`BufferWriter`] publishes entries and takes buffers back. A [`BufferReader`] reads
//!   entries and hands out [`Held`] frames, read in place: no copy.
//!
//! Buffers are reused, so "which buffer" is not enough to name a frame. Every buffer has a
//! **ticket** in the pool page that changes when the publisher starts taking the buffer back.
//! An entry carries the ticket each buffer had when the frame was published; a reader that
//! finds a different ticket knows the frame is gone and never looks at the buffer.
//!
//! What keeps a buffer from being reused while a reader still looks at it is a hold mark. This
//! crate defines where marks plug in ([`Marker`] for a reader, [`Holders`] for the publisher)
//! and `orion-ring-holds` supplies them, with the limits. Without marks ([`NoMarks`],
//! [`NoHolders`]) a reader may be reading a buffer while the publisher reuses it.
//!
//! A buffer number means nothing on another board. Off the board, video is an ordinary bytes
//! topic.

#![cfg(target_os = "linux")]

pub mod entry;
#[allow(unsafe_code)]
mod map;
pub mod page;
mod pool;
mod reader;
mod writer;

use core::fmt;

pub use pool::{BufferDesc, Pool, PoolView};
pub use reader::{Batch, BufferReader, Held, NextBatch};
pub use writer::BufferWriter;

/// A buffer of a pool: its position in the pool's list.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BufferIndex(pub u32);

/// The most buffers one pool holds.
pub const MAX_BUFFERS: u32 = 1024;
/// The most buffers one frame names.
pub const MAX_PER_FRAME: usize = 8;
/// The most subscribers that can hold buffers of one pool at once.
pub const MAX_READERS: u32 = 64;

/// The publisher's view of who holds what. Supplied by `orion-ring-holds`.
pub trait Holders {
    /// Whether any reader holds `buffer`. Called after the buffer's ticket has gone up, so a
    /// reader that has not marked it by now never will.
    fn held(&self, buffer: BufferIndex) -> bool;
}

/// Nobody ever holds anything: the publisher reuses buffers as it likes.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoHolders;

impl Holders for NoHolders {
    fn held(&self, _buffer: BufferIndex) -> bool {
        false
    }
}

/// A reader's way of holding buffers. Supplied by `orion-ring-holds`.
pub trait Marker {
    /// Marks `buffer` as held, visibly to the publisher, before the reader checks the buffer's
    /// ticket. `false` if the reader is at its limit and may not hold another.
    fn mark(&self, buffer: BufferIndex) -> bool;
    /// Removes the mark.
    fn unmark(&self, buffer: BufferIndex);
    /// Which reader slot of the pool these marks are in, so the reader can see when the
    /// publisher cuts it off. `None` for a reader that holds nothing.
    fn slot(&self) -> Option<u32> {
        None
    }
}

/// Holds nothing: frames can change while they are read.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoMarks;

impl Marker for NoMarks {
    fn mark(&self, _buffer: BufferIndex) -> bool {
        true
    }

    fn unmark(&self, _buffer: BufferIndex) {}
}

impl<K: Marker> Marker for &K {
    fn mark(&self, buffer: BufferIndex) -> bool {
        (**self).mark(buffer)
    }

    fn unmark(&self, buffer: BufferIndex) {
        (**self).unmark(buffer);
    }

    fn slot(&self) -> Option<u32> {
        (**self).slot()
    }
}

impl<K: Marker> Marker for std::sync::Arc<K> {
    fn mark(&self, buffer: BufferIndex) -> bool {
        (**self).mark(buffer)
    }

    fn unmark(&self, buffer: BufferIndex) {
        (**self).unmark(buffer);
    }

    fn slot(&self) -> Option<u32> {
        (**self).slot()
    }
}

/// Why a buffer operation was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BufferError {
    /// A frame must name 1 to [`MAX_PER_FRAME`] buffers.
    Count,
    /// The buffer isn't in the pool.
    NoSuchBuffer(BufferIndex),
    /// The buffer is already in a published frame and hasn't been taken back.
    NotFree(BufferIndex),
    /// The buffers and metadata don't fit the ring's largest payload.
    TooLarge,
}

impl fmt::Display for BufferError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Count => f.write_str("a frame names 1 to 8 buffers"),
            Self::NoSuchBuffer(buffer) => write!(f, "buffer {} is not in the pool", buffer.0),
            Self::NotFree(buffer) => write!(f, "buffer {} is still published", buffer.0),
            Self::TooLarge => f.write_str("the frame entry does not fit the ring"),
        }
    }
}

impl std::error::Error for BufferError {}

/// Every buffer is published and held by some reader, so there is nowhere to put a frame. The
/// publisher drops the frame; it never waits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Dropped;

impl fmt::Display for Dropped {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("no buffer is free; the frame is dropped")
    }
}

impl std::error::Error for Dropped {}

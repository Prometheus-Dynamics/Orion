//! The topic ring: one writer, many readers, in shared memory.
//!
//! A publish is a handful of memory writes: no system call, no reply, no allocation. Readers map
//! the ring read-only and can never disturb the writer or each other. A reader treats everything
//! in the ring as untrusted: every index is checked against the shape it validated when it
//! attached, every copy is checked for having been overwritten meanwhile, and every loop is
//! bounded. A writer can damage only its own topic.
//!
//! `docs/ring.md` pins the layout. In short: a header, one lane of `history` slots per field, and
//! an index that gives readers the order samples were published in across fields. The newest
//! value of a field, its history and the order of everything all read the same slots.
//!
//! The algorithms are safe code over [`Mem`], an array of 64-bit words. [`Ring`] puts that array
//! in a sealed memfd; `ring-model` puts it under a model checker.

#![cfg(target_os = "linux")]

#[cfg(target_endian = "big")]
compile_error!("the ring layout is little-endian");

mod layout;
#[allow(unsafe_code)]
mod map;
mod mem;
mod reader;
mod ring;
#[cfg(test)]
mod tests;
mod writer;

use core::fmt;
use std::io;

pub use layout::{Geometry, MAX_FIELDS, MAX_HISTORY, MAX_PAYLOAD, Spec, format};
pub use map::{ReadMap, WriteMap};
pub use mem::{Heap, Mem, MemMut};
pub use orion_types::FrameTime;
pub use reader::{Next, Reader, Sample};
pub use ring::Ring;
pub use writer::Writer;

/// A field of a topic: its position in the declaration.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FieldId(pub u16);

/// Generic flags on a sample. The ring carries them and gives them no meaning.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Flags(pub u8);

impl Flags {
    /// No flags.
    pub const NONE: Self = Self(0);
    /// Others depend on this one.
    pub const OTHERS_DEPEND: Self = Self(1 << 0);

    /// Whether every flag in `other` is set.
    #[must_use]
    pub const fn has(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

/// Why a ring could not be made or attached.
#[derive(Debug)]
pub enum RingError {
    /// The declaration asks for a shape the ring doesn't allow.
    Spec(&'static str),
    /// The memory doesn't hold a ring this build reads. Says what was wrong.
    Malformed(&'static str),
    /// The descriptor isn't a memfd sealed against shrinking, so reading it could fault.
    Unsealed,
    /// A system call failed.
    Io(io::Error),
}

impl fmt::Display for RingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Spec(why) => write!(f, "ring shape not allowed: {why}"),
            Self::Malformed(why) => write!(f, "not a ring: {why}"),
            Self::Unsealed => f.write_str("ring descriptor is not sealed against shrinking"),
            Self::Io(err) => write!(f, "ring: {err}"),
        }
    }
}

impl std::error::Error for RingError {}

impl From<io::Error> for RingError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

/// Why a publish was refused. Nothing was written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishError {
    /// The field isn't in the declaration.
    NoSuchField,
    /// The payload is larger than the declaration's largest sample.
    TooLarge,
}

impl fmt::Display for PublishError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NoSuchField => "field is not in the declaration",
            Self::TooLarge => "payload is larger than the declared maximum",
        })
    }
}

impl std::error::Error for PublishError {}

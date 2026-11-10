//! A frame's ring entry: the payload of a `Buffers` field.
//!
//! | Bytes | Holds |
//! | --- | --- |
//! | 0 to 3 | The pool's generation |
//! | 4 | How many buffers, 1 to 8 |
//! | 5 to 7 | Reserved, zero |
//! | 8 on | For each buffer: its index (4 bytes) and its ticket (4 bytes, the low half) |
//! | then | Metadata, opaque, to the end |
//!
//! Little-endian.

use crate::{BufferIndex, MAX_PER_FRAME};

const HEADER: usize = 8;
const PER_BUFFER: usize = 8;

/// Bytes an entry of `buffers` buffers and `meta` bytes of metadata takes.
#[must_use]
pub const fn len(buffers: usize, meta: usize) -> usize {
    HEADER + buffers * PER_BUFFER + meta
}

/// Writes an entry at the start of `out` and returns its length, or `None` if it doesn't fit or
/// names no buffers or too many.
pub fn write(
    out: &mut [u8],
    generation: u32,
    buffers: &[(BufferIndex, u64)],
    meta: &[u8],
) -> Option<usize> {
    if buffers.is_empty() || buffers.len() > MAX_PER_FRAME {
        return None;
    }
    let total = len(buffers.len(), meta.len());
    let out = out.get_mut(..total)?;
    out[..4].copy_from_slice(&generation.to_le_bytes());
    out[4..HEADER].copy_from_slice(&[buffers.len() as u8, 0, 0, 0]);
    let (list, tail) = out[HEADER..].split_at_mut(buffers.len() * PER_BUFFER);
    for ((index, ticket), out) in buffers.iter().zip(list.as_chunks_mut::<PER_BUFFER>().0) {
        out[..4].copy_from_slice(&index.0.to_le_bytes());
        out[4..].copy_from_slice(&(*ticket as u32).to_le_bytes());
    }
    tail.copy_from_slice(meta);
    Some(total)
}

/// An entry read from a payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Entry<'a> {
    /// The generation of the pool the buffers belong to.
    pub generation: u32,
    /// The buffers, with the low half of the ticket each had when the frame was published.
    pub buffers: [(BufferIndex, u32); MAX_PER_FRAME],
    /// How many of `buffers` are used.
    pub count: usize,
    /// The metadata.
    pub meta: &'a [u8],
}

/// Reads an entry. `None` if the payload isn't one.
#[must_use]
pub fn read(payload: &[u8]) -> Option<Entry<'_>> {
    let header = payload.get(..HEADER)?;
    let generation = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    let count = usize::from(header[4]);
    if count == 0 || count > MAX_PER_FRAME {
        return None;
    }
    let list = payload.get(HEADER..HEADER + count * PER_BUFFER)?;
    let mut buffers = [(BufferIndex(0), 0); MAX_PER_FRAME];
    for (slot, bytes) in buffers.iter_mut().zip(list.as_chunks::<PER_BUFFER>().0) {
        *slot = (
            BufferIndex(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])),
            u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
        );
    }
    Some(Entry {
        generation,
        buffers,
        count,
        meta: &payload[HEADER + count * PER_BUFFER..],
    })
}

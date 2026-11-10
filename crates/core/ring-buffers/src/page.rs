//! The pool page: a sealed page the publisher writes and every reader maps read-only. Safe code
//! over [`Mem`], so a model checker can run it.
//!
//! | Word | Holds |
//! | --- | --- |
//! | 0 | Magic: `ORIONPL1` |
//! | 1 | Generation (low 32 bits), buffers (high 32 bits) |
//! | 2 to 7 | Reserved, zero |
//! | 8 | Tickets: one word per buffer, `uses << 1 | taken back` |
//! | 8 + buffers | Cut-off: one word per reader slot, nonzero once that reader is cut off |
//!
//! A buffer's ticket is even while the buffer is free or published, and odd from the moment the
//! publisher starts taking it back until it is free again. `uses` counts how many frames the
//! buffer has been freed from. A frame's entry carries the even ticket it was published with, so
//! the frame can be held only while the ticket is still exactly that.

use std::sync::atomic::Ordering;

use orion_ring::{Mem, MemMut, RingError};

use crate::{BufferIndex, MAX_BUFFERS, MAX_READERS};

const MAGIC: u64 = u64::from_le_bytes(*b"ORIONPL1");
const W_MAGIC: usize = 0;
const W_SHAPE: usize = 1;
const TICKETS_AT: usize = 8;

/// Bytes a pool page of `buffers` buffers needs.
#[must_use]
pub const fn bytes(buffers: u32) -> usize {
    (TICKETS_AT + buffers as usize + MAX_READERS as usize) * 8
}

/// Writes a fresh pool page into zeroed `mem`.
///
/// # Errors
///
/// [`RingError::Spec`] if there are no buffers, too many, or `mem` is too small.
pub fn format(mem: &impl MemMut, generation: u32, buffers: u32) -> Result<(), RingError> {
    if buffers == 0 || buffers > MAX_BUFFERS {
        return Err(RingError::Spec("a pool holds 1 to 1024 buffers"));
    }
    if mem.words() * 8 < bytes(buffers) {
        return Err(RingError::Spec("memory is smaller than the pool page"));
    }
    mem.store(
        W_SHAPE,
        u64::from(generation) | u64::from(buffers) << 32,
        Ordering::Relaxed,
    );
    mem.store(W_MAGIC, MAGIC, Ordering::Release);
    Ok(())
}

/// The shape of the pool page in `mem`: its generation and how many buffers it has.
///
/// # Errors
///
/// [`RingError::Malformed`] if `mem` doesn't hold a pool page that fits in it.
pub fn read(mem: &impl Mem) -> Result<(u32, u32), RingError> {
    if mem.words() <= TICKETS_AT || mem.load(W_MAGIC, Ordering::Acquire) != MAGIC {
        return Err(RingError::Malformed("not a pool page"));
    }
    let shape = mem.load(W_SHAPE, Ordering::Relaxed);
    let (generation, buffers) = (shape as u32, (shape >> 32) as u32);
    if buffers == 0 || buffers > MAX_BUFFERS || mem.words() * 8 < bytes(buffers) {
        return Err(RingError::Malformed("pool page shape out of range"));
    }
    Ok((generation, buffers))
}

/// The ticket of `buffer`, which must be in the pool. Plain read: for the publisher, and for a
/// reader that only wants to know.
#[must_use]
pub fn ticket(mem: &impl Mem, buffer: BufferIndex) -> u64 {
    mem.load(TICKETS_AT + buffer.0 as usize, Ordering::Acquire)
}

/// Publisher: starts taking `buffer` back. After this no reader can newly hold the frame that
/// was in it; the caller then asks its [`Holders`](crate::Holders) whether someone already
/// does, and calls [`release`] once nobody does. Doing it again before then changes nothing.
pub fn retire<M: MemMut>(mem: &M, buffer: BufferIndex) {
    let ticket = ticket(mem, buffer);
    if ticket & 1 == 0 {
        mem.store(TICKETS_AT + buffer.0 as usize, ticket | 1, Ordering::SeqCst);
    }
    M::fence(Ordering::SeqCst);
}

/// Publisher: `buffer`, taken back and held by nobody, is free for its next frame.
pub fn release(mem: &impl MemMut, buffer: BufferIndex) {
    let ticket = ticket(mem, buffer);
    if ticket & 1 == 1 {
        mem.store(
            TICKETS_AT + buffer.0 as usize,
            ticket.wrapping_add(1),
            Ordering::Release,
        );
    }
}

/// Which use of `buffer` this is: the same from when a frame is published in it until the
/// buffer is free again. What tells one hold of a buffer from the next.
#[must_use]
pub fn use_of(mem: &impl Mem, buffer: BufferIndex) -> u64 {
    ticket(mem, buffer) >> 1
}

/// Reader, after marking `buffer`: whether the frame published with `ticket` (the low half, as
/// an entry carries it) is still in it. The fence pairs with the one in [`retire`]: either the
/// publisher sees the mark, or this sees the new ticket.
pub fn still<M: Mem>(mem: &M, buffer: BufferIndex, ticket: u32) -> bool {
    M::fence(Ordering::SeqCst);
    self::ticket(mem, buffer) as u32 == ticket
}

/// Publisher: cuts reader `slot` off. It is told the next time it looks.
pub fn cut_off(mem: &impl MemMut, buffers: u32, slot: u32) {
    if slot < MAX_READERS {
        mem.store(
            TICKETS_AT + buffers as usize + slot as usize,
            1,
            Ordering::Release,
        );
    }
}

/// Publisher: makes reader `slot` usable again, for the next subscriber given it.
pub fn clear_cut_off(mem: &impl MemMut, buffers: u32, slot: u32) {
    if slot < MAX_READERS {
        mem.store(
            TICKETS_AT + buffers as usize + slot as usize,
            0,
            Ordering::Release,
        );
    }
}

/// Reader: whether reader `slot` has been cut off.
#[must_use]
pub fn is_cut_off(mem: &impl Mem, buffers: u32, slot: u32) -> bool {
    slot < MAX_READERS
        && mem.load(
            TICKETS_AT + buffers as usize + slot as usize,
            Ordering::Acquire,
        ) != 0
}

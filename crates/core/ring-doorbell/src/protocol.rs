//! The part of a doorbell that lives in shared memory, as safe code over [`Mem`] so a model
//! checker can run it.
//!
//! Word 0 of the subscriber's page is `sleeps << 1 | asleep`: how many times the subscriber has
//! gone to sleep, and whether it is asleep now.
//!
//! The two sides each do "write mine, fence, read yours":
//!
//! - subscriber: store asleep, fence, look for samples (read the ring's head);
//! - publisher: publish (store the ring's head), fence, read the page.
//!
//! The fences are sequentially consistent, so one comes first. If the subscriber's is first, the
//! publisher's read sees it asleep and rings. If the publisher's is first, the subscriber's look
//! sees the sample and it doesn't sleep. A wake-up cannot fall between them.

use std::sync::atomic::Ordering;

use orion_ring::{Mem, MemMut};

/// The word of the page that holds the state.
pub const STATE_WORD: usize = 0;

/// Subscriber: about to sleep, for the `sleeps`-th time. Look for samples after this, before
/// sleeping.
pub fn arm<M: MemMut>(page: &M, sleeps: u64) {
    page.store(STATE_WORD, sleeps << 1 | 1, Ordering::SeqCst);
    M::fence(Ordering::SeqCst);
}

/// Subscriber: awake.
pub fn disarm<M: MemMut>(page: &M, sleeps: u64) {
    page.store(STATE_WORD, sleeps << 1, Ordering::Release);
}

/// Publisher, after publishing: whether to ring. `rung` is the publisher's own note of the sleep
/// it last rang for, so one sleep gets one ring however many samples follow.
pub fn wants_ring<M: Mem>(page: &M, rung: &mut u64) -> bool {
    M::fence(Ordering::SeqCst);
    let state = page.load(STATE_WORD, Ordering::Acquire);
    if state & 1 == 0 || state == *rung {
        return false;
    }
    *rung = state;
    true
}

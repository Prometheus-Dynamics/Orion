//! The publisher's side: every reader's marks, and the limits on them.

use std::os::fd::OwnedFd;
use std::time::Duration;

use orion_ring::{Mem, MemMut, ReadMap, RingError};
use orion_ring_buffers::{BufferIndex, Holders, MAX_READERS, page};

use crate::marks::marked;

/// What one reader is allowed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Limits {
    /// How many buffers it may hold at once. Zero for no limit.
    pub max_held: u32,
    /// How long it may hold one. `None` for no limit.
    pub max_hold: Option<Duration>,
}

#[derive(Debug)]
struct Slot<M> {
    page: M,
    limits: Limits,
    /// For each buffer this reader marks: which use of the buffer it was and the time on the
    /// publisher's clock when the mark was first seen. The reader's own word for how long is
    /// not taken.
    seen: Box<[Option<(u64, u64)>]>,
    cut_off: bool,
}

/// The publisher's table of readers. A buffer is held while any reader that hasn't been cut off
/// marks it.
#[derive(Debug)]
pub struct HoldTable<M: Mem = ReadMap> {
    buffers: u32,
    slots: Box<[Option<Slot<M>>]>,
}

impl HoldTable<ReadMap> {
    /// Adds a reader in `slot`: its page of marks, read-only, and its limits.
    ///
    /// # Errors
    ///
    /// [`RingError`] if the slot is out of range or taken, or the page can't be mapped or is
    /// too small for the pool.
    pub fn add(&mut self, slot: u32, page: OwnedFd, limits: Limits) -> Result<(), RingError> {
        self.add_over(slot, ReadMap::new(page)?, limits)
    }
}

impl<M: Mem> HoldTable<M> {
    /// A table for a pool of `buffers` buffers, with no readers.
    #[must_use]
    pub fn new(buffers: u32) -> Self {
        Self {
            buffers,
            slots: (0..MAX_READERS).map(|_| None).collect(),
        }
    }

    /// A slot no reader is in, to give the next one.
    #[must_use]
    pub fn free_slot(&self) -> Option<u32> {
        self.slots
            .iter()
            .position(Option::is_none)
            .map(|slot| slot as u32)
    }

    /// Adds a reader whose marks are in `page`.
    ///
    /// # Errors
    ///
    /// [`RingError`] if the slot is out of range or taken, or the page is too small.
    pub fn add_over(&mut self, slot: u32, page: M, limits: Limits) -> Result<(), RingError> {
        if page.words() < self.buffers as usize {
            return Err(RingError::Malformed(
                "the marks page is smaller than the pool",
            ));
        }
        match self.slots.get_mut(slot as usize) {
            Some(place @ None) => {
                *place = Some(Slot {
                    page,
                    limits,
                    seen: vec![None; self.buffers as usize].into_boxed_slice(),
                    cut_off: false,
                });
                Ok(())
            }
            Some(Some(_)) => Err(RingError::Spec("the reader slot is taken")),
            None => Err(RingError::Spec("no such reader slot")),
        }
    }

    /// Removes the reader in `slot`: it unsubscribed, or it is gone. Its marks go with its
    /// page. `pool_page` is the pool page, so the slot is clean for whoever gets it next.
    pub fn remove(&mut self, slot: u32, pool_page: &impl MemMut) {
        if let Some(place) = self.slots.get_mut(slot as usize) {
            *place = None;
            page::clear_cut_off(pool_page, self.buffers, slot);
        }
    }

    /// How many readers are in the table, cut off or not.
    #[must_use]
    pub fn readers(&self) -> usize {
        self.slots.iter().flatten().count()
    }

    /// Whether the reader in `slot` has been cut off.
    #[must_use]
    pub fn is_cut_off(&self, slot: u32) -> bool {
        matches!(self.slots.get(slot as usize), Some(Some(reader)) if reader.cut_off)
    }

    /// Applies every reader's limits at `now_ns`, a time on any steady clock of the
    /// publisher's (a frame time will do). Readers over a limit are cut off: their marks stop
    /// counting, and they are told through `pool_page`. Returns how many were cut off by this
    /// call. Memory reads only; call it before taking a buffer for each frame.
    pub fn enforce(&mut self, now_ns: u64, pool_page: &impl MemMut) -> u32 {
        let mut cut = 0;
        for (slot, reader) in self.slots.iter_mut().enumerate() {
            let Some(reader) = reader else { continue };
            if reader.cut_off {
                continue;
            }
            let mut held = 0_u32;
            let mut stuck = false;
            for (index, seen) in reader.seen.iter_mut().enumerate() {
                let buffer = BufferIndex(index as u32);
                if !marked(&reader.page, buffer) {
                    *seen = None;
                    continue;
                }
                held += 1;
                // The same hold is one seen on the same use of the buffer: a buffer that has
                // been freed and published again since is a new hold.
                let ticket = page::use_of(pool_page, buffer);
                match *seen {
                    Some((seen_ticket, since)) if seen_ticket == ticket => {
                        stuck |= reader.limits.max_hold.is_some_and(|max| {
                            u128::from(now_ns.saturating_sub(since)) > max.as_nanos()
                        });
                    }
                    _ => *seen = Some((ticket, now_ns)),
                }
            }
            if stuck || (reader.limits.max_held != 0 && held > reader.limits.max_held) {
                reader.cut_off = true;
                page::cut_off(pool_page, self.buffers, slot as u32);
                cut += 1;
            }
        }
        cut
    }
}

impl<M: Mem> Holders for HoldTable<M> {
    fn held(&self, buffer: BufferIndex) -> bool {
        buffer.0 < self.buffers
            && self
                .slots
                .iter()
                .flatten()
                .any(|reader| !reader.cut_off && marked(&reader.page, buffer))
    }
}

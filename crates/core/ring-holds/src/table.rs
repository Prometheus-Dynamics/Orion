//! The publisher's side: every reader's marks, and the limits on them.

use std::os::fd::OwnedFd;
use std::time::Duration;

use orion_ring::{Mem, MemMut, ReadMap, RingError};
use orion_ring_buffers::{BufferIndex, Holders, MAX_READERS, page};

use crate::marks::{MARKS_AT, count, marked};

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
    /// Since when, on the publisher's clock, its marks could not be counted at one instant.
    uncounted_since: Option<u64>,
    cut_off: bool,
}

/// How long a reader's marks may stay uncountable when it has no hold-time limit of its own.
const UNCOUNTED_FOR: Duration = Duration::from_secs(10);

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
        if page.words() < MARKS_AT + self.buffers as usize {
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
                    uncounted_since: None,
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
            // How many it holds has to be a count at one instant. Reading the marks one by one
            // while the reader lets one buffer go and takes another can see both, and a reader
            // must not be cut off for a count it never reached.
            let over = match count(&reader.page, self.buffers) {
                Some(held) => {
                    reader.uncounted_since = None;
                    reader.limits.max_held != 0 && held > reader.limits.max_held
                }
                None => {
                    // It was changing a mark every time it was looked at. A reader that is
                    // merely busy is counted on some later pass. One whose thread was stopped
                    // halfway through a change stays like this for as long as it is stopped,
                    // so this is judged by time, the same time a hold may last: a reader
                    // stopped for that long is stuck by its own limit.
                    let since = *reader.uncounted_since.get_or_insert(now_ns);
                    let allowed = reader.limits.max_hold.unwrap_or(UNCOUNTED_FOR);
                    u128::from(now_ns.saturating_sub(since)) > allowed.as_nanos()
                }
            };
            let mut stuck = false;
            for (index, seen) in reader.seen.iter_mut().enumerate() {
                let buffer = BufferIndex(index as u32);
                if !marked(&reader.page, buffer) {
                    *seen = None;
                    continue;
                }
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
            if stuck || over {
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

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::sync::atomic::Ordering;

    use orion_ring::Heap;

    use super::*;
    use crate::marks::VERSION_WORD;

    const BUFFERS: u32 = 4;
    const MS: u64 = 1_000_000;

    /// A reader's page that changes while the publisher is reading it, the way a reader on
    /// another core changes it: after `after` loads, `change` runs once.
    struct Racing<'a, F: Fn(&Heap)> {
        page: &'a Heap,
        after: u32,
        loads: Cell<u32>,
        change: F,
    }

    impl<F: Fn(&Heap)> Mem for Racing<'_, F> {
        fn words(&self) -> usize {
            self.page.words()
        }

        fn load(&self, index: usize, order: Ordering) -> u64 {
            self.loads.set(self.loads.get() + 1);
            if self.loads.get() == self.after {
                (self.change)(self.page);
            }
            self.page.load(index, order)
        }
    }

    /// Changes one mark the way `Marks` does: version odd, the mark, version even.
    fn set(page: &Heap, buffer: u32, mark: u64) {
        let version = page.load(VERSION_WORD, Ordering::SeqCst);
        page.store(VERSION_WORD, version + 1, Ordering::SeqCst);
        page.store(MARKS_AT + buffer as usize, mark, Ordering::SeqCst);
        page.store(VERSION_WORD, version + 2, Ordering::SeqCst);
    }

    fn pool_page() -> Heap {
        let page = Heap::new(page::bytes(BUFFERS) / 8);
        page::format(&page, 1, BUFFERS).expect("pool page");
        page
    }

    const TWO: Limits = Limits {
        max_held: 2,
        max_hold: None,
    };

    /// The race that was found on a busy machine: a reader holding two buffers lets one go and
    /// takes another while the publisher counts. Read one word at a time, that looks like
    /// three. Cutting the reader off for it let the publisher write over frames it held.
    #[test]
    fn a_reader_swapping_buffers_during_the_count_is_not_seen_over_its_limit() {
        // The swap can land between any two of the publisher's loads.
        for after in 1..=8 {
            let marks = Heap::new(MARKS_AT + BUFFERS as usize);
            set(&marks, 0, 1);
            set(&marks, 1, 1);
            let racing = Racing {
                page: &marks,
                after,
                loads: Cell::new(0),
                change: |page: &Heap| {
                    set(page, 0, 0);
                    set(page, 2, 1);
                },
            };
            let pool = pool_page();
            let mut table = HoldTable::new(BUFFERS);
            table.add_over(0, racing, TWO).expect("reader");
            assert_eq!(table.enforce(MS, &pool), 0, "swap after {after} loads");
            assert!(!table.is_cut_off(0));
            // What it holds now is honoured.
            assert!(table.held(BufferIndex(1)) && table.held(BufferIndex(2)));
            assert!(!table.held(BufferIndex(0)));
        }
    }

    #[test]
    fn a_reader_really_over_its_limit_is_cut_off() {
        let marks = Heap::new(MARKS_AT + BUFFERS as usize);
        for buffer in 0..3 {
            set(&marks, buffer, 1);
        }
        let pool = pool_page();
        let mut table = HoldTable::new(BUFFERS);
        table.add_over(0, &marks, TWO).expect("reader");
        assert_eq!(table.enforce(MS, &pool), 1);
        assert!(table.is_cut_off(0) && !table.held(BufferIndex(0)));
        assert!(page::is_cut_off(&pool, BUFFERS, 0));
    }

    /// A reader whose thread is stopped halfway through changing a mark leaves its version odd
    /// until it runs again. That is not a reader misbehaving, however many times the publisher
    /// looks; it is one that is stuck, and it is judged by the time a hold may last.
    #[test]
    fn a_reader_stopped_mid_change_is_judged_by_time_not_by_how_often_it_is_looked_at() {
        let marks = Heap::new(MARKS_AT + BUFFERS as usize);
        set(&marks, 0, 1);
        marks.store(VERSION_WORD, 3, Ordering::SeqCst);
        let pool = pool_page();
        let limits = Limits {
            max_held: 2,
            max_hold: Some(Duration::from_millis(500)),
        };
        let mut table = HoldTable::new(BUFFERS);
        table.add_over(0, &marks, limits).expect("reader");
        // Ten thousand looks in half that time: still honoured.
        for pass in 0..10_000_u64 {
            assert_eq!(table.enforce(pass * 25_000, &pool), 0, "pass {pass}");
        }
        assert!(table.held(BufferIndex(0)));
        // It runs again and finishes the change: counted, and the clock starts over.
        marks.store(VERSION_WORD, 4, Ordering::SeqCst);
        set(&marks, 0, 0);
        assert_eq!(table.enforce(260 * MS, &pool), 0);
        marks.store(VERSION_WORD, 7, Ordering::SeqCst);
        assert_eq!(table.enforce(700 * MS, &pool), 0);
        // Stopped for longer than a hold may last, it is cut off like any stuck reader.
        assert_eq!(table.enforce(1201 * MS, &pool), 1);
        assert!(table.is_cut_off(0));

        // With no hold-time limit of its own, the bound is ten seconds.
        let mut table = HoldTable::new(BUFFERS);
        table.add_over(0, &marks, TWO).expect("reader");
        assert_eq!(table.enforce(0, &pool), 0);
        assert_eq!(table.enforce(9_999 * MS, &pool), 0);
        assert_eq!(table.enforce(10_001 * MS, &pool), 1);
    }
}

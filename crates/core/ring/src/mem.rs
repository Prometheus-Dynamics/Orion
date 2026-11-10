//! The memory a ring lives in: an array of 64-bit words, every access atomic.
//!
//! Atomic words are what make a reader racing the writer defined behaviour instead of a data
//! race. The orderings carry the algorithm; see `docs/ring.md`, "Why a reader never sees a torn
//! sample".

use std::sync::atomic::{AtomicU64, Ordering};

/// Readable ring memory. An index past the end reads as zero: a reader never faults on a bad
/// index, it just fails the check that follows.
pub trait Mem {
    /// How many words there are.
    fn words(&self) -> usize;
    /// Reads word `index`.
    fn load(&self, index: usize, order: Ordering) -> u64;
    /// A memory fence. A model checker replaces this with its own.
    fn fence(order: Ordering) {
        std::sync::atomic::fence(order);
    }
}

/// Writable ring memory. A store past the end does nothing.
pub trait MemMut: Mem {
    /// Writes word `index`.
    fn store(&self, index: usize, value: u64, order: Ordering);
}

/// Ring memory on the heap, for tests and for a ring that never leaves its process.
#[derive(Debug)]
pub struct Heap(Box<[AtomicU64]>);

impl Heap {
    /// `words` zeroed words.
    #[must_use]
    pub fn new(words: usize) -> Self {
        Self((0..words).map(|_| AtomicU64::new(0)).collect())
    }
}

impl Mem for Heap {
    fn words(&self) -> usize {
        self.0.len()
    }

    fn load(&self, index: usize, order: Ordering) -> u64 {
        self.0.get(index).map_or(0, |word| word.load(order))
    }
}

impl MemMut for Heap {
    fn store(&self, index: usize, value: u64, order: Ordering) {
        if let Some(word) = self.0.get(index) {
            word.store(value, order);
        }
    }
}

impl<M: Mem> Mem for &M {
    fn words(&self) -> usize {
        (**self).words()
    }

    fn load(&self, index: usize, order: Ordering) -> u64 {
        (**self).load(index, order)
    }

    fn fence(order: Ordering) {
        M::fence(order);
    }
}

impl<M: MemMut> MemMut for &M {
    fn store(&self, index: usize, value: u64, order: Ordering) {
        (**self).store(index, value, order);
    }
}

impl<M: Mem> Mem for std::sync::Arc<M> {
    fn words(&self) -> usize {
        (**self).words()
    }

    fn load(&self, index: usize, order: Ordering) -> u64 {
        (**self).load(index, order)
    }

    fn fence(order: Ordering) {
        M::fence(order);
    }
}

impl<M: MemMut> MemMut for std::sync::Arc<M> {
    fn store(&self, index: usize, value: u64, order: Ordering) {
        (**self).store(index, value, order);
    }
}

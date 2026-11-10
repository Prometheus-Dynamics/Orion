//! A reader's marks.

use std::os::fd::OwnedFd;
use std::sync::Mutex;
use std::sync::atomic::Ordering;

use orion_ring::{Mem, MemMut, RingError, WriteMap, read_only, sealed_memfd};
use orion_ring_buffers::{BufferIndex, MAX_BUFFERS, Marker};

/// A reader's page of marks for one pool: word `b` is nonzero while the reader holds buffer
/// `b`. Only this reader writes it.
#[derive(Debug)]
pub struct Marks<M: MemMut = WriteMap> {
    page: M,
    slot: u32,
    max_held: u32,
    /// How many holds this reader has on each buffer, and in all. A buffer's word in the page
    /// is nonzero exactly while its count is. Frames can be dropped on other threads than the
    /// one reading, so the count and the page change together under this lock; it is never
    /// contended in the common case and then costs no system call.
    counts: Mutex<Counts>,
}

#[derive(Debug)]
struct Counts {
    each: Box<[u32]>,
    total: u32,
}

impl Marks<WriteMap> {
    /// A page of marks for a pool of `buffers` buffers. `slot` is the reader slot the publisher
    /// gave this reader; `max_held` is how many buffers it may hold at once, zero for no limit.
    ///
    /// # Errors
    ///
    /// If the page can't be made.
    pub fn create(buffers: u32, slot: u32, max_held: u32) -> Result<Self, RingError> {
        if buffers == 0 || buffers > MAX_BUFFERS {
            return Err(RingError::Spec("a pool holds 1 to 1024 buffers"));
        }
        let page = WriteMap::new(sealed_memfd(buffers as usize * 8)?)?;
        Ok(Self::over(page, buffers, slot, max_held))
    }

    /// The page, read-only, to send to the publisher.
    ///
    /// # Errors
    ///
    /// If the descriptor can't be reopened, which needs `/proc`.
    pub fn page_fd(&self) -> Result<OwnedFd, RingError> {
        Ok(read_only(self.page.fd())?)
    }
}

impl<M: MemMut> Marks<M> {
    /// Marks kept in `page`, which must be zeroed and hold at least `buffers` words.
    pub fn over(page: M, buffers: u32, slot: u32, max_held: u32) -> Self {
        let buffers = (buffers as usize).min(page.words());
        Self {
            page,
            slot,
            max_held,
            counts: Mutex::new(Counts {
                each: vec![0; buffers].into_boxed_slice(),
                total: 0,
            }),
        }
    }

    /// How many buffers this reader holds now.
    #[must_use]
    pub fn held(&self) -> u32 {
        self.counts().total
    }

    fn counts(&self) -> std::sync::MutexGuard<'_, Counts> {
        // A panic while holding the lock leaves the counts as they were, which is still right.
        self.counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl<M: MemMut> Marker for Marks<M> {
    fn mark(&self, buffer: BufferIndex) -> bool {
        let mut counts = self.counts();
        let Some(count) = counts.each.get(buffer.0 as usize).copied() else {
            return false;
        };
        if count == 0 {
            if self.max_held != 0 && counts.total >= self.max_held {
                return false;
            }
            counts.total += 1;
            // Visible to the publisher before the reader checks the ticket: the fence that
            // orders the two is in `page::still`, which the reader calls next.
            self.page.store(buffer.0 as usize, 1, Ordering::SeqCst);
        }
        counts.each[buffer.0 as usize] = count + 1;
        true
    }

    fn unmark(&self, buffer: BufferIndex) {
        let mut counts = self.counts();
        let Some(count) = counts.each.get(buffer.0 as usize).copied() else {
            return;
        };
        match count {
            0 => {}
            1 => {
                counts.each[buffer.0 as usize] = 0;
                counts.total -= 1;
                self.page.store(buffer.0 as usize, 0, Ordering::Release);
            }
            more => counts.each[buffer.0 as usize] = more - 1,
        }
    }

    fn slot(&self) -> Option<u32> {
        Some(self.slot)
    }
}

/// Whether the reader whose page is `page` marks `buffer`. The publisher's read.
pub(crate) fn marked(page: &impl Mem, buffer: BufferIndex) -> bool {
    page.load(buffer.0 as usize, Ordering::Acquire) != 0
}

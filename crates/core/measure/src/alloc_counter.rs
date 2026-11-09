use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    static ALLOCS: Cell<u64> = const { Cell::new(0) };
    static DEALLOCS: Cell<u64> = const { Cell::new(0) };
    static BYTES: Cell<u64> = const { Cell::new(0) };
}

/// What the current thread has allocated.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AllocCount {
    /// Calls to allocate, including reallocations.
    pub allocs: u64,
    /// Calls to free.
    pub deallocs: u64,
    /// Bytes requested.
    pub bytes: u64,
}

impl AllocCount {
    /// What happened between `earlier` and this reading.
    #[must_use]
    pub const fn since(self, earlier: Self) -> Self {
        Self {
            allocs: self.allocs.wrapping_sub(earlier.allocs),
            deallocs: self.deallocs.wrapping_sub(earlier.deallocs),
            bytes: self.bytes.wrapping_sub(earlier.bytes),
        }
    }
}

/// The current thread's counts. Each thread counts itself, so parallel tests don't disturb each
/// other.
#[must_use]
pub fn thread_counts() -> AllocCount {
    AllocCount {
        allocs: ALLOCS.try_with(Cell::get).unwrap_or(0),
        deallocs: DEALLOCS.try_with(Cell::get).unwrap_or(0),
        bytes: BYTES.try_with(Cell::get).unwrap_or(0),
    }
}

fn bump(cell: &'static std::thread::LocalKey<Cell<u64>>, by: u64) {
    // `try_with` because the allocator can run while a thread is being torn down.
    let _ = cell.try_with(|value| value.set(value.get().wrapping_add(by)));
}

/// A global allocator that counts, per thread, and passes everything to `A`.
///
/// Install it in a test or a bench:
///
/// ```ignore
/// #[global_allocator]
/// static GLOBAL: orion_measure::CountingAlloc = orion_measure::CountingAlloc::system();
/// ```
#[derive(Debug, Default)]
pub struct CountingAlloc<A = System>(A);

impl CountingAlloc<System> {
    /// Counts on top of the system allocator.
    #[must_use]
    pub const fn system() -> Self {
        Self(System)
    }
}

impl<A> CountingAlloc<A> {
    /// Counts on top of `inner`.
    pub const fn new(inner: A) -> Self {
        Self(inner)
    }
}

// SAFETY: every call goes straight to `A` with the arguments it was given, so `A`'s guarantees
// carry over. The counters touch only const-initialized thread locals, which never allocate.
unsafe impl<A: GlobalAlloc> GlobalAlloc for CountingAlloc<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        bump(&ALLOCS, 1);
        bump(&BYTES, layout.size() as u64);
        // SAFETY: the caller upholds `alloc`'s contract, which is `A::alloc`'s.
        unsafe { self.0.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        bump(&DEALLOCS, 1);
        // SAFETY: the caller upholds `dealloc`'s contract, which is `A::dealloc`'s.
        unsafe { self.0.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        bump(&ALLOCS, 1);
        bump(&BYTES, layout.size() as u64);
        // SAFETY: the caller upholds `alloc_zeroed`'s contract, which is `A::alloc_zeroed`'s.
        unsafe { self.0.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        bump(&ALLOCS, 1);
        bump(&BYTES, new_size as u64);
        // SAFETY: the caller upholds `realloc`'s contract, which is `A::realloc`'s.
        unsafe { self.0.realloc(ptr, layout, new_size) }
    }
}

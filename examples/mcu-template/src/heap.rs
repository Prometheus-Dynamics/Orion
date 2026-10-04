//! A tiny first-fit heap for bare-metal targets. **Replaceable**: any `GlobalAlloc` works
//! (`embedded-alloc`, your RTOS heap, a vendor `malloc` wrapper, ...).
//!
//! Free blocks form an address-ordered singly linked list stored inside the free memory itself,
//! so the heap has no overhead per allocation (`dealloc` receives the size from the layout).
//! Adjacent free blocks are merged on free. Every block size and address is a multiple of
//! [`UNIT`] (two machine words), so splitting never leaves unusable slivers.
//!
//! Locking is a spin flag: fine for a single main loop. If interrupt handlers allocate, mask
//! interrupts around allocation instead (or use a critical-section based allocator).

use core::alloc::{GlobalAlloc, Layout};
use core::cell::UnsafeCell;
use core::mem::{align_of, size_of};
use core::ptr::{self, null_mut};
use core::sync::atomic::{AtomicBool, Ordering};

/// Allocation granularity and minimum block size.
pub const UNIT: usize = 2 * size_of::<usize>();

#[repr(C)]
struct FreeBlock {
    size: usize,
    next: *mut FreeBlock,
}

#[repr(C, align(16))]
struct Arena<const N: usize>([u8; N]);

/// A first-fit heap over an `N`-byte static arena.
pub struct FreeListHeap<const N: usize> {
    arena: UnsafeCell<Arena<N>>,
    head: UnsafeCell<*mut FreeBlock>,
    initialized: UnsafeCell<bool>,
    locked: AtomicBool,
}

// SAFETY: all access to the cells happens while `locked` is held.
unsafe impl<const N: usize> Sync for FreeListHeap<N> {}

impl<const N: usize> Default for FreeListHeap<N> {
    fn default() -> Self {
        Self::new()
    }
}

const fn round_up(value: usize, align: usize) -> Option<usize> {
    match value.checked_add(align - 1) {
        Some(sum) => Some(sum & !(align - 1)),
        None => None,
    }
}

/// Size and alignment actually used for `layout`.
fn adjust(layout: Layout) -> Option<(usize, usize)> {
    let align = layout.align().max(UNIT);
    let size = round_up(layout.size().max(UNIT), UNIT)?;
    Some((size, align))
}

impl<const N: usize> FreeListHeap<N> {
    /// An empty heap; the arena is set up on the first allocation.
    pub const fn new() -> Self {
        Self {
            arena: UnsafeCell::new(Arena([0; N])),
            head: UnsafeCell::new(null_mut()),
            initialized: UnsafeCell::new(false),
            locked: AtomicBool::new(false),
        }
    }

    fn lock(&self) {
        while self
            .locked
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            core::hint::spin_loop();
        }
    }

    fn unlock(&self) {
        self.locked.store(false, Ordering::Release);
    }

    /// Bytes currently free (walks the list).
    pub fn free_bytes(&self) -> usize {
        self.lock();
        // SAFETY: the lock is held.
        let total = unsafe {
            self.init();
            let mut total = 0usize;
            let mut block = *self.head.get();
            while !block.is_null() {
                total += (*block).size;
                block = (*block).next;
            }
            total
        };
        self.unlock();
        total
    }

    /// # Safety
    /// The lock must be held.
    unsafe fn init(&self) {
        // SAFETY: the caller holds the lock, so we have exclusive access to the cells.
        unsafe {
            if *self.initialized.get() {
                return;
            }
            *self.initialized.get() = true;
            let start = self.arena.get().cast::<u8>();
            let usable = N & !(UNIT - 1);
            if usable >= UNIT && align_of::<Arena<N>>() >= UNIT {
                let block = start.cast::<FreeBlock>();
                block.write(FreeBlock {
                    size: usable,
                    next: null_mut(),
                });
                *self.head.get() = block;
            }
        }
    }

    /// # Safety
    /// The lock must be held.
    unsafe fn alloc_locked(&self, layout: Layout) -> *mut u8 {
        let Some((size, align)) = adjust(layout) else {
            return null_mut();
        };
        // SAFETY: the lock is held; every list node lies inside the arena and is a valid,
        // UNIT-aligned FreeBlock written by `init`/`dealloc_locked`/this function.
        unsafe {
            let mut link: *mut *mut FreeBlock = self.head.get();
            while !(*link).is_null() {
                let block = *link;
                let block_start = block as usize;
                let block_size = (*block).size;
                let block_end = block_start + block_size;
                let Some(start) = round_up(block_start, align) else {
                    return null_mut();
                };
                let fits = start.checked_add(size).is_some_and(|end| end <= block_end);
                if fits {
                    let end = start + size;
                    let next = (*block).next;
                    // Tail remainder becomes a free block (or nothing).
                    let mut rest = next;
                    if block_end > end {
                        let tail = block
                            .cast::<u8>()
                            .add(end - block_start)
                            .cast::<FreeBlock>();
                        tail.write(FreeBlock {
                            size: block_end - end,
                            next,
                        });
                        rest = tail;
                    }
                    if start > block_start {
                        // Front remainder (alignment gap) stays in the list in place.
                        (*block).size = start - block_start;
                        (*block).next = rest;
                    } else {
                        *link = rest;
                    }
                    return block.cast::<u8>().add(start - block_start);
                }
                link = ptr::addr_of_mut!((*block).next);
            }
            null_mut()
        }
    }

    /// # Safety
    /// The lock must be held; `ptr`/`layout` must come from a previous `alloc_locked`.
    unsafe fn dealloc_locked(&self, ptr: *mut u8, layout: Layout) {
        let Some((size, _)) = adjust(layout) else {
            return;
        };
        let start = ptr as usize;
        // SAFETY: as in `alloc_locked`; the freed range was handed out by this heap, lies inside
        // the arena, and is UNIT-aligned, so a FreeBlock fits at its start.
        unsafe {
            // Find the insertion point: `prev` is the last free block before `start`.
            let mut prev: *mut FreeBlock = null_mut();
            let mut next = *self.head.get();
            while !next.is_null() && (next as usize) < start {
                prev = next;
                next = (*next).next;
            }
            let block = ptr.cast::<FreeBlock>();
            block.write(FreeBlock { size, next });
            // Merge with the following block.
            if !next.is_null() && start + size == next as usize {
                (*block).size += (*next).size;
                (*block).next = (*next).next;
            }
            // Link from (and merge with) the preceding block.
            if prev.is_null() {
                *self.head.get() = block;
            } else if prev as usize + (*prev).size == start {
                (*prev).size += (*block).size;
                (*prev).next = (*block).next;
            } else {
                (*prev).next = block;
            }
        }
    }
}

// SAFETY: blocks are handed out at most once until freed, are aligned to at least
// `layout.align()`, and are at least `layout.size()` bytes inside the arena.
unsafe impl<const N: usize> GlobalAlloc for FreeListHeap<N> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        self.lock();
        // SAFETY: the lock is held.
        let ptr = unsafe {
            self.init();
            self.alloc_locked(layout)
        };
        self.unlock();
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.lock();
        // SAFETY: the lock is held and the caller guarantees `ptr`/`layout` came from `alloc`.
        unsafe { self.dealloc_locked(ptr, layout) };
        self.unlock();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocates_frees_and_coalesces() {
        let heap: FreeListHeap<1024> = FreeListHeap::new();
        let total = heap.free_bytes();
        assert_eq!(total, 1024);
        let a = Layout::from_size_align(10, 1).unwrap();
        let b = Layout::from_size_align(100, 8).unwrap();
        let c = Layout::from_size_align(64, 64).unwrap();
        unsafe {
            let pa = heap.alloc(a);
            let pb = heap.alloc(b);
            let pc = heap.alloc(c);
            assert!(!pa.is_null() && !pb.is_null() && !pc.is_null());
            assert_eq!(pc as usize % 64, 0);
            pa.write_bytes(0xAA, 10);
            pb.write_bytes(0xBB, 100);
            pc.write_bytes(0xCC, 64);
            assert!(heap.free_bytes() < total);
            heap.dealloc(pb, b);
            heap.dealloc(pa, a);
            heap.dealloc(pc, c);
        }
        assert_eq!(heap.free_bytes(), total);
        // After coalescing, one allocation can take the whole arena again.
        let all = Layout::from_size_align(1024, 16).unwrap();
        unsafe {
            let p = heap.alloc(all);
            assert!(!p.is_null());
            assert!(heap.alloc(a).is_null(), "exhausted");
            heap.dealloc(p, all);
        }
    }

    #[test]
    fn survives_churn() {
        let heap: FreeListHeap<4096> = FreeListHeap::new();
        let mut live: [(usize, *mut u8); 16] = [(0, null_mut()); 16];
        let mut seed = 0x1234_5678u32;
        for _ in 0..10_000 {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            let slot = (seed % 16) as usize;
            let (size, ptr) = live[slot];
            unsafe {
                if ptr.is_null() {
                    let size = 1 + (seed >> 8) as usize % 200;
                    let ptr = heap.alloc(Layout::from_size_align(size, 4).unwrap());
                    if !ptr.is_null() {
                        ptr.write_bytes(slot as u8, size);
                        live[slot] = (size, ptr);
                    }
                } else {
                    assert!((0..size).all(|i| *ptr.add(i) == slot as u8), "corrupted");
                    heap.dealloc(ptr, Layout::from_size_align(size, 4).unwrap());
                    live[slot] = (0, null_mut());
                }
            }
        }
        for (size, ptr) in live {
            if !ptr.is_null() {
                unsafe { heap.dealloc(ptr, Layout::from_size_align(size, 4).unwrap()) };
            }
        }
        assert_eq!(heap.free_bytes(), 4096);
    }
}

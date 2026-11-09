//! The gate for "this path allocates nothing".
//!
//! Warm a path up first, so caches and lazily built state are out of the way, then count:
//!
//! ```ignore
//! orion_measure::harness::assert_steady_no_allocs("publish", 16, 4096, || writer.publish(..));
//! ```

use crate::alloc_counter::{AllocCount, thread_counts};

/// Runs `f` and reports what it allocated on this thread.
pub fn measure<R>(f: impl FnOnce() -> R) -> (R, AllocCount) {
    let before = thread_counts();
    let result = f();
    (result, thread_counts().since(before))
}

/// Whether the counting allocator is the global allocator. Without it every count is zero, and a
/// gate would pass for the wrong reason.
#[must_use]
pub fn installed() -> bool {
    let (_, count) = measure(|| drop(std::hint::black_box(Box::new(0_u64))));
    count.allocs > 0
}

/// Runs `f` `warmup` times, then `iterations` more times, and returns what the second part
/// allocated.
///
/// # Panics
///
/// If the counting allocator isn't installed.
pub fn steady_allocs(warmup: usize, iterations: usize, mut f: impl FnMut()) -> AllocCount {
    assert!(
        installed(),
        "orion-measure: install CountingAlloc as #[global_allocator] before gating allocations"
    );
    for _ in 0..warmup {
        f();
    }
    let (_, count) = measure(|| {
        for _ in 0..iterations {
            f();
        }
    });
    count
}

/// The gate. Panics, naming the path, if `f` allocates once it is warmed up.
///
/// # Panics
///
/// If `f` allocated, or the counting allocator isn't installed.
pub fn assert_steady_no_allocs(label: &str, warmup: usize, iterations: usize, f: impl FnMut()) {
    let count = steady_allocs(warmup, iterations, f);
    assert!(
        count.allocs == 0,
        "{label}: {} allocations ({} bytes) in {iterations} iterations after warm-up",
        count.allocs,
        count.bytes
    );
}

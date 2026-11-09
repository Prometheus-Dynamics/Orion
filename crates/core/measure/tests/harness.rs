#![cfg(all(feature = "alloc-counter", feature = "costs"))]

use std::hint::black_box;

use orion_measure::harness::{assert_steady_no_allocs, installed, measure, steady_allocs};
use orion_measure::{Costs, CountingAlloc};

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc::system();

#[test]
fn it_knows_it_is_installed() {
    assert!(installed());
}

#[test]
fn a_path_that_allocates_nothing_passes() {
    let mut total = 0_u64;
    assert_steady_no_allocs("adding", 4, 1000, || total = black_box(total + 1));
    assert_eq!(total, 1004);
}

#[test]
#[should_panic(expected = "leaky path: 10 allocations")]
fn a_deliberate_allocation_is_caught() {
    assert_steady_no_allocs("leaky path", 4, 10, || drop(black_box(vec![1_u8; 32])));
}

#[test]
fn warm_up_is_not_counted() {
    let mut cache: Option<Vec<u8>> = None;
    assert_steady_no_allocs("lazy", 1, 100, || {
        black_box(cache.get_or_insert_with(|| vec![0; 64]));
    });
}

#[test]
fn counts_and_bytes_are_exact() {
    let (_, count) = measure(|| drop(black_box(Vec::<u8>::with_capacity(100))));
    assert_eq!((count.allocs, count.deallocs, count.bytes), (1, 1, 100));
    let steady = steady_allocs(0, 3, || drop(black_box(Box::new(1_u32))));
    assert_eq!((steady.allocs, steady.bytes), (3, 12));
}

#[test]
fn other_threads_do_not_count() {
    let (_, count) = measure(|| {
        std::thread::scope(|scope| {
            scope.spawn(|| {
                for _ in 0..100 {
                    drop(black_box(vec![0_u8; 8]));
                }
            });
        });
    });
    assert!(count.allocs < 100, "{count:?}");
}

#[test]
fn costs_add_up() {
    let costs = Costs::default();
    costs.message(100, 2_000);
    costs.message(50, 1_000);
    costs.wakeup();
    let snapshot = costs.snapshot();
    assert_eq!(
        (snapshot.messages, snapshot.bytes, snapshot.wakeups),
        (2, 150, 1)
    );
    assert_eq!(snapshot.nanos_per_message(), 1_500);
}

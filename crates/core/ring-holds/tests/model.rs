//! A mark against a reuse, under `loom`: in every interleaving, a buffer the reader holds is one
//! the publisher leaves alone.
//!
//! The reader runs the real `Marks` and `page::still`; the publisher runs the real
//! `page::retire` and `HoldTable`.

use std::sync::atomic::Ordering;

use loom::sync::Arc;
use loom::sync::atomic::AtomicU64;
use orion_ring::{Mem, MemMut};
use orion_ring_buffers::{BufferIndex, Holders, Marker, page};
use orion_ring_holds::{HoldTable, Limits, Marks};

#[derive(Clone)]
struct Model(Arc<Vec<AtomicU64>>);

impl Model {
    fn new(words: usize) -> Self {
        Self(Arc::new((0..words).map(|_| AtomicU64::new(0)).collect()))
    }
}

impl Mem for Model {
    fn words(&self) -> usize {
        self.0.len()
    }

    fn load(&self, index: usize, order: Ordering) -> u64 {
        self.0.get(index).map_or(0, |word| word.load(order))
    }

    fn fence(order: Ordering) {
        loom::sync::atomic::fence(order);
    }
}

impl MemMut for Model {
    fn store(&self, index: usize, value: u64, order: Ordering) {
        if let Some(word) = self.0.get(index) {
            word.store(value, order);
        }
    }
}

const BUFFER: BufferIndex = BufferIndex(0);

/// One frame is published in the buffer. The reader tries to hold it while the publisher tries
/// to take the buffer back. `mark_first` is the real order on the reader's side.
fn race(mark_first: bool) {
    let pool = Model::new(page::bytes(1) / 8);
    page::format(&pool, 1, 1).expect("pool page");
    let marks_page = Model::new(1);
    let published_with = page::ticket(&pool, BUFFER) as u32;

    let mut table = HoldTable::new(1);
    table
        .add_over(0, marks_page.clone(), Limits::default())
        .expect("reader");
    let publisher = {
        let pool = pool.clone();
        loom::thread::spawn(move || {
            page::retire(&pool, BUFFER);
            // Free to write over, or not.
            let free = !table.held(BUFFER);
            if free {
                page::release(&pool, BUFFER);
            }
            free
        })
    };
    let marks = Marks::over(marks_page, 1, 0, 0);
    let holds = if mark_first {
        marks.mark(BUFFER) && page::still(&pool, BUFFER, published_with)
    } else {
        // A reader that looks at the ticket and only then marks.
        page::still(&pool, BUFFER, published_with) && marks.mark(BUFFER)
    };
    let free = publisher.join().expect("publisher");
    assert!(
        !(holds && free),
        "the publisher reused a buffer the reader holds"
    );
}

#[test]
fn a_held_buffer_is_never_reused() {
    loom::model(|| race(true));
}

/// The model has teeth: checking the ticket before marking leaves a gap the publisher fits in.
#[test]
#[should_panic(expected = "reused a buffer the reader holds")]
fn checking_before_marking_is_caught() {
    loom::model(|| race(false));
}

/// And a reader that lets go and holds again, against a publisher that tries twice.
#[test]
fn letting_go_frees_the_buffer_and_never_before() {
    loom::model(|| {
        let pool = Model::new(page::bytes(1) / 8);
        page::format(&pool, 1, 1).expect("pool page");
        let marks_page = Model::new(1);
        let mut table = HoldTable::new(1);
        table
            .add_over(0, marks_page.clone(), Limits::default())
            .expect("reader");
        let marks = Marks::over(marks_page, 1, 0, 0);
        assert!(marks.mark(BUFFER) && page::still(&pool, BUFFER, 0));
        let publisher = {
            let pool = pool.clone();
            loom::thread::spawn(move || {
                // Two tries at taking the buffer back, as on two frames.
                page::retire(&pool, BUFFER);
                let first = !table.held(BUFFER);
                page::retire(&pool, BUFFER);
                (first, !table.held(BUFFER))
            })
        };
        // The reader held the frame from before the publisher started, then lets go.
        marks.unmark(BUFFER);
        let (first, second) = publisher.join().expect("publisher");
        // Once the reader has let go the buffer is free; it can never be free and then held.
        assert!(!first || second, "a buffer went from free back to held");
    });
}

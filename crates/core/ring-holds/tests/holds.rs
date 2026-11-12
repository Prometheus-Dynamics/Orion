//! Holds and limits with real pages: stuck, slow, dishonest and killed readers never starve the
//! others or the publisher.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use orion_measure::CountingAlloc;
use orion_measure::harness::assert_steady_no_allocs;
use orion_ring::{FieldId, Ring, SampleTime, Spec, WriteMap};
use orion_ring_buffers::{
    BufferIndex, BufferReader, BufferWriter, Dropped, Held, Marker, NextBatch, Pool,
};
use orion_ring_holds::{HoldTable, Limits, Marks};

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc::system();

const FRAME: usize = 4096;
const MS: u64 = 1_000_000;

type Reader = BufferReader<orion_ring::ReadMap, Marks>;

struct Topic {
    ring: Ring,
    writer: BufferWriter,
    table: HoldTable,
    buffers: u32,
}

impl Topic {
    fn new(buffers: u32) -> Self {
        let ring = Ring::create(&Spec {
            fields: 1,
            history: 16,
            max_payload: 64,
            expiry_ms: 0,
        })
        .expect("ring");
        let pool = Pool::in_memory(1, buffers, FRAME).expect("pool");
        let writer = BufferWriter::new(ring.writer().expect("writer"), FieldId(0), pool);
        Self {
            ring,
            writer,
            table: HoldTable::new(buffers),
            buffers,
        }
    }

    fn reader(&mut self, limits: Limits) -> (u32, Reader) {
        let slot = self.table.free_slot().expect("a free slot");
        let marks = Marks::create(self.buffers, slot, limits.max_held).expect("marks");
        self.table
            .add(slot, marks.page_fd().expect("page"), limits)
            .expect("add reader");
        let view = self.writer.pool().view().expect("view");
        let reader =
            BufferReader::new(self.ring.reader().expect("reader"), FieldId(0), view, marks);
        (slot, reader)
    }

    /// One frame at `now_ms`, every byte `fill`. `Err` if it had to be dropped.
    fn send(&mut self, fill: u8, now_ms: u64) -> Result<BufferIndex, Dropped> {
        self.table.enforce(now_ms * MS, self.writer.pool().page());
        let buffer = self.writer.take(&self.table)?;
        self.writer.buffer_mut(buffer).expect("free").fill(fill);
        self.writer
            .publish(&[buffer], SampleTime::from_nanos(now_ms * MS), &[fill])
            .expect("publish");
        Ok(buffer)
    }
}

/// Reads until a frame comes, and holds it.
fn hold(reader: &mut Reader) -> (u8, Held<Marks>) {
    for _ in 0..100 {
        if let NextBatch::Batch(frame) = reader.next() {
            return (frame.meta[0], frame.held);
        }
    }
    panic!("no frame");
}

fn whole(held: &Held<Marks>, fill: u8) -> bool {
    held.bytes(0)
        .expect("bytes")
        .iter()
        .all(|byte| *byte == fill)
}

#[test]
fn a_held_frame_is_never_written_over_and_comes_back_when_let_go() {
    let mut topic = Topic::new(3);
    let (_, mut reader) = topic.reader(Limits::default());
    let buffer = topic.send(1, 0).expect("frame");
    let (fill, held) = hold(&mut reader);
    assert_eq!((fill, held.buffer(0)), (1, Some(buffer)));

    // Many more frames go through the other two buffers. The held one is left alone.
    for round in 2..50_u8 {
        assert_ne!(topic.send(round, u64::from(round)).expect("frame"), buffer);
        assert!(whole(&held, 1));
    }
    drop(held);
    let reused = (50..53_u8).any(|round| topic.send(round, 60).expect("frame") == buffer);
    assert!(reused, "the buffer comes back once it is let go");
}

#[test]
fn a_slow_reader_within_its_limits_does_not_cost_the_others_a_frame() {
    let mut topic = Topic::new(4);
    let (_, mut fast) = topic.reader(Limits::default());
    let (_, mut slow) = topic.reader(Limits {
        max_held: 2,
        max_hold: None,
    });
    // The slow reader keeps the two oldest frames it took for the whole run.
    let mut kept = Vec::new();
    let mut seen = 0;
    for round in 0..200_u8 {
        topic
            .send(round, u64::from(round))
            .expect("no frame is dropped");
        let (fill, frame) = hold(&mut fast);
        assert_eq!(fill, round);
        assert!(whole(&frame, round));
        seen += 1;
        if kept.len() < 2 {
            kept.push(hold(&mut slow));
        }
    }
    assert_eq!(seen, 200);
    for (fill, held) in &kept {
        assert!(
            whole(held, *fill),
            "what the slow reader holds is still its frame"
        );
    }
    // At its limit it is told so, and gets frames again after letting go.
    topic.send(250, 300).expect("frame");
    assert!(matches!(
        slow.next(),
        NextBatch::Skipped(_) | NextBatch::Limited
    ));
    while !matches!(slow.next(), NextBatch::Empty) {}
    kept.clear();
    topic.send(251, 301).expect("frame");
    assert_eq!(hold(&mut slow).0, 251);
}

#[test]
fn a_stuck_reader_is_cut_off_and_its_buffers_come_back() {
    let mut topic = Topic::new(3);
    let (good_slot, mut good) = topic.reader(Limits::default());
    let (stuck_slot, mut stuck) = topic.reader(Limits {
        max_held: 0,
        max_hold: Some(Duration::from_millis(100)),
    });
    // It takes two frames and never lets go.
    topic.send(1, 0).expect("frame");
    let first = hold(&mut stuck);
    topic.send(2, 10).expect("frame");
    let second = hold(&mut stuck);

    // Within the hold time its buffers are honoured: with the newest frame kept too, there is
    // nowhere to put a frame, so frames are dropped. The publisher does not wait.
    assert_eq!(topic.send(3, 20), Ok(BufferIndex(2)));
    assert_eq!(topic.send(4, 90), Err(Dropped));
    assert!(whole(&first.1, 1) && whole(&second.1, 2));
    assert!(!topic.table.is_cut_off(stuck_slot));

    // Past it, the reader is cut off and frames flow again through its buffers.
    for round in 5..20_u8 {
        topic
            .send(round, 200 + u64::from(round))
            .expect("frames flow again");
    }
    assert!(topic.table.is_cut_off(stuck_slot));
    assert!(!topic.table.is_cut_off(good_slot));
    assert!(matches!(stuck.next(), NextBatch::CutOff));
    assert!(matches!(stuck.next(), NextBatch::CutOff));
    // The reader that behaved never noticed.
    let mut last = 0;
    loop {
        match good.next() {
            NextBatch::Batch(frame) => last = frame.meta[0],
            NextBatch::Empty => break,
            // It was not reading while the others ran, so the oldest frames are gone.
            NextBatch::Skipped(_) => {}
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(last, 19);
}

#[test]
fn a_reader_that_marks_more_than_its_limit_is_cut_off() {
    let mut topic = Topic::new(4);
    let (_, mut good) = topic.reader(Limits::default());
    // A dishonest reader: the publisher was told 1, its own marks are unlimited.
    let slot = topic.table.free_slot().expect("slot");
    let marks = Marks::create(4, slot, 0).expect("marks");
    let limits = Limits {
        max_held: 1,
        max_hold: None,
    };
    topic
        .table
        .add(slot, marks.page_fd().expect("page"), limits)
        .expect("add");
    for buffer in 0..3 {
        assert!(marks.mark(BufferIndex(buffer)));
    }
    assert_eq!(marks.held(), 3);
    // It pins three of four buffers, for exactly one frame.
    for round in 0..20_u8 {
        topic.send(round, u64::from(round)).expect("frames flow");
        assert_eq!(hold(&mut good).0, round);
    }
    assert!(topic.table.is_cut_off(slot));
}

#[test]
fn a_reader_that_is_gone_frees_its_buffers_when_its_slot_is_removed() {
    let mut topic = Topic::new(2);
    let (slot, mut reader) = topic.reader(Limits::default());
    topic.send(1, 0).expect("frame");
    let held = hold(&mut reader);
    topic.send(2, 1).expect("frame");
    // A process that dies does not run destructors: its marks stay in its page.
    std::mem::forget(held);
    std::mem::forget(reader);
    assert_eq!(topic.send(3, 2), Err(Dropped));
    assert_eq!(topic.table.readers(), 1);
    // The comm core sees the connection close and removes the reader.
    topic.table.remove(slot, topic.writer.pool().page());
    assert_eq!(topic.table.readers(), 0);
    topic.send(4, 3).expect("its buffer is back");
    // The slot is clean for the next reader, even if the last one had been cut off.
    let (again, mut next) = topic.reader(Limits::default());
    assert_eq!(again, slot);
    topic.send(5, 4).expect("frame");
    assert_eq!(hold(&mut next).0, 5);
}

#[test]
fn slots_and_pages_are_checked() {
    let mut table: HoldTable = HoldTable::new(8);
    let marks = Marks::create(8, 0, 0).expect("marks");
    table
        .add(0, marks.page_fd().expect("page"), Limits::default())
        .expect("add");
    assert!(
        table
            .add(0, marks.page_fd().expect("page"), Limits::default())
            .is_err()
    );
    assert!(
        table
            .add(64, marks.page_fd().expect("page"), Limits::default())
            .is_err()
    );
    assert_eq!(table.free_slot(), Some(1));
    // A page too small for the pool, and marks for buffers that aren't there.
    let mut large: HoldTable = HoldTable::new(1024);
    assert!(
        large
            .add(0, marks.page_fd().expect("page"), Limits::default())
            .is_err()
    );
    assert!(!marks.mark(BufferIndex(8)));
    marks.unmark(BufferIndex(8));
    assert!(Marks::create(0, 0, 0).is_err());
    // A buffer marked twice is held until it is let go twice.
    assert!(marks.mark(BufferIndex(3)) && marks.mark(BufferIndex(3)));
    marks.unmark(BufferIndex(3));
    assert_eq!(marks.held(), 1);
    marks.unmark(BufferIndex(3));
    marks.unmark(BufferIndex(3));
    assert_eq!(marks.held(), 0);
    // And a reader cannot write the page the publisher was given.
    assert!(WriteMap::new(marks.page_fd().expect("page")).is_err());
}

#[test]
fn frames_held_across_threads_are_never_torn() {
    // The publisher runs flat out through a small pool. Readers hold each frame for a moment,
    // on another thread than the one that read it, and check every byte is still the frame's.
    // Twenty rounds, because the failures this is here for showed about once in ten.
    const FRAMES: u64 = 30_000;
    for round in 0..20 {
        let mut topic = Topic::new(4);
        let readers: Vec<Reader> = (0..2)
            .map(|_| {
                topic
                    .reader(Limits {
                        max_held: 2,
                        max_hold: None,
                    })
                    .1
            })
            .collect();
        let done = AtomicBool::new(false);
        let checked = AtomicU64::new(0);
        std::thread::scope(|scope| {
            for mut reader in readers {
                let (done, checked) = (&done, &checked);
                scope.spawn(move || {
                    let (send, receive) = std::sync::mpsc::sync_channel::<(u8, Held<Marks>)>(1);
                    let checker = scope.spawn(move || {
                        for (fill, held) in receive {
                            std::thread::yield_now();
                            assert!(whole(&held, fill), "a held frame changed");
                            checked.fetch_add(1, Ordering::Relaxed);
                        }
                    });
                    while !done.load(Ordering::Acquire) {
                        if let NextBatch::Batch(frame) = reader.next() {
                            let fill = frame.meta[0];
                            assert!(whole(&frame.held, fill), "a frame was torn when read");
                            send.send((fill, frame.held)).expect("checker");
                        }
                    }
                    drop(send);
                    checker.join().expect("checker");
                });
            }
            let mut dropped = 0_u64;
            for frame in 0..FRAMES {
                if topic.send(frame as u8, frame).is_err() {
                    dropped += 1;
                }
            }
            done.store(true, Ordering::Release);
            assert!(dropped < FRAMES, "every frame was dropped");
        });
        assert!(
            checked.into_inner() > 0,
            "round {round}: no frame was ever held and checked"
        );
        // No reader was cut off: nothing they did was over a limit.
        assert!(
            !topic.table.is_cut_off(0) && !topic.table.is_cut_off(1),
            "round {round}"
        );
    }
}

#[test]
fn holding_and_enforcing_allocate_nothing() {
    let mut topic = Topic::new(4);
    let (_, mut reader) = topic.reader(Limits {
        max_held: 2,
        max_hold: Some(Duration::from_secs(1)),
    });
    let mut kept: Option<Held<Marks>> = None;
    let mut round = 0_u64;
    let mut read = 0_u64;
    assert_steady_no_allocs("frames with holds", 16, 10_000, || {
        round += 1;
        topic.send(round as u8, round).expect("frame");
        if let NextBatch::Batch(frame) = reader.next() {
            read += u64::from(whole(&frame.held, round as u8));
            // Keep each frame until the next one, so a hold is always live.
            kept = Some(frame.held);
        }
    });
    assert_eq!(read, 10_016);
}

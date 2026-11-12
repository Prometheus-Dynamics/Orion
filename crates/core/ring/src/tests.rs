//! The ring on heap memory: what it promises, and what a hostile writer cannot do to a reader.

use std::sync::atomic::Ordering;

use crate::layout::{W_HEAD, format};
use crate::{
    FieldId, Flags, Heap, Mem, MemMut, Next, PublishError, Reader, SampleTime, Spec, Writer,
};

fn ring(fields: u32, history: u32, max_payload: u32) -> Heap {
    let spec = Spec {
        fields,
        history,
        max_payload,
        expiry_ms: 0,
    };
    let mem = Heap::new(crate::Geometry::for_spec(&spec).expect("shape").bytes() / 8);
    format(&mem, &spec).expect("format");
    mem
}

fn time(nanos: u64) -> SampleTime {
    SampleTime::from_nanos(nanos)
}

/// A payload of `len` bytes that says which sample it belongs to in every byte.
fn payload_of(seq: u64, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (seq as u8).wrapping_add(i as u8))
        .collect()
}

#[test]
fn samples_come_back_in_order_with_every_part() {
    let mem = ring(2, 8, 20);
    let mut writer = Writer::over(&mem).expect("writer");
    let mut reader = Reader::over(&mem).expect("reader");
    assert_eq!(reader.next(), Next::Empty);
    // Every payload length around the word boundary, on alternating fields.
    for seq in 0..12_u64 {
        let payload = payload_of(seq, seq as usize);
        let field = FieldId((seq % 2) as u16);
        let flags = Flags(seq as u8 * 3);
        assert_eq!(
            writer.publish(field, time(seq * 10), flags, &payload),
            Ok(seq)
        );
        match reader.next() {
            Next::Sample(sample) => {
                assert_eq!(
                    (sample.seq, sample.field, sample.flags),
                    (seq, field, flags)
                );
                assert_eq!(sample.time, time(seq * 10));
                assert_eq!(sample.payload, payload);
            }
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(reader.next(), Next::Empty);
    assert_eq!((reader.missed(), writer.published()), (0, 12));
}

#[test]
fn a_publish_outside_the_declaration_is_refused_and_writes_nothing() {
    let mem = ring(2, 2, 8);
    let mut writer = Writer::over(&mem).expect("writer");
    let mut reader = Reader::over(&mem).expect("reader");
    assert_eq!(
        writer.publish(FieldId(2), time(0), Flags::NONE, b""),
        Err(PublishError::NoSuchField)
    );
    assert_eq!(
        writer.publish(FieldId(0), time(0), Flags::NONE, &[0; 9]),
        Err(PublishError::TooLarge)
    );
    assert_eq!(reader.next(), Next::Empty);
    assert_eq!(
        writer.publish(FieldId(1), time(0), Flags::NONE, &[7; 8]),
        Ok(0)
    );
}

#[test]
fn a_reader_that_attaches_late_starts_at_new_samples_and_can_rewind() {
    let mem = ring(1, 4, 8);
    let mut writer = Writer::over(&mem).expect("writer");
    for seq in 0..3_u64 {
        writer
            .publish(FieldId(0), time(seq), Flags::NONE, &[seq as u8])
            .expect("publish");
    }
    let mut reader = Reader::over(&mem).expect("reader");
    assert_eq!(reader.next(), Next::Empty);
    reader.rewind();
    for seq in 0..3_u64 {
        assert!(matches!(reader.next(), Next::Sample(sample) if sample.seq == seq));
    }
    assert_eq!(reader.next(), Next::Empty);
}

#[test]
fn a_slow_reader_skips_ahead_and_counts_what_it_missed() {
    let mem = ring(1, 4, 8);
    let mut writer = Writer::over(&mem).expect("writer");
    let mut reader = Reader::over(&mem).expect("reader");
    for seq in 0..10_u64 {
        writer
            .publish(FieldId(0), time(seq), Flags::NONE, &[seq as u8])
            .expect("publish");
    }
    // Four are kept, so six are gone.
    assert_eq!(reader.next(), Next::Skipped(6));
    for seq in 6..10_u64 {
        match reader.next() {
            Next::Sample(sample) => {
                assert_eq!((sample.seq, sample.payload), (seq, &[seq as u8][..]))
            }
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(reader.next(), Next::Empty);
    assert_eq!(reader.missed(), 6);
}

#[test]
fn a_busy_field_overwrites_only_its_own_lane() {
    // Field 0 is published once, then field 1 many times. Field 0's sample is still there.
    let mem = ring(2, 2, 8);
    let mut writer = Writer::over(&mem).expect("writer");
    let mut reader = Reader::over(&mem).expect("reader");
    writer
        .publish(FieldId(0), time(1), Flags::NONE, b"slow")
        .expect("publish");
    for seq in 0..3_u64 {
        writer
            .publish(FieldId(1), time(seq), Flags::NONE, &[seq as u8])
            .expect("publish");
    }
    // In order: field 0's sample, then the one overwritten sample of field 1 is counted.
    assert!(matches!(reader.next(), Next::Sample(sample) if sample.payload == b"slow"));
    assert_eq!(reader.next(), Next::Skipped(1));
    assert!(matches!(reader.next(), Next::Sample(sample) if sample.payload == [1]));
    assert!(matches!(reader.next(), Next::Sample(sample) if sample.payload == [2]));
    assert_eq!(reader.next(), Next::Empty);
    let latest = reader.latest(FieldId(0)).expect("latest");
    assert_eq!((latest.seq, latest.payload), (0, &b"slow"[..]));
}

#[test]
fn latest_and_history_read_the_same_slots() {
    let mem = ring(3, 4, 8);
    let mut writer = Writer::over(&mem).expect("writer");
    let mut reader = Reader::over(&mem).expect("reader");
    assert_eq!(reader.latest(FieldId(1)), None);
    assert_eq!(reader.latest(FieldId(3)), None);
    for round in 0..6_u64 {
        writer
            .publish(FieldId(1), time(round), Flags::NONE, &[round as u8])
            .expect("publish");
    }
    assert_eq!(reader.lane_head(FieldId(1)), 6);
    assert_eq!(reader.lane_head(FieldId(0)), 0);
    assert_eq!(reader.lane_head(FieldId(9)), 0);
    let latest = reader.latest(FieldId(1)).expect("latest");
    assert_eq!((latest.time, latest.payload), (time(5), &[5][..]));
    // Samples 2 to 5 are kept; 0 and 1 were overwritten; 6 isn't published.
    for lane_seq in 2..6_u64 {
        let sample = reader.at(FieldId(1), lane_seq).expect("kept");
        assert_eq!(sample.payload, [lane_seq as u8]);
    }
    assert_eq!(reader.at(FieldId(1), 1), None);
    assert_eq!(reader.at(FieldId(1), 6), None);
    assert_eq!(reader.at(FieldId(7), 0), None);
    // The time alone, for the same samples and the same refusals.
    for lane_seq in 2..6_u64 {
        assert_eq!(reader.time_at(FieldId(1), lane_seq), Some(time(lane_seq)));
    }
    for (field, lane_seq) in [(1, 1), (1, 6), (7, 0), (0, 0)] {
        assert_eq!(reader.time_at(FieldId(field), lane_seq), None);
    }
}

#[test]
fn a_new_writer_carries_on_where_the_last_one_stopped() {
    let mem = ring(2, 4, 8);
    let mut reader = Reader::over(&mem).expect("reader");
    let mut first = Writer::over(&mem).expect("writer");
    first
        .publish(FieldId(0), time(1), Flags::NONE, b"a")
        .expect("publish");
    first
        .publish(FieldId(1), time(2), Flags::NONE, b"b")
        .expect("publish");
    let mut second = Writer::over(&mem).expect("writer");
    assert_eq!(second.published(), 2);
    assert_eq!(
        second.publish(FieldId(1), time(3), Flags::NONE, b"c"),
        Ok(2)
    );
    let seen: Vec<u8> = std::iter::from_fn(|| match reader.next() {
        Next::Sample(sample) => Some(sample.payload[0]),
        _ => None,
    })
    .collect();
    assert_eq!(seen, b"abc");
}

#[test]
fn a_sample_caught_mid_write_is_not_returned() {
    let mem = ring(1, 2, 16);
    let mut writer = Writer::over(&mem).expect("writer");
    let mut reader = Reader::over(&mem).expect("reader");
    writer
        .publish(FieldId(0), time(1), Flags::NONE, b"whole")
        .expect("publish");
    let at = reader.geometry().slot_at(0, 0);
    let stamp = mem.load(at, Ordering::Relaxed);
    // As the writer leaves it while rewriting the slot.
    mem.store(at, stamp | 1, Ordering::Relaxed);
    assert_eq!(reader.latest(FieldId(0)), None);
    assert_eq!(reader.next(), Next::Skipped(1));
    assert_eq!(reader.next(), Next::Empty);
    mem.store(at, stamp, Ordering::Relaxed);
    assert!(reader.latest(FieldId(0)).is_some());
}

/// Memory that changes a slot's stamp after a set number of loads, as a writer overwriting the
/// slot in the middle of a reader's copy would.
struct Overwritten<'a> {
    mem: &'a Heap,
    stamp_at: usize,
    loads_left: &'a std::cell::Cell<u32>,
}

impl Mem for Overwritten<'_> {
    fn words(&self) -> usize {
        self.mem.words()
    }

    fn load(&self, index: usize, order: Ordering) -> u64 {
        if self.loads_left.get() == 0 {
            self.mem.store(self.stamp_at, 9998, Ordering::Relaxed);
        }
        self.loads_left.set(self.loads_left.get().saturating_sub(1));
        self.mem.load(index, order)
    }
}

#[test]
fn a_sample_overwritten_during_the_copy_is_not_returned() {
    let mem = ring(1, 2, 64);
    let mut writer = Writer::over(&mem).expect("writer");
    writer
        .publish(FieldId(0), time(1), Flags::NONE, &[0xAA; 64])
        .expect("publish");
    let stamp_at = crate::Geometry::read(&mem).expect("shape").slot_at(0, 0);
    // Overwrite after every possible number of loads into the copy. The copy is the lane head,
    // the stamp, time, meta, lane sequence, eight payload words and the stamp again.
    for loads in 2..14 {
        mem.store(stamp_at, 2, Ordering::Relaxed);
        let loads_left = std::cell::Cell::new(u32::MAX);
        let racing = Overwritten {
            mem: &mem,
            stamp_at,
            loads_left: &loads_left,
        };
        let mut reader = Reader::over(racing).expect("reader");
        loads_left.set(loads);
        assert_eq!(reader.at(FieldId(0), 0), None, "after {loads} loads");
    }
    // Left alone for all fourteen loads, the same read succeeds.
    mem.store(stamp_at, 2, Ordering::Relaxed);
    let loads_left = std::cell::Cell::new(u32::MAX);
    let quiet = Overwritten {
        mem: &mem,
        stamp_at,
        loads_left: &loads_left,
    };
    let mut reader = Reader::over(quiet).expect("reader");
    loads_left.set(14);
    assert!(reader.at(FieldId(0), 0).is_some());
}

/// A small, fixed random sequence, so a failure can be run again.
struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

#[test]
fn a_hostile_writer_cannot_send_a_reader_out_of_bounds_or_into_a_loop() {
    let mut random = Random(0x9E37_79B9_7F4A_7C15);
    for round in 0..200 {
        let mem = ring(3, 4, 24);
        let mut writer = Writer::over(&mem).expect("writer");
        let mut reader = Reader::over(&mem).expect("reader");
        for seq in 0..6_u64 {
            writer
                .publish(FieldId((seq % 3) as u16), time(seq), Flags::NONE, &[1; 24])
                .expect("publish");
        }
        let words = mem.words();
        // Scribble over everything after the reader attached: header, lane heads, index, slots.
        // Half the rounds scribble wild values, half values near the real ones.
        for _ in 0..(random.next() % 64 + 1) {
            let index = (random.next() as usize) % words;
            let value = if round % 2 == 0 {
                random.next()
            } else {
                mem.load(index, Ordering::Relaxed) ^ (1 << (random.next() % 20))
            };
            mem.store(index, value, Ordering::Relaxed);
        }
        // Every call returns, and whatever it returns is inside what the reader validated.
        for _ in 0..300 {
            if let Next::Sample(sample) = reader.next() {
                assert!(sample.payload.len() <= 24);
                assert!(sample.field.0 < 3);
            }
        }
        for field in 0..5_u16 {
            if let Some(sample) = reader.latest(FieldId(field)) {
                assert!(sample.payload.len() <= 24 && field < 3);
            }
            let head = reader.lane_head(FieldId(field));
            let _ = reader.at(FieldId(field), head.wrapping_sub(1));
            let _ = reader.at(FieldId(field), random.next());
        }
    }
}

#[test]
fn a_head_that_runs_wild_costs_one_skip_not_a_long_walk() {
    let mem = ring(1, 4, 8);
    let mut reader = Reader::over(&mem).expect("reader");
    mem.store(W_HEAD, u64::MAX, Ordering::Relaxed);
    assert_eq!(reader.next(), Next::Skipped(u64::MAX - 4));
    // Four entries are in the window and none is real.
    assert_eq!(reader.next(), Next::Skipped(4));
    assert_eq!(reader.next(), Next::Empty);
    // And a head that goes backwards is followed, not chased.
    mem.store(W_HEAD, 1, Ordering::Relaxed);
    assert_eq!(reader.next(), Next::Empty);
    assert_eq!(reader.missed(), u64::MAX);
}

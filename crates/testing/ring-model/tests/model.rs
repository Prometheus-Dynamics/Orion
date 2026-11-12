//! Every interleaving of one writer against readers, under `loom`, with its model of what a
//! relaxed load may return. The ring's own code runs here unchanged, over loom's atomics.
//!
//! What must hold in every interleaving:
//!
//! - a sample a reader returns is whole: its time, length and every payload byte belong to the
//!   sequence number it carries;
//! - `next` returns samples in order, and every sample is either returned or counted as missed;
//! - `latest` never goes backwards.

use std::sync::atomic::Ordering;

use loom::sync::Arc;
use loom::sync::atomic::AtomicU64;
use orion_ring::{
    FieldId, Flags, Geometry, Mem, MemMut, Next, Reader, Sample, SampleTime, Spec, Writer, format,
};

/// Ring memory made of loom's atomics.
#[derive(Clone)]
struct Model(Arc<Vec<AtomicU64>>);

impl Model {
    fn ring(fields: u32, history: u32, max_payload: u32) -> Self {
        let spec = Spec {
            fields,
            history,
            max_payload,
            expiry_ms: 0,
        };
        let words = Geometry::for_spec(&spec).expect("shape").bytes() / 8;
        let mem = Self(Arc::new((0..words).map(|_| AtomicU64::new(0)).collect()));
        format(&mem, &spec).expect("format");
        mem
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

/// Sample `seq` is published with a time, a length and bytes that all name `seq`.
const LEN: [usize; 4] = [9, 16, 1, 12];

fn payload(seq: u64) -> ([u8; 16], usize) {
    let mut bytes = [0_u8; 16];
    let len = LEN[(seq % 4) as usize];
    for (i, byte) in bytes[..len].iter_mut().enumerate() {
        *byte = (seq as u8 + 1) * 16 + i as u8;
    }
    (bytes, len)
}

fn publish(writer: &mut Writer<Model>, field: u16) {
    let seq = writer.published();
    let (bytes, len) = payload(seq);
    let time = SampleTime::from_nanos(1000 + seq);
    assert_eq!(
        writer.publish(FieldId(field), time, Flags(seq as u8), &bytes[..len]),
        Ok(seq)
    );
}

fn assert_whole(sample: &Sample<'_>) {
    let (bytes, len) = payload(sample.seq);
    assert_eq!(
        sample.time.as_nanos(),
        1000 + sample.seq,
        "time from another sample"
    );
    assert_eq!(
        sample.flags.0, sample.seq as u8,
        "flags from another sample"
    );
    assert_eq!(sample.payload, &bytes[..len], "payload from another sample");
}

fn model(preemptions: usize, body: impl Fn() + Sync + Send + 'static) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(preemptions);
    builder.check(body);
}

#[test]
fn next_returns_whole_samples_in_order_and_accounts_for_all() {
    // History 2 and three publishes: the writer overwrites a slot the reader may be copying.
    model(3, || {
        let mem = Model::ring(1, 2, 16);
        let mut writer = Writer::over(mem.clone()).expect("writer");
        let mut reader = Reader::over(mem).expect("reader");
        let publisher = loom::thread::spawn(move || {
            for _ in 0..3 {
                publish(&mut writer, 0);
            }
        });
        let mut seen = 0_u64;
        let mut last = None;
        let mut read = |reader: &mut Reader<Model>, rounds: usize| {
            for _ in 0..rounds {
                if let Next::Sample(sample) = reader.next() {
                    assert_whole(&sample);
                    assert!(last.is_none_or(|last| sample.seq > last), "out of order");
                    last = Some(sample.seq);
                    seen += 1;
                }
            }
        };
        read(&mut reader, 2);
        publisher.join().expect("publisher");
        read(&mut reader, 8);
        assert_eq!(
            seen + reader.missed(),
            3,
            "a sample was neither read nor counted"
        );
        assert_eq!(reader.next(), Next::Empty);
    });
}

#[test]
fn latest_is_whole_and_never_goes_backwards() {
    // History 2 and two publishes on top of two: the writer reuses both slots while the reader
    // looks. `latest` looks again when it is lapped, which multiplies the interleavings, so the
    // scenario is kept this small. It is the model that caught `latest` going backwards when a
    // slot holding a newer sample was accepted.
    model(2, || {
        let mem = Model::ring(1, 2, 16);
        let mut writer = Writer::over(mem.clone()).expect("writer");
        let mut reader = Reader::over(mem).expect("reader");
        publish(&mut writer, 0);
        publish(&mut writer, 0);
        let publisher = loom::thread::spawn(move || {
            publish(&mut writer, 0);
            publish(&mut writer, 0);
        });
        let mut last = 0;
        for _ in 0..2 {
            // `None` is allowed while the writer laps the reader, a stale or torn value never.
            if let Some(sample) = reader.latest(FieldId(0)) {
                assert_whole(&sample);
                assert!(sample.seq >= last, "latest went backwards");
                last = sample.seq;
            }
        }
        publisher.join().expect("publisher");
        let sample = reader.latest(FieldId(0)).expect("the writer is done");
        assert_whole(&sample);
        assert_eq!(sample.seq, 3);
    });
}

#[test]
fn two_fields_keep_their_own_history_under_a_racing_reader() {
    model(2, || {
        let mem = Model::ring(2, 2, 16);
        let mut writer = Writer::over(mem.clone()).expect("writer");
        let mut reader = Reader::over(mem).expect("reader");
        publish(&mut writer, 0);
        let publisher = loom::thread::spawn(move || {
            publish(&mut writer, 1);
            publish(&mut writer, 1);
            publish(&mut writer, 1);
        });
        // Field 0 is never written again, so its newest is always there and always sample 0.
        let quiet = reader
            .latest(FieldId(0))
            .expect("the quiet field keeps its value");
        assert_eq!(quiet.seq, 0);
        assert_whole(&quiet);
        if let Some(sample) = reader.at(FieldId(1), 0) {
            assert_whole(&sample);
            assert_eq!(sample.seq, 1);
        }
        if let Next::Sample(sample) = reader.next() {
            assert_whole(&sample);
        }
        publisher.join().expect("publisher");
        assert_eq!(reader.at(FieldId(1), 0), None, "overwritten");
        let busy = reader.latest(FieldId(1)).expect("newest");
        assert_eq!(busy.seq, 3);
    });
}

#[test]
fn two_readers_do_not_disturb_each_other() {
    model(2, || {
        let mem = Model::ring(1, 2, 16);
        let mut writer = Writer::over(mem.clone()).expect("writer");
        let readers: Vec<_> = (0..2)
            .map(|_| {
                let mut reader = Reader::over(mem.clone()).expect("reader");
                loom::thread::spawn(move || {
                    let mut seen = 0;
                    for _ in 0..2 {
                        if let Next::Sample(sample) = reader.next() {
                            assert_whole(&sample);
                            seen += 1;
                        }
                    }
                    (reader, seen)
                })
            })
            .collect();
        publish(&mut writer, 0);
        publish(&mut writer, 0);
        for reader in readers {
            let (mut reader, mut seen) = reader.join().expect("reader");
            while let Next::Sample(sample) = reader.next() {
                assert_whole(&sample);
                seen += 1;
            }
            assert_eq!(seen + reader.missed(), 2);
        }
    });
}

/// The model has teeth: a reader that skips the second stamp check is caught returning a torn
/// sample. Without this, the tests above passing would prove nothing.
#[test]
#[should_panic(expected = "from another sample")]
fn a_reader_without_the_second_check_is_caught() {
    model(3, || {
        let mem = Model::ring(1, 2, 16);
        let mut writer = Writer::over(mem.clone()).expect("writer");
        let shape = *writer.geometry();
        let publisher = loom::thread::spawn(move || {
            for _ in 0..3 {
                publish(&mut writer, 0);
            }
        });
        // Slot 0 of field 0, read the way the reader does but without checking the stamp again.
        let at = shape.bytes() / 8 - 2 * 6;
        let stamp = mem.load(at, Ordering::Acquire);
        if stamp != 0 && stamp & 1 == 0 {
            let seq = (stamp >> 1) - 1;
            let time = mem.load(at + 1, Ordering::Relaxed);
            let first = mem.load(at + 4, Ordering::Relaxed);
            let (bytes, _) = payload(seq);
            assert_eq!(time, 1000 + seq, "time from another sample");
            assert_eq!(
                first.to_le_bytes()[0],
                bytes[0],
                "payload from another sample"
            );
        }
        publisher.join().expect("publisher");
    });
}

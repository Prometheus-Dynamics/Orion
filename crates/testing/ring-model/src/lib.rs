//! The ring's proof against a hostile writer, shared by a test on the stable toolchain and the
//! `ring` fuzz target. The concurrency models are in `tests/model.rs`.

use std::sync::atomic::Ordering;

use orion_ring::{
    FieldId, Flags, FrameTime, Geometry, Heap, Mem, MemMut, Next, Reader, Spec, Writer, format,
};

/// Words of memory a hostile ring is given. Small, so the fuzzer reaches every part of it.
const WORDS: usize = 192;

fn word(bytes: &[u8]) -> u64 {
    let mut le = [0_u8; 8];
    le[..bytes.len()].copy_from_slice(bytes);
    u64::from_le_bytes(le)
}

/// Drives a reader over a ring whose contents come from `data`, and panics if the reader ever
/// returns something outside the shape it validated. Reading out of bounds or looping forever
/// would show as a crash or a hang.
///
/// The first byte picks how the ring is made:
///
/// - even: the memory is `data` itself, so the header is hostile too;
/// - odd: a real ring with real samples, then `data` is a list of words to overwrite, so the
///   reader has attached to something valid that then turns hostile.
pub fn hostile(data: &[u8]) {
    let Some((&mode, rest)) = data.split_first() else {
        return;
    };
    let mem = Heap::new(WORDS);
    if mode & 1 == 0 {
        for (index, bytes) in rest.chunks(8).take(WORDS).enumerate() {
            mem.store(index, word(bytes), Ordering::Relaxed);
        }
        let Ok(reader) = Reader::over(&mem) else {
            return;
        };
        exercise(reader, &mem, rest);
    } else {
        let spec = Spec {
            fields: 3,
            history: 4,
            max_payload: 24,
            expiry_ms: 0,
        };
        if format(&mem, &spec).is_err() {
            return;
        }
        let (Ok(mut writer), Ok(reader)) = (Writer::over(&mem), Reader::over(&mem)) else {
            return;
        };
        for seq in 0..7_u64 {
            let field = FieldId((seq % 3) as u16);
            let _ = writer.publish(
                field,
                FrameTime::from_nanos(seq),
                Flags::NONE,
                &[seq as u8; 24],
            );
        }
        for patch in rest.chunks(10) {
            let index = usize::from(word(&patch[..patch.len().min(2)]) as u16) % WORDS;
            mem.store(index, word(&patch[patch.len().min(2)..]), Ordering::Relaxed);
        }
        exercise(reader, &mem, rest);
    }
}

fn exercise(mut reader: Reader<&Heap>, mem: &Heap, data: &[u8]) {
    let shape: Geometry = *reader.geometry();
    assert!(shape.bytes() / 8 <= mem.words());
    let check = |field: FieldId, len: usize| {
        assert!(
            u32::from(field.0) < shape.fields(),
            "field out of the shape"
        );
        assert!(
            len <= shape.max_payload() as usize,
            "payload out of the shape"
        );
    };
    for _ in 0..256 {
        match reader.next() {
            Next::Sample(sample) => check(sample.field, sample.payload.len()),
            Next::Empty => break,
            Next::Skipped(count) => assert!(count > 0),
        }
    }
    for (step, byte) in data.iter().take(64).enumerate() {
        let field = FieldId(u16::from(*byte));
        if let Some(sample) = reader.latest(field) {
            check(sample.field, sample.payload.len());
            assert_eq!(sample.field, field);
        }
        let head = reader.lane_head(field);
        for lane_seq in [
            head.wrapping_sub(1),
            head,
            step as u64,
            u64::from(*byte) << 56,
        ] {
            if let Some(sample) = reader.at(field, lane_seq) {
                check(sample.field, sample.payload.len());
            }
        }
    }
    reader.rewind();
    for _ in 0..64 {
        if let Next::Sample(sample) = reader.next() {
            check(sample.field, sample.payload.len());
        }
    }
}

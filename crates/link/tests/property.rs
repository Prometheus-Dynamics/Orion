//! Property-style tests: thousands of random message sequences with random faults, driven by a
//! deterministic PRNG so failures reproduce from the printed case number.

mod common;

use common::{Message, Rng};
use orion_link::frame::frame_len;
use orion_link::{
    FrameHeader, Reassembler, Segment, SegmentMtu, Segmenter, StreamDecoder, StreamEncoder,
};

const CASES: u64 = 3000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    None,
    BitFlip,
    Truncate,
    ExtraZero,
    GarbageBefore,
}

/// Checks that `delivered` is an in-order subsequence of `sent` (by unique seq) and returns the
/// delivered indices.
fn delivered_indices(sent: &[Message], delivered: &[Message], case: u64) -> Vec<usize> {
    let mut indices = Vec::new();
    let mut next = 0;
    for got in delivered {
        let offset = sent[next..]
            .iter()
            .position(|m| m == got)
            .unwrap_or_else(|| {
                panic!("case {case}: delivered frame was never sent or out of order")
            });
        indices.push(next + offset);
        next += offset;
    }
    indices
}

#[test]
fn stream_random_faults() {
    const N: usize = 300;
    for case in 0..CASES {
        let mut rng = Rng::new(case);
        let count = 1 + rng.below(6);
        let mut sent: Vec<Message> = Vec::new();
        let mut must_arrive = Vec::new();
        let mut must_not_arrive = Vec::new();
        let mut wire = Vec::new();
        for i in 0..count {
            let header = FrameHeader::new(rng.byte(), i as u16);
            let payload = rng.payload(320);
            let fits = frame_len(payload.len()) <= N;
            let mut encoder = StreamEncoder::for_message(header, &payload);
            if rng.chance(20) {
                encoder = encoder.with_leading_delimiter();
            }
            let mut bytes: Vec<u8> = encoder.collect();
            let fault = match rng.below(10) {
                0 => Fault::BitFlip,
                1 => Fault::Truncate,
                2 => Fault::ExtraZero,
                3 => Fault::GarbageBefore,
                _ => Fault::None,
            };
            let body = bytes.len() - 1; // never touch the trailing delimiter
            match fault {
                Fault::None => {}
                Fault::BitFlip => {
                    let at = rng.below(body);
                    bytes[at] ^= 1 << rng.below(8);
                }
                Fault::Truncate => {
                    let keep = rng.below(body);
                    bytes.drain(keep..body);
                }
                Fault::ExtraZero => {
                    // Strictly inside the frame, after any leading delimiter.
                    let lead = usize::from(bytes[0] == 0);
                    let at = lead + 1 + rng.below(body - lead - 1);
                    bytes.insert(at, 0);
                }
                Fault::GarbageBefore => {
                    let garbage: Vec<u8> = (0..1 + rng.below(20)).map(|_| rng.byte() | 1).collect();
                    // Garbage lands after any leading delimiter, so it merges into this frame.
                    let at = usize::from(bytes[0] == 0);
                    bytes.splice(at..at, garbage);
                }
            }
            let clean = fault == Fault::None;
            if clean && fits {
                must_arrive.push(i);
            }
            if !fits || matches!(fault, Fault::BitFlip | Fault::Truncate | Fault::ExtraZero) {
                must_not_arrive.push(i);
            }
            wire.extend(bytes);
            sent.push((header.kind, header.seq, payload));
        }

        let mut decoder = StreamDecoder::<N>::new();
        let mut delivered = Vec::new();
        let mut errors = 0u32;
        let mut rest = &wire[..];
        while !rest.is_empty() {
            let chunk = (1 + rng.below(64)).min(rest.len());
            let (used, result) = decoder.push_slice(&rest[..chunk]);
            match result {
                Ok(Some(frame)) => {
                    delivered.push((frame.kind(), frame.seq(), frame.payload().to_vec()))
                }
                Ok(None) => {}
                Err(_) => errors += 1,
            }
            rest = &rest[used..];
        }

        let got = delivered_indices(&sent, &delivered, case);
        for i in &must_arrive {
            assert!(got.contains(i), "case {case}: clean message {i} lost");
        }
        for i in &must_not_arrive {
            assert!(
                !got.contains(i),
                "case {case}: corrupt message {i} delivered"
            );
        }
        let stats = decoder.stats();
        assert_eq!(stats.frames as usize, delivered.len(), "case {case}");
        assert_eq!(stats.dropped(), errors, "case {case}");
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SegFault {
    None,
    Drop,
    Duplicate,
    Swap,
    CorruptPayload,
    CorruptAnything,
}

#[test]
fn packet_random_faults() {
    for case in 0..CASES {
        let mut rng = Rng::new(case ^ 0xC0FFEE);
        let mtu = match rng.below(4) {
            0 => SegmentMtu::CLASSIC,
            1 => SegmentMtu::FD,
            _ => SegmentMtu::new([12, 16, 20, 24, 32, 48][rng.below(6)]).unwrap(),
        };
        let count = 1 + rng.below(5);
        let mut sent: Vec<Message> = Vec::new();
        let mut must_arrive = Vec::new();
        let mut must_not_arrive = Vec::new();
        let mut wire: Vec<Vec<u8>> = Vec::new();
        for i in 0..count {
            let header = FrameHeader::new(rng.byte(), i as u16);
            let payload = rng.payload(700);
            let mut segs: Vec<Vec<u8>> = Segmenter::for_message(header, &payload, mtu)
                .map(|s: Segment| s.as_bytes().to_vec())
                .collect();
            let n = segs.len();
            let fault = match rng.below(12) {
                0 if n > 1 => SegFault::Drop,
                1 => SegFault::Duplicate,
                2 if n > 2 => SegFault::Swap,
                3 => SegFault::CorruptPayload,
                4 => SegFault::CorruptAnything,
                _ => SegFault::None,
            };
            match fault {
                SegFault::None => {}
                SegFault::Drop => {
                    segs.remove(rng.below(n));
                }
                SegFault::Duplicate => {
                    let at = rng.below(n);
                    let copy = segs[at].clone();
                    segs.insert(at, copy);
                }
                SegFault::Swap => {
                    let at = rng.below(n - 1);
                    segs.swap(at, at + 1);
                }
                SegFault::CorruptPayload => {
                    let seg = &mut segs[rng.below(n)];
                    let at = 1 + rng.below(seg.len() - 1);
                    seg[at] ^= 1 << rng.below(8);
                }
                SegFault::CorruptAnything => {
                    let seg = &mut segs[rng.below(n)];
                    let at = rng.below(seg.len());
                    seg[at] ^= 1 << rng.below(8);
                }
            }
            match fault {
                SegFault::None | SegFault::Duplicate => must_arrive.push(i),
                SegFault::Drop | SegFault::Swap | SegFault::CorruptPayload => {
                    must_not_arrive.push(i)
                }
                SegFault::CorruptAnything => {}
            }
            wire.extend(segs);
            sent.push((header.kind, header.seq, payload));
        }

        let mut rx = Reassembler::<1024>::new();
        let mut delivered = Vec::new();
        for seg in &wire {
            if let Ok(Some(frame)) = rx.push(seg) {
                delivered.push((frame.kind(), frame.seq(), frame.payload().to_vec()));
            }
        }
        // A duplicated single-segment message may legitimately arrive twice.
        delivered.dedup();
        let got = delivered_indices(&sent, &delivered, case);
        for i in &must_arrive {
            assert!(got.contains(i), "case {case}: message {i} lost ({mtu:?})");
        }
        for i in &must_not_arrive {
            assert!(
                !got.contains(i),
                "case {case}: broken message {i} delivered"
            );
        }
    }
}

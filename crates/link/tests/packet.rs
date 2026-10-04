mod common;

use common::{Message, Rng, frame_bytes};
use orion_link::packet::{
    CAN_FD_LENGTHS, SEGMENT_END, SEGMENT_START, can_fd_len_at_least, can_fd_len_at_most,
    is_can_fd_len,
};
use orion_link::{
    CanLinkIds, FrameError, FrameHeader, PacketError, PacketStats, Reassembler, Segment,
    SegmentMtu, Segmenter,
};

fn segments(header: FrameHeader, payload: &[u8], mtu: SegmentMtu) -> Vec<Segment> {
    Segmenter::for_message(header, payload, mtu).collect()
}

fn feed<const N: usize>(
    rx: &mut Reassembler<N>,
    segs: &[Segment],
) -> (Vec<Message>, Vec<PacketError>) {
    let mut frames = Vec::new();
    let mut errors = Vec::new();
    for seg in segs {
        match rx.push(seg.as_bytes()) {
            Ok(Some(frame)) => frames.push((frame.kind(), frame.seq(), frame.payload().to_vec())),
            Ok(None) => {}
            Err(err) => errors.push(err),
        }
    }
    (frames, errors)
}

#[test]
fn can_fd_length_helpers() {
    for len in 0..=64 {
        assert_eq!(is_can_fd_len(len), CAN_FD_LENGTHS.contains(&(len as u8)));
        let up = can_fd_len_at_least(len).unwrap();
        assert!(up >= len && is_can_fd_len(up));
        let down = can_fd_len_at_most(len);
        assert!(down <= len && is_can_fd_len(down));
    }
    assert_eq!(can_fd_len_at_least(9), Some(12));
    assert_eq!(can_fd_len_at_least(49), Some(64));
    assert_eq!(can_fd_len_at_least(65), None);
    assert_eq!(can_fd_len_at_most(47), 32);
    assert_eq!(SegmentMtu::new(9), None);
    assert_eq!(SegmentMtu::new(1), None);
    assert_eq!(SegmentMtu::new(65), None);
    assert_eq!(SegmentMtu::new(8), Some(SegmentMtu::CLASSIC));
    assert_eq!(SegmentMtu::new(64), Some(SegmentMtu::FD));
    assert_eq!(SegmentMtu::FD.payload_len(), 63);
}

#[test]
fn segment_headers_and_lengths_follow_the_spec() {
    let payload = vec![0xA5; 100]; // 108-byte frame
    let segs = segments(FrameHeader::new(1, 1), &payload, SegmentMtu::CLASSIC);
    assert_eq!(segs.len(), 108usize.div_ceil(7));
    for (i, seg) in segs.iter().enumerate() {
        assert_eq!(seg.counter() as usize, i % 64);
        assert_eq!(seg.is_start(), i == 0);
        assert_eq!(seg.is_end(), i == segs.len() - 1);
        assert!(seg.as_bytes().len() <= 8);
    }
    assert_eq!(segs[0].header(), SEGMENT_START);
    let total: usize = segs.iter().map(|s| s.as_bytes().len() - 1).sum();
    assert_eq!(total, 108);

    // A frame that fits one segment carries both flags.
    let single = segments(FrameHeader::new(1, 1), b"abc", SegmentMtu::FD);
    assert_eq!(single.len(), 1);
    assert_eq!(single[0].header(), SEGMENT_START | SEGMENT_END);
    assert_eq!(single[0].as_bytes().len(), 12);
    // An empty-payload frame (8 bytes) would need a 9-byte CAN FD frame, which does not exist,
    // so it goes out unpadded as 8 + 2 bytes.
    let empty = segments(FrameHeader::new(1, 1), &[], SegmentMtu::FD);
    let lens: Vec<usize> = empty.iter().map(|s| s.as_bytes().len()).collect();
    assert_eq!(lens, vec![8, 2]);
}

#[test]
fn can_fd_segments_are_never_padded() {
    for payload_len in 0..400 {
        let payload = vec![0x11; payload_len];
        for mtu_len in [8usize, 12, 16, 20, 24, 32, 48, 64] {
            let mtu = SegmentMtu::new(mtu_len).unwrap();
            let segs = segments(FrameHeader::new(1, 1), &payload, mtu);
            let total: usize = segs.iter().map(|s| s.as_bytes().len() - 1).sum();
            assert_eq!(total, payload_len + 8);
            for seg in &segs {
                let len = seg.as_bytes().len();
                assert!(
                    is_can_fd_len(len) && len <= mtu_len && len >= 2,
                    "len {len}"
                );
            }
            // Only the tail may be shorter than the MTU.
            let full = segs
                .iter()
                .take_while(|s| s.as_bytes().len() == mtu_len)
                .count();
            assert!(segs.len() - full <= 3, "{payload_len}/{mtu_len}");
        }
    }
}

#[test]
fn classic_and_fd_round_trips() {
    let mut rng = Rng::new(21);
    for mtu in [
        SegmentMtu::CLASSIC,
        SegmentMtu::FD,
        SegmentMtu::new(32).unwrap(),
    ] {
        let mut rx = Reassembler::<2048>::new();
        for len in [0usize, 1, 6, 7, 55, 56, 62, 63, 64, 500, 2040] {
            let payload: Vec<u8> = (0..len).map(|_| rng.byte()).collect();
            let header = rng.header();
            let (frames, errors) = feed(&mut rx, &segments(header, &payload, mtu));
            assert!(errors.is_empty(), "{errors:?}");
            assert_eq!(frames, vec![(header.kind, header.seq, payload)]);
        }
        assert_eq!(rx.stats().frames, 11);
        assert_eq!(rx.stats().dropped(), 0);
    }
}

#[test]
fn next_into_and_for_frame_match_iterator() {
    let header = FrameHeader::new(2, 3);
    let payload = vec![7u8; 50];
    let expected = segments(header, &payload, SegmentMtu::CLASSIC);
    let frame = frame_bytes(header, &payload);
    let mut seg = Segmenter::for_frame(&frame, SegmentMtu::CLASSIC);
    assert_eq!(seg.remaining_segments(), expected.len());
    let mut small = [0u8; 4];
    assert!(matches!(
        seg.next_into(&mut small),
        Err(PacketError::BufferTooSmall {
            needed: 8,
            available: 4
        })
    ));
    let mut out = [0u8; 8];
    for want in &expected {
        assert_eq!(seg.next_len(), Some(want.as_bytes().len()));
        let n = seg.next_into(&mut out).unwrap().unwrap();
        assert_eq!(&out[..n], want.as_bytes());
    }
    assert!(seg.is_done());
    assert_eq!(seg.next_into(&mut out), Ok(None));
}

#[test]
fn counter_wraps_beyond_64_segments() {
    let payload: Vec<u8> = (0..1000).map(|i| (i * 7) as u8).collect();
    let segs = segments(FrameHeader::new(9, 9), &payload, SegmentMtu::CLASSIC);
    assert!(segs.len() > 128);
    assert_eq!(segs[64].counter(), 0);
    assert_eq!(segs[65].counter(), 1);
    let mut rx = Reassembler::<1024>::new();
    let (frames, errors) = feed(&mut rx, &segs);
    assert!(errors.is_empty());
    assert_eq!(frames[0].2, payload);

    // Losing exactly 64 segments keeps the counter in step; the CRC still catches it.
    let mut lossy = segs.clone();
    lossy.drain(10..74);
    let (frames, errors) = feed(&mut rx, &lossy);
    assert!(frames.is_empty());
    assert!(matches!(
        errors[..],
        [PacketError::Frame(FrameError::CrcMismatch { .. })]
    ));
}

#[test]
fn lost_segment_discards_message_and_next_one_survives() {
    let a = segments(FrameHeader::new(1, 1), &[1u8; 40], SegmentMtu::CLASSIC);
    let b = segments(FrameHeader::new(2, 2), &[2u8; 40], SegmentMtu::CLASSIC);
    for lost in 0..a.len() {
        let mut rx = Reassembler::<128>::new();
        let mut wire = a.clone();
        wire.remove(lost);
        wire.extend(&b);
        let (frames, errors) = feed(&mut rx, &wire);
        assert_eq!(frames, vec![(2, 2, vec![2u8; 40])], "lost {lost}");
        assert_eq!(errors.len(), 1, "lost {lost}: {errors:?}");
        let stats = rx.stats();
        let expected_error = if lost == 0 {
            PacketError::Orphan
        } else if lost == a.len() - 1 {
            PacketError::Interrupted
        } else {
            PacketError::OutOfOrder {
                expected: lost as u8,
                received: lost as u8 + 1,
            }
        };
        assert_eq!(errors[0], expected_error, "lost {lost}");
        assert_eq!(stats.dropped(), 1);
    }
}

#[test]
fn duplicated_segments_are_ignored() {
    let segs = segments(FrameHeader::new(3, 3), &[3u8; 30], SegmentMtu::CLASSIC);
    for dup in 0..segs.len() {
        let mut rx = Reassembler::<128>::new();
        let mut wire = segs.clone();
        wire.insert(dup, segs[dup]);
        let (frames, errors) = feed(&mut rx, &wire);
        if dup == segs.len() - 1 {
            // The repeated end segment arrives after completion: an orphan, but nothing is lost.
            assert_eq!(frames.len(), 1);
            assert_eq!(errors, vec![PacketError::Orphan]);
        } else {
            assert_eq!(frames.len(), 1, "dup {dup}");
            assert!(errors.is_empty(), "dup {dup}: {errors:?}");
            assert_eq!(rx.stats().duplicates, 1);
        }
    }
}

#[test]
fn reordered_segments_discard_the_message() {
    let segs = segments(FrameHeader::new(4, 4), &[4u8; 30], SegmentMtu::CLASSIC);
    let tail = segments(FrameHeader::new(5, 5), b"next", SegmentMtu::CLASSIC);
    for swap in 1..segs.len() - 1 {
        let mut rx = Reassembler::<128>::new();
        let mut wire = segs.clone();
        wire.swap(swap, swap + 1);
        wire.extend(&tail);
        let (frames, errors) = feed(&mut rx, &wire);
        assert_eq!(frames, vec![(5, 5, b"next".to_vec())], "swap {swap}");
        assert!(matches!(errors[0], PacketError::OutOfOrder { .. }));
        // When the end segment jumps ahead, the late segment after it is an orphan.
        let late_orphan = usize::from(swap + 1 == segs.len() - 1);
        assert_eq!(errors.len(), 1 + late_orphan, "swap {swap}: {errors:?}");
        assert_eq!(rx.stats().sequence_errors, 1);
    }
}

#[test]
fn overflow_and_empty_segments_are_reported_once() {
    let mut rx = Reassembler::<16>::new();
    let big = segments(FrameHeader::new(1, 1), &[9u8; 40], SegmentMtu::CLASSIC);
    let (frames, errors) = feed(&mut rx, &big);
    assert!(frames.is_empty());
    assert_eq!(errors, vec![PacketError::Overflow]);
    assert_eq!(rx.push(&[]), Err(PacketError::EmptySegment));
    let ok = segments(FrameHeader::new(2, 2), b"fits", SegmentMtu::CLASSIC);
    let (frames, _) = feed(&mut rx, &ok);
    assert_eq!(frames.len(), 1);
    assert_eq!(
        rx.stats(),
        PacketStats {
            frames: 1,
            overflows: 1,
            framing_errors: 1,
            ..PacketStats::default()
        }
    );
    rx.reset();
    rx.reset_stats();
    assert_eq!(rx.stats(), PacketStats::default());
}

#[test]
fn corrupted_segment_fails_crc() {
    let mut segs = segments(FrameHeader::new(6, 6), &[6u8; 20], SegmentMtu::CLASSIC);
    let mut bytes = [0u8; 8];
    let len = segs[1].as_bytes().len();
    bytes[..len].copy_from_slice(segs[1].as_bytes());
    bytes[3] ^= 0x10;
    let mut rx = Reassembler::<64>::new();
    assert_eq!(rx.push(segs[0].as_bytes()), Ok(None));
    assert_eq!(rx.push(&bytes[..len]), Ok(None));
    segs.drain(..2);
    let (frames, errors) = feed(&mut rx, &segs);
    assert!(frames.is_empty());
    assert!(matches!(
        errors[..],
        [PacketError::Frame(FrameError::CrcMismatch { .. })]
    ));
    assert_eq!(rx.stats().crc_errors, 1);
}

#[test]
fn can_link_ids_for_address() {
    let base = CanLinkIds::new(0x100, 0x200, false);
    assert!(base.is_valid());
    let ids = CanLinkIds::for_address(base, 0x10).unwrap();
    assert_eq!(ids, CanLinkIds::new(0x110, 0x210, false));
    assert_eq!(CanLinkIds::for_address(base, 0x600), None);
    assert_eq!(
        CanLinkIds::for_address(CanLinkIds::new(1, 1, false), 0),
        None
    );
    let ext = CanLinkIds::new(0x1800_0000, 0x1900_0000, true);
    assert!(CanLinkIds::for_address(ext, 0xFF_FFFF).is_some());
    assert_eq!(CanLinkIds::for_address(ext, 0x0800_0000), None);
    assert_eq!(CanLinkIds::for_address(ext, u32::MAX), None);
}

#[test]
fn random_segments_never_panic() {
    let mut rng = Rng::new(77);
    let mut rx = Reassembler::<64>::new();
    for _ in 0..20_000 {
        let len = rng.below(65);
        let seg: Vec<u8> = (0..len).map(|_| rng.byte()).collect();
        let _ = rx.push(&seg);
    }
    let mut zero = Reassembler::<0>::new();
    let _ = zero.push(&[SEGMENT_START | SEGMENT_END, 1, 2]);
}

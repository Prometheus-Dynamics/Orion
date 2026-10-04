mod common;

use common::{Message, Rng, frame_bytes, stream_bytes};
use orion_link::{
    FrameError, FrameHeader, StreamDecoder, StreamEncoder, StreamError, StreamStats, encode_frame,
    encode_message, max_encoded_len,
};

fn cobs(raw: &[u8]) -> Vec<u8> {
    StreamEncoder::for_frame(raw).collect()
}

/// Feeds `bytes` and collects every delivered `(kind, seq, payload)` plus every error.
fn decode_all<const N: usize>(
    decoder: &mut StreamDecoder<N>,
    mut bytes: &[u8],
) -> (Vec<Message>, Vec<StreamError>) {
    let mut frames = Vec::new();
    let mut errors = Vec::new();
    while !bytes.is_empty() {
        let (used, result) = decoder.push_slice(bytes);
        assert!(used > 0 && used <= bytes.len());
        match result {
            Ok(Some(frame)) => frames.push((frame.kind(), frame.seq(), frame.payload().to_vec())),
            Ok(None) => assert_eq!(used, bytes.len()),
            Err(err) => errors.push(err),
        }
        bytes = &bytes[used..];
    }
    (frames, errors)
}

#[test]
fn cobs_matches_reference_vectors() {
    assert_eq!(cobs(&[]), [0x01, 0x00]);
    assert_eq!(cobs(&[0x00]), [0x01, 0x01, 0x00]);
    assert_eq!(cobs(&[0x00, 0x00]), [0x01, 0x01, 0x01, 0x00]);
    assert_eq!(cobs(&[0x00, 0x11, 0x00]), [0x01, 0x02, 0x11, 0x01, 0x00]);
    assert_eq!(
        cobs(&[0x11, 0x22, 0x00, 0x33]),
        [0x03, 0x11, 0x22, 0x02, 0x33, 0x00]
    );
    assert_eq!(
        cobs(&[0x11, 0x22, 0x33, 0x44]),
        [0x05, 0x11, 0x22, 0x33, 0x44, 0x00]
    );
    assert_eq!(
        cobs(&[0x11, 0x00, 0x00, 0x00]),
        [0x02, 0x11, 0x01, 0x01, 0x01, 0x00]
    );

    let run: Vec<u8> = (1..=254).collect();
    let mut expected = vec![0xFF];
    expected.extend(&run);
    expected.push(0x00);
    assert_eq!(cobs(&run), expected);

    let with_zero: Vec<u8> = (0..=254).collect();
    let mut expected = vec![0x01, 0xFF];
    expected.extend(1..=254u8);
    expected.push(0x00);
    assert_eq!(cobs(&with_zero), expected);

    let long: Vec<u8> = (1..=255).collect();
    let mut expected = vec![0xFF];
    expected.extend(1..=254u8);
    expected.extend([0x02, 0xFF, 0x00]);
    assert_eq!(cobs(&long), expected);
}

#[test]
fn encoded_length_stays_within_bound() {
    let mut rng = Rng::new(3);
    for _ in 0..500 {
        let raw = rng.payload(1200);
        let encoded = cobs(&raw);
        assert!(encoded.len() <= max_encoded_len(raw.len()));
        assert!(!encoded[..encoded.len() - 1].contains(&0));
        assert_eq!(encoded.last(), Some(&0));
    }
}

#[test]
fn slice_encoders_match_iterator() {
    let header = FrameHeader::new(5, 77);
    let payload = [0u8, 1, 2, 0, 0, 3];
    let expected = stream_bytes(header, &payload);
    let mut out = [0u8; 64];
    let n = encode_message(header, &payload, &mut out).unwrap();
    assert_eq!(&out[..n], &expected[..]);
    let n = encode_frame(&frame_bytes(header, &payload), &mut out).unwrap();
    assert_eq!(&out[..n], &expected[..]);
    // An exactly sized buffer works; one byte less does not.
    let mut exact = vec![0u8; expected.len()];
    assert_eq!(
        encode_message(header, &payload, &mut exact),
        Ok(expected.len())
    );
    let mut short = vec![0u8; expected.len() - 1];
    assert!(matches!(
        encode_message(header, &payload, &mut short),
        Err(StreamError::BufferTooSmall { .. })
    ));
}

#[test]
fn fill_in_small_chunks_matches_iterator() {
    let header = FrameHeader::new(1, 2);
    let payload: Vec<u8> = (0..600).map(|i| (i % 7) as u8).collect();
    let expected = stream_bytes(header, &payload);
    let mut encoder = StreamEncoder::for_message(header, &payload);
    let mut got = Vec::new();
    let mut chunk = [0u8; 5];
    loop {
        let n = encoder.fill(&mut chunk);
        if n == 0 {
            break;
        }
        got.extend_from_slice(&chunk[..n]);
    }
    assert!(encoder.is_done());
    assert_eq!(got, expected);
}

#[test]
fn round_trips_empty_and_max_size_frames() {
    const N: usize = 512;
    let mut decoder = StreamDecoder::<N>::new();
    let mut rng = Rng::new(11);
    for len in [0usize, 1, 246, 247, 248, 253, 254, 255, N - 8] {
        let payload: Vec<u8> = (0..len).map(|_| rng.byte()).collect();
        let header = rng.header();
        let (frames, errors) = decode_all(&mut decoder, &stream_bytes(header, &payload));
        assert!(errors.is_empty(), "len {len}: {errors:?}");
        assert_eq!(frames, vec![(header.kind, header.seq, payload)]);
    }
    // One byte over capacity overflows and the decoder recovers.
    let too_big = vec![0xAB; N - 7];
    let (frames, errors) = decode_all(
        &mut decoder,
        &stream_bytes(FrameHeader::new(1, 1), &too_big),
    );
    assert!(frames.is_empty());
    assert_eq!(errors, vec![StreamError::Overflow]);
    let (frames, _) = decode_all(&mut decoder, &stream_bytes(FrameHeader::new(2, 2), b"ok"));
    assert_eq!(frames, vec![(2, 2, b"ok".to_vec())]);
    assert_eq!(decoder.stats().overflows, 1);
}

#[test]
fn leading_delimiter_and_idle_zeros_are_ignored() {
    let mut decoder = StreamDecoder::<64>::new();
    let mut wire = vec![0, 0, 0];
    wire.extend(StreamEncoder::for_message(FrameHeader::new(1, 1), b"a").with_leading_delimiter());
    wire.extend([0, 0]);
    let (frames, errors) = decode_all(&mut decoder, &wire);
    assert_eq!(frames.len(), 1);
    assert!(errors.is_empty());
    assert_eq!(decoder.stats().dropped(), 0);
}

#[test]
fn bit_flips_are_dropped_and_counted() {
    let header = FrameHeader::new(7, 7);
    let payload = b"bit flip victim\x00\x00 with zeros";
    let good = stream_bytes(header, payload);
    for bit in 0..(good.len() - 1) * 8 {
        let mut decoder = StreamDecoder::<128>::new();
        let mut wire = good.clone();
        wire[bit / 8] ^= 1 << (bit % 8);
        wire.extend(&good);
        let (frames, errors) = decode_all(&mut decoder, &wire);
        // The intact copy after the corrupted one is always delivered.
        assert_eq!(frames.last(), Some(&(7, 7, payload.to_vec())), "bit {bit}");
        let stats = decoder.stats();
        assert_eq!(frames.len() as u32, stats.frames);
        assert_eq!(errors.len() as u32, stats.dropped());
        // A flip that creates a 0x00 can split the packet; otherwise exactly one drop.
        assert!(
            stats.frames == 1 && stats.dropped() >= 1,
            "bit {bit}: {stats:?}"
        );
    }
}

#[test]
fn truncated_frames_resync_at_next_delimiter() {
    let a = stream_bytes(FrameHeader::new(1, 1), b"first frame");
    let b = stream_bytes(FrameHeader::new(2, 2), b"second");
    for cut in 1..a.len() - 1 {
        let mut decoder = StreamDecoder::<64>::new();
        let mut wire = a[..cut].to_vec();
        wire.push(0); // line break, then the next frame
        wire.extend(&b);
        let (frames, errors) = decode_all(&mut decoder, &wire);
        assert_eq!(frames, vec![(2, 2, b"second".to_vec())], "cut {cut}");
        assert_eq!(errors.len(), 1);
        let stats = decoder.stats();
        assert_eq!(stats.dropped(), 1);
        assert_eq!(stats.crc_errors + stats.framing_errors, 1);
    }
}

#[test]
fn extra_zero_inside_frame_drops_both_halves() {
    let good = stream_bytes(FrameHeader::new(4, 4), b"split me please");
    let mut decoder = StreamDecoder::<64>::new();
    let mut wire = good[..6].to_vec();
    wire.push(0);
    wire.extend(&good[6..]);
    wire.extend(&good);
    let (frames, errors) = decode_all(&mut decoder, &wire);
    assert_eq!(frames.len(), 1);
    assert_eq!(errors.len(), 2);
    assert_eq!(decoder.stats().dropped(), 2);
}

#[test]
fn oversized_garbage_resyncs() {
    let mut decoder = StreamDecoder::<32>::new();
    let mut wire = vec![0x55; 500];
    wire.push(0);
    wire.extend(stream_bytes(FrameHeader::new(1, 9), b"after"));
    let (frames, errors) = decode_all(&mut decoder, &wire);
    assert_eq!(frames, vec![(1, 9, b"after".to_vec())]);
    assert_eq!(errors, vec![StreamError::Overflow]);
    assert_eq!(
        decoder.stats(),
        StreamStats {
            frames: 1,
            overflows: 1,
            ..StreamStats::default()
        }
    );
}

#[test]
fn garbage_between_frames_costs_at_most_the_next_frame() {
    let a = stream_bytes(FrameHeader::new(1, 1), b"one");
    let b = stream_bytes(FrameHeader::new(2, 2), b"two");
    let c = stream_bytes(FrameHeader::new(3, 3), b"three");

    // Garbage without a zero merges into the next packet: that frame is lost, then resync.
    let mut decoder = StreamDecoder::<64>::new();
    let mut wire = a.clone();
    wire.extend([0x13, 0x37, 0x42]);
    wire.extend(&b);
    wire.extend(&c);
    let (frames, errors) = decode_all(&mut decoder, &wire);
    let kinds: Vec<u8> = frames.iter().map(|f| f.0).collect();
    assert_eq!(kinds, vec![1, 3]);
    assert_eq!(errors.len(), 1);

    // With a leading delimiter on each frame, garbage costs nothing.
    let mut decoder = StreamDecoder::<64>::new();
    let mut wire: Vec<u8> = StreamEncoder::for_message(FrameHeader::new(1, 1), b"one")
        .with_leading_delimiter()
        .collect();
    wire.extend([0x13, 0x37, 0x42]);
    wire.extend(
        StreamEncoder::for_message(FrameHeader::new(2, 2), b"two").with_leading_delimiter(),
    );
    let (frames, errors) = decode_all(&mut decoder, &wire);
    assert_eq!(frames.len(), 2);
    assert_eq!(errors.len(), 1);
    assert_eq!(decoder.stats().framing_errors, 1);
}

#[test]
fn short_and_malformed_packets_are_framing_errors() {
    let mut decoder = StreamDecoder::<64>::new();
    // A valid COBS packet that decodes to 3 bytes: too short for a frame.
    let (_, errors) = decode_all(&mut decoder, &cobs(&[1, 2, 3]));
    assert_eq!(
        errors,
        vec![StreamError::Frame(FrameError::TooShort { len: 3 })]
    );
    // A code byte promising more data than arrives.
    let (_, errors) = decode_all(&mut decoder, &[0x09, 1, 2, 0]);
    assert_eq!(errors, vec![StreamError::Cobs]);
    assert_eq!(decoder.stats().framing_errors, 2);
}

#[test]
fn byte_by_byte_push_matches_slices() {
    let wire = stream_bytes(FrameHeader::new(8, 8), b"bytewise");
    let mut decoder = StreamDecoder::<64>::new();
    let mut delivered = 0;
    for &byte in &wire {
        if let Some(frame) = decoder.push(byte).unwrap() {
            assert_eq!(frame.payload(), b"bytewise");
            delivered += 1;
        }
    }
    assert_eq!(delivered, 1);
}

#[test]
fn reset_drops_partial_packets() {
    let wire = stream_bytes(FrameHeader::new(8, 8), b"partial");
    let mut decoder = StreamDecoder::<64>::new();
    let _ = decoder.push_slice(&wire[..4]);
    decoder.reset();
    let (frames, errors) = decode_all(&mut decoder, &wire);
    assert_eq!(frames.len(), 1);
    assert!(errors.is_empty());
    decoder.reset_stats();
    assert_eq!(decoder.stats(), StreamStats::default());
    assert_eq!(decoder.capacity(), 64);
}

#[test]
fn random_bytes_never_panic() {
    let mut rng = Rng::new(1234);
    let mut decoder = StreamDecoder::<48>::new();
    for _ in 0..200 {
        let wire: Vec<u8> = (0..rng.below(400))
            .map(|_| if rng.chance(5) { 0 } else { rng.byte() })
            .collect();
        let (frames, errors) = decode_all(&mut decoder, &wire);
        let _ = (frames, errors);
    }
    let mut tiny = StreamDecoder::<0>::new();
    let (frames, _) = decode_all(&mut tiny, &stream_bytes(FrameHeader::new(1, 1), b""));
    assert!(frames.is_empty());
}

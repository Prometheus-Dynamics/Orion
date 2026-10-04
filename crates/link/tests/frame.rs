mod common;

use common::{Rng, frame_bytes};
use orion_link::frame::{self, encode_in_place, payload_area};
use orion_link::{Crc32c, FRAME_OVERHEAD, FrameError, FrameHeader, LINK_PROTOCOL_VERSION, crc32c};

#[test]
fn crc32c_matches_published_vectors() {
    assert_eq!(crc32c(b""), 0x0000_0000);
    assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    assert_eq!(crc32c(b"a"), 0xC1D0_4330);
    // RFC 3720 (iSCSI) appendix B.4 vectors.
    assert_eq!(crc32c(&[0u8; 32]), 0x8A91_36AA);
    assert_eq!(crc32c(&[0xFFu8; 32]), 0x62A8_AB43);
    let ascending: Vec<u8> = (0u8..32).collect();
    assert_eq!(crc32c(&ascending), 0x46DD_794E);
    let descending: Vec<u8> = (0u8..32).rev().collect();
    assert_eq!(crc32c(&descending), 0x113F_DB5C);
}

#[test]
fn crc32c_incremental_matches_one_shot() {
    let mut rng = Rng::new(7);
    for _ in 0..200 {
        let data = rng.payload(300);
        let split = rng.below(data.len() + 1);
        let mut crc = Crc32c::new();
        crc.update(&data[..split]);
        for &byte in &data[split..] {
            crc.update_byte(byte);
        }
        assert_eq!(crc.finish(), crc32c(&data));
    }
}

#[test]
fn frame_layout_is_header_payload_crc() {
    let bytes = frame_bytes(FrameHeader::new(0x42, 0x1234), b"abc");
    assert_eq!(&bytes[..4], &[LINK_PROTOCOL_VERSION, 0x42, 0x34, 0x12]);
    assert_eq!(&bytes[4..7], b"abc");
    assert_eq!(&bytes[7..], &crc32c(&bytes[..7]).to_le_bytes());
}

#[test]
fn frame_round_trips_empty_and_large_payloads() {
    let mut rng = Rng::new(1);
    for len in [0usize, 1, 2, 253, 254, 255, 1024, 4096] {
        let payload: Vec<u8> = (0..len).map(|_| rng.byte()).collect();
        let header = FrameHeader::new(rng.byte(), rng.next_u64() as u16);
        let bytes = frame_bytes(header, &payload);
        assert_eq!(bytes.len(), len + FRAME_OVERHEAD);
        let view = frame::decode(&bytes).unwrap();
        assert_eq!(view.header(), header);
        assert_eq!(view.payload(), &payload[..]);
        assert!(view.header().is_current_version());
    }
}

#[test]
fn encode_reports_small_buffers() {
    let mut out = [0u8; 10];
    assert_eq!(
        frame::encode(FrameHeader::new(1, 1), b"abc", &mut out),
        Err(FrameError::BufferTooSmall {
            needed: 11,
            available: 10
        })
    );
    assert_eq!(
        frame::encode(FrameHeader::new(1, 1), b"ab", &mut out),
        Ok(10)
    );
    assert!(matches!(
        encode_in_place(FrameHeader::new(1, 1), usize::MAX, &mut out),
        Err(FrameError::BufferTooSmall { .. })
    ));
}

#[test]
fn encode_in_place_matches_encode() {
    let mut buf = [0u8; 64];
    payload_area(&mut buf)[..5].copy_from_slice(b"hello");
    let n = encode_in_place(FrameHeader::new(9, 300), 5, &mut buf).unwrap();
    assert_eq!(
        &buf[..n],
        &frame_bytes(FrameHeader::new(9, 300), b"hello")[..]
    );
    assert!(payload_area(&mut [0u8; 7]).is_empty());
    assert_eq!(payload_area(&mut [0u8; 8]).len(), 0);
    assert_eq!(payload_area(&mut [0u8; 12]).len(), 4);
}

#[test]
fn decode_rejects_short_and_corrupt_frames() {
    for len in 0..FRAME_OVERHEAD {
        assert_eq!(
            frame::decode(&vec![0u8; len]),
            Err(FrameError::TooShort { len })
        );
    }
    let bytes = frame_bytes(FrameHeader::new(3, 4), b"payload");
    for bit in 0..bytes.len() * 8 {
        let mut corrupt = bytes.clone();
        corrupt[bit / 8] ^= 1 << (bit % 8);
        assert!(matches!(
            frame::decode(&corrupt),
            Err(FrameError::CrcMismatch { .. })
        ));
    }
    // Truncation never validates.
    for cut in 0..bytes.len() {
        assert!(frame::decode(&bytes[..cut]).is_err());
    }
}

#[test]
fn decode_reports_foreign_versions_instead_of_dropping() {
    let header = FrameHeader {
        version: LINK_PROTOCOL_VERSION.wrapping_add(1),
        kind: 1,
        seq: 2,
    };
    let bytes = frame_bytes(header, b"x");
    let view = frame::decode(&bytes).unwrap();
    assert_eq!(view.version(), LINK_PROTOCOL_VERSION.wrapping_add(1));
    assert!(!view.header().is_current_version());
}

#[test]
fn decode_never_panics_on_random_bytes() {
    let mut rng = Rng::new(99);
    for _ in 0..5000 {
        let len = rng.below(40);
        let bytes: Vec<u8> = (0..len).map(|_| rng.byte()).collect();
        let _ = frame::decode(&bytes);
    }
}

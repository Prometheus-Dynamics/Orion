//! A frame round-trips in every framing, and none of it allocates.

use orion_measure::harness::assert_steady_no_allocs;
use orion_wire::socket::{self, Parsed, SocketError};
use orion_wire::stream::{StreamDecoder, encode_message, max_encoded_len};
use orion_wire::{FrameHeader, Reassembler, SegmentMtu, Segmenter, frame, frame_len, kind};

#[global_allocator]
static GLOBAL: orion_measure::CountingAlloc = orion_measure::CountingAlloc::system();

fn payload(len: usize) -> Vec<u8> {
    // Holds zeros on purpose: COBS has to escape them.
    (0..len).map(|i| (i % 7) as u8).collect()
}

const SIZES: [usize; 8] = [0, 1, 7, 8, 63, 64, 254, 600];

#[test]
fn plain_frames() {
    let mut buf = [0; 700];
    for size in SIZES {
        let body = payload(size);
        let len = frame::encode(FrameHeader::new(kind::PING, 9), &body, &mut buf).unwrap();
        assert_eq!(len, frame_len(size));
        let view = frame::decode(&buf[..len]).unwrap();
        assert_eq!(
            (view.kind(), view.seq(), view.payload()),
            (kind::PING, 9, &body[..])
        );
    }
}

#[test]
fn a_flipped_bit_is_caught() {
    let mut buf = [0; 64];
    let len = frame::encode(FrameHeader::new(kind::PING, 1), b"hello", &mut buf).unwrap();
    for byte in 0..len {
        let mut bad = buf;
        bad[byte] ^= 0x10;
        assert!(frame::decode(&bad[..len]).is_err(), "byte {byte}");
    }
}

#[test]
fn cobs_streams() {
    let mut out = [0; 800];
    for size in SIZES {
        let body = payload(size);
        assert!(max_encoded_len(size) <= out.len());
        let len = encode_message(FrameHeader::new(kind::PONG, 2), &body, &mut out).unwrap();
        assert!(!out[..len - 1].contains(&0), "only the delimiter is zero");
        // Any chunking gives the same frame.
        for chunk in [1, 3, len] {
            let mut rx = StreamDecoder::<700>::new();
            let mut delivered = None;
            for part in out[..len].chunks(chunk) {
                let mut rest = part;
                while !rest.is_empty() {
                    let (used, result) = rx.push_slice(rest);
                    if let Ok(Some(view)) = result {
                        delivered = Some((view.kind(), view.payload().to_vec()));
                    }
                    rest = &rest[used..];
                }
            }
            assert_eq!(
                delivered,
                Some((kind::PONG, body.clone())),
                "size {size} chunk {chunk}"
            );
        }
    }
}

#[test]
fn can_segments() {
    for mtu in [SegmentMtu::CLASSIC, SegmentMtu::FD] {
        for size in SIZES {
            let body = payload(size);
            let mut rx = Reassembler::<700>::new();
            let mut delivered = None;
            for segment in Segmenter::for_message(FrameHeader::new(kind::PUT, 3), &body, mtu) {
                assert!(segment.as_bytes().len() <= mtu.frame_len());
                if let Some(view) = rx.push(segment.as_bytes()).unwrap() {
                    delivered = Some((view.kind(), view.payload().to_vec()));
                }
            }
            assert_eq!(
                delivered,
                Some((kind::PUT, body)),
                "mtu {mtu:?} size {size}"
            );
        }
    }
}

#[test]
fn sockets() {
    let mut buf = [0; 700];
    for size in SIZES {
        let body = payload(size);
        let len = socket::encode(FrameHeader::new(kind::GET, 5), &body, &mut buf).unwrap();
        for short in [0, 3, len - 1] {
            assert!(matches!(
                socket::parse(&buf[..short], 700).unwrap(),
                Parsed::Incomplete { .. }
            ));
        }
        let Parsed::Frame { frame, consumed } = socket::parse(&buf[..len], 700).unwrap() else {
            panic!("no frame");
        };
        assert_eq!(consumed, len);
        assert_eq!((frame.kind(), frame.payload()), (kind::GET, &body[..]));
    }
}

#[test]
fn a_socket_frame_over_the_limit_is_refused_before_it_arrives() {
    let prefix = 1_000_u32.to_le_bytes();
    assert!(matches!(
        socket::parse(&prefix, 100),
        Err(SocketError::TooLarge {
            len: 1000,
            max: 100
        })
    ));
}

#[test]
fn a_bad_socket_frame_keeps_the_stream_in_step() {
    let mut buf = [0; 64];
    let len = socket::encode(FrameHeader::new(kind::GET, 5), b"x", &mut buf).unwrap();
    buf[6] ^= 1;
    let Err(SocketError::Frame { consumed, .. }) = socket::parse(&buf[..len], 64) else {
        panic!("expected a bad frame");
    };
    assert_eq!(consumed, len);
}

#[test]
fn unknown_kinds_are_not_in_the_table() {
    assert!(kind::info(kind::HELLO).is_some());
    assert!(kind::info(0x00).is_none());
    assert!(kind::info(0x7F).is_none());
    assert!(kind::info(0xF0).is_none());
}

#[test]
fn no_framing_allocates() {
    let body = [0x55_u8; 200];
    let header = FrameHeader::new(kind::TOPIC_SAMPLE, 1);
    let (mut a, mut b) = ([0_u8; 400], [0_u8; 400]);
    assert_steady_no_allocs("plain frame", 1, 200, || {
        let len = frame::encode(header, &body, &mut a).unwrap();
        frame::decode(&a[..len]).unwrap();
    });
    assert_steady_no_allocs("socket frame", 1, 200, || {
        let len = socket::encode(header, &body, &mut a).unwrap();
        socket::parse(&a[..len], 400).unwrap();
    });
    let mut stream = StreamDecoder::<400>::new();
    assert_steady_no_allocs("cobs", 1, 200, || {
        let len = encode_message(header, &body, &mut b).unwrap();
        let mut rest = &b[..len];
        while !rest.is_empty() {
            let (used, _) = stream.push_slice(rest);
            rest = &rest[used..];
        }
    });
    let mut can = Reassembler::<400>::new();
    assert_steady_no_allocs("can", 1, 200, || {
        for segment in Segmenter::for_message(header, &body, SegmentMtu::FD) {
            can.push(segment.as_bytes()).unwrap();
        }
    });
}

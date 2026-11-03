//! Firmware transfer encodings (feature `xfer`): every kind 0x20-0x28 is encoded from the device
//! codec's views, compared byte for byte with `tests/fixtures/firmware_encodings.txt`, and (with
//! `alloc`) checked against the postcard encoding of the same fields through serde.
//!
//! The fixture is separate from `link_encodings.txt` (which covers the `Message` model): these
//! kinds are device-codec bodies. Regenerate with
//! `ORION_UPDATE_LINK_ENCODINGS=1 cargo test -p orion-link --features xfer --test firmware_encoding`.

#![cfg(feature = "xfer")]

use std::fmt::Write as _;
use std::path::PathBuf;

use orion_link::crc32c;
use orion_link::frame::{self, FrameHeader};
use orion_link::wire::{
    self, ActivateMode, ActivateView, ConfirmView, DeviceInfoView, Encode, XferAbortView,
    XferAckView, XferBeginView, XferChunkView, XferEndView, XferResultView, XferStatus, kind,
};

const FIXTURE: &str = "tests/fixtures/firmware_encodings.txt";

fn frame_of<B: Encode + ?Sized>(kind: u8, seq: u16, body: &B) -> Vec<u8> {
    let mut buf = vec![0u8; 256];
    let len = wire::encode_frame(kind, seq, body, &mut buf).expect("fits");
    buf.truncate(len);
    buf
}

/// Canonical frames, in fixture order.
fn canonical() -> Vec<(&'static str, u8, Vec<u8>)> {
    let data = b"abc";
    vec![
        (
            "device_info",
            kind::DEVICE_INFO,
            frame_of(
                kind::DEVICE_INFO,
                1,
                &DeviceInfoView {
                    fw_version: "1.2",
                    active_slot: 0,
                    slots: 2,
                    trial: false,
                    xfer_max_chunk: 103,
                    xfer_window: 1,
                    bootloader: "sim-boot 1",
                },
            ),
        ),
        (
            "xfer_begin",
            kind::XFER_BEGIN,
            frame_of(
                kind::XFER_BEGIN,
                2,
                &XferBeginView {
                    xfer_id: 7,
                    component: "motor-app",
                    version: "1.3",
                    total_len: 3_000,
                    chunk_len: 128,
                    image_crc32c: 0xDEAD_BEEF,
                    target_slot: 1,
                },
            ),
        ),
        (
            "xfer_chunk",
            kind::XFER_CHUNK,
            frame_of(
                kind::XFER_CHUNK,
                3,
                &XferChunkView {
                    xfer_id: 7,
                    offset: 128,
                    crc32c: crc32c(data),
                    data,
                },
            ),
        ),
        (
            "xfer_ack.ok",
            kind::XFER_ACK,
            frame_of(
                kind::XFER_ACK,
                4,
                &XferAckView {
                    xfer_id: 7,
                    next_offset: 131,
                    status: XferStatus::Ok,
                },
            ),
        ),
        (
            "xfer_ack.bad_crc",
            kind::XFER_ACK,
            frame_of(
                kind::XFER_ACK,
                5,
                &XferAckView {
                    xfer_id: 7,
                    next_offset: 128,
                    status: XferStatus::BadChunkCrc,
                },
            ),
        ),
        (
            "xfer_end",
            kind::XFER_END,
            frame_of(kind::XFER_END, 6, &XferEndView { xfer_id: 7 }),
        ),
        (
            "xfer_result.ok",
            kind::XFER_RESULT,
            frame_of(
                kind::XFER_RESULT,
                7,
                &XferResultView {
                    xfer_id: 7,
                    status: XferStatus::Ok,
                },
            ),
        ),
        (
            "xfer_result.bad_digest",
            kind::XFER_RESULT,
            frame_of(
                kind::XFER_RESULT,
                8,
                &XferResultView {
                    xfer_id: 7,
                    status: XferStatus::BadDigest,
                },
            ),
        ),
        (
            "xfer_abort",
            kind::XFER_ABORT,
            frame_of(
                kind::XFER_ABORT,
                9,
                &XferAbortView {
                    xfer_id: 7,
                    reason: XferStatus::Busy,
                },
            ),
        ),
        (
            "activate",
            kind::ACTIVATE,
            frame_of(
                kind::ACTIVATE,
                10,
                &ActivateView {
                    xfer_id: 7,
                    mode: ActivateMode::Trial,
                },
            ),
        ),
        (
            "confirm",
            kind::CONFIRM,
            frame_of(kind::CONFIRM, 11, &ConfirmView { slot: 1 }),
        ),
    ]
}

#[test]
fn kinds_are_stable_and_inside_the_reserved_range() {
    assert_eq!(
        [
            kind::DEVICE_INFO,
            kind::XFER_BEGIN,
            kind::XFER_CHUNK,
            kind::XFER_ACK,
            kind::XFER_END,
            kind::XFER_RESULT,
            kind::XFER_ABORT,
            kind::ACTIVATE,
            kind::CONFIRM,
        ],
        [0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28]
    );
    assert!((0x20..=0x28).all(kind::is_firmware));
}

#[test]
fn firmware_frames_match_the_recorded_bytes() {
    let mut text = String::from(
        "# Canonical firmware transfer frames checked by crates/link/tests/firmware_encoding.rs.\n\
         # Format: <name> <frame length> <lowercase hex of the complete frame>.\n",
    );
    for (name, _, frame) in canonical() {
        write!(text, "{name} {} ", frame.len()).unwrap();
        for byte in &frame {
            write!(text, "{byte:02x}").unwrap();
        }
        text.push('\n');
    }
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FIXTURE);
    if std::env::var_os("ORION_UPDATE_LINK_ENCODINGS").is_some() {
        std::fs::write(&path, &text).expect("fixture should be writable");
        return;
    }
    let expected = std::fs::read_to_string(&path).expect("fixture should be readable");
    assert_eq!(
        text, expected,
        "firmware wire encoding drifted; regenerate with ORION_UPDATE_LINK_ENCODINGS=1 only if the \
         change is intended (new kinds are additive, changed bodies need a protocol bump)"
    );
}

#[test]
fn every_frame_decodes_back_to_its_fields() {
    let mut buf = vec![0u8; 256];
    let frames = canonical();
    for (name, kind_, frame_bytes) in &frames {
        let view = frame::decode(frame_bytes).expect("frame");
        assert_eq!(view.kind(), *kind_, "{name}");
        assert!(view.header().is_current_version());
        let payload = view.payload();
        match *kind_ {
            kind::DEVICE_INFO => {
                let info = DeviceInfoView::decode(payload).unwrap();
                assert_eq!((info.fw_version, info.slots, info.trial), ("1.2", 2, false));
            }
            kind::XFER_BEGIN => {
                let begin = XferBeginView::decode(payload).unwrap();
                assert_eq!((begin.total_len, begin.target_slot), (3_000, 1));
            }
            kind::XFER_CHUNK => {
                let chunk = XferChunkView::decode(payload).unwrap();
                assert_eq!(chunk.data, b"abc");
                assert_eq!(chunk.crc32c, crc32c(b"abc"));
            }
            kind::XFER_ACK => {
                let ack = XferAckView::decode(payload).unwrap();
                assert!(ack.next_offset == 131 || ack.next_offset == 128);
            }
            kind::XFER_END => assert_eq!(XferEndView::decode(payload).unwrap().xfer_id, 7),
            kind::XFER_RESULT => {
                XferResultView::decode(payload).unwrap();
            }
            kind::XFER_ABORT => {
                assert_eq!(
                    XferAbortView::decode(payload).unwrap().reason,
                    XferStatus::Busy
                );
            }
            kind::ACTIVATE => {
                assert_eq!(
                    ActivateView::decode(payload).unwrap().mode,
                    ActivateMode::Trial
                );
            }
            kind::CONFIRM => assert_eq!(ConfirmView::decode(payload).unwrap().slot, 1),
            other => panic!("unexpected kind {other:#04x}"),
        }
        // Re-encoding the decoded body gives the same frame.
        let header = FrameHeader::new(*kind_, view.seq());
        let len = frame::encode(header, payload, &mut buf).unwrap();
        assert_eq!(&buf[..len], frame_bytes.as_slice(), "{name} re-encodes");
    }
}

#[test]
fn malformed_bodies_are_errors_not_panics() {
    for kind_ in [
        kind::DEVICE_INFO,
        kind::XFER_BEGIN,
        kind::XFER_CHUNK,
        kind::XFER_ACK,
        kind::XFER_END,
        kind::XFER_RESULT,
        kind::XFER_ABORT,
        kind::ACTIVATE,
        kind::CONFIRM,
    ] {
        let _ = kind_;
    }
    assert!(
        XferChunkView::decode(&[7, 0x80]).is_err(),
        "truncated varint"
    );
    assert!(
        XferChunkView::decode(&[7, 0, 0, 200]).is_err(),
        "data longer than the payload"
    );
    assert!(XferAckView::decode(&[]).is_err());
    assert!(
        DeviceInfoView::decode(&[0x02, b'a']).is_err(),
        "truncated string"
    );
    assert!(ActivateView::decode(&[1]).is_err(), "mode missing");
    // Unknown status and mode codes decode and keep their value.
    assert_eq!(XferStatus::from_code(42), XferStatus::Other(42));
    assert_eq!(ActivateMode::from_code(9), ActivateMode::Other(9));
}

#[cfg(feature = "alloc")]
mod postcard_twins {
    //! The same fields as serde structs: postcard must produce the bytes the codec writes.
    use super::*;
    use serde::Serialize;

    #[derive(Serialize)]
    struct Begin<'a> {
        xfer_id: u32,
        component: &'a str,
        version: &'a str,
        total_len: u32,
        chunk_len: u16,
        image_crc32c: u32,
        target_slot: u8,
    }

    #[derive(Serialize)]
    struct Chunk<'a> {
        xfer_id: u32,
        offset: u32,
        crc32c: u32,
        data: &'a [u8],
    }

    #[derive(Serialize)]
    struct Info<'a> {
        fw_version: &'a str,
        active_slot: u8,
        slots: u8,
        trial: bool,
        xfer_max_chunk: u16,
        xfer_window: u8,
        bootloader: &'a str,
    }

    #[test]
    fn codec_bodies_match_postcard_of_the_same_fields() {
        let begin = postcard::to_allocvec(&Begin {
            xfer_id: 7,
            component: "motor-app",
            version: "1.3",
            total_len: 3_000,
            chunk_len: 128,
            image_crc32c: 0xDEAD_BEEF,
            target_slot: 1,
        })
        .unwrap();
        assert_eq!(
            begin,
            frame::decode(&frame_of(
                kind::XFER_BEGIN,
                2,
                &XferBeginView {
                    xfer_id: 7,
                    component: "motor-app",
                    version: "1.3",
                    total_len: 3_000,
                    chunk_len: 128,
                    image_crc32c: 0xDEAD_BEEF,
                    target_slot: 1,
                }
            ))
            .unwrap()
            .payload()
        );
        let chunk = postcard::to_allocvec(&Chunk {
            xfer_id: 7,
            offset: 128,
            crc32c: crc32c(b"abc"),
            data: b"abc",
        })
        .unwrap();
        assert_eq!(chunk, frame::decode(&canonical()[2].2).unwrap().payload());
        let info = postcard::to_allocvec(&Info {
            fw_version: "1.2",
            active_slot: 0,
            slots: 2,
            trial: false,
            xfer_max_chunk: 103,
            xfer_window: 1,
            bootloader: "sim-boot 1",
        })
        .unwrap();
        assert_eq!(info, frame::decode(&canonical()[0].2).unwrap().payload());
    }
}

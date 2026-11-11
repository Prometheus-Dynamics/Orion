//! The C API over CAN (`--no-default-features --features standalone,ffi-can`), against a
//! host: the device says hello in segments, declares its topic and publishes.

#![cfg(feature = "ffi-can")]

use orion_mcu_template::ffi::*;
use orion_msg_topics::Sample;
use orion_wire::kind;

mod common;
use common::{CanReader, Peer, segments};

#[test]
fn a_c_firmware_on_can_declares_and_publishes() {
    // SAFETY: literals.
    unsafe {
        assert!(orion_init(c"can-board".as_ptr()));
        assert_eq!(
            orion_topic(c"can-board.temp".as_ptr(), ORION_FIELD_INT, c"mC".as_ptr()),
            0
        );
    }
    let (mut peer, mut reader) = (Peer::default(), CanReader::default());
    let mut published = 0;
    for now in 1..300_u32 {
        let events = orion_poll(now);
        if orion_subscribers(0) > 0 && orion_publish_int(0, u64::from(now), 21_500) == 0 {
            published += 1;
        }
        let _ = events;
        let mut data = [0_u8; 64];
        loop {
            // SAFETY: `data` holds 64 bytes.
            let n = unsafe { orion_can_tx(data.as_mut_ptr()) };
            if n == 0 {
                break;
            }
            assert!(n <= 8, "classic CAN");
            if let Some(sent) = reader.read(&data[..n]) {
                for frame in peer.answer(sent) {
                    for segment in segments(&frame) {
                        // SAFETY: valid for its length.
                        unsafe { orion_can_rx(segment.as_ptr(), segment.len()) };
                    }
                }
            }
        }
    }
    assert_eq!(peer.name, "can-board");
    assert_eq!(orion_state(), 2);
    let samples = peer.take(kind::TOPIC_SAMPLE);
    assert!(
        published > 10 && samples.len() == published,
        "{published} {}",
        samples.len()
    );
    let sample: Sample = samples[0].body();
    assert_eq!(sample.payload.0, &21_500_i64.to_le_bytes()[..]);
}

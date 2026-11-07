//! Any kind and payload: `Message::decode_payload` returns a message or an error, never panics,
//! and a message it returns encodes again and decodes to the same message.
#![no_main]

use libfuzzer_sys::fuzz_target;
use orion_link::message::Message;

fuzz_target!(|data: &[u8]| {
    let Some((&kind, payload)) = data.split_first() else {
        return;
    };
    let Ok(message) = Message::decode_payload(kind, payload) else {
        return;
    };
    let mut buf = vec![0u8; 64 * 1024];
    if let Ok(len) = message.encode(1, &mut buf) {
        let frame = orion_link::frame::decode(&buf[..len]).expect("an encoded frame decodes");
        let again = Message::decode(&frame).expect("an encoded message decodes");
        assert_eq!(again, message);
    }
});

#![no_main]

use orion_wire_vectors::decoders;

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    decoders::socket(data);
});

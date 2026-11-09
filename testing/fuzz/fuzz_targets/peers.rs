#![no_main]

use orion_wire::kind::Concept;
use orion_wire_vectors::decoders;

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    decoders::decode_concept(Concept::Peers, data);
});

#![no_main]

// A reader over a ring whose contents are the input: a hostile header, or a real ring that then
// turns hostile. See `orion_ring_model::hostile`.
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    orion_ring_model::hostile(data);
});

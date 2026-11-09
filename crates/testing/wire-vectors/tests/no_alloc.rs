//! Decoding a body takes nothing from the allocator.

use orion_measure::harness::assert_steady_no_allocs;
use orion_wire_vectors::{all, decoders};

#[global_allocator]
static GLOBAL: orion_measure::CountingAlloc = orion_measure::CountingAlloc::system();

#[test]
fn no_body_allocates_to_decode_or_encode() {
    for sample in all() {
        assert_steady_no_allocs("body", 1, 50, || {
            assert!(decoders::decode_kind(sample.kind, &sample.bytes));
        });
    }
}

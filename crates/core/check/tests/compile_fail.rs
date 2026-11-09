//! These don't compile, on purpose. That is the guarantee.
#![cfg(feature = "enforce")]

#[test]
fn the_check_cannot_be_skipped() {
    trybuild::TestCases::new().compile_fail("tests/ui/*.rs");
}

//! The shared library's SONAME carries the surface's major version (`ORION_ABI_MAJOR`):
//! `liborion.so.1`. `scripts/capi.sh` checks the two agree.

const MAJOR: u32 = 1;

fn main() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux") {
        println!("cargo::rustc-cdylib-link-arg=-Wl,-soname,liborion.so.{MAJOR}");
    }
}

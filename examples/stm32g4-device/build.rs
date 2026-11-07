//! Puts `memory.x` and an empty `device.x` where the `cortex-m-rt` linker script finds them.
use std::{env, fs, path::PathBuf};

fn main() {
    let out = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR"));
    fs::copy("memory.x", out.join("memory.x")).expect("copy memory.x");
    // `cortex-m-rt`'s `device` feature includes `device.x` for the interrupt handler names. The
    // firmware enables no interrupt, so it has none.
    fs::write(out.join("device.x"), "").expect("write device.x");
    println!("cargo:rustc-link-search={}", out.display());
    println!("cargo:rerun-if-changed=memory.x");
}

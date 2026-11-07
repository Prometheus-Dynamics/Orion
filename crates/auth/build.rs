//! Tells `link_channel` whether ChaCha20 runs its SIMD backend. On aarch64, chacha20 0.9 uses NEON
//! only when the build passes `--cfg chacha20_force_neon` (RUSTFLAGS, or `rustflags` in
//! `.cargo/config.toml` of the top-level build); without it, sealing a 4 KiB frame takes 37 us
//! instead of 16 us on a Cortex-A76. The node warns when a `tcp:` link runs on the portable
//! code (`link_channel::PORTABLE_CHACHA20`).

fn main() {
    println!("cargo::rustc-check-cfg=cfg(link_channel_portable_chacha20)");
    println!("cargo::rerun-if-env-changed=CARGO_ENCODED_RUSTFLAGS");
    if std::env::var("CARGO_CFG_TARGET_ARCH").as_deref() != Ok("aarch64") {
        // x86 picks AVX2 or SSE2 at run time; other targets have no SIMD backend.
        return;
    }
    let flags = std::env::var("CARGO_ENCODED_RUSTFLAGS").unwrap_or_default();
    let flags: Vec<&str> = flags.split('\x1f').collect();
    let neon = flags
        .windows(2)
        .any(|pair| pair == ["--cfg", "chacha20_force_neon"])
        || flags.contains(&"--cfg=chacha20_force_neon");
    if !neon {
        println!("cargo::rustc-cfg=link_channel_portable_chacha20");
    }
}

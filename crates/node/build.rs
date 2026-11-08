//! Defines two cfg names so feature combinations are gated on one name each:
//!
//! - `peer_sync`: a peer-sync transport is compiled in (`transport-http` or `peer-tcp`), so the
//!   transport-independent sync engine is built.
//! - `net_transport`: any network transport is compiled in (`transport-http`, `transport-tcp`,
//!   `transport-quic` or `peer-tcp`), so per-endpoint communication metrics are built.

fn main() {
    println!("cargo::rustc-check-cfg=cfg(peer_sync)");
    println!("cargo::rustc-check-cfg=cfg(net_transport)");
    let enabled = |feature: &str| std::env::var_os(format!("CARGO_FEATURE_{feature}")).is_some();
    let peer_sync = enabled("TRANSPORT_HTTP") || enabled("PEER_TCP");
    if peer_sync {
        println!("cargo::rustc-cfg=peer_sync");
    }
    if peer_sync || enabled("TRANSPORT_TCP") || enabled("TRANSPORT_QUIC") {
        println!("cargo::rustc-cfg=net_transport");
    }
    state_layout();
    build_floor();
}

/// `ORION_BUILD_FLOOR_MS`: the earliest date a time source may report. The build's own date when
/// the build states one (`SOURCE_DATE_EPOCH`, so builds stay reproducible), and never below
/// 2026-01-01.
fn build_floor() {
    const FLOOR_MS: i64 = 1_767_225_600_000;
    println!("cargo::rerun-if-env-changed=SOURCE_DATE_EPOCH");
    let stated = std::env::var("SOURCE_DATE_EPOCH")
        .ok()
        .and_then(|value| value.trim().parse::<i64>().ok())
        .map_or(0, |seconds| seconds.saturating_mul(1000));
    println!(
        "cargo::rustc-env=ORION_BUILD_FLOOR_MS={}",
        stated.max(FLOOR_MS)
    );
}

/// `ORION_STATE_LAYOUT`: a hash of the sources of the persisted types (`src/storage/layout.rs`).
fn state_layout() {
    use std::path::{Path, PathBuf};

    fn collect(dir: &Path, files: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for path in entries.flatten().map(|entry| entry.path()) {
            if path.is_dir() {
                collect(&path, files);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                files.push(path);
            }
        }
    }

    let mut files = vec![PathBuf::from("src/storage.rs")];
    for dir in ["../core/src", "../control-plane/src"] {
        println!("cargo::rerun-if-changed={dir}");
        collect(Path::new(dir), &mut files);
    }
    println!("cargo::rerun-if-changed=src/storage.rs");
    println!("cargo::rerun-if-changed=build.rs");
    files.sort();
    // FNV-1a: the stamp has to change, not to resist anyone.
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for file in &files {
        for byte in std::fs::read(file).unwrap_or_default() {
            hash = (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    println!("cargo::rustc-env=ORION_STATE_LAYOUT={hash:016x}");
}

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
}

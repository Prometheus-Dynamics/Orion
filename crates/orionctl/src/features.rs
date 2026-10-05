//! Optional orionctl capabilities (Cargo features) and the error for a disabled one.

/// Error returned when a request needs a capability this build was compiled without.
#[cfg_attr(
    all(feature = "http", feature = "yaml", feature = "toml"),
    allow(dead_code)
)]
pub(crate) fn disabled(capability: &str, feature: &str) -> String {
    format!(
        "{capability} is not available: this orionctl was built without the `{feature}` feature \
         (rebuild with `cargo build -p orionctl --features {feature}`, or use the default build)"
    )
}

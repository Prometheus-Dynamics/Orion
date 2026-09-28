//! Control-protocol wire compatibility.
//!
//! Orion's control protocol (local IPC and peer/`orionctl --http` HTTP) carries rkyv archives,
//! which are layout-exact: any added, removed, reordered, or resized field in a protocol type makes
//! archives from other builds undecodable. Instead of surfacing that as an opaque rkyv validation
//! error, every control exchange carries [`CONTROL_PROTOCOL_VERSION`] in a small fixed-layout
//! preamble (IPC) or header (HTTP) that is checked *before* any archived payload is decoded.
//!
//! This is the single source of truth for that version. See `docs/protocol-compatibility.md`.

use std::fmt;

/// Wire-layout version of the Orion control protocol.
///
/// Bump this whenever the rkyv layout of any control-protocol type changes (for example a field
/// added to `NodeObservabilitySnapshot`), and update [`CONTROL_PROTOCOL_LAYOUT_FINGERPRINT`] in
/// the same change. Peers with different values refuse to talk to each other with a
/// [`ControlProtocolMismatch`] error instead of failing on undecodable archives.
///
/// History:
/// - `1`: implicit, unversioned layout before the preamble existed.
/// - `2`: `NodeObservabilitySnapshot::resource_usage` section; version preamble/header added.
pub const CONTROL_PROTOCOL_VERSION: u16 = 2;

/// Fingerprint of the archived layout of the control-protocol types at
/// [`CONTROL_PROTOCOL_VERSION`].
///
/// Guarded by `crates/orion/tests/control_protocol_layout.rs`, which recomputes it from the
/// archived type sizes/alignments and fails when the layout changes without this constant (and
/// the version) being updated.
pub const CONTROL_PROTOCOL_LAYOUT_FINGERPRINT: u64 = 0x6c28_18a1_3bdd_2f91;

/// HTTP header carrying [`CONTROL_PROTOCOL_VERSION`] on every control request and response.
pub const CONTROL_PROTOCOL_HTTP_HEADER: &str = "x-orion-control-protocol";

/// Two sides of a control connection speak different control-protocol wire versions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ControlProtocolMismatch {
    /// Version spoken by this process.
    pub local: u16,
    /// Version announced by the remote side.
    pub remote: u16,
}

impl ControlProtocolMismatch {
    pub const fn new(local: u16, remote: u16) -> Self {
        Self { local, remote }
    }

    /// Returns `Ok(())` when `remote` matches [`CONTROL_PROTOCOL_VERSION`].
    pub const fn check(remote: u16) -> Result<(), Self> {
        if remote == CONTROL_PROTOCOL_VERSION {
            Ok(())
        } else {
            Err(Self::new(CONTROL_PROTOCOL_VERSION, remote))
        }
    }
}

impl fmt::Display for ControlProtocolMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Orion control protocol mismatch: this side speaks v{}, the remote side speaks v{}; \
             upgrade orionctl, client libraries, and orion-node together so they share the same \
             protocol version",
            self.local, self.remote
        )
    }
}

impl std::error::Error for ControlProtocolMismatch {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_accepts_only_the_local_version() {
        assert_eq!(
            ControlProtocolMismatch::check(CONTROL_PROTOCOL_VERSION),
            Ok(())
        );
        let err = ControlProtocolMismatch::check(CONTROL_PROTOCOL_VERSION + 1)
            .expect_err("different version should be rejected");
        assert_eq!(err.local, CONTROL_PROTOCOL_VERSION);
        assert_eq!(err.remote, CONTROL_PROTOCOL_VERSION + 1);
        assert!(err.to_string().contains("upgrade orionctl"));
    }
}

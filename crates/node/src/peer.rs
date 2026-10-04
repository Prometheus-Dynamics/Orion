use orion::{
    CompatibilityState, NodeId, OrionError, Revision,
    control_plane::{
        DesiredStateSectionFingerprints, PeerSyncErrorKind, PeerSyncStatus as PublicPeerSyncStatus,
    },
};
use orion_core::{PeerBaseUrl, PublicKeyHex};
use std::{fmt, path::PathBuf, time::Duration};

const MAX_BACKOFF_EXPONENT: u32 = 10;
const MIN_BACKOFF_MS: u128 = 1;
const FNV1A_OFFSET_BASIS: u64 = 1_469_598_103_934_665_603;
const FNV1A_PRIME: u64 = 1_099_511_628_211;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PeerConfig {
    pub node_id: NodeId,
    pub base_url: PeerBaseUrl,
    pub trusted_public_key_hex: Option<PublicKeyHex>,
    pub tls_root_cert_path: Option<PathBuf>,
}

impl PeerConfig {
    pub fn new(node_id: impl Into<NodeId>, base_url: impl Into<PeerBaseUrl>) -> Self {
        Self {
            node_id: node_id.into(),
            base_url: base_url.into(),
            trusted_public_key_hex: None,
            tls_root_cert_path: None,
        }
    }

    pub fn try_new(
        node_id: impl Into<String>,
        base_url: impl Into<String>,
    ) -> Result<Self, OrionError> {
        Ok(Self {
            node_id: NodeId::try_new(node_id)?,
            base_url: PeerBaseUrl::try_new(base_url)?,
            trusted_public_key_hex: None,
            tls_root_cert_path: None,
        })
    }

    pub fn try_with_trusted_public_key_hex(
        mut self,
        public_key_hex: impl Into<String>,
    ) -> Result<Self, OrionError> {
        self.trusted_public_key_hex = Some(PublicKeyHex::try_new(public_key_hex)?);
        Ok(self)
    }

    pub fn with_trusted_public_key_hex(mut self, public_key_hex: impl Into<PublicKeyHex>) -> Self {
        self.trusted_public_key_hex = Some(public_key_hex.into());
        self
    }

    pub fn with_tls_root_cert_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.tls_root_cert_path = Some(path.into());
        self
    }
}

impl fmt::Display for PeerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.node_id, self.base_url)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerTrustStatus {
    pub node_id: NodeId,
    pub base_url: Option<PeerBaseUrl>,
    pub configured_public_key_hex: Option<PublicKeyHex>,
    pub trusted_public_key_hex: Option<PublicKeyHex>,
    pub configured_tls_root_cert_path: Option<String>,
    pub learned_tls_root_cert_fingerprint: Option<String>,
    pub revoked: bool,
    pub sync_status: Option<PublicPeerSyncStatus>,
    pub last_error: Option<String>,
    pub last_error_kind: Option<PeerSyncErrorKind>,
    pub troubleshooting_hint: Option<String>,
}

#[derive(Clone, Copy, Debug)]
pub struct PeerSyncBackoff<'a> {
    pub base: Duration,
    pub max: Duration,
    pub max_jitter_ms: u64,
    pub retry_scope: &'a str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PeerSyncStatus {
    Configured,
    Negotiating,
    Ready,
    BackingOff,
    Syncing,
    Synced,
    Error,
}

impl From<PeerSyncStatus> for PublicPeerSyncStatus {
    fn from(value: PeerSyncStatus) -> Self {
        match value {
            PeerSyncStatus::Configured => Self::Configured,
            PeerSyncStatus::Negotiating => Self::Negotiating,
            PeerSyncStatus::Ready => Self::Ready,
            PeerSyncStatus::BackingOff => Self::BackingOff,
            PeerSyncStatus::Syncing => Self::Syncing,
            PeerSyncStatus::Synced => Self::Synced,
            PeerSyncStatus::Error => Self::Error,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerState {
    pub node_id: NodeId,
    pub base_url: PeerBaseUrl,
    pub tls_root_cert_path: Option<PathBuf>,
    pub compatibility: Option<CompatibilityState>,
    pub desired_revision: Revision,
    pub desired_fingerprint: u64,
    pub desired_section_fingerprints: DesiredStateSectionFingerprints,
    pub observed_revision: Revision,
    pub applied_revision: Revision,
    pub sync_status: PeerSyncStatus,
    pub last_error: Option<String>,
    pub last_error_kind: Option<PeerSyncErrorKind>,
    pub consecutive_failures: u32,
    pub next_sync_allowed_at_ms: Option<u64>,
    /// Fingerprint of the local observed slice last pushed to this peer, and when.
    pub observed_pushed_fingerprint: Option<u64>,
    pub observed_pushed_at_ms: Option<u64>,
}

impl PeerState {
    pub fn from_config(config: PeerConfig) -> Self {
        Self {
            node_id: config.node_id,
            base_url: config.base_url,
            tls_root_cert_path: config.tls_root_cert_path,
            compatibility: None,
            desired_revision: Revision::ZERO,
            desired_fingerprint: 0,
            desired_section_fingerprints: DesiredStateSectionFingerprints {
                nodes: 0,
                artifacts: 0,
                workloads: 0,
                resources: 0,
                providers: 0,
                executors: 0,
                leases: 0,
            },
            observed_revision: Revision::ZERO,
            applied_revision: Revision::ZERO,
            sync_status: PeerSyncStatus::Configured,
            last_error: None,
            last_error_kind: None,
            consecutive_failures: 0,
            next_sync_allowed_at_ms: None,
            observed_pushed_fingerprint: None,
            observed_pushed_at_ms: None,
        }
    }

    /// `true` when the local observed slice changed since the last push to this peer, or the
    /// last push is older than `refresh_ms`.
    pub fn observed_push_due(&self, fingerprint: u64, now_ms: u64, refresh_ms: u64) -> bool {
        self.observed_pushed_fingerprint != Some(fingerprint)
            || self
                .observed_pushed_at_ms
                .is_none_or(|at| now_ms.saturating_sub(at) >= refresh_ms)
    }

    pub fn note_observed_pushed(&mut self, fingerprint: u64, now_ms: u64) {
        self.observed_pushed_fingerprint = Some(fingerprint);
        self.observed_pushed_at_ms = Some(now_ms);
    }

    pub fn record_hello(
        &mut self,
        desired_revision: Revision,
        desired_fingerprint: u64,
        desired_section_fingerprints: DesiredStateSectionFingerprints,
        observed_revision: Revision,
        applied_revision: Revision,
        compatibility: CompatibilityState,
    ) {
        self.desired_revision = desired_revision;
        self.desired_fingerprint = desired_fingerprint;
        self.desired_section_fingerprints = desired_section_fingerprints;
        self.observed_revision = observed_revision;
        self.applied_revision = applied_revision;
        self.compatibility = Some(compatibility);
        self.sync_status = PeerSyncStatus::Ready;
        self.last_error = None;
        self.last_error_kind = None;
        self.consecutive_failures = 0;
        self.next_sync_allowed_at_ms = None;
    }

    pub fn record_assumed_desired_state(
        &mut self,
        desired_revision: Revision,
        desired_fingerprint: u64,
        desired_section_fingerprints: DesiredStateSectionFingerprints,
        compatibility: CompatibilityState,
    ) {
        self.desired_revision = desired_revision;
        self.desired_fingerprint = desired_fingerprint;
        self.desired_section_fingerprints = desired_section_fingerprints;
        self.compatibility = Some(compatibility);
        self.sync_status = PeerSyncStatus::Ready;
        self.last_error = None;
        self.last_error_kind = None;
        self.consecutive_failures = 0;
        self.next_sync_allowed_at_ms = None;
    }

    pub fn set_sync_status(&mut self, status: PeerSyncStatus) {
        self.sync_status = status;
    }

    pub fn set_error(&mut self, error: impl Into<String>) {
        self.sync_status = PeerSyncStatus::Error;
        self.last_error = Some(error.into());
        self.last_error_kind = None;
    }

    pub fn note_sync_success(&mut self) {
        self.sync_status = PeerSyncStatus::Synced;
        self.last_error = None;
        self.last_error_kind = None;
        self.consecutive_failures = 0;
        self.next_sync_allowed_at_ms = None;
    }

    pub fn set_last_error_kind(&mut self, kind: Option<PeerSyncErrorKind>) {
        self.last_error_kind = kind;
    }

    pub fn note_sync_failure(
        &mut self,
        error: impl Into<String>,
        error_kind: Option<PeerSyncErrorKind>,
        now_ms: u64,
        backoff: PeerSyncBackoff<'_>,
    ) {
        self.last_error = Some(error.into());
        self.last_error_kind = error_kind;
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);

        let exponent = self
            .consecutive_failures
            .saturating_sub(1)
            .min(MAX_BACKOFF_EXPONENT);
        let multiplier = 1u128 << exponent;
        let base_ms = backoff.base.as_millis().max(MIN_BACKOFF_MS);
        let max_ms = backoff.max.as_millis().max(base_ms);
        let mut delay_ms = base_ms.saturating_mul(multiplier).min(max_ms) as u64;

        if backoff.max_jitter_ms > 0 {
            delay_ms = delay_ms
                .saturating_add(self.retry_jitter_ms(backoff.max_jitter_ms, backoff.retry_scope));
        }
        let capped_delay_ms = delay_ms.min(max_ms as u64);
        self.next_sync_allowed_at_ms = Some(now_ms.saturating_add(capped_delay_ms));
        self.sync_status = PeerSyncStatus::BackingOff;
    }

    pub fn can_attempt_sync_at(&self, now_ms: u64) -> bool {
        self.next_sync_allowed_at_ms
            .map(|next| now_ms >= next)
            .unwrap_or(true)
    }

    fn retry_jitter_ms(&self, max_jitter_ms: u64, retry_scope: &str) -> u64 {
        if max_jitter_ms == 0 {
            return 0;
        }
        let seed = format!(
            "{retry_scope}:{}:{}",
            self.node_id, self.consecutive_failures
        );
        let mut hash = FNV1A_OFFSET_BASIS;
        for byte in seed.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV1A_PRIME);
        }
        hash % max_jitter_ms.saturating_add(1)
    }
}

/// URL scheme of peers reached over the plain TCP peer transport (feature `peer-tcp`).
pub const PEER_TCP_SCHEME: &str = "orion+tcp";

/// Transport a peer is synced over, chosen by the scheme of its base URL. See
/// `docs/peer-sync.md`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PeerTransportKind {
    /// `http://` (feature `transport-http`).
    Http,
    /// `https://` (feature `transport-http`).
    Https,
    /// `orion+tcp://` (feature `peer-tcp`).
    Tcp,
}

impl PeerTransportKind {
    /// Maps a peer base URL to its transport, or explains why the scheme is unknown.
    pub fn from_base_url(base_url: &str) -> Result<Self, String> {
        let Some((scheme, rest)) = base_url.split_once("://") else {
            return Err(format!(
                "peer URL `{base_url}` has no scheme; use http://, https:// or {PEER_TCP_SCHEME}://"
            ));
        };
        if rest.is_empty() {
            return Err(format!("peer URL `{base_url}` has no host"));
        }
        match scheme.to_ascii_lowercase().as_str() {
            "http" => Ok(Self::Http),
            "https" => Ok(Self::Https),
            PEER_TCP_SCHEME => Ok(Self::Tcp),
            other => Err(format!(
                "peer URL `{base_url}` uses unsupported scheme `{other}`; use http://, https:// \
                 or {PEER_TCP_SCHEME}://"
            )),
        }
    }

    /// The cargo feature of `orion-node` that provides this transport.
    pub const fn cargo_feature(self) -> &'static str {
        match self {
            Self::Http | Self::Https => "transport-http",
            Self::Tcp => "peer-tcp",
        }
    }

    /// Whether this build of `orion-node` includes the transport.
    pub const fn is_compiled_in(self) -> bool {
        match self {
            Self::Http | Self::Https => cfg!(feature = "transport-http"),
            Self::Tcp => cfg!(feature = "peer-tcp"),
        }
    }

    /// Short label for logs and metrics (`http` or `tcp`).
    pub const fn label(self) -> &'static str {
        match self {
            Self::Http | Self::Https => "http",
            Self::Tcp => "tcp",
        }
    }

    /// Checks that this build can sync with a peer at `base_url`.
    pub fn check_supported(base_url: &str) -> Result<Self, String> {
        let kind = Self::from_base_url(base_url)?;
        if kind.is_compiled_in() {
            Ok(kind)
        } else {
            Err(format!(
                "peer URL `{base_url}` needs orion-node built with the `{}` feature",
                kind.cargo_feature()
            ))
        }
    }
}

/// Splits an `orion+tcp://host:port[/]` URL into its `host:port` authority.
#[cfg(any(test, feature = "peer-tcp"))]
pub(crate) fn peer_tcp_authority(base_url: &str) -> Result<&str, String> {
    let rest = base_url
        .split_once("://")
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case(PEER_TCP_SCHEME))
        .map(|(_, rest)| rest)
        .ok_or_else(|| format!("`{base_url}` is not an {PEER_TCP_SCHEME}:// URL"))?;
    let authority = rest.trim_end_matches('/');
    if authority.is_empty() || authority.contains('/') || !authority.contains(':') {
        return Err(format!(
            "`{base_url}` must have the form {PEER_TCP_SCHEME}://host:port"
        ));
    }
    Ok(authority)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_transport_kind_follows_the_url_scheme() {
        assert_eq!(
            PeerTransportKind::from_base_url("http://a:1"),
            Ok(PeerTransportKind::Http)
        );
        assert_eq!(
            PeerTransportKind::from_base_url("HTTPS://a:1"),
            Ok(PeerTransportKind::Https)
        );
        assert_eq!(
            PeerTransportKind::from_base_url("orion+tcp://10.0.0.2:9200"),
            Ok(PeerTransportKind::Tcp)
        );
        assert!(PeerTransportKind::from_base_url("ftp://a:1").is_err());
        assert!(PeerTransportKind::from_base_url("a:1").is_err());
        assert_eq!(
            peer_tcp_authority("orion+tcp://10.0.0.2:9200/"),
            Ok("10.0.0.2:9200")
        );
        assert!(peer_tcp_authority("orion+tcp://host").is_err());
        assert!(peer_tcp_authority("http://host:1").is_err());
    }

    #[test]
    fn retry_jitter_uses_local_retry_scope() {
        let peer = PeerState::from_config(PeerConfig::new("node-peer", "http://peer.test"));

        let jitter_a = peer.retry_jitter_ms(150, "node-a");
        let jitter_b = peer.retry_jitter_ms(150, "node-b");

        assert_ne!(jitter_a, jitter_b);
    }
}

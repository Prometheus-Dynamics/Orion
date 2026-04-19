use orion::{
    CompatibilityState, NodeId, Revision,
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
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_jitter_uses_local_retry_scope() {
        let peer = PeerState::from_config(PeerConfig::new("node-peer", "http://peer.test"));

        let jitter_a = peer.retry_jitter_ms(150, "node-a");
        let jitter_b = peer.retry_jitter_ms(150, "node-b");

        assert_ne!(jitter_a, jitter_b);
    }
}

//! What sync keeps about one peer.

use std::collections::HashSet;

use orion_types::HlcTimestamp;

/// Where sync with a peer is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerStatus {
    /// There is no connection to it.
    Down,
    /// Connected, and the two have not been seen to agree since.
    Syncing,
    /// The two agreed when it was last heard from.
    InSync,
    /// It was away longer than deletes are remembered. It is not synced with.
    Stale,
}

#[derive(Debug, Default)]
pub(crate) struct Peer {
    pub(crate) up: bool,
    pub(crate) stale: bool,
    in_sync: bool,
    /// The newest stamp it said it has.
    pub(crate) newest: HlcTimestamp,
    /// The digest it last said it has.
    pub(crate) fingerprint: u64,
    /// Whether it has said one since it connected.
    pub(crate) heard_summary: bool,
    /// The digest this node last told it.
    pub(crate) told: Option<u64>,
    /// The pair of digests it was last asked to send for.
    pub(crate) asked: Option<(u64, u64)>,
    /// The pair of digests everything was last sent for.
    pub(crate) full_for: Option<(u64, u64)>,
    pub(crate) last_full_ms: Option<u64>,
    /// When everything may be sent again, if it was held back.
    pub(crate) full_due: Option<u64>,
    /// When the two last agreed.
    pub(crate) agreed_ms: Option<u64>,
    /// Versions taken from it since the last changes were sent on: not sent back to it.
    heard: HashSet<(u8, String, HlcTimestamp)>,
}

impl Peer {
    pub(crate) const fn status(&self) -> PeerStatus {
        match (self.up, self.stale, self.in_sync) {
            (false, ..) => PeerStatus::Down,
            (true, true, _) => PeerStatus::Stale,
            (true, false, true) => PeerStatus::InSync,
            (true, false, false) => PeerStatus::Syncing,
        }
    }

    pub(crate) fn connected(&mut self, now_ms: u64, stale_after_ms: u64) {
        self.up = true;
        self.in_sync = false;
        self.heard_summary = false;
        self.told = None;
        self.asked = None;
        self.full_for = None;
        self.full_due = None;
        self.heard.clear();
        self.stale = self
            .agreed_ms
            .is_some_and(|at| now_ms.saturating_sub(at) > stale_after_ms);
    }

    pub(crate) fn disconnected(&mut self, now_ms: u64) {
        if self.up && self.in_sync {
            // They agreed for as long as they were connected.
            self.agreed_ms = Some(now_ms);
        }
        self.up = false;
        self.in_sync = false;
        self.full_due = None;
        self.heard.clear();
    }

    pub(crate) const fn agreed(&mut self, now_ms: u64) {
        self.in_sync = true;
        self.agreed_ms = Some(now_ms);
        self.full_due = None;
    }

    pub(crate) const fn differs(&mut self) {
        self.in_sync = false;
    }

    pub(crate) fn heard(&mut self, class: u8, key: &str, stamp: HlcTimestamp) {
        self.heard.insert((class, key.to_owned(), stamp));
    }

    pub(crate) fn has_heard(&self, class: u8, key: &str, stamp: HlcTimestamp) -> bool {
        !self.heard.is_empty() && self.heard.contains(&(class, key.to_owned(), stamp))
    }

    pub(crate) fn forget_heard(&mut self) {
        self.heard.clear();
    }
}

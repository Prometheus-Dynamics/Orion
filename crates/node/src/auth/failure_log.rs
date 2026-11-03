//! Rate-limited warn logs for failed authentication (hellos, operator requests, peer responses).
//!
//! One line per `(side, peer, check)` per window, with how many failures the window suppressed.
//! Fields are identities, fingerprints and lengths only: never key or signature bytes.

use std::{
    collections::BTreeMap,
    sync::{Mutex, PoisonError},
    time::{Duration, Instant},
};
use tracing::warn;

const WINDOW: Duration = Duration::from_secs(10);

static BUCKETS: Mutex<BTreeMap<String, (Option<Instant>, u64)>> = Mutex::new(BTreeMap::new());

/// One failed authentication check.
pub(crate) struct AuthFailure<'a> {
    /// Which side and message failed, e.g. `"node serves peer request"`.
    pub(crate) side: &'static str,
    /// The peer's address, or its node or operator id when no address is known.
    pub(crate) peer: &'a str,
    /// Fingerprint of the key the peer presented (`sha256:...`), or `"none"`.
    pub(crate) key_fingerprint: &'a str,
    /// Which check failed, e.g. `"signature"` or `"key"`.
    pub(crate) check: &'static str,
    /// Length of the signed transcript: the canonical bytes, or the request frame on the serve path.
    pub(crate) transcript_len: usize,
    /// Length of the nonce the message carries, or 0 when it has none.
    pub(crate) nonce_len: usize,
    pub(crate) error: &'a str,
}

/// Logs `failure` at warn, at most once per window for its `(side, peer, check)`.
pub(crate) fn record(failure: AuthFailure<'_>) {
    let key = format!("{}|{}|{}", failure.side, failure.peer, failure.check);
    let now = Instant::now();
    let suppressed = {
        let mut buckets = BUCKETS.lock().unwrap_or_else(PoisonError::into_inner);
        let entry = buckets.entry(key).or_insert((None, 0));
        if entry
            .0
            .is_some_and(|last| now.duration_since(last) < WINDOW)
        {
            entry.1 += 1;
            return;
        }
        let suppressed = entry.1;
        *entry = (Some(now), 0);
        suppressed
    };
    warn!(
        side = failure.side,
        peer = failure.peer,
        key_fingerprint = failure.key_fingerprint,
        check = failure.check,
        transcript_len = failure.transcript_len,
        nonce_len = failure.nonce_len,
        suppressed,
        error = failure.error,
        "authentication failed"
    );
}

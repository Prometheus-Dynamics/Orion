//! Rate-limited warn logs for failed authentication (hellos, operator requests, peer responses).
//!
//! The first failure of a kind (side, host without port, check, error) logs in full; repeats log
//! one summary line per window with how many were suppressed.
//! Fields are identities, fingerprints and lengths only: never key or signature bytes.

use std::{
    collections::BTreeMap,
    sync::{Mutex, PoisonError},
    time::{Duration, Instant},
};
use tracing::warn;

/// After the first line of a repeating failure, at most one summary line per window.
const WINDOW: Duration = Duration::from_secs(300);
/// Distinct failures tracked at once; past it the stalest is forgotten (and would log again).
const MAX_TRACKED: usize = 256;

/// Per failure key: when its last line was logged, and how many failures it suppressed since.
static BUCKETS: Mutex<BTreeMap<String, (Instant, u64)>> = Mutex::new(BTreeMap::new());

pub(crate) struct AuthFailure<'a> {
    pub(crate) side: &'static str,
    pub(crate) peer: &'a str,
    pub(crate) key_fingerprint: &'a str,
    pub(crate) check: &'static str,
    pub(crate) transcript_len: usize,
    pub(crate) nonce_len: usize,
    pub(crate) error: &'a str,
}

/// The peer without its source port: every new connection from one host comes from another
/// ephemeral port, and is still the same failing client.
fn host(peer: &str) -> &str {
    match peer.rsplit_once(':') {
        Some((host, port)) if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) => {
            host.trim_start_matches('[').trim_end_matches(']')
        }
        _ => peer,
    }
}

/// What to log for one failure: `None` to stay quiet, `Some(n)` for a line that also reports
/// the `n` identical failures suppressed since the last one.
fn admit(key: String, now: Instant) -> Option<u64> {
    let mut buckets = BUCKETS.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(entry) = buckets.get_mut(&key) {
        if now.duration_since(entry.0) < WINDOW {
            entry.1 += 1;
            return None;
        }
        let suppressed = entry.1;
        *entry = (now, 0);
        return Some(suppressed);
    }
    if buckets.len() >= MAX_TRACKED {
        let stalest = buckets
            .iter()
            .min_by_key(|(_, (at, _))| *at)
            .map(|(key, _)| key.clone());
        if let Some(stalest) = stalest {
            buckets.remove(&stalest);
        }
    }
    buckets.insert(key, (now, 0));
    Some(0)
}

/// Logs a failed authentication: the first time in full, then a summary at most once per
/// [`WINDOW`] with the count of identical failures (same side, host, check and error).
pub(crate) fn record(failure: AuthFailure<'_>) {
    let host = host(failure.peer);
    let key = format!(
        "{}|{host}|{}|{}",
        failure.side, failure.check, failure.error
    );
    let Some(suppressed) = admit(key, Instant::now()) else {
        return;
    };
    warn!(
        side = failure.side,
        peer = failure.peer,
        key_fingerprint = failure.key_fingerprint,
        check = failure.check,
        transcript_len = failure.transcript_len,
        nonce_len = failure.nonce_len,
        suppressed,
        window_s = WINDOW.as_secs(),
        error = failure.error,
        "authentication failed"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_source_port_is_not_part_of_the_failure() {
        assert_eq!(host("172.31.209.221:53474"), "172.31.209.221");
        assert_eq!(host("[fe80::1]:5900"), "fe80::1");
        assert_eq!(host("node-a"), "node-a");
    }

    #[test]
    fn a_repeating_failure_logs_once_then_a_summary_per_window() {
        let key = || "test|10.0.0.1|request|repeat".to_owned();
        let start = Instant::now();
        assert_eq!(admit(key(), start), Some(0));
        assert_eq!(admit(key(), start + Duration::from_secs(20)), None);
        assert_eq!(admit(key(), start + Duration::from_secs(40)), None);
        assert_eq!(
            admit(key(), start + WINDOW + Duration::from_secs(1)),
            Some(2)
        );
    }
}

//! Client-side de-duplication of state publishes (provider and executor snapshots).
//!
//! A publish whose encoded message is byte-identical to the last one the node accepted is skipped,
//! so a client that re-publishes an unchanged snapshot several times a second stops costing a
//! round trip and node work each time. The node treats an accepted publish as proof of life (it
//! refreshes the client's activity time), so a skipped publish must still be refreshed: an
//! identical publish is sent again once `refresh` has elapsed since the last accepted one. Keep
//! `refresh` well below the node's `local_session_ttl` (300 s by default).
//!
//! Skipping also delays the node seeing a desired-state revision: the node records which revision
//! each provider or executor last synced against when it accepts a snapshot, and an identical
//! snapshot is not re-sent until the refresh. Pick `refresh` with that latency in mind.

use std::{
    sync::{Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant},
};

use orion_control_plane::ControlMessage;
use orion_core::encode_to_vec;
use orion_transport_ipc::LocalControlTransport;

use crate::{error::ClientError, session::ClientSession};

/// Sends a state publish, skipping it when `dedupe` says the node already has these exact bytes.
pub(crate) fn send_state_publish<T: LocalControlTransport>(
    session: &ClientSession<T>,
    dedupe: Option<&PublishDedupe>,
    message: ControlMessage,
) -> Result<(), ClientError> {
    let Some(dedupe) = dedupe else {
        return session.send_control_and_expect_accepted(message);
    };
    // An encode failure is left to the send, which reports it.
    let Ok(bytes) = encode_to_vec(&message) else {
        return session.send_control_and_expect_accepted(message);
    };
    if dedupe.can_skip(&bytes) {
        return Ok(());
    }
    let result = session.send_control_and_expect_accepted(message);
    match &result {
        Ok(()) => dedupe.record_accepted(bytes),
        Err(_) => dedupe.forget(),
    }
    result
}

#[derive(Debug)]
pub(crate) struct PublishDedupe {
    refresh: Duration,
    last_accepted: Mutex<Option<Accepted>>,
}

#[derive(Debug)]
struct Accepted {
    bytes: Vec<u8>,
    accepted_at: Instant,
}

impl PublishDedupe {
    pub(crate) fn new(refresh: Duration) -> Self {
        Self {
            refresh,
            last_accepted: Mutex::new(None),
        }
    }

    /// Whether a publish of `bytes` can be skipped: it matches the last accepted publish and that
    /// publish is younger than the refresh interval.
    pub(crate) fn can_skip(&self, bytes: &[u8]) -> bool {
        self.lock().as_ref().is_some_and(|accepted| {
            accepted.bytes == bytes && accepted.accepted_at.elapsed() < self.refresh
        })
    }

    /// Records a publish of `bytes` that the node accepted.
    pub(crate) fn record_accepted(&self, bytes: Vec<u8>) {
        *self.lock() = Some(Accepted {
            bytes,
            accepted_at: Instant::now(),
        });
    }

    /// Forgets the last accepted publish, so the next publish is always sent (after a failure,
    /// the node's state is unknown).
    pub(crate) fn forget(&self) {
        *self.lock() = None;
    }

    fn lock(&self) -> MutexGuard<'_, Option<Accepted>> {
        self.last_accepted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_publish_is_skipped_until_refresh_elapses() {
        let dedupe = PublishDedupe::new(Duration::from_secs(10));
        assert!(!dedupe.can_skip(b"snapshot"), "nothing sent yet");
        dedupe.record_accepted(b"snapshot".to_vec());
        assert!(dedupe.can_skip(b"snapshot"));
        assert!(
            !dedupe.can_skip(b"changed"),
            "a changed snapshot is always sent"
        );
    }

    #[test]
    fn zero_refresh_never_skips() {
        let dedupe = PublishDedupe::new(Duration::ZERO);
        dedupe.record_accepted(b"snapshot".to_vec());
        assert!(!dedupe.can_skip(b"snapshot"));
    }

    #[test]
    fn forget_forces_the_next_publish_to_send() {
        let dedupe = PublishDedupe::new(Duration::from_secs(10));
        dedupe.record_accepted(b"snapshot".to_vec());
        dedupe.forget();
        assert!(!dedupe.can_skip(b"snapshot"));
    }
}

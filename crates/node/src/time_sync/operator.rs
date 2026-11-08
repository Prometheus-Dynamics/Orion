//! The `operator` time source: the clock of a remote operator already connected to the node, so
//! a board on a network with no internet needs no address for its time source.
//!
//! The operator drives the exchange (`RemoteOperator::offer_time`). Its first `OfferTime` gets a
//! token holding the node's monotonic time. It answers at once with its clock and the token. The
//! node then knows the round trip from its own monotonic clock, and takes the operator's clock as
//! read half a round trip before the offer arrived.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use orion::control_plane::{TimeOffer, TimeTurn};
use tokio::sync::Notify;

use super::sntp::{self, Sample};

/// The entry in `ORION_NODE_TIME_SERVERS` that names this source.
pub(crate) const OPERATOR_SOURCE: &str = "operator";
/// A sample waiting longer than this is not used.
const FRESH: Duration = Duration::from_secs(30);

/// Samples offered by operators, waiting for the sync loop.
pub(crate) struct OperatorTime {
    epoch: Instant,
    listening: AtomicBool,
    poll_ms: AtomicU64,
    latest: Mutex<Option<(Instant, Sample)>>,
    pub(super) arrived: Notify,
}

impl Default for OperatorTime {
    fn default() -> Self {
        Self {
            epoch: Instant::now(),
            listening: AtomicBool::new(false),
            poll_ms: AtomicU64::new(2_000),
            latest: Mutex::new(None),
            arrived: Notify::new(),
        }
    }
}

impl OperatorTime {
    pub(super) fn listen(&self) {
        self.listening.store(true, Ordering::Relaxed);
    }

    pub(super) fn set_poll(&self, poll: Duration) {
        let ms = u64::try_from(poll.as_millis()).unwrap_or(u64::MAX);
        self.poll_ms.store(ms, Ordering::Relaxed);
    }

    /// One step of the exchange. Refused unless the sync lists `operator` as a source.
    pub(crate) fn offer(&self, offer: &TimeOffer) -> Result<TimeTurn, String> {
        if !self.listening.load(Ordering::Relaxed) {
            return Err("this node does not take its time from operators".to_owned());
        }
        // Never zero: a zero token opens an exchange.
        let now = u64::try_from(self.epoch.elapsed().as_nanos())
            .unwrap_or(u64::MAX)
            .max(1);
        if offer.token == 0 {
            return Ok(TimeTurn {
                token: now,
                next_ms: 0,
            });
        }
        let delay_ns = now
            .checked_sub(offer.token)
            .and_then(|delay| i64::try_from(delay).ok())
            .ok_or("the time token is not one this node issued")?;
        let theirs = i128::from(offer.unix_ns) + i128::from(delay_ns / 2);
        let offset_ns = i64::try_from(theirs - sntp::unix_now_ns())
            .map_err(|_| "the offered time is out of range".to_owned())?;
        let sample = Sample {
            offset_ns,
            delay_ns,
            stratum: offer.stratum.clamp(1, 15),
        };
        *self.latest_lock() = Some((Instant::now(), sample));
        self.arrived.notify_one();
        Ok(TimeTurn {
            token: 0,
            next_ms: self.poll_ms.load(Ordering::Relaxed),
        })
    }

    /// The waiting sample, used at most once.
    pub(super) fn take(&self) -> Option<Sample> {
        self.latest_lock()
            .take()
            .filter(|(at, _)| at.elapsed() <= FRESH)
            .map(|(_, sample)| sample)
    }

    /// Drops the waiting sample: it was measured against a clock that has since been stepped.
    pub(super) fn forget(&self) {
        *self.latest_lock() = None;
    }

    fn latest_lock(&self) -> std::sync::MutexGuard<'_, Option<(Instant, Sample)>> {
        self.latest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

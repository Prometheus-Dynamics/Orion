use core::fmt;

use serde::{Deserialize, Serialize};

/// The time a sample belongs to: nanoseconds of `CLOCK_MONOTONIC` on the board that took it.
///
/// A sample and everything computed from it carry the same value, which is what makes "the
/// value at or before this sample" a lookup. The monotonic clock is never stepped, so a lookup
/// does not jump when time sync sets the wall clock. A sample carried to another board is moved
/// onto that board's monotonic clock on the way in; `orion.time` says how far each board's wall
/// clock is from its monotonic one, for whoever wants a sample's wall time.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct SampleTime(u64);

impl SampleTime {
    /// The earliest time.
    pub const ZERO: Self = Self(0);

    /// Now, on this board's `CLOCK_MONOTONIC`.
    #[cfg(feature = "now")]
    #[must_use]
    pub fn now() -> Self {
        let now = rustix::time::clock_gettime(rustix::time::ClockId::Monotonic);
        // The monotonic clock counts from boot: never negative.
        let (secs, nanos) = (now.tv_sec as u64, now.tv_nsec as u64);
        Self(secs.saturating_mul(1_000_000_000).saturating_add(nanos))
    }

    /// A time from nanoseconds.
    #[must_use]
    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    /// This time in nanoseconds.
    #[must_use]
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    /// Nanoseconds since `earlier`, or zero if `earlier` is later.
    #[must_use]
    pub const fn saturating_since(self, earlier: Self) -> u64 {
        self.0.saturating_sub(earlier.0)
    }
}

impl fmt::Display for SampleTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}ns", self.0)
    }
}

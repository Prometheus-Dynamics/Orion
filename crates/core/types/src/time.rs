use core::fmt;

use serde::{Deserialize, Serialize};

/// The time a sample belongs to, in nanoseconds. A frame and everything computed from it carry the
/// same value, which is what makes "the value at or before this frame" a lookup.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct FrameTime(u64);

impl FrameTime {
    /// The earliest time.
    pub const ZERO: Self = Self(0);

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

impl fmt::Display for FrameTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}ns", self.0)
    }
}

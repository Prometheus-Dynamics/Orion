//! Hybrid logical clock (HLC) used to version desired-state objects.
//!
//! A [`HlcTimestamp`] is `(physical_ms, logical, node)`, ordered lexicographically. The physical
//! part follows the wall clock in milliseconds, the logical counter orders events inside one
//! millisecond (and keeps timestamps increasing while the wall clock stands still or steps
//! backwards), and the node tag makes timestamps from different nodes distinct. See
//! `docs/peer-sync.md`.
//!
//! The clock never reads the system time itself: callers pass the wall clock in, which keeps the
//! type `no_std` and makes it deterministic in tests.

use core::fmt;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

const FNV1A_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1A_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Stable 64-bit tag of a node id (FNV-1a), used as the HLC tie-breaker.
///
/// Stable across platforms, Rust versions and Orion releases, unlike `DefaultHasher`.
pub const fn hlc_node_tag(node_id: &str) -> u64 {
    let bytes = node_id.as_bytes();
    let mut hash = FNV1A_OFFSET_BASIS;
    let mut index = 0;
    while index < bytes.len() {
        hash ^= bytes[index] as u64;
        hash = hash.wrapping_mul(FNV1A_PRIME);
        index += 1;
    }
    hash
}

/// One hybrid logical clock reading. Field order is the comparison order.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[rkyv(derive(Debug, PartialEq, Eq, PartialOrd, Ord))]
pub struct HlcTimestamp {
    /// Wall-clock milliseconds since the Unix epoch (or the largest one seen).
    pub physical_ms: u64,
    /// Counter for events within the same physical millisecond.
    pub logical: u32,
    /// [`hlc_node_tag`] of the node that produced the timestamp.
    pub node: u64,
}

impl HlcTimestamp {
    /// The smallest timestamp. Used for state migrated from before per-object versioning.
    pub const ZERO: Self = Self {
        physical_ms: 0,
        logical: 0,
        node: 0,
    };

    pub const fn new(physical_ms: u64, logical: u32, node: u64) -> Self {
        Self {
            physical_ms,
            logical,
            node,
        }
    }

    /// Returns `true` when this timestamp is more than `max_drift_ms` ahead of `wall_ms`.
    pub const fn is_too_far_ahead(&self, wall_ms: u64, max_drift_ms: u64) -> bool {
        self.physical_ms > wall_ms.saturating_add(max_drift_ms)
    }
}

impl fmt::Display for HlcTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{}@{:016x}",
            self.physical_ms, self.logical, self.node
        )
    }
}

/// A remote timestamp was further in the future than the configured maximum drift.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HlcClockSkew {
    /// The rejected timestamp.
    pub remote: HlcTimestamp,
    /// The local wall clock when it was checked.
    pub wall_ms: u64,
    /// The configured maximum drift.
    pub max_drift_ms: u64,
}

impl HlcClockSkew {
    /// How far the remote physical clock is ahead of the local wall clock.
    pub const fn ahead_by_ms(&self) -> u64 {
        self.remote.physical_ms.saturating_sub(self.wall_ms)
    }
}

impl fmt::Display for HlcClockSkew {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "remote HLC timestamp {} is {}ms ahead of the local clock (max drift {}ms)",
            self.remote,
            self.ahead_by_ms(),
            self.max_drift_ms
        )
    }
}

impl core::error::Error for HlcClockSkew {}

/// Hybrid logical clock of one node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HybridLogicalClock {
    last: HlcTimestamp,
    max_drift_ms: u64,
}

impl HybridLogicalClock {
    /// Creates a clock for the node with tag `node` that rejects remote timestamps more than
    /// `max_drift_ms` ahead of the local wall clock.
    pub const fn new(node: u64, max_drift_ms: u64) -> Self {
        Self {
            last: HlcTimestamp::new(0, 0, node),
            max_drift_ms,
        }
    }

    /// The node tag stamped on local timestamps.
    pub const fn node(&self) -> u64 {
        self.last.node
    }

    pub const fn max_drift_ms(&self) -> u64 {
        self.max_drift_ms
    }

    /// The most recent timestamp issued or observed (with this node's tag).
    pub const fn last(&self) -> HlcTimestamp {
        self.last
    }

    /// Returns a new timestamp for a local event, strictly greater than every timestamp this
    /// clock has issued or observed.
    pub fn now(&mut self, wall_ms: u64) -> HlcTimestamp {
        if wall_ms > self.last.physical_ms {
            self.last.physical_ms = wall_ms;
            self.last.logical = 0;
        } else if self.last.logical == u32::MAX {
            self.last.physical_ms = self.last.physical_ms.saturating_add(1);
            self.last.logical = 0;
        } else {
            self.last.logical += 1;
        }
        self.last
    }

    /// Checks a remote timestamp against the maximum drift without changing the clock.
    pub const fn check(&self, remote: HlcTimestamp, wall_ms: u64) -> Result<(), HlcClockSkew> {
        if remote.is_too_far_ahead(wall_ms, self.max_drift_ms) {
            Err(HlcClockSkew {
                remote,
                wall_ms,
                max_drift_ms: self.max_drift_ms,
            })
        } else {
            Ok(())
        }
    }

    /// Folds an accepted remote timestamp into the clock so later local timestamps order after
    /// it. Timestamps beyond the maximum drift are rejected and leave the clock unchanged.
    pub fn observe(&mut self, remote: HlcTimestamp, wall_ms: u64) -> Result<(), HlcClockSkew> {
        self.check(remote, wall_ms)?;
        self.advance_to(remote);
        Ok(())
    }

    /// Advances the clock to at least `seen` without a drift check. Used to seed the clock from
    /// persisted state at startup.
    pub fn advance_to(&mut self, seen: HlcTimestamp) {
        if (seen.physical_ms, seen.logical) > (self.last.physical_ms, self.last.logical) {
            self.last.physical_ms = seen.physical_ms;
            self.last.logical = seen.logical;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_tag_is_stable_fnv1a() {
        assert_eq!(hlc_node_tag(""), FNV1A_OFFSET_BASIS);
        // Reference value of FNV-1a 64 for "a".
        assert_eq!(hlc_node_tag("a"), 0xaf63_dc4c_8601_ec8c);
        assert_ne!(hlc_node_tag("node-a"), hlc_node_tag("node-b"));
    }

    #[test]
    fn ordering_is_physical_then_logical_then_node() {
        let a = HlcTimestamp::new(10, 5, 9);
        assert!(HlcTimestamp::new(11, 0, 0) > a);
        assert!(HlcTimestamp::new(10, 6, 0) > a);
        assert!(HlcTimestamp::new(10, 5, 10) > a);
        assert!(HlcTimestamp::ZERO < a);
    }

    #[test]
    fn local_timestamps_strictly_increase_even_when_wall_clock_steps_back() {
        let mut clock = HybridLogicalClock::new(7, 1_000);
        let first = clock.now(100);
        let second = clock.now(100);
        let third = clock.now(50);
        let fourth = clock.now(200);
        assert_eq!(first, HlcTimestamp::new(100, 0, 7));
        assert_eq!(second, HlcTimestamp::new(100, 1, 7));
        assert_eq!(third, HlcTimestamp::new(100, 2, 7));
        assert_eq!(fourth, HlcTimestamp::new(200, 0, 7));
    }

    #[test]
    fn logical_overflow_carries_into_physical() {
        let mut clock = HybridLogicalClock::new(1, 0);
        clock.advance_to(HlcTimestamp::new(5, u32::MAX, 2));
        assert_eq!(clock.now(0), HlcTimestamp::new(6, 0, 1));
    }

    #[test]
    fn observe_orders_later_local_events_after_remote_ones() {
        let mut clock = HybridLogicalClock::new(1, 10_000);
        clock
            .observe(HlcTimestamp::new(5_000, 3, 2), 1_000)
            .expect("within drift");
        let next = clock.now(1_000);
        assert!(next > HlcTimestamp::new(5_000, 3, 2));
        assert_eq!(next, HlcTimestamp::new(5_000, 4, 1));
    }

    #[test]
    fn observe_rejects_timestamps_beyond_max_drift() {
        let mut clock = HybridLogicalClock::new(1, 100);
        let before = clock.last();
        let err = clock
            .observe(HlcTimestamp::new(1_201, 0, 2), 1_100)
            .expect_err("too far ahead");
        assert_eq!(err.ahead_by_ms(), 101);
        assert_eq!(clock.last(), before);
        clock
            .observe(HlcTimestamp::new(1_200, 0, 2), 1_100)
            .expect("exactly at the limit is accepted");
    }
}

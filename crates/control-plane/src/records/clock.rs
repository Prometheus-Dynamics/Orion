//! Clock facts a node reports about its own timebase.
//!
//! Orion never disciplines clocks. A node only observes its clock source and synchronization state
//! (from the kernel, or as declared by the operator) and publishes them in its observed
//! [`NodeRecord`](super::NodeRecord), so producers can stamp data in a declared timebase and
//! consumers can judge whether timestamps from two nodes are comparable.

use alloc::string::String;
use core::fmt;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

/// What disciplines (or is declared to discipline) a node's clock.
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum ClockSourceKind {
    /// Nothing is known about the clock (for example a non-Linux node).
    Unknown,
    /// The kernel system clock, with no declared discipline daemon.
    System,
    Ntp,
    Chrony,
    Ptp,
    Gps,
    /// Any other operator-declared source, by name.
    Other(String),
}

impl ClockSourceKind {
    /// Stable lowercase label: `unknown`, `system`, `ntp`, `chrony`, `ptp`, `gps`, or the
    /// declared name for [`ClockSourceKind::Other`].
    pub fn as_str(&self) -> &str {
        match self {
            Self::Unknown => "unknown",
            Self::System => "system",
            Self::Ntp => "ntp",
            Self::Chrony => "chrony",
            Self::Ptp => "ptp",
            Self::Gps => "gps",
            Self::Other(name) => name.as_str(),
        }
    }

    /// Parses a label case-insensitively. Known names map to their variant; any other non-empty
    /// label becomes [`ClockSourceKind::Other`]. Returns `None` for an empty or blank label.
    pub fn from_label(label: &str) -> Option<Self> {
        let label = label.trim();
        if label.is_empty() {
            return None;
        }
        let known = [
            ("unknown", Self::Unknown),
            ("system", Self::System),
            ("ntp", Self::Ntp),
            ("chrony", Self::Chrony),
            ("ptp", Self::Ptp),
            ("gps", Self::Gps),
        ];
        Some(
            known
                .into_iter()
                .find(|(name, _)| label.eq_ignore_ascii_case(name))
                .map(|(_, kind)| kind)
                .unwrap_or_else(|| Self::Other(label.into())),
        )
    }
}

impl fmt::Display for ClockSourceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A node's clock source and synchronization state, published as an observed node fact.
///
/// Every measurement is optional because not every platform or source reports it. Offsets and
/// errors are in nanoseconds; `checked_at_ms` is Unix time in milliseconds.
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct NodeClockFacts {
    pub source: ClockSourceKind,
    /// Whether the clock is synchronized to its reference, when known.
    pub synchronized: Option<bool>,
    /// Estimated offset from the reference (positive: local clock ahead), when known.
    pub offset_ns: Option<i64>,
    /// Upper bound on the clock error, when known (kernel `maxerror`).
    pub max_error_ns: Option<u64>,
    /// Estimated clock error, when known (kernel `esterror`).
    pub estimated_error_ns: Option<u64>,
    /// NTP stratum, when known.
    pub stratum: Option<u8>,
    /// PTP grandmaster clock identity, when known.
    pub ptp_grandmaster_id: Option<String>,
    /// Name of the timebase producers on this node stamp in (for example `UTC`, `TAI`,
    /// `monotonic`), when declared.
    pub timebase: Option<String>,
    /// When these facts were last checked, in Unix milliseconds.
    pub checked_at_ms: u64,
}

impl NodeClockFacts {
    /// Facts for a clock nothing is known about.
    pub fn unknown(checked_at_ms: u64) -> Self {
        Self {
            source: ClockSourceKind::Unknown,
            synchronized: None,
            offset_ns: None,
            max_error_ns: None,
            estimated_error_ns: None,
            stratum: None,
            ptp_grandmaster_id: None,
            timebase: None,
            checked_at_ms,
        }
    }

    pub fn with_source(mut self, source: ClockSourceKind) -> Self {
        self.source = source;
        self
    }

    pub fn with_synchronized(mut self, synchronized: bool) -> Self {
        self.synchronized = Some(synchronized);
        self
    }

    pub fn with_offset_ns(mut self, offset_ns: i64) -> Self {
        self.offset_ns = Some(offset_ns);
        self
    }

    pub fn with_max_error_ns(mut self, max_error_ns: u64) -> Self {
        self.max_error_ns = Some(max_error_ns);
        self
    }

    pub fn with_estimated_error_ns(mut self, estimated_error_ns: u64) -> Self {
        self.estimated_error_ns = Some(estimated_error_ns);
        self
    }

    pub fn with_stratum(mut self, stratum: u8) -> Self {
        self.stratum = Some(stratum);
        self
    }

    pub fn with_ptp_grandmaster_id(mut self, id: impl Into<String>) -> Self {
        self.ptp_grandmaster_id = Some(id.into());
        self
    }

    pub fn with_timebase(mut self, timebase: impl Into<String>) -> Self {
        self.timebase = Some(timebase.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clock_source_labels_round_trip() {
        for kind in [
            ClockSourceKind::Unknown,
            ClockSourceKind::System,
            ClockSourceKind::Ntp,
            ClockSourceKind::Chrony,
            ClockSourceKind::Ptp,
            ClockSourceKind::Gps,
            ClockSourceKind::Other("rubidium".into()),
        ] {
            assert_eq!(ClockSourceKind::from_label(kind.as_str()), Some(kind));
        }
        assert_eq!(
            ClockSourceKind::from_label(" PTP "),
            Some(ClockSourceKind::Ptp)
        );
        assert_eq!(ClockSourceKind::from_label("  "), None);
    }
}

//! Clock facts detection.
//!
//! The node reads the kernel's clock discipline state with `adjtimex(2)` in read-only mode
//! (`modes = 0`, no privileges needed) and maps it to [`NodeClockFacts`]. Orion never adjusts the
//! clock. The kernel cannot say which daemon disciplines it, so the source defaults to
//! [`ClockSourceKind::System`] and operators declare PTP, chrony, GPS, and so on with
//! `ORION_NODE_CLOCK_SOURCE`, plus the producer timebase with `ORION_NODE_TIMEBASE`. Off Linux the
//! facts are [`ClockSourceKind::Unknown`] with no measurements.

use orion::control_plane::{ClockSourceKind, NodeClockFacts};

/// `STA_UNSYNC`: the kernel clock is not synchronized.
pub const STA_UNSYNC: i32 = 0x0040;
/// `STA_CLOCKERR`: clock hardware fault.
pub const STA_CLOCKERR: i32 = 0x1000;
/// `STA_NANO`: `offset` is in nanoseconds instead of microseconds.
pub const STA_NANO: i32 = 0x2000;
/// `TIME_ERROR`: `adjtimex` return state for an unsynchronized clock.
pub const TIME_ERROR: i32 = 5;

/// Offsets that move by less than this are not republished.
const OFFSET_CHANGE_FLOOR_NS: u64 = 1_000_000;
/// Error bounds are republished when they halve or double and move by at least this much.
const ERROR_CHANGE_FLOOR_NS: u64 = 10_000_000;

/// Raw kernel clock discipline state, as returned by `adjtimex(2)`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KernelClockReading {
    /// Return value of `adjtimex` (`TIME_OK`, ..., `TIME_ERROR`).
    pub state: i32,
    /// `timex.status` bits (`STA_*`).
    pub status: i32,
    /// `timex.offset`: microseconds, or nanoseconds when `STA_NANO` is set.
    pub offset: i64,
    /// `timex.maxerror` in microseconds.
    pub max_error_us: i64,
    /// `timex.esterror` in microseconds.
    pub estimated_error_us: i64,
}

/// Where the node reads its kernel clock state from. Injectable so embedders and tests can supply
/// readings; [`KernelClockStatusSource`] is the real one.
pub trait ClockStatusSource: Send + Sync {
    /// Returns the current reading, or `None` when the platform does not report one.
    fn read(&self) -> Option<KernelClockReading>;
}

/// Reads the kernel clock with read-only `adjtimex(2)` on Linux; reports nothing elsewhere.
#[derive(Clone, Copy, Debug, Default)]
pub struct KernelClockStatusSource;

impl ClockStatusSource for KernelClockStatusSource {
    #[cfg(target_os = "linux")]
    // `timex` fields are `c_long`, which is `i64` only on 64-bit targets.
    #[allow(clippy::useless_conversion)]
    fn read(&self) -> Option<KernelClockReading> {
        // SAFETY: `timex` is plain old data, so all-zero is a valid value.
        let mut timex: libc::timex = unsafe { std::mem::zeroed() };
        timex.modes = 0;
        // SAFETY: `timex` is a valid, exclusively borrowed struct; `modes = 0` only reads.
        let state = unsafe { libc::adjtimex(&mut timex) };
        if state < 0 {
            return None;
        }
        Some(KernelClockReading {
            state,
            status: timex.status,
            offset: i64::from(timex.offset),
            max_error_us: i64::from(timex.maxerror),
            estimated_error_us: i64::from(timex.esterror),
        })
    }

    #[cfg(not(target_os = "linux"))]
    fn read(&self) -> Option<KernelClockReading> {
        None
    }
}

fn micros_to_nanos(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0).saturating_mul(1_000)
}

/// Maps a kernel reading plus the operator declarations to clock facts.
///
/// The clock counts as synchronized unless `STA_UNSYNC` or `STA_CLOCKERR` is set or the state is
/// `TIME_ERROR`. The maximum error is always reported; the offset and estimated error only while
/// synchronized, because the kernel leaves stale values behind otherwise.
pub fn clock_facts_from_reading(
    reading: Option<KernelClockReading>,
    source_override: Option<&ClockSourceKind>,
    timebase: Option<&str>,
    checked_at_ms: u64,
) -> NodeClockFacts {
    let mut facts = NodeClockFacts::unknown(checked_at_ms);
    facts.timebase = timebase.map(Into::into);
    let Some(reading) = reading else {
        facts.source = source_override.cloned().unwrap_or(ClockSourceKind::Unknown);
        return facts;
    };
    facts.source = source_override.cloned().unwrap_or(ClockSourceKind::System);
    let synchronized =
        reading.status & (STA_UNSYNC | STA_CLOCKERR) == 0 && reading.state != TIME_ERROR;
    facts.synchronized = Some(synchronized);
    facts.max_error_ns = Some(micros_to_nanos(reading.max_error_us));
    if synchronized {
        facts.offset_ns = Some(if reading.status & STA_NANO != 0 {
            reading.offset
        } else {
            reading.offset.saturating_mul(1_000)
        });
        facts.estimated_error_ns = Some(micros_to_nanos(reading.estimated_error_us));
    }
    facts
}

fn offset_changed(previous: Option<i64>, next: Option<i64>) -> bool {
    match (previous, next) {
        (Some(previous), Some(next)) => previous.abs_diff(next) > OFFSET_CHANGE_FLOOR_NS,
        (previous, next) => previous.is_some() != next.is_some(),
    }
}

fn error_bound_changed(previous: Option<u64>, next: Option<u64>) -> bool {
    match (previous, next) {
        (Some(previous), Some(next)) => {
            let doubled_or_halved =
                next > previous.saturating_mul(2) || next.saturating_mul(2) < previous;
            doubled_or_halved && previous.abs_diff(next) >= ERROR_CHANGE_FLOOR_NS
        }
        (previous, next) => previous.is_some() != next.is_some(),
    }
}

/// Whether `next` should replace the published `previous` facts.
///
/// Republishes on any change of source, synchronization, stratum, grandmaster, or timebase; when
/// the offset moves by more than 1 ms; when an error bound halves or doubles by at least 10 ms; and
/// when the published facts are `max_age_ms` old. Small jitter is left out of the observed record
/// (the Prometheus export always has the latest sample).
pub fn clock_facts_need_publish(
    previous: Option<&NodeClockFacts>,
    next: &NodeClockFacts,
    max_age_ms: u64,
) -> bool {
    let Some(previous) = previous else {
        return true;
    };
    previous.source != next.source
        || previous.synchronized != next.synchronized
        || previous.stratum != next.stratum
        || previous.ptp_grandmaster_id != next.ptp_grandmaster_id
        || previous.timebase != next.timebase
        || offset_changed(previous.offset_ns, next.offset_ns)
        || error_bound_changed(previous.max_error_ns, next.max_error_ns)
        || error_bound_changed(previous.estimated_error_ns, next.estimated_error_ns)
        || next.checked_at_ms.saturating_sub(previous.checked_at_ms) >= max_age_ms
}

#[cfg(test)]
mod tests {
    use super::*;

    const SYNCED_NANO: KernelClockReading = KernelClockReading {
        state: 0,
        status: STA_NANO,
        offset: -2_500,
        max_error_us: 16_000,
        estimated_error_us: 3,
    };

    #[cfg(target_os = "linux")]
    #[test]
    fn kernel_source_reads_the_host_clock() {
        // Sandboxes may filter adjtimex; only check the mapping when a reading comes back.
        if let Some(reading) = KernelClockStatusSource.read() {
            let facts = clock_facts_from_reading(Some(reading), None, None, 0);
            assert_eq!(facts.source, ClockSourceKind::System);
            assert!(facts.synchronized.is_some());
            assert!(facts.max_error_ns.is_some());
        }
    }

    #[test]
    fn synchronized_reading_maps_offset_and_errors() {
        let facts = clock_facts_from_reading(Some(SYNCED_NANO), None, Some("UTC"), 42);
        assert_eq!(facts.source, ClockSourceKind::System);
        assert_eq!(facts.synchronized, Some(true));
        assert_eq!(facts.offset_ns, Some(-2_500));
        assert_eq!(facts.max_error_ns, Some(16_000_000));
        assert_eq!(facts.estimated_error_ns, Some(3_000));
        assert_eq!(facts.timebase.as_deref(), Some("UTC"));
        assert_eq!(facts.checked_at_ms, 42);
    }

    #[test]
    fn microsecond_offsets_are_scaled_without_sta_nano() {
        let reading = KernelClockReading {
            status: 0,
            offset: -7,
            ..SYNCED_NANO
        };
        let facts = clock_facts_from_reading(Some(reading), None, None, 0);
        assert_eq!(facts.offset_ns, Some(-7_000));
    }

    #[test]
    fn unsync_bits_and_time_error_mark_the_clock_unsynchronized() {
        for reading in [
            KernelClockReading {
                status: STA_UNSYNC,
                ..SYNCED_NANO
            },
            KernelClockReading {
                status: STA_NANO | STA_CLOCKERR,
                ..SYNCED_NANO
            },
            KernelClockReading {
                state: TIME_ERROR,
                ..SYNCED_NANO
            },
        ] {
            let facts = clock_facts_from_reading(Some(reading), None, None, 0);
            assert_eq!(facts.synchronized, Some(false), "{reading:?}");
            assert_eq!(facts.offset_ns, None);
            assert_eq!(facts.estimated_error_ns, None);
            assert_eq!(facts.max_error_ns, Some(16_000_000));
        }
    }

    #[test]
    fn declared_source_overrides_detection_and_missing_readings_stay_unknown() {
        let facts =
            clock_facts_from_reading(Some(SYNCED_NANO), Some(&ClockSourceKind::Ptp), None, 0);
        assert_eq!(facts.source, ClockSourceKind::Ptp);

        let facts = clock_facts_from_reading(None, None, Some("TAI"), 0);
        assert_eq!(facts, NodeClockFacts::unknown(0).with_timebase("TAI"));

        let facts = clock_facts_from_reading(None, Some(&ClockSourceKind::Gps), None, 0);
        assert_eq!(facts.source, ClockSourceKind::Gps);
        assert_eq!(facts.synchronized, None);
    }

    #[test]
    fn negative_kernel_errors_clamp_to_zero() {
        let reading = KernelClockReading {
            max_error_us: -1,
            ..SYNCED_NANO
        };
        let facts = clock_facts_from_reading(Some(reading), None, None, 0);
        assert_eq!(facts.max_error_ns, Some(0));
    }

    #[test]
    fn publish_decision_ignores_jitter_and_reacts_to_meaningful_changes() {
        let base = clock_facts_from_reading(Some(SYNCED_NANO), None, None, 1_000);
        let max_age = 300_000;
        assert!(clock_facts_need_publish(None, &base, max_age));
        assert!(!clock_facts_need_publish(Some(&base), &base, max_age));

        let jitter = NodeClockFacts {
            offset_ns: Some(400_000),
            max_error_ns: Some(18_000_000),
            estimated_error_ns: Some(9_000),
            checked_at_ms: 11_000,
            ..base.clone()
        };
        assert!(!clock_facts_need_publish(Some(&base), &jitter, max_age));

        let offset_jump = NodeClockFacts {
            offset_ns: Some(1_500_000),
            ..base.clone()
        };
        assert!(clock_facts_need_publish(Some(&base), &offset_jump, max_age));

        let error_growth = NodeClockFacts {
            max_error_ns: Some(40_000_000),
            ..base.clone()
        };
        assert!(clock_facts_need_publish(
            Some(&base),
            &error_growth,
            max_age
        ));

        let lost_sync = NodeClockFacts {
            synchronized: Some(false),
            ..base.clone()
        };
        assert!(clock_facts_need_publish(Some(&base), &lost_sync, max_age));

        let declared = base.clone().with_source(ClockSourceKind::Chrony);
        assert!(clock_facts_need_publish(Some(&base), &declared, max_age));

        let stale = NodeClockFacts {
            checked_at_ms: 1_000 + max_age,
            ..base.clone()
        };
        assert!(clock_facts_need_publish(Some(&base), &stale, max_age));
    }
}

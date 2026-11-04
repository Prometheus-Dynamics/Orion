//! Picks the clock offset from RTT samples.
//!
//! A sample's error is at most half its RTT, so the sample with the smallest RTT is the best one.
//! Only recent samples compete: a best sample that never expired would pin the offset to one
//! moment, so a slow start or clock drift would never be corrected.

use std::collections::VecDeque;
use std::time::Duration;

/// How long a sample competes for the estimate. Drift under 1 ppm stays under 30 us in this time.
const WINDOW_US: i64 = Duration::from_secs(30).as_micros() as i64;
/// Samples in a row that must fail to beat the window's best before the estimate counts as settled.
const SETTLE_SAMPLES: u32 = 10;

/// One RTT measurement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Sample {
    /// Local time the reply arrived.
    pub(crate) at_us: i64,
    pub(crate) rtt_us: i64,
    pub(crate) offset_us: i64,
}

/// The samples of one connection.
#[derive(Debug, Default)]
pub(crate) struct ClockFilter {
    samples: VecDeque<Sample>,
    best: Option<Sample>,
    without_better: u32,
}

impl ClockFilter {
    /// Adds a sample and returns the best one in the window, if that changed.
    pub(crate) fn add(&mut self, sample: Sample) -> Option<Sample> {
        if self.best.is_some_and(|best| sample.rtt_us >= best.rtt_us) {
            self.without_better = self.without_better.saturating_add(1);
        } else {
            self.without_better = 0;
        }
        self.samples.push_back(sample);
        while self
            .samples
            .front()
            .is_some_and(|oldest| sample.at_us - oldest.at_us > WINDOW_US)
        {
            self.samples.pop_front();
        }
        let best = self.samples.iter().min_by_key(|s| s.rtt_us).copied();
        let changed = best != self.best;
        self.best = best;
        best.filter(|_| changed)
    }

    /// `true` once recent samples stopped improving, so pings can slow down.
    pub(crate) fn settled(&self) -> bool {
        self.without_better >= SETTLE_SAMPLES
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND: i64 = 1_000_000;

    fn sample(at_s: i64, rtt_us: i64, offset_us: i64) -> Sample {
        Sample {
            at_us: at_s * SECOND,
            rtt_us,
            offset_us,
        }
    }

    #[test]
    fn a_smaller_rtt_replaces_the_estimate_and_a_larger_one_does_not() {
        let mut filter = ClockFilter::default();
        assert_eq!(filter.add(sample(0, 500, 1)).map(|s| s.offset_us), Some(1));
        assert_eq!(filter.add(sample(1, 900, 2)), None);
        assert_eq!(filter.add(sample(2, 300, 3)).map(|s| s.offset_us), Some(3));
    }

    #[test]
    fn the_best_sample_expires_so_the_offset_follows_drift() {
        let mut filter = ClockFilter::default();
        filter.add(sample(0, 100, 1000));
        assert_eq!(filter.add(sample(30, 400, 1040)), None);
        let next = filter.add(sample(31, 500, 1041)).unwrap();
        assert_eq!((next.rtt_us, next.offset_us), (400, 1040));
    }

    #[test]
    fn a_bad_start_expires_even_if_no_later_sample_beats_its_rtt() {
        let mut filter = ClockFilter::default();
        filter.add(sample(0, 2_000, -13_000));
        for second in (5..=35).step_by(5) {
            filter.add(sample(second, 2_500, 700));
        }
        assert_eq!(filter.best.map(|s| s.offset_us), Some(700));
    }

    #[test]
    fn it_settles_after_samples_stop_improving_and_unsettles_on_a_better_one() {
        let mut filter = ClockFilter::default();
        filter.add(sample(0, 500, 0));
        for second in 1..SETTLE_SAMPLES as i64 {
            filter.add(sample(second, 600, 0));
            assert!(!filter.settled());
        }
        filter.add(sample(10, 600, 0));
        assert!(filter.settled());
        filter.add(sample(11, 100, 0));
        assert!(!filter.settled());
    }

    #[test]
    fn the_window_stays_bounded() {
        let mut filter = ClockFilter::default();
        for second in 0..10_000 {
            filter.add(sample(second, 500, 0));
        }
        assert!(filter.samples.len() <= 32);
    }
}

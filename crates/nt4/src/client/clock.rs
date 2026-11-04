//! Picks the clock offset from RTT samples.
//!
//! A sample's error is at most half its RTT, plus what the clocks drifted since it was taken. The
//! sample with the lowest such bound wins. All samples age at the same rate, so one that is ahead
//! stays ahead until a better one arrives, and only the best needs keeping.
//!
//! RTT can swing by a server's whole loop period, on a cycle of a minute or more. A fixed window
//! would expire a good sample at the top of that cycle and take a bad one. Ageing keeps the good
//! sample until a comparable one replaces it, and still follows drift.

/// Assumed clock drift, 20 ppm: one microsecond of error per this many microseconds of age. A
/// bound near the real drift keeps a good sample longest. Too high, and mediocre samples replace it.
const AGE_US_PER_ERROR_US: i64 = 50_000;

/// One RTT measurement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Sample {
    /// Local time the reply arrived.
    pub(crate) at_us: i64,
    pub(crate) rtt_us: i64,
    pub(crate) offset_us: i64,
}

/// The best sample of one connection.
#[derive(Debug, Default)]
pub(crate) struct ClockFilter {
    best: Option<Sample>,
}

impl ClockFilter {
    /// Adds a sample and returns it if it is the new best.
    pub(crate) fn add(&mut self, sample: Sample) -> Option<Sample> {
        let replaces = self.best.is_none_or(|best| {
            let age_us = sample.at_us - best.at_us;
            sample.rtt_us / 2 <= best.rtt_us / 2 + age_us / AGE_US_PER_ERROR_US
        });
        replaces.then(|| {
            self.best = Some(sample);
            sample
        })
    }

    /// `server_time - local_time` from the best sample.
    pub(crate) fn offset_us(&self) -> Option<i64> {
        self.best.map(|best| best.offset_us)
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
        assert!(filter.add(sample(0, 500, 1)).is_some());
        assert!(filter.add(sample(0, 900, 2)).is_none());
        assert_eq!(filter.offset_us(), Some(1));
        assert!(filter.add(sample(0, 300, 3)).is_some());
        assert_eq!(filter.offset_us(), Some(3));
    }

    #[test]
    fn a_good_sample_survives_a_cycle_of_high_rtt() {
        let mut filter = ClockFilter::default();
        filter.add(sample(0, 900, 450));
        // One server-loop sawtooth: RTT climbs to 20 ms and the offset error with it.
        for second in 1..=60 {
            let rtt_us = 900 + (60 - second) * 330;
            filter.add(sample(second, rtt_us, rtt_us / 2));
        }
        assert!(filter.offset_us().unwrap() < 1_500);
    }

    #[test]
    fn the_same_rtt_later_replaces_the_estimate_so_the_offset_follows_drift() {
        let mut filter = ClockFilter::default();
        filter.add(sample(0, 900, 1000));
        filter.add(sample(5, 900, 1005));
        filter.add(sample(10, 950, 1010));
        assert_eq!(filter.offset_us(), Some(1010));
    }

    #[test]
    fn a_bad_start_ages_out_even_if_no_later_sample_beats_its_rtt() {
        let mut filter = ClockFilter::default();
        filter.add(sample(0, 2_000, -13_000));
        assert!(filter.add(sample(5, 2_500, 700)).is_none());
        assert!(filter.add(sample(13, 2_500, 700)).is_some());
        assert_eq!(filter.offset_us(), Some(700));
    }
}

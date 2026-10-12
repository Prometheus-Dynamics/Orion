//! Maps the page's clock (`performance.now()`) onto the tool's clock, from ping round trips.

use std::collections::VecDeque;

/// Samples kept: at a 200 ms ping interval, about eight seconds, so drift is tracked.
const KEEP: usize = 40;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Sample {
    rtt_us: i64,
    /// `tool_time = page_time + offset_us`.
    offset_us: i64,
}

/// The offset from the page to the tool. Each ping gives one sample; the estimate uses the sample
/// with the smallest round trip, because the least queueing delay means the least error (the usual
/// minimum-RTT rule).
#[derive(Debug, Default)]
pub struct Clock {
    samples: VecDeque<Sample>,
}

impl Clock {
    /// A ping sent at `sent_us` (tool clock) was answered at `page_us` (page clock) and received
    /// at `received_us` (tool clock).
    pub fn add_pong(&mut self, sent_us: i64, received_us: i64, page_us: i64) {
        let rtt_us = received_us - sent_us;
        if rtt_us < 0 {
            return;
        }
        // The page answered somewhere inside the round trip. Assume the middle.
        let offset_us = sent_us + rtt_us / 2 - page_us;
        if self.samples.len() == KEEP {
            self.samples.pop_front();
        }
        self.samples.push_back(Sample { rtt_us, offset_us });
    }

    /// Samples held so far.
    pub fn sample_count(&self) -> usize {
        self.samples.len()
    }

    /// The best round trip seen in the window, in microseconds.
    pub fn best_rtt_us(&self) -> Option<i64> {
        self.best().map(|s| s.rtt_us)
    }

    /// A page timestamp on the tool clock, or `None` before the first sample.
    pub fn page_to_tool(&self, page_us: i64) -> Option<i64> {
        self.best().map(|s| page_us + s.offset_us)
    }

    fn best(&self) -> Option<Sample> {
        self.samples.iter().copied().min_by_key(|s| s.rtt_us)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_rtt_sample_wins() {
        // The page clock runs 1_000_000 us behind the tool clock: tool = page + 1_000_000.
        const TRUE_OFFSET: i64 = 1_000_000;
        let mut clock = Clock::default();
        // Slow ping, answered late in its round trip: its midpoint is off by 1_900 us.
        clock.add_pong(0, 4_000, 3_900 - TRUE_OFFSET);
        // Fast ping, answered at the midpoint: exact.
        clock.add_pong(10_000, 10_200, 10_100 - TRUE_OFFSET);
        // Very slow ping: its estimate is the worst of the three.
        clock.add_pong(20_000, 26_000, 25_000 - TRUE_OFFSET);
        assert_eq!(clock.sample_count(), 3);
        assert_eq!(clock.best_rtt_us(), Some(200));
        assert_eq!(clock.page_to_tool(50_000 - TRUE_OFFSET), Some(50_000));
    }

    #[test]
    fn negative_rtt_is_ignored_and_window_is_bounded() {
        let mut clock = Clock::default();
        clock.add_pong(100, 50, 0);
        assert_eq!(clock.sample_count(), 0);
        assert_eq!(clock.page_to_tool(1), None);
        for i in 0..(KEEP as i64 + 5) {
            clock.add_pong(i * 1_000, i * 1_000 + 100, i * 1_000);
        }
        assert_eq!(clock.sample_count(), KEEP);
    }
}

//! Noticing that the wall clock was stepped. The time sync steps it when the board gets the
//! time, hours after start on a board with no clock battery; what node logic keeps on the
//! wall clock (when a lease ends, when a peer was last agreed with) is then moved by as much.
//!
//! A step is seen as a change in the wall clock less the monotonic one. The kernel slews both
//! alike, so only a step moves it. It is looked at before anything reads the wall clock, at
//! the start of each request and each turn, so nothing is decided on a stepped clock with
//! times that were not moved.

/// Smaller changes are two clocks read a moment apart, not a step: the time sync steps only
/// what is over 400 ms.
const STEP_MS: i64 = 200;

/// The wall clock less the monotonic one, as last seen.
#[derive(Debug, Default)]
pub(crate) struct StepWatch(Option<i64>);

impl StepWatch {
    /// By how much the wall clock was stepped since this was last asked, if it was, in
    /// milliseconds. `wall_ms` and `now_ms` are the two clocks, read together.
    pub(crate) fn stepped(&mut self, wall_ms: u64, now_ms: u64) -> Option<i64> {
        let offset = (wall_ms as i64).wrapping_sub(now_ms as i64);
        let by = offset.wrapping_sub(self.0.replace(offset)?);
        if by.abs() < STEP_MS {
            // Kept as it was: many small changes are not let add up unseen.
            self.0 = Some(offset - by);
            return None;
        }
        Some(by)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_step_is_seen_once_and_two_clocks_read_apart_are_not_one() {
        let mut watch = StepWatch::default();
        // The first reading is where it starts.
        assert_eq!(watch.stepped(1_000_000, 500), None);
        assert_eq!(watch.stepped(1_001_000, 1_500), None);
        // Read a little apart, now early, now late: not a step, however often.
        for turn in 1..=10 {
            let late = if turn % 2 == 0 { 150 } else { 0 };
            assert_eq!(
                watch.stepped(1_001_000 + turn * 1_000 + late, 1_500 + turn * 1_000),
                None
            );
        }
        // Small changes that go one way add up to one: 150 already there, and 100 more.
        assert_eq!(watch.stepped(1_012_250, 12_500), Some(250));
        // Five hours forward, seen once.
        let hours = 5 * 3_600_000;
        assert_eq!(watch.stepped(1_013_250 + hours, 13_500), Some(hours as i64));
        assert_eq!(watch.stepped(1_014_250 + hours, 14_500), None);
        // And back.
        assert_eq!(watch.stepped(1_015_250, 15_500), Some(-(hours as i64)));
    }
}

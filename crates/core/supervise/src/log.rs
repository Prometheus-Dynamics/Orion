/// Rate-limits a noisy log line: at most one per period, carrying how many were held back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogLimit {
    period_ms: u64,
    last_ms: Option<u64>,
    held_back: u32,
}

impl LogLimit {
    /// At most one line per `period_ms`.
    #[must_use]
    pub const fn new(period_ms: u64) -> Self {
        Self {
            period_ms,
            last_ms: None,
            held_back: 0,
        }
    }

    /// Whether to log now. `Some(n)` means log, and that `n` lines were held back since the last.
    pub fn allow(&mut self, now_ms: u64) -> Option<u32> {
        match self.last_ms {
            Some(last) if now_ms.saturating_sub(last) < self.period_ms => {
                self.held_back = self.held_back.saturating_add(1);
                None
            }
            _ => {
                self.last_ms = Some(now_ms);
                Some(core::mem::take(&mut self.held_back))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_line_per_period_with_a_count() {
        let mut limit = LogLimit::new(10_000);
        assert_eq!(limit.allow(0), Some(0));
        assert_eq!(limit.allow(1), None);
        assert_eq!(limit.allow(9_999), None);
        assert_eq!(limit.allow(10_000), Some(2));
        assert_eq!(limit.allow(10_001), None);
        assert_eq!(limit.allow(50_000), Some(1));
    }
}

//! CPU utilisation from `/proc/stat` deltas.

use std::time::{Duration, Instant};

/// Cumulative busy and total time of one `/proc/stat` CPU line, in clock ticks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CpuTimes {
    pub busy: u64,
    pub total: u64,
}

/// The aggregate `cpu` line and the per-CPU `cpuN` lines of a `/proc/stat` document.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProcStat {
    pub all: CpuTimes,
    pub cores: Vec<CpuTimes>,
}

/// Parses `/proc/stat`. Idle time is `idle + iowait`; total covers `user` to `steal` (guest time
/// is already counted in `user` / `nice`). `None` without an aggregate `cpu` line.
pub fn parse_proc_stat(contents: &str) -> Option<ProcStat> {
    let mut all = None;
    let mut cores = Vec::new();
    for line in contents.lines() {
        let mut fields = line.split_whitespace();
        let Some(name) = fields.next() else {
            continue;
        };
        let Some(suffix) = name.strip_prefix("cpu") else {
            continue;
        };
        let values: Vec<u64> = fields.take(8).map_while(|v| v.parse().ok()).collect();
        if values.len() < 4 {
            continue;
        }
        let idle = values[3].saturating_add(values.get(4).copied().unwrap_or(0));
        let total = values.iter().fold(0u64, |sum, v| sum.saturating_add(*v));
        let times = CpuTimes {
            busy: total.saturating_sub(idle),
            total,
        };
        if suffix.is_empty() {
            all = Some(times);
        } else if suffix.bytes().all(|b| b.is_ascii_digit()) {
            cores.push(times);
        }
    }
    all.map(|all| ProcStat { all, cores })
}

/// CPU utilisation over one window.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CpuUsage {
    /// Busy share of all CPUs, per mille.
    pub busy_milli: u32,
    /// Busy share of each CPU, per mille.
    pub core_busy_milli: Vec<u32>,
    /// Window length.
    pub window: Duration,
}

fn busy_milli(previous: CpuTimes, next: CpuTimes) -> u32 {
    let total = next.total.saturating_sub(previous.total);
    if total == 0 {
        return 0;
    }
    let busy = next.busy.saturating_sub(previous.busy).min(total);
    u32::try_from(busy.saturating_mul(1000) / total).unwrap_or(1000)
}

/// Turns successive `/proc/stat` readings into utilisation figures.
///
/// The first reading only sets the baseline. A reading taken less than `min_window` after the
/// baseline returns the previous figures without moving it, so several readers sharing one
/// tracker never shrink the window below `min_window`.
#[derive(Debug)]
pub struct CpuUsageTracker {
    min_window: Duration,
    baseline: Option<(ProcStat, Instant)>,
    latest: Option<CpuUsage>,
}

impl CpuUsageTracker {
    pub fn new(min_window: Duration) -> Self {
        Self {
            min_window,
            baseline: None,
            latest: None,
        }
    }

    /// Feeds a reading taken at `now` and returns the utilisation of the latest window.
    pub fn update(&mut self, reading: ProcStat, now: Instant) -> Option<CpuUsage> {
        let Some((baseline, at)) = &self.baseline else {
            self.baseline = Some((reading, now));
            return None;
        };
        let window = now.saturating_duration_since(*at);
        if window < self.min_window {
            return self.latest.clone();
        }
        let usage = CpuUsage {
            busy_milli: busy_milli(baseline.all, reading.all),
            core_busy_milli: reading
                .cores
                .iter()
                .enumerate()
                .map(|(index, core)| {
                    // A CPU that came online since the baseline is measured from zero.
                    busy_milli(
                        baseline.cores.get(index).copied().unwrap_or_default(),
                        *core,
                    )
                })
                .collect(),
            window,
        };
        self.baseline = Some((reading, now));
        self.latest = Some(usage.clone());
        Some(usage)
    }

    /// Utilisation of the latest completed window.
    pub fn latest(&self) -> Option<&CpuUsage> {
        self.latest.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STAT_A: &str = "cpu  100 0 100 700 100 0 0 0 0 0\n\
cpu0 50 0 50 350 50 0 0 0 0 0\n\
cpu1 50 0 50 350 50 0 0 0 0 0\n\
intr 12345 0 0\n\
ctxt 999\n\
cpufreq 1 2 3 4\n";

    const STAT_B: &str = "cpu  250 0 150 1000 100 0 0 0 0 0\n\
cpu0 150 0 100 400 50 0 0 0 0 0\n\
cpu1 100 0 50 600 50 0 0 0 0 0\n";

    #[test]
    fn parses_aggregate_and_per_cpu_lines() {
        let stat = parse_proc_stat(STAT_A).expect("cpu line");
        assert_eq!(
            stat.all,
            CpuTimes {
                busy: 200,
                total: 1000
            }
        );
        assert_eq!(stat.cores.len(), 2, "cpufreq-like lines are not CPUs");
        assert_eq!(
            stat.cores[0],
            CpuTimes {
                busy: 100,
                total: 500
            }
        );
        assert_eq!(parse_proc_stat("intr 1\n"), None);
        assert_eq!(parse_proc_stat("cpu 1 2\n"), None, "too few fields");
    }

    #[test]
    fn tracker_reports_busy_share_between_readings() {
        let start = Instant::now();
        let mut tracker = CpuUsageTracker::new(Duration::from_millis(250));
        let a = parse_proc_stat(STAT_A).expect("a");
        let b = parse_proc_stat(STAT_B).expect("b");
        assert_eq!(
            tracker.update(a, start),
            None,
            "first reading is the baseline"
        );
        assert_eq!(
            tracker.update(b.clone(), start + Duration::from_millis(100)),
            None,
            "too short a window keeps the baseline"
        );
        let usage = tracker
            .update(b.clone(), start + Duration::from_secs(1))
            .expect("usage");
        // all: busy 200 -> 400 of total 1000 -> 1500.
        assert_eq!(usage.busy_milli, 400);
        // cpu0: busy 100 -> 250 of 500 -> 700; cpu1: busy 100 -> 150 of 500 -> 800.
        assert_eq!(usage.core_busy_milli, vec![750, 166]);
        assert_eq!(usage.window, Duration::from_secs(1));
        // A reader right after sees the same figures.
        let again = tracker
            .update(b.clone(), start + Duration::from_millis(1100))
            .expect("cached");
        assert_eq!(again, usage);
        // An idle window reads zero, never divides by zero.
        let idle = tracker
            .update(b, start + Duration::from_secs(3))
            .expect("idle");
        assert_eq!(idle.busy_milli, 0);
        assert_eq!(tracker.latest(), Some(&idle));
    }
}

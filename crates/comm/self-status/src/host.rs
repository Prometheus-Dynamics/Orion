//! The board's vital signs, from the operating system.

use std::path::{Path, PathBuf};

/// One reading of the host.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct HostSample {
    /// Seconds since the board started.
    pub uptime_s: u64,
    /// All memory, in kibibytes.
    pub mem_total_kb: u64,
    /// Memory that can be given to programs without swapping, in kibibytes.
    pub mem_available_kb: u64,
    /// How busy the processors were since the reading before, 0 to 100. `None` on the first
    /// reading, when there is nothing to compare with.
    pub cpu_percent: Option<f64>,
    /// The hottest temperature the board reports, in degrees Celsius. `None` if it reports
    /// none.
    pub temperature_c: Option<f64>,
}

/// Something that can read the host. The comm core asks only while someone is watching.
pub trait HostSource {
    /// Reads the host now.
    fn sample(&mut self) -> HostSample;
}

/// Reads `/proc` and `/sys`.
#[derive(Debug)]
pub struct ProcSource {
    root: PathBuf,
    /// Busy and total processor time at the last reading.
    last_cpu: Option<(u64, u64)>,
}

impl Default for ProcSource {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcSource {
    /// Reads the running system.
    #[must_use]
    pub fn new() -> Self {
        Self::with_root("/")
    }

    /// Reads a tree shaped like the running system's, under `root`.
    #[must_use]
    pub fn with_root(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            last_cpu: None,
        }
    }

    fn read(&self, path: &str) -> Option<String> {
        std::fs::read_to_string(self.root.join(path.trim_start_matches('/'))).ok()
    }

    fn temperature(&self) -> Option<f64> {
        let zones = std::fs::read_dir(self.root.join("sys/class/thermal")).ok()?;
        zones
            .flatten()
            .filter(|zone| {
                zone.file_name()
                    .to_string_lossy()
                    .starts_with("thermal_zone")
            })
            .filter_map(|zone| millidegrees(&zone.path().join("temp")))
            .fold(None, |hottest: Option<f64>, reading| {
                Some(hottest.map_or(reading, |hottest| hottest.max(reading)))
            })
    }
}

fn millidegrees(path: &Path) -> Option<f64> {
    let text = std::fs::read_to_string(path).ok()?;
    let reading: f64 = text.trim().parse().ok()?;
    // Rounded to a tenth: the last digits are noise, and a value that never settles would
    // be published on every reading.
    Some((reading / 100.0).round() / 10.0)
}

/// `key:   <number> kB` from `/proc/meminfo`.
fn meminfo(text: &str, key: &str) -> Option<u64> {
    text.lines()
        .find_map(|line| line.strip_prefix(key)?.strip_prefix(':'))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

/// Busy and total processor time from the first line of `/proc/stat`.
fn cpu_times(text: &str) -> Option<(u64, u64)> {
    let mut fields = text
        .lines()
        .next()?
        .strip_prefix("cpu ")?
        .split_whitespace();
    let mut times = [0_u64; 8];
    for time in &mut times {
        *time = fields.next()?.parse().ok()?;
    }
    let total: u64 = times.iter().sum();
    // Idle and waiting on I/O are the two that are not work.
    Some((total - times[3] - times[4], total))
}

impl HostSource for ProcSource {
    fn sample(&mut self) -> HostSample {
        let meminfo_text = self.read("/proc/meminfo").unwrap_or_default();
        let cpu = self.read("/proc/stat").as_deref().and_then(cpu_times);
        let cpu_percent = match (self.last_cpu, cpu) {
            (Some((busy_before, total_before)), Some((busy, total))) if total > total_before => {
                let share = (busy - busy_before.min(busy)) as f64 / (total - total_before) as f64;
                Some((share * 1000.0).round() / 10.0)
            }
            _ => None,
        };
        self.last_cpu = cpu;
        HostSample {
            uptime_s: self
                .read("/proc/uptime")
                .and_then(|text| text.split_whitespace().next()?.parse::<f64>().ok())
                .map_or(0, |seconds| seconds as u64),
            mem_total_kb: meminfo(&meminfo_text, "MemTotal").unwrap_or(0),
            mem_available_kb: meminfo(&meminfo_text, "MemAvailable").unwrap_or(0),
            cpu_percent,
            temperature_c: self.temperature(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_running_system_can_be_read() {
        let mut source = ProcSource::new();
        let first = source.sample();
        assert!(first.uptime_s > 0 && first.mem_total_kb > 0);
        assert!(first.mem_available_kb > 0 && first.mem_available_kb <= first.mem_total_kb);
        assert_eq!(
            first.cpu_percent, None,
            "nothing to compare the first reading with"
        );
        // Do some work, so the processor times have moved by the second reading.
        let mut work = 0_u64;
        let started = std::time::Instant::now();
        while started.elapsed() < std::time::Duration::from_millis(60) {
            work = std::hint::black_box(work.wrapping_add(1));
        }
        let second = source.sample();
        let busy = second.cpu_percent.expect("a second reading");
        assert!((0.0..=100.0).contains(&busy), "{busy}");
    }

    #[test]
    fn a_tree_is_read_as_the_system_would_be() {
        let root = std::env::temp_dir().join(format!("orion-host-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        for zone in ["thermal_zone0", "thermal_zone1", "cooling_device0"] {
            std::fs::create_dir_all(root.join("sys/class/thermal").join(zone)).expect("dir");
        }
        std::fs::create_dir_all(root.join("proc")).expect("dir");
        let write = |path: &str, text: &str| std::fs::write(root.join(path), text).expect("write");
        write("proc/uptime", "12345.67 9999.00\n");
        write(
            "proc/meminfo",
            "MemTotal:        4000000 kB\nMemFree:  1 kB\nMemAvailable:    3000000 kB\n",
        );
        write(
            "proc/stat",
            "cpu  100 0 100 700 100 0 0 0 0 0\ncpu0 1 2 3\n",
        );
        write("sys/class/thermal/thermal_zone0/temp", "41234\n");
        write("sys/class/thermal/thermal_zone1/temp", "52987\n");
        write("sys/class/thermal/cooling_device0/temp", "99999\n");

        let mut source = ProcSource::with_root(&root);
        let first = source.sample();
        assert_eq!(
            (first.uptime_s, first.mem_total_kb, first.mem_available_kb),
            (12345, 4_000_000, 3_000_000)
        );
        assert_eq!(
            (first.cpu_percent, first.temperature_c),
            (None, Some(53.0)),
            "the hottest zone"
        );
        // 1000 more ticks, 250 of them work.
        write("proc/stat", "cpu  300 0 150 1400 150 0 0 0 0 0\n");
        assert_eq!(source.sample().cpu_percent, Some(25.0));

        // A board that reports no temperature, and files that are not there.
        let empty = root.join("empty");
        std::fs::create_dir_all(&empty).expect("dir");
        assert_eq!(
            ProcSource::with_root(&empty).sample(),
            HostSample::default()
        );
        let _ = std::fs::remove_dir_all(root);
    }
}

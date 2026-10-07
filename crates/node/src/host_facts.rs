//! Host facts sources.
//!
//! The node samples a [`HostFactsSource`] every `ORION_NODE_HOST_FACTS_REFRESH_MS`: identity facts
//! go into its observed node record (when they change), volatile metrics into the status lane
//! under `node/<id>` (see `docs/host-facts.md`). [`LinuxHostFactsSource`] is the default; an
//! embedder replaces it with `NodeAppBuilder::with_host_facts_source` or layers richer facts on
//! top with `NodeAppBuilder::with_host_facts_overlay` (for example a hardware-abstraction crate
//! that knows the board serial number or a PMIC temperature).
//!
//! Sources must be cheap: the node calls them on a blocking thread once per refresh interval.

mod cpu;

pub use cpu::{CpuTimes, CpuUsage, CpuUsageTracker, ProcStat, parse_proc_stat};
pub use orion::control_plane::HostFactsSource;
use orion::control_plane::{HostFacts, HostMetricsSample, HostTemperature, NodeHostFacts};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

/// Most temperature sensors reported per sample (also bounds status-lane entries).
pub const MAX_HOST_TEMPERATURES: usize = 32;

/// A base source with overlays merged on top ([`HostFacts::merge`]): set fields of an overlay
/// replace the base's, labels and extra metrics are added, temperatures replace readings of the
/// same sensor.
pub struct LayeredHostFactsSource {
    base: Arc<dyn HostFactsSource>,
    overlays: Vec<Arc<dyn HostFactsSource>>,
}

impl LayeredHostFactsSource {
    pub fn new(base: Arc<dyn HostFactsSource>) -> Self {
        Self {
            base,
            overlays: Vec::new(),
        }
    }

    pub fn with_overlay(mut self, overlay: Arc<dyn HostFactsSource>) -> Self {
        self.overlays.push(overlay);
        self
    }
}

impl HostFactsSource for LayeredHostFactsSource {
    fn sample(&self) -> HostFacts {
        let mut facts = self.base.sample();
        for overlay in &self.overlays {
            facts.merge(overlay.sample());
        }
        facts.metrics.temperatures.truncate(MAX_HOST_TEMPERATURES);
        facts
    }
}

/// Reads `/proc`, `/sys`, and `/etc/os-release` (relative to a root directory, `/` by default).
///
/// - identity: `/proc/sys/kernel/hostname`, `/etc/os-release` (or `/usr/lib/os-release`: `ID`,
///   `NAME`, `VERSION_ID`, `IMAGE_ID`, `IMAGE_VERSION`), the configured image files,
///   `/proc/sys/kernel/osrelease`, `/proc/sys/kernel/random/boot_id`,
///   `/proc/device-tree/serial-number` (or DMI `product_serial` / `board_serial`),
///   `/proc/device-tree/model` (or DMI `product_name`), `/etc/machine-id`,
///   `/sys/devices/system/cpu/online`, `MemTotal` from `/proc/meminfo`;
/// - metrics: `/proc/uptime`, `/proc/loadavg`, `MemAvailable`, CPU utilisation since the
///   previous sample from `/proc/stat` (none on the first sample), and every
///   `/sys/class/thermal/thermal_zone*/temp` labeled by its `type`.
///
/// Clones share the CPU baseline.
#[derive(Clone, Debug)]
pub struct LinuxHostFactsSource {
    root: PathBuf,
    image_files: Vec<PathBuf>,
    cpu: Arc<Mutex<CpuUsageTracker>>,
}

impl Default for LinuxHostFactsSource {
    fn default() -> Self {
        Self::new()
    }
}

impl LinuxHostFactsSource {
    pub fn new() -> Self {
        Self::with_root("/")
    }

    /// Reads every file below `root` instead of `/` (for tests and containers that mount the host
    /// file system elsewhere).
    pub fn with_root(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            image_files: Vec::new(),
            cpu: Arc::new(Mutex::new(CpuUsageTracker::new(Duration::ZERO))),
        }
    }

    /// Files that declare the system image (`ORION_NODE_IMAGE_VERSION_FILE`), tried in order; the
    /// first readable one wins over `IMAGE_ID`/`IMAGE_VERSION` from `os-release`. Absolute paths
    /// are resolved below the root. See [`parse_image_file`] for the format.
    pub fn with_image_files(mut self, files: impl IntoIterator<Item = PathBuf>) -> Self {
        self.image_files = files.into_iter().collect();
        self
    }

    fn path(&self, path: impl AsRef<Path>) -> PathBuf {
        let path = path.as_ref();
        self.root.join(path.strip_prefix("/").unwrap_or(path))
    }

    fn read(&self, path: impl AsRef<Path>) -> Option<String> {
        fs::read_to_string(self.path(path)).ok()
    }

    fn read_trimmed(&self, path: &str) -> Option<String> {
        self.read(path)
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
    }

    /// The first readable, non-empty firmware string (device tree properties end in NUL; DMI
    /// files are often root-only, which simply leaves the fact unset).
    fn first_firmware_string(&self, paths: &[&str]) -> Option<String> {
        paths.iter().find_map(|path| {
            fs::read(self.path(path))
                .ok()
                .and_then(|bytes| firmware_string(&bytes))
        })
    }

    fn identity(&self, meminfo: Option<&str>) -> NodeHostFacts {
        let os_release = self
            .read("/etc/os-release")
            .or_else(|| self.read("/usr/lib/os-release"))
            .map(|contents| parse_os_release(&contents))
            .unwrap_or_default();
        let mut facts = NodeHostFacts {
            hostname: self.read_trimmed("/proc/sys/kernel/hostname"),
            os_id: os_release.id,
            os_name: os_release.name,
            os_version: os_release.version_id,
            image_name: os_release.image_id,
            image_version: os_release.image_version,
            kernel_release: self.read_trimmed("/proc/sys/kernel/osrelease"),
            architecture: Some(std::env::consts::ARCH.to_owned()),
            boot_id: self.read_trimmed("/proc/sys/kernel/random/boot_id"),
            board_serial: self.first_firmware_string(&[
                "/proc/device-tree/serial-number",
                "/sys/class/dmi/id/product_serial",
                "/sys/class/dmi/id/board_serial",
            ]),
            board_model: self.first_firmware_string(&[
                "/proc/device-tree/model",
                "/sys/class/dmi/id/product_name",
            ]),
            machine_id: self.read_trimmed("/etc/machine-id"),
            cpu_count: self
                .read("/sys/devices/system/cpu/online")
                .and_then(|contents| parse_cpu_list(&contents)),
            memory_total_bytes: meminfo.and_then(|contents| meminfo_bytes(contents, "MemTotal")),
            labels: Default::default(),
        };
        if let Some(image) = self
            .image_files
            .iter()
            .find_map(|path| self.read(path).map(|contents| parse_image_file(&contents)))
        {
            if image.name.is_some() {
                facts.image_name = image.name;
            }
            if image.version.is_some() {
                facts.image_version = image.version;
            }
        }
        facts
    }

    /// CPU utilisation since the previous call (`None` on the first).
    fn cpu_usage(&self) -> Option<CpuUsage> {
        let reading = parse_proc_stat(&self.read("/proc/stat")?)?;
        let mut tracker = self
            .cpu
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        tracker.update(reading, Instant::now())
    }

    /// Temperatures from every `/sys/class/thermal/thermal_zone*` (see [`thermal_readings`]).
    pub fn temperatures(&self) -> Vec<HostTemperature> {
        thermal_zone_temperatures(&self.path("/sys/class/thermal"))
    }
}

/// Temperatures of every `thermal_zone*` below `dir` (normally `/sys/class/thermal`), labeled as
/// [`thermal_readings`] describes, in zone order.
pub fn thermal_zone_temperatures(dir: &Path) -> Vec<HostTemperature> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut zones: Vec<(String, Option<String>, String)> = entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let zone = entry.file_name().to_string_lossy().into_owned();
            if !zone.starts_with("thermal_zone") {
                return None;
            }
            let dir = entry.path();
            let temp = fs::read_to_string(dir.join("temp")).ok()?;
            let kind = fs::read_to_string(dir.join("type")).ok();
            Some((zone, kind, temp))
        })
        .collect();
    zones.sort_by_key(|a| zone_order(&a.0));
    thermal_readings(
        zones
            .iter()
            .map(|(zone, kind, temp)| (zone.as_str(), kind.as_deref(), temp.as_str())),
    )
}

fn zone_order(zone: &str) -> (u64, String) {
    let number = zone
        .trim_start_matches("thermal_zone")
        .parse()
        .unwrap_or(u64::MAX);
    (number, zone.to_owned())
}

impl HostFactsSource for LinuxHostFactsSource {
    fn sample(&self) -> HostFacts {
        let meminfo = self.read("/proc/meminfo");
        let loadavg = self.read("/proc/loadavg").map(|text| parse_loadavg(&text));
        let (load_1, load_5, load_15) = loadavg.unwrap_or_default();
        let cpu = self.cpu_usage();
        HostFacts {
            identity: self.identity(meminfo.as_deref()),
            metrics: HostMetricsSample {
                uptime_seconds: self
                    .read("/proc/uptime")
                    .and_then(|text| parse_uptime_seconds(&text)),
                load_1_milli: load_1,
                load_5_milli: load_5,
                load_15_milli: load_15,
                memory_available_bytes: meminfo
                    .as_deref()
                    .and_then(|contents| meminfo_bytes(contents, "MemAvailable")),
                cpu_busy_milli: cpu.as_ref().map(|usage| usage.busy_milli),
                cpu_core_busy_milli: cpu.map(|usage| usage.core_busy_milli).unwrap_or_default(),
                temperatures: self.temperatures(),
                extra: Default::default(),
            },
            sampled_at_ms: 0,
        }
    }
}

/// A firmware string (device tree property or DMI file) with trailing NULs and surrounding
/// whitespace removed; otherwise reported raw. `None` when empty.
pub fn firmware_string(bytes: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(bytes);
    let text = text
        .trim_end_matches(['\0', ' ', '\n', '\r', '\t'])
        .trim_start();
    (!text.is_empty()).then(|| text.to_owned())
}

/// The `os-release` fields Orion reports.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OsRelease {
    pub id: Option<String>,
    pub name: Option<String>,
    pub version_id: Option<String>,
    pub image_id: Option<String>,
    pub image_version: Option<String>,
}

fn unquote(value: &str) -> String {
    let value = value.trim();
    let value = value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .or_else(|| {
            value
                .strip_prefix('\'')
                .and_then(|value| value.strip_suffix('\''))
        })
        .unwrap_or(value);
    value.replace("\\\"", "\"").replace("\\\\", "\\")
}

fn key_values(contents: &str) -> impl Iterator<Item = (&str, String)> {
    contents.lines().filter_map(|line| {
        let line = line.trim();
        if line.starts_with('#') {
            return None;
        }
        let (key, value) = line.split_once('=')?;
        let value = unquote(value);
        (!value.is_empty()).then_some((key.trim(), value))
    })
}

/// Parses an `os-release(5)` document.
pub fn parse_os_release(contents: &str) -> OsRelease {
    let mut release = OsRelease::default();
    for (key, value) in key_values(contents) {
        let slot = match key {
            "ID" => &mut release.id,
            "NAME" => &mut release.name,
            "VERSION_ID" => &mut release.version_id,
            "IMAGE_ID" => &mut release.image_id,
            "IMAGE_VERSION" => &mut release.image_version,
            _ => continue,
        };
        *slot = Some(value);
    }
    release
}

/// Image name and version from an image file.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ImageInfo {
    pub name: Option<String>,
    pub version: Option<String>,
}

/// Parses an image file: either `KEY=value` lines (`IMAGE_ID` or `IMAGE_NAME` or `NAME` or `ID`
/// for the name; `IMAGE_VERSION` or `VERSION_ID` or `VERSION` for the version, os-release style),
/// or a single line that is the version.
pub fn parse_image_file(contents: &str) -> ImageInfo {
    let pairs: Vec<(&str, String)> = key_values(contents).collect();
    if pairs.is_empty() {
        return ImageInfo {
            name: None,
            version: contents
                .lines()
                .map(str::trim)
                .find(|line| !line.is_empty() && !line.starts_with('#'))
                .map(str::to_owned),
        };
    }
    let first = |keys: &[&str]| {
        keys.iter().find_map(|wanted| {
            pairs
                .iter()
                .find(|(key, _)| key == wanted)
                .map(|(_, value)| value.clone())
        })
    };
    ImageInfo {
        name: first(&["IMAGE_ID", "IMAGE_NAME", "NAME", "ID"]),
        version: first(&["IMAGE_VERSION", "VERSION_ID", "VERSION"]),
    }
}

/// Whole seconds from `/proc/uptime`.
pub fn parse_uptime_seconds(contents: &str) -> Option<u64> {
    contents
        .split_whitespace()
        .next()?
        .split('.')
        .next()?
        .parse()
        .ok()
}

/// The three load averages from `/proc/loadavg`, multiplied by 1000.
pub fn parse_loadavg(contents: &str) -> (Option<u64>, Option<u64>, Option<u64>) {
    let mut fields = contents.split_whitespace().map(decimal_to_milli);
    (
        fields.next().flatten(),
        fields.next().flatten(),
        fields.next().flatten(),
    )
}

/// A `/proc/meminfo` field in bytes (the file reports kB).
pub fn meminfo_bytes(contents: &str, key: &str) -> Option<u64> {
    contents.lines().find_map(|line| {
        let (line_key, rest) = line.split_once(':')?;
        if line_key.trim() != key {
            return None;
        }
        let mut parts = rest.split_whitespace();
        let value: u64 = parts.next()?.parse().ok()?;
        match parts.next() {
            Some("kB") => Some(value.saturating_mul(1024)),
            None => Some(value),
            Some(_) => None,
        }
    })
}

/// Number of CPUs in a kernel CPU list such as `0-3,6,8-9` (`/sys/devices/system/cpu/online`).
pub fn parse_cpu_list(contents: &str) -> Option<u32> {
    let mut count: u32 = 0;
    for part in contents.trim().split(',').filter(|part| !part.is_empty()) {
        count = count.saturating_add(match part.split_once('-') {
            Some((start, end)) => {
                let (start, end): (u32, u32) = (start.parse().ok()?, end.parse().ok()?);
                end.checked_sub(start)?.saturating_add(1)
            }
            None => {
                part.parse::<u32>().ok()?;
                1
            }
        });
    }
    (count > 0).then_some(count)
}

/// Temperature readings from thermal zones given as `(zone name, type file, temp file)`.
/// Sensors are labeled by their `type`; a type shared by several zones gets the zone name
/// appended (`cpu-thermal/thermal_zone1`). Unreadable values are skipped.
pub fn thermal_readings<'a>(
    zones: impl IntoIterator<Item = (&'a str, Option<&'a str>, &'a str)>,
) -> Vec<HostTemperature> {
    let zones: Vec<(&str, String, i32)> = zones
        .into_iter()
        .filter_map(|(zone, kind, temp)| {
            let millidegrees = temp.trim().parse::<i32>().ok()?;
            let kind = kind
                .map(str::trim)
                .filter(|kind| !kind.is_empty())
                .unwrap_or(zone)
                .to_owned();
            Some((zone, kind, millidegrees))
        })
        .collect();
    zones
        .iter()
        .map(|(zone, kind, millidegrees)| {
            let shared = zones.iter().filter(|(_, other, _)| other == kind).count() > 1;
            let sensor = if shared {
                format!("{kind}/{zone}")
            } else {
                kind.clone()
            };
            HostTemperature::new(sensor, *millidegrees)
        })
        .take(MAX_HOST_TEMPERATURES)
        .collect()
}

fn decimal_to_milli(value: &str) -> Option<u64> {
    let (whole, fractional) = value.split_once('.').unwrap_or((value, ""));
    let whole = whole.parse::<u64>().ok()?;
    let mut digits: String = fractional.chars().take(3).collect();
    while digits.len() < 3 {
        digits.push('0');
    }
    Some(
        whole
            .saturating_mul(1000)
            .saturating_add(digits.parse::<u64>().ok()?),
    )
}

#[cfg(test)]
mod tests;

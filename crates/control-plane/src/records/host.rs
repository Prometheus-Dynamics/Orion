//! Host facts a node reports about the machine it runs on.
//!
//! A [`HostFacts`] sample has two halves with different lifetimes:
//!
//! - [`NodeHostFacts`]: slow-changing identity (hostname, OS, image, kernel, architecture, boot id,
//!   board serial and model, machine id, CPU count, total memory, extra labels). It is published in the node's own observed
//!   [`NodeRecord`](super::NodeRecord) only when it changes, so it is replicated to peers with the
//!   observed slice like the clock facts.
//! - [`HostMetricsSample`]: volatile metrics (uptime, load, available memory, temperatures, extra
//!   metrics). They go to the node's volatile status lane under the node subject and to the
//!   observability snapshot, never into records.
//!
//! Every field is optional: a source fills in what its platform reports.

use crate::TypedConfigValue;
// `Arc` needs pointer-width atomics (absent on thumbv6m and riscv32imc).
#[cfg(target_has_atomic = "ptr")]
use alloc::sync::Arc;
use alloc::{boxed::Box, collections::BTreeMap, string::String, vec::Vec};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

/// Slow-changing identity facts of a node's host, published in its observed node record.
#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct NodeHostFacts {
    pub hostname: Option<String>,
    /// `ID` from `os-release` (for example `debian`).
    pub os_id: Option<String>,
    /// `NAME` from `os-release` (for example `Debian GNU/Linux`).
    pub os_name: Option<String>,
    /// `VERSION_ID` from `os-release`.
    pub os_version: Option<String>,
    /// Name of the system image, when the host declares one (`IMAGE_ID` or a configured file).
    pub image_name: Option<String>,
    /// Version of the system image (`IMAGE_VERSION` or a configured file).
    pub image_version: Option<String>,
    /// Kernel release (`uname -r`).
    pub kernel_release: Option<String>,
    /// CPU architecture (for example `x86_64`, `aarch64`).
    pub architecture: Option<String>,
    /// Identifier of the current boot; changes on every reboot.
    pub boot_id: Option<String>,
    /// Board or system serial number as the platform reports it (device tree `serial-number`,
    /// or DMI `product_serial` / `board_serial`); raw, consumers normalize.
    pub board_serial: Option<String>,
    /// Board or system model (device tree `model`, or DMI `product_name`).
    pub board_model: Option<String>,
    /// `/etc/machine-id`.
    pub machine_id: Option<String>,
    /// Online logical CPUs.
    pub cpu_count: Option<u32>,
    pub memory_total_bytes: Option<u64>,
    /// Extra labeled facts from a host-facts source (for example an asset tag or a carrier-board
    /// revision).
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

impl NodeHostFacts {
    /// Overlays `other`: its fields that are set replace this value's, and its labels are added
    /// (replacing labels with the same key).
    pub fn merge(&mut self, other: NodeHostFacts) {
        macro_rules! take {
            ($($field:ident),*) => {$(
                if other.$field.is_some() {
                    self.$field = other.$field;
                }
            )*};
        }
        take!(
            hostname,
            os_id,
            os_name,
            os_version,
            image_name,
            image_version,
            kernel_release,
            architecture,
            boot_id,
            board_serial,
            board_model,
            machine_id,
            cpu_count,
            memory_total_bytes
        );
        self.labels.extend(other.labels);
    }
}

/// One temperature reading.
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct HostTemperature {
    /// Sensor label (for example `cpu-thermal` or `thermal_zone0`).
    pub sensor: String,
    /// Temperature in millidegrees Celsius.
    pub millidegrees_c: i32,
}

impl HostTemperature {
    pub fn new(sensor: impl Into<String>, millidegrees_c: i32) -> Self {
        Self {
            sensor: sensor.into(),
            millidegrees_c,
        }
    }
}

/// Volatile host metrics of one sample.
#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct HostMetricsSample {
    pub uptime_seconds: Option<u64>,
    /// Load averages multiplied by 1000.
    pub load_1_milli: Option<u64>,
    pub load_5_milli: Option<u64>,
    pub load_15_milli: Option<u64>,
    pub memory_available_bytes: Option<u64>,
    /// Busy share of all CPUs, per mille (0 to 1000), since the source's previous sample.
    #[serde(default)]
    pub cpu_busy_milli: Option<u32>,
    /// Busy share of each CPU, per mille, in kernel CPU order, over the same window.
    #[serde(default)]
    pub cpu_core_busy_milli: Vec<u32>,
    #[serde(default)]
    pub temperatures: Vec<HostTemperature>,
    /// Extra metrics from a host-facts source (for example fan speed or supply voltage).
    #[serde(default)]
    pub extra: BTreeMap<String, TypedConfigValue>,
}

impl HostMetricsSample {
    /// Overlays `other`: set fields (and a non-empty per-core CPU list) replace, temperatures
    /// replace readings of the same sensor, and extra metrics are added.
    pub fn merge(&mut self, other: HostMetricsSample) {
        macro_rules! take {
            ($($field:ident),*) => {$(
                if other.$field.is_some() {
                    self.$field = other.$field;
                }
            )*};
        }
        take!(
            uptime_seconds,
            load_1_milli,
            load_5_milli,
            load_15_milli,
            memory_available_bytes,
            cpu_busy_milli
        );
        if !other.cpu_core_busy_milli.is_empty() {
            self.cpu_core_busy_milli = other.cpu_core_busy_milli;
        }
        for reading in other.temperatures {
            match self
                .temperatures
                .iter_mut()
                .find(|existing| existing.sensor == reading.sensor)
            {
                Some(existing) => *existing = reading,
                None => self.temperatures.push(reading),
            }
        }
        self.extra.extend(other.extra);
    }
}

/// One sample of a host-facts source: identity facts plus volatile metrics.
#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct HostFacts {
    pub identity: NodeHostFacts,
    pub metrics: HostMetricsSample,
    /// When the sample was taken, in Unix milliseconds.
    pub sampled_at_ms: u64,
}

impl HostFacts {
    /// Overlays another source's sample (see [`NodeHostFacts::merge`] and
    /// [`HostMetricsSample::merge`]); the later `sampled_at_ms` wins.
    pub fn merge(&mut self, other: HostFacts) {
        self.identity.merge(other.identity);
        self.metrics.merge(other.metrics);
        self.sampled_at_ms = self.sampled_at_ms.max(other.sampled_at_ms);
    }
}

/// Where a node reads host facts from. Implement it to supply facts from another platform or a
/// hardware-abstraction layer; fields a source cannot report stay `None`. Lives here (not in
/// `orion-node`) so adapter crates only need the `no_std` model crate.
pub trait HostFactsSource: Send + Sync {
    /// Takes one sample. `sampled_at_ms` may be left `0`; the node stamps it.
    fn sample(&self) -> HostFacts;
}

#[cfg(target_has_atomic = "ptr")]
impl<T: HostFactsSource + ?Sized> HostFactsSource for Arc<T> {
    fn sample(&self) -> HostFacts {
        (**self).sample()
    }
}

impl<T: HostFactsSource + ?Sized> HostFactsSource for Box<T> {
    fn sample(&self) -> HostFacts {
        (**self).sample()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_overlays_set_fields_and_unions_maps() {
        let mut base = HostFacts {
            identity: NodeHostFacts {
                hostname: Some("edge-1".into()),
                os_id: Some("debian".into()),
                ..NodeHostFacts::default()
            },
            metrics: HostMetricsSample {
                uptime_seconds: Some(10),
                temperatures: alloc::vec![HostTemperature::new("cpu", 40_000)],
                ..HostMetricsSample::default()
            },
            sampled_at_ms: 5,
        };
        let mut labels = BTreeMap::new();
        labels.insert(String::from("board.serial"), String::from("A1"));
        base.merge(HostFacts {
            identity: NodeHostFacts {
                image_version: Some("2.1".into()),
                labels,
                ..NodeHostFacts::default()
            },
            metrics: HostMetricsSample {
                temperatures: alloc::vec![
                    HostTemperature::new("cpu", 41_000),
                    HostTemperature::new("pmic", 30_000),
                ],
                ..HostMetricsSample::default()
            },
            sampled_at_ms: 7,
        });
        assert_eq!(base.identity.hostname.as_deref(), Some("edge-1"));
        assert_eq!(base.identity.image_version.as_deref(), Some("2.1"));
        assert_eq!(base.identity.labels["board.serial"], "A1");
        assert_eq!(base.metrics.uptime_seconds, Some(10));
        assert_eq!(
            base.metrics.temperatures,
            alloc::vec![
                HostTemperature::new("cpu", 41_000),
                HostTemperature::new("pmic", 30_000)
            ]
        );
        assert_eq!(base.sampled_at_ms, 7);
    }
}

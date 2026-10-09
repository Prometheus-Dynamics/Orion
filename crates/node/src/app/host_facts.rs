//! Publishing host facts: identity facts into the node's observed record, volatile metrics into
//! the status lane under `node/<id>` (`docs/host-facts.md`).

use super::{NodeApp, ReconcileLoopHandle};
use crate::host_facts::{CpuUsageTracker, HostFactsSource, thermal_zone_temperatures};
use orion::control_plane::{
    HostFacts, HostMetricsSnapshot, NodeHostFacts, NodeRecord, StatusEntry, StatusSubject,
    TypedConfigValue,
};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// Status-lane publisher name of the node's own host metrics.
const HOST_STATUS_PUBLISHER: &str = "node:host-facts";
/// Status entries live this many refresh intervals, so a stalled sampler lets them expire.
const HOST_STATUS_TTL_INTERVALS: u32 = 3;
/// Most per-CPU utilisation entries published to the status lane per sample.
pub(crate) const MAX_CPU_STATUS_ENTRIES: usize = 64;
/// Most extra metrics published to the status lane per sample.
const MAX_EXTRA_STATUS_METRICS: usize = 64;

/// Shortest CPU window reported in observability snapshots: readers polling faster share the
/// previous window's figures instead of shrinking it.
const SNAPSHOT_CPU_MIN_WINDOW: Duration = Duration::from_millis(250);

/// The host-facts source a node samples, set by the builder, and the CPU baseline shared by
/// observability snapshots.
pub(super) struct HostFactsState {
    pub(super) source: Arc<dyn HostFactsSource>,
    snapshot_cpu: Mutex<CpuUsageTracker>,
}

impl HostFactsState {
    pub(super) fn new(source: Arc<dyn HostFactsSource>) -> Self {
        let mut tracker = CpuUsageTracker::new(SNAPSHOT_CPU_MIN_WINDOW);
        // Set the baseline now, so the first snapshot already covers a window.
        if let Some(reading) = read_proc_stat() {
            tracker.update(reading, Instant::now());
        }
        Self {
            source,
            snapshot_cpu: Mutex::new(tracker),
        }
    }
}

fn read_proc_stat() -> Option<crate::host_facts::ProcStat> {
    crate::host_facts::parse_proc_stat(&std::fs::read_to_string("/proc/stat").ok()?)
}

impl NodeApp {
    /// Samples `source` once: records the sample for observability, publishes the volatile
    /// metrics to the status lane under `node/<id>`, and replaces the identity facts in this
    /// node's observed record when they changed. Returns whether the observed record changed.
    ///
    /// `spawn_host_facts_loop` calls this with the configured source every
    /// `host_facts.refresh_interval`; embedders and tests can call it with their own source.
    pub fn refresh_host_facts_from(&self, source: &dyn HostFactsSource) -> bool {
        let mut facts = source.sample();
        let now_ms = Self::current_time_ms();
        if facts.sampled_at_ms == 0 {
            facts.sampled_at_ms = now_ms;
        }
        self.publish_host_metrics(&facts);
        self.observability_lock().host_facts = Some(facts.clone());
        let changed = self.publish_host_identity(facts.identity);
        if changed {
            self.notify_observed_watchers();
        }
        changed
    }

    /// Samples the configured source (the default Linux source, or the one set with
    /// `NodeAppBuilder::with_host_facts_source` / `with_host_facts_overlay`).
    pub fn refresh_host_facts(&self) -> bool {
        let source = self.state.host_facts.source.clone();
        self.refresh_host_facts_from(source.as_ref())
    }

    /// Fills the CPU utilisation (shared baseline, see `HostMetricsSnapshot::cpu_busy_milli`) and
    /// the `/sys/class/thermal` temperatures of an observability snapshot.
    pub(super) fn fill_live_host_metrics(&self, host: &mut HostMetricsSnapshot) {
        if let Some(reading) = read_proc_stat() {
            let usage = self
                .state
                .host_facts
                .snapshot_cpu
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .update(reading, Instant::now());
            if let Some(usage) = usage {
                host.cpu_busy_milli = Some(usage.busy_milli);
                host.cpu_core_busy_milli = usage.core_busy_milli;
                host.cpu_window_ms =
                    Some(u64::try_from(usage.window.as_millis()).unwrap_or(u64::MAX));
            }
        }
        host.temperatures = thermal_zone_temperatures(Path::new("/sys/class/thermal"));
    }

    /// Host identity facts currently published in this node's observed record.
    pub fn published_host_facts(&self) -> Option<NodeHostFacts> {
        let store = self.store_read();
        store
            .observed
            .nodes
            .get(&store.local_node_id)
            .and_then(|record| record.host.clone())
    }

    /// Latest host-facts sample (identity plus volatile metrics).
    pub fn latest_host_facts(&self) -> Option<HostFacts> {
        self.observability_read().host_facts.clone()
    }

    fn publish_host_identity(&self, identity: NodeHostFacts) -> bool {
        self.with_store_mut(|store| {
            let node_id = store.local_node_id.clone();
            if store
                .observed
                .nodes
                .get(&node_id)
                .and_then(|record| record.host.as_ref())
                == Some(&identity)
            {
                return false;
            }
            let template = store.desired.nodes.get(&node_id).cloned();
            let record = store
                .observed
                .nodes
                .entry(node_id)
                .or_insert_with_key(|node_id| {
                    let mut record =
                        template.unwrap_or_else(|| NodeRecord::builder(node_id.clone()).build());
                    record.clock = None;
                    record.host = None;
                    record
                });
            record.host = Some(identity);
            true
        })
    }

    fn publish_host_metrics(&self, facts: &HostFacts) {
        let subject = StatusSubject::Node(self.config.node_id.clone());
        let tuning = &self.config.runtime_tuning;
        let ttl_ms = u64::try_from(
            tuning
                .host_facts
                .refresh_interval
                .saturating_mul(HOST_STATUS_TTL_INTERVALS)
                .as_millis(),
        )
        .unwrap_or(u64::MAX);
        let entries = host_status_entries(&subject, facts)
            .into_iter()
            .map(|entry| entry.with_ttl_ms(ttl_ms))
            .collect::<Vec<_>>();
        if entries.is_empty() {
            return;
        }
        if let Err(error) = self.publish_status_as(HOST_STATUS_PUBLISHER, entries) {
            warn!(node = %self.config.node_id, error = %error, "failed to publish host metrics to the status lane");
        }
    }

    /// Samples host facts every `host_facts.refresh_interval` on a blocking thread. `None` when
    /// host facts are turned off (`ORION_NODE_HOST_FACTS_REFRESH_MS=0`).
    pub fn spawn_host_facts_loop(&self) -> Option<ReconcileLoopHandle> {
        let tuning = &self.config.runtime_tuning.host_facts;
        if !tuning.enabled() {
            return None;
        }
        Some(
            self.spawn_background_loop(tuning.refresh_interval, |app| async move {
                let node_id = app.config.node_id.clone();
                let changed = tokio::task::spawn_blocking(move || app.refresh_host_facts())
                    .await
                    .unwrap_or(false);
                if changed {
                    debug!(node = %node_id, "published host identity facts");
                }
            }),
        )
    }
}

/// Status-lane entries for one sample: `host.uptime_seconds`, `host.load1_milli`,
/// `host.load5_milli`, `host.load15_milli`, `host.memory_available_bytes`,
/// `host.memory_total_bytes`, `host.cpu_busy_milli`, `host.cpu<N>_busy_milli` (per CPU, at most
/// [`MAX_CPU_STATUS_ENTRIES`]), `host.temperature.<sensor>` (millidegrees Celsius), and
/// `host.extra.<key>` for extra metrics.
pub(crate) fn host_status_entries(subject: &StatusSubject, facts: &HostFacts) -> Vec<StatusEntry> {
    let metrics = &facts.metrics;
    let entry =
        |key: String, value: TypedConfigValue| StatusEntry::new(subject.clone(), key, value);
    let mut entries: Vec<StatusEntry> = [
        ("host.uptime_seconds", metrics.uptime_seconds),
        ("host.load1_milli", metrics.load_1_milli),
        ("host.load5_milli", metrics.load_5_milli),
        ("host.load15_milli", metrics.load_15_milli),
        (
            "host.memory_available_bytes",
            metrics.memory_available_bytes,
        ),
        ("host.memory_total_bytes", facts.identity.memory_total_bytes),
        ("host.cpu_busy_milli", metrics.cpu_busy_milli.map(u64::from)),
    ]
    .into_iter()
    .filter_map(|(key, value)| {
        value.map(|value| entry(key.to_owned(), TypedConfigValue::UInt(value)))
    })
    .collect();
    entries.extend(
        metrics
            .cpu_core_busy_milli
            .iter()
            .take(MAX_CPU_STATUS_ENTRIES)
            .enumerate()
            .map(|(index, milli)| {
                entry(
                    format!("host.cpu{index}_busy_milli"),
                    TypedConfigValue::UInt(u64::from(*milli)),
                )
            }),
    );
    entries.extend(metrics.temperatures.iter().map(|reading| {
        entry(
            format!("host.temperature.{}", reading.sensor),
            TypedConfigValue::Int(i64::from(reading.millidegrees_c)),
        )
    }));
    entries.extend(
        metrics
            .extra
            .iter()
            .take(MAX_EXTRA_STATUS_METRICS)
            .map(|(key, value)| entry(format!("host.extra.{key}"), value.clone())),
    );
    // A batch is stored atomically, so drop entries the lane would refuse instead of losing all.
    entries.retain(|entry| {
        let value_len = match &entry.value {
            TypedConfigValue::String(value) => value.len(),
            TypedConfigValue::Bytes(value) => value.len(),
            _ => 0,
        };
        entry.key.len() <= super::status_lane::store::MAX_STATUS_KEY_BYTES
            && value_len <= super::status_lane::store::MAX_STATUS_VALUE_BYTES
    });
    entries
}

//! Process, configuration, and statistics helpers for the appliance memory soak test.

use std::{
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

use orion::control_plane::NodeResourceUsageSnapshot;

/// Unix socket paths must fit in `sun_path` (108 bytes including the terminator).
const MAX_SOCKET_PATH_BYTES: usize = 100;

pub const NODE_ID: &str = "node.soak";

/// Soak knobs read from `ORION_SOAK_*` environment variables.
#[derive(Clone, Debug)]
pub struct SoakConfig {
    pub duration: Duration,
    pub warmup: Duration,
    pub sample_interval: Duration,
    pub max_slope_kib_per_min: f64,
    /// Shortest post-warm-up window over which the slope limit is enforced.
    pub min_slope_window: Duration,
    pub provider_hz: u64,
    pub resources: usize,
    pub executor_hz: u64,
    pub mutation_interval: Duration,
    pub workload_slots: usize,
    pub max_mutation_history: u64,
}

impl SoakConfig {
    pub fn from_env() -> Self {
        let duration = Duration::from_secs(env_u64("ORION_SOAK_DURATION_SECS", 600).max(10));
        let default_warmup = (duration.as_secs() / 5).clamp(10, 120);
        let warmup = Duration::from_secs(env_u64("ORION_SOAK_WARMUP_SECS", default_warmup));
        Self {
            duration,
            warmup: warmup.min(duration / 2),
            sample_interval: Duration::from_millis(env_u64("ORION_SOAK_SAMPLE_MS", 2_000).max(100)),
            max_slope_kib_per_min: env_f64("ORION_SOAK_MAX_SLOPE_KIB_PER_MIN", 256.0),
            min_slope_window: Duration::from_secs(env_u64("ORION_SOAK_MIN_SLOPE_WINDOW_SECS", 120)),
            provider_hz: env_u64("ORION_SOAK_PROVIDER_HZ", 50).clamp(1, 1_000),
            resources: env_u64("ORION_SOAK_RESOURCES", 4).clamp(1, 64) as usize,
            executor_hz: env_u64("ORION_SOAK_EXECUTOR_HZ", 5).clamp(1, 1_000),
            mutation_interval: Duration::from_millis(
                env_u64("ORION_SOAK_MUTATION_INTERVAL_MS", 250).max(10),
            ),
            workload_slots: env_u64("ORION_SOAK_WORKLOAD_SLOTS", 6).clamp(1, 256) as usize,
            max_mutation_history: env_u64("ORION_SOAK_MAX_MUTATION_HISTORY", 64).max(1),
        }
    }
}

fn env_u64(key: &str, default: u64) -> u64 {
    match std::env::var(key) {
        Ok(raw) => raw
            .trim()
            .parse()
            .unwrap_or_else(|_| panic!("{key} must be an unsigned integer, got {raw:?}")),
        Err(_) => default,
    }
}

fn env_f64(key: &str, default: f64) -> f64 {
    match std::env::var(key) {
        Ok(raw) => raw
            .trim()
            .parse()
            .unwrap_or_else(|_| panic!("{key} must be a number, got {raw:?}")),
        Err(_) => default,
    }
}

/// A spawned `orion-node` in the single-node IPC-only appliance profile.
///
/// Dropping the guard kills the child and removes its scratch directory, including on panic.
pub struct ApplianceNode {
    child: Child,
    root: PathBuf,
    pub ipc_socket: PathBuf,
    pub ipc_stream_socket: PathBuf,
}

impl ApplianceNode {
    pub fn spawn(config: &SoakConfig) -> Self {
        let root = short_scratch_dir();
        std::fs::create_dir_all(root.join("state")).expect("soak scratch dir should be created");
        let ipc_socket = root.join("c.sock");
        let ipc_stream_socket = root.join("s.sock");
        assert!(
            ipc_stream_socket.as_os_str().len() < MAX_SOCKET_PATH_BYTES,
            "socket path too long: {}",
            ipc_stream_socket.display()
        );

        let mut command = Command::new(env!("CARGO_BIN_EXE_orion-node"));
        command
            .env("ORION_NODE_ID", NODE_ID)
            .env("ORION_NODE_HTTP_ADDR", "off")
            .env("ORION_NODE_RUNTIME_WORKER_THREADS", "2")
            .env("ORION_NODE_PEER_AUTH", "disabled")
            .env("ORION_NODE_STATE_DIR", root.join("state"))
            .env("ORION_NODE_IPC_SOCKET", &ipc_socket)
            .env("ORION_NODE_IPC_STREAM_SOCKET", &ipc_stream_socket)
            .env(
                "ORION_NODE_MAX_MUTATION_HISTORY",
                config.max_mutation_history.to_string(),
            )
            .env(
                "RUST_LOG",
                std::env::var("RUST_LOG").unwrap_or_else(|_| "warn".to_owned()),
            )
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());

        let mut child = command.spawn().expect("orion-node binary should start");
        let stdout = child.stdout.take().expect("child stdout should be piped");
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        reader
            .read_line(&mut line)
            .expect("orion-node should emit a startup line");
        assert!(
            line.contains("http=off"),
            "HTTP listener should be disabled in the appliance profile: {line}"
        );
        // Keep draining stdout so the child can never block on a full pipe.
        std::thread::spawn(move || {
            let mut sink = Vec::new();
            let _ = reader.read_to_end(&mut sink);
        });

        Self {
            child,
            root,
            ipc_socket,
            ipc_stream_socket,
        }
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// Fails the test when the node exited early instead of silently sampling a dead socket.
    pub fn assert_running(&mut self) {
        if let Ok(Some(status)) = self.child.try_wait() {
            panic!("orion-node exited during the soak: {status}");
        }
    }
}

impl Drop for ApplianceNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn short_scratch_dir() -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time should be after epoch")
        .subsec_nanos();
    let name = format!("orion-soak-{}-{nanos:x}", std::process::id());
    let preferred = std::env::temp_dir().join(&name);
    // Leave room for "/s.sock"; fall back to /tmp when TMPDIR is deep.
    if preferred.as_os_str().len() + 8 < MAX_SOCKET_PATH_BYTES {
        preferred
    } else {
        Path::new("/tmp").join(name)
    }
}

/// One observation of the node's resource usage.
#[derive(Clone, Debug)]
pub struct Sample {
    pub t_secs: f64,
    pub memory_kib: u64,
    pub rss_anon_kib: u64,
    pub usage: NodeResourceUsageSnapshot,
}

impl Sample {
    pub fn new(t_secs: f64, usage: NodeResourceUsageSnapshot) -> Option<Self> {
        let process = &usage.process;
        let memory = process.pss_bytes.or(process.vm_rss_bytes)?;
        Some(Self {
            t_secs,
            memory_kib: memory / 1024,
            rss_anon_kib: process.rss_anon_bytes.unwrap_or(0) / 1024,
            usage,
        })
    }

    pub fn header() -> &'static str {
        "   t(s)  mem_kib  anon_kib  hist_b  hist_kib  q_depth  q_events  dropped  clients  endpts  events  obs_res  wl"
    }

    pub fn row(&self) -> String {
        let u = &self.usage;
        let history = &u.mutation_history;
        let streams = &u.local_streams;
        format!(
            "{:>7.1} {:>8} {:>9} {:>7} {:>9} {:>8} {:>9} {:>8} {:>8} {:>7} {:>7} {:>8} {:>3}",
            self.t_secs,
            self.memory_kib,
            self.rss_anon_kib,
            history.batches,
            history.encoded_bytes.unwrap_or(0) / 1024,
            streams.send_queue_depth_max,
            streams.queued_client_events_max,
            streams.dropped_client_events_total,
            u.registries.local_clients,
            u.registries.communication_endpoints,
            u.registries.recent_events,
            u.state.observed.resources,
            u.state.desired.workloads,
        )
    }
}

/// Least-squares slope of `values` over `times`, in units per second.
pub fn slope_per_sec(points: &[(f64, f64)]) -> f64 {
    let n = points.len() as f64;
    if points.len() < 2 {
        return 0.0;
    }
    let mean_t = points.iter().map(|(t, _)| t).sum::<f64>() / n;
    let mean_v = points.iter().map(|(_, v)| v).sum::<f64>() / n;
    let (mut num, mut den) = (0.0, 0.0);
    for (t, v) in points {
        num += (t - mean_t) * (v - mean_v);
        den += (t - mean_t) * (t - mean_t);
    }
    if den == 0.0 { 0.0 } else { num / den }
}

/// Slope of a per-sample metric after warm-up, in units per minute.
pub fn slope_per_min(samples: &[&Sample], metric: impl Fn(&Sample) -> u64) -> f64 {
    let points: Vec<(f64, f64)> = samples
        .iter()
        .map(|sample| (sample.t_secs, metric(sample) as f64))
        .collect();
    slope_per_sec(&points) * 60.0
}

/// Asserts every bounded structure stays within its configured cap.
pub fn assert_within_caps(sample: &Sample, max_mutation_history: u64) {
    let u = &sample.usage;
    let history = &u.mutation_history;
    assert!(
        history.batches <= history.max_batches && history.max_batches <= max_mutation_history,
        "mutation history {} batches exceeds cap {} at t={:.1}s",
        history.batches,
        history.max_batches,
        sample.t_secs
    );
    if let Some(bytes) = history.encoded_bytes {
        assert!(
            bytes <= history.max_bytes,
            "mutation history {bytes} bytes exceeds cap {} at t={:.1}s",
            history.max_bytes,
            sample.t_secs
        );
    }
    let streams = &u.local_streams;
    assert!(
        streams.send_queue_depth_max <= streams.send_queue_capacity,
        "stream send queue depth {} exceeds capacity {} at t={:.1}s",
        streams.send_queue_depth_max,
        streams.send_queue_capacity,
        sample.t_secs
    );
    assert!(
        streams.queued_client_events_max <= streams.client_event_queue_limit,
        "client event queue {} exceeds limit {} at t={:.1}s",
        streams.queued_client_events_max,
        streams.client_event_queue_limit,
        sample.t_secs
    );
    for queue in &u.worker_queues {
        assert!(
            queue.depth <= queue.capacity,
            "worker queue {} depth {} exceeds capacity {} at t={:.1}s",
            queue.name,
            queue.depth,
            queue.capacity,
            sample.t_secs
        );
    }
    let registries = &u.registries;
    assert!(
        registries.communication_endpoints <= registries.communication_endpoint_limit,
        "communication endpoints {} exceed limit {}",
        registries.communication_endpoints,
        registries.communication_endpoint_limit
    );
    assert!(
        registries.recent_events <= registries.recent_event_limit,
        "recent events {} exceed limit {}",
        registries.recent_events,
        registries.recent_event_limit
    );
}

#[cfg(test)]
mod tests {
    use super::slope_per_sec;

    #[test]
    fn slope_matches_linear_series() {
        let points: Vec<(f64, f64)> = (0..10).map(|t| (t as f64, 3.0 * t as f64 + 7.0)).collect();
        assert!((slope_per_sec(&points) - 3.0).abs() < 1e-9);
        assert_eq!(slope_per_sec(&[(1.0, 5.0)]), 0.0);
    }
}

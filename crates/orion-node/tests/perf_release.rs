use std::{
    fs,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::OnceLock,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use orion::{
    ArtifactId, NodeId, RuntimeType, WorkloadId,
    control_plane::{ArtifactRecord, DesiredState, WorkloadRecord},
};
use orion_node::{NodeApp, NodeConfig, NodeStorage};

const RELEASE_SHUTDOWN_AFTER_INIT_MS: u64 = 30_000;

#[derive(Clone, Copy)]
enum ReplayScenario {
    CurrentSnapshot,
    StaleSnapshotHistoryTail,
    CompactedHistoryOnly,
}

impl ReplayScenario {
    fn label(self) -> &'static str {
        match self {
            Self::CurrentSnapshot => "current_snapshot",
            Self::StaleSnapshotHistoryTail => "stale_snapshot_history_tail",
            Self::CompactedHistoryOnly => "compacted_history_only",
        }
    }
}

#[test]
#[ignore = "perf baseline harness; requires release binary"]
fn orion_node_release_reports_whole_process_memory_baseline_by_workload_count() {
    let binary = ensure_release_binary();

    for workload_count in [0usize, 50, 250, 1000] {
        let state_dir = seed_state_dir(
            &format!("release-memory-{workload_count}"),
            workload_count,
            "node.perf.release.memory",
            ReplayScenario::CurrentSnapshot,
        );
        let mut process = spawn_release_node(
            &binary,
            "node.perf.release.memory",
            &state_dir,
            RELEASE_SHUTDOWN_AFTER_INIT_MS,
        );
        std::thread::sleep(Duration::from_millis(250));
        let rss =
            process_rss_bytes(process.child.id()).expect("release process rss should be readable");
        println!(
            "release_memory_baseline workloads={} rss_bytes={rss}",
            workload_count
        );
        process.cleanup();
        let _ = fs::remove_dir_all(state_dir);
    }
}

#[test]
#[ignore = "perf baseline harness; requires release binary"]
fn orion_node_release_reports_cold_replay_time() {
    let binary = ensure_release_binary();

    for scenario in [
        ReplayScenario::CurrentSnapshot,
        ReplayScenario::StaleSnapshotHistoryTail,
        ReplayScenario::CompactedHistoryOnly,
    ] {
        for workload_count in [10usize, 100, 500] {
            let state_dir = seed_state_dir(
                &format!("release-replay-{}-{workload_count}", scenario.label()),
                workload_count,
                "node.perf.release.replay",
                scenario,
            );
            let start = Instant::now();
            let mut process =
                spawn_release_node(&binary, "node.perf.release.replay", &state_dir, 0);
            let elapsed = start.elapsed();
            println!(
                "release_cold_replay scenario={} workloads={} elapsed_ms={}",
                scenario.label(),
                workload_count,
                elapsed.as_secs_f64() * 1000.0
            );
            process.cleanup();
            let _ = fs::remove_dir_all(state_dir);
        }
    }
}

struct SpawnedReleaseNode {
    child: Child,
    state_dir: PathBuf,
}

impl SpawnedReleaseNode {
    fn cleanup(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = fs::remove_dir_all(&self.state_dir);
    }
}

impl Drop for SpawnedReleaseNode {
    fn drop(&mut self) {
        self.cleanup();
    }
}

fn ensure_release_binary() -> PathBuf {
    static RELEASE_BINARY: OnceLock<PathBuf> = OnceLock::new();
    RELEASE_BINARY
        .get_or_init(|| {
            let workspace_root = workspace_root();
            let output = Command::new("cargo")
                .args(["build", "--release", "-p", "orion-node"])
                .current_dir(&workspace_root)
                .output()
                .expect("release build command should run");
            if !output.status.success() {
                panic!(
                    "release build failed\nstdout:\n{}\nstderr:\n{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            workspace_root.join("target/release/orion-node")
        })
        .clone()
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory should have parent")
        .parent()
        .expect("workspace root should exist")
        .to_path_buf()
}

fn spawn_release_node(
    binary: &Path,
    node_id: &str,
    state_dir: &Path,
    shutdown_after_init_ms: u64,
) -> SpawnedReleaseNode {
    let mut command = Command::new(binary);
    command
        .env("ORION_NODE_ID", node_id)
        .env("ORION_NODE_HTTP_ADDR", "127.0.0.1:0")
        .env("ORION_NODE_RECONCILE_MS", "25")
        .env("ORION_NODE_STATE_DIR", state_dir)
        .env(
            "ORION_NODE_SHUTDOWN_AFTER_INIT_MS",
            shutdown_after_init_ms.to_string(),
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = command
        .spawn()
        .expect("release orion-node binary should start");
    let stdout = child.stdout.take().expect("stdout should be piped");
    let reader = BufReader::new(stdout);
    for line in reader.lines() {
        let line = line.expect("stdout line should be readable");
        if line.starts_with("orion-node: initialized") {
            break;
        }
    }

    SpawnedReleaseNode {
        child,
        state_dir: state_dir.to_path_buf(),
    }
}

fn seed_state_dir(
    name: &str,
    workload_count: usize,
    node_id: &str,
    scenario: ReplayScenario,
) -> PathBuf {
    let state_dir = temp_state_dir(name);
    let app = NodeApp::builder()
        .config(
            NodeConfig::for_local_node(NodeId::new(node_id))
                .with_http_bind_addr("127.0.0.1:0".parse().expect("socket address should parse"))
                .with_ipc_socket_path(NodeConfig::default_ipc_socket_path_for("node-test"))
                .with_reconcile_interval(Duration::from_millis(10))
                .with_state_dir(state_dir.clone())
                .with_peer_sync_execution(
                    NodeConfig::try_peer_sync_execution_from_env()
                        .expect("peer sync execution defaults should parse"),
                )
                .with_runtime_tuning_mut(|tuning| {
                    let tuning = tuning.with_snapshot_rewrite_cadence(match scenario {
                        ReplayScenario::CurrentSnapshot => 1,
                        ReplayScenario::StaleSnapshotHistoryTail => u64::MAX,
                        ReplayScenario::CompactedHistoryOnly => u64::MAX,
                    });
                    tuning.with_max_mutation_history_batches(match scenario {
                        ReplayScenario::CompactedHistoryOnly => 1,
                        _ => workload_count.max(1) * 4,
                    })
                }),
        )
        .try_build()
        .expect("node app should build");

    for idx in 0..workload_count {
        let artifact_id = ArtifactId::new(format!("artifact.release.perf.{idx}"));
        app.put_artifact_content(
            &ArtifactRecord::builder(artifact_id.clone())
                .content_type("application/octet-stream")
                .size_bytes(256)
                .build(),
            &[7; 256],
        )
        .expect("artifact should persist");

        let mut desired = app.state_snapshot().state.desired;
        desired.put_workload(
            WorkloadRecord::builder(
                WorkloadId::new(format!("workload.release.perf.{idx}")),
                RuntimeType::new("graph.exec.v1"),
                artifact_id,
            )
            .desired_state(DesiredState::Stopped)
            .assigned_to(NodeId::new(node_id))
            .build(),
        );
        app.replace_desired(desired);
    }

    app.persist_state().expect("state should persist");

    if matches!(scenario, ReplayScenario::CompactedHistoryOnly) {
        let storage = NodeStorage::new(state_dir.clone());
        fs::remove_file(storage.snapshot_manifest_path())
            .expect("snapshot manifest should be removable for history-only replay");
    }

    state_dir
}

fn temp_state_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("orion-node-release-perf-{name}-{nanos}"))
}

fn process_rss_bytes(pid: u32) -> Result<u64, String> {
    let statm = fs::read_to_string(format!("/proc/{pid}/statm")).map_err(|err| err.to_string())?;
    let pages = statm
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| "statm did not contain rss column".to_owned())?
        .parse::<u64>()
        .map_err(|err| err.to_string())?;
    Ok(pages * 4096)
}

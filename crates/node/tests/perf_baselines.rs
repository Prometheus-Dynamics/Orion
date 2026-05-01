use std::{
    fs,
    io::{BufRead, BufReader},
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use orion::{
    ArtifactId, NodeId, RuntimeType, WorkloadId,
    control_plane::{ArtifactRecord, DesiredState, WorkloadRecord},
};
use orion_node::{NodeApp, NodeConfig};

#[test]
#[ignore = "perf baseline harness"]
fn orion_node_reports_memory_baseline_by_workload_count() {
    for workload_count in [0usize, 50, 250, 1000] {
        let rss_before = current_rss_bytes().expect("rss should be readable");
        let app = seeded_app(workload_count, None);
        let rss_after = current_rss_bytes().expect("rss should be readable");
        println!(
            "memory_baseline workloads={} rss_delta_bytes={}",
            workload_count,
            rss_after.saturating_sub(rss_before)
        );
        drop(app);
    }
}

#[tokio::test]
#[ignore = "perf baseline harness"]
async fn orion_node_reports_idle_cpu_baseline() {
    let process = spawn_node("node.cpu.idle");
    let start_ticks = process_cpu_ticks(process.child.id()).expect("cpu ticks should be readable");
    tokio::time::sleep(Duration::from_secs(2)).await;
    let end_ticks = process_cpu_ticks(process.child.id()).expect("cpu ticks should be readable");
    let ticks_per_second = clock_ticks_per_second().expect("clock ticks should be readable");
    let cpu_seconds = (end_ticks.saturating_sub(start_ticks)) as f64 / ticks_per_second as f64;
    let cpu_percent = (cpu_seconds / 2.0) * 100.0;
    println!(
        "idle_cpu_baseline pid={} window_secs=2 cpu_seconds={cpu_seconds:.6} cpu_percent={cpu_percent:.3}",
        process.child.id()
    );
}

#[test]
#[ignore = "perf baseline harness"]
fn orion_node_reports_persistence_replay_time() {
    for workload_count in [10usize, 100, 500] {
        let state_dir = temp_state_dir(&format!("replay-{workload_count}"));
        let seed = seeded_app(workload_count, Some(state_dir.clone()));
        seed.persist_state().expect("state should persist");
        drop(seed);

        let replay = NodeApp::builder()
            .config(
                NodeConfig::for_local_node(NodeId::new("node.perf.replay"))
                    .with_http_bind_addr(
                        "127.0.0.1:0".parse().expect("socket address should parse"),
                    )
                    .with_ipc_socket_path(NodeConfig::default_ipc_socket_path_for("node-test"))
                    .with_reconcile_interval(Duration::from_millis(10))
                    .with_state_dir(state_dir.clone())
                    .with_peer_sync_execution(
                        NodeConfig::try_peer_sync_execution_from_env()
                            .expect("peer sync execution defaults should parse"),
                    )
                    .with_runtime_tuning(
                        NodeConfig::try_runtime_tuning_from_env()
                            .expect("runtime tuning defaults should parse"),
                    ),
            )
            .with_startup_replay(false)
            .try_build()
            .expect("node app should build");

        let start = Instant::now();
        let replayed = replay.replay_state().expect("replay should succeed");
        let elapsed = start.elapsed();
        assert!(replayed, "seeded state should be replayed");
        println!(
            "persistence_replay workloads={} elapsed_ms={}",
            workload_count,
            elapsed.as_secs_f64() * 1000.0
        );

        let _ = fs::remove_dir_all(state_dir);
    }
}

fn seeded_app(workload_count: usize, state_dir: Option<PathBuf>) -> NodeApp {
    let mut config = NodeConfig::for_local_node(NodeId::new("node.perf.replay"))
        .with_http_bind_addr("127.0.0.1:0".parse().expect("socket address should parse"))
        .with_ipc_socket_path(NodeConfig::default_ipc_socket_path_for("node-test"))
        .with_reconcile_interval(Duration::from_millis(10))
        .with_peer_sync_execution(
            NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
        )
        .with_runtime_tuning(
            NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        );
    if let Some(state_dir) = state_dir {
        config = config.with_state_dir(state_dir);
    }
    let app = NodeApp::builder()
        .config(config)
        .try_build()
        .expect("node app should build");

    for idx in 0..workload_count {
        let artifact_id = ArtifactId::new(format!("artifact.perf.{idx}"));
        app.put_artifact_content(
            &ArtifactRecord::builder(artifact_id.clone())
                .content_type("application/octet-stream")
                .size_bytes(256)
                .build(),
            &[5; 256],
        )
        .expect("artifact should persist");

        let mut desired = app.state_snapshot().state.desired;
        desired.put_workload(
            WorkloadRecord::builder(
                WorkloadId::new(format!("workload.perf.{idx}")),
                RuntimeType::new("graph.exec.v1"),
                artifact_id,
            )
            .desired_state(DesiredState::Stopped)
            .assigned_to(NodeId::new("node.perf.replay"))
            .build(),
        );
        app.replace_desired(desired);
    }

    app
}

struct SpawnedNode {
    child: Child,
    _state_dir: PathBuf,
}

impl Drop for SpawnedNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = fs::remove_dir_all(&self._state_dir);
    }
}

fn spawn_node(node_id: &str) -> SpawnedNode {
    let state_dir = temp_state_dir(node_id);
    let mut command = Command::new(env!("CARGO_BIN_EXE_orion-node"));
    command
        .env("ORION_NODE_ID", node_id)
        .env("ORION_NODE_HTTP_ADDR", "127.0.0.1:0")
        .env("ORION_NODE_RECONCILE_MS", "25")
        .env("ORION_NODE_STATE_DIR", &state_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = command.spawn().expect("orion-node binary should start");
    let stdout = child.stdout.take().expect("stdout should be piped");
    let reader = BufReader::new(stdout);
    for line in reader.lines() {
        let line = line.expect("stdout line should be readable");
        if line.starts_with("orion-node: initialized") {
            break;
        }
    }

    SpawnedNode {
        child,
        _state_dir: state_dir,
    }
}

fn temp_state_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("orion-node-perf-{name}-{nanos}"))
}

fn current_rss_bytes() -> Result<u64, String> {
    let statm = fs::read_to_string("/proc/self/statm").map_err(|err| err.to_string())?;
    let pages = statm
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| "statm did not contain rss column".to_owned())?
        .parse::<u64>()
        .map_err(|err| err.to_string())?;
    Ok(pages * 4096)
}

fn process_cpu_ticks(pid: u32) -> Result<u64, String> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).map_err(|err| err.to_string())?;
    let after_comm = stat
        .rsplit_once(") ")
        .ok_or_else(|| "unexpected /proc stat format".to_owned())?
        .1;
    let fields: Vec<&str> = after_comm.split_whitespace().collect();
    let user = fields
        .get(11)
        .ok_or_else(|| "missing utime".to_owned())?
        .parse::<u64>()
        .map_err(|err| err.to_string())?;
    let system = fields
        .get(12)
        .ok_or_else(|| "missing stime".to_owned())?
        .parse::<u64>()
        .map_err(|err| err.to_string())?;
    Ok(user + system)
}

fn clock_ticks_per_second() -> Result<u64, String> {
    let output = Command::new("getconf")
        .arg("CLK_TCK")
        .output()
        .map_err(|err| err.to_string())?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_owned());
    }
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<u64>()
        .map_err(|err| err.to_string())
}

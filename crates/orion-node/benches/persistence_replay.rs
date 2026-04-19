use std::{
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use orion::{
    ArtifactId, NodeId, RuntimeType, WorkloadId,
    control_plane::{ArtifactRecord, DesiredState, WorkloadRecord},
};
use orion_node::{NodeApp, NodeConfig};

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

fn temp_state_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("orion-node-bench-{name}-{nanos}"))
}

fn seed_state_dir(workload_count: usize, scenario: ReplayScenario) -> PathBuf {
    let state_dir = temp_state_dir(&format!("replay-{}-{}", scenario.label(), workload_count));
    let app = NodeApp::builder()
        .config(
            NodeConfig::for_local_node(NodeId::new("node-bench"))
                .with_http_bind_addr("127.0.0.1:0".parse().expect("socket address should parse"))
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
        .with_snapshot_rewrite_cadence(match scenario {
            ReplayScenario::CurrentSnapshot => 1,
            ReplayScenario::StaleSnapshotHistoryTail => u64::MAX,
            ReplayScenario::CompactedHistoryOnly => u64::MAX,
        })
        .with_max_mutation_history_batches(match scenario {
            ReplayScenario::CompactedHistoryOnly => 1,
            _ => workload_count.max(1) * 4,
        })
        .try_build()
        .expect("node app should build");

    for idx in 0..workload_count {
        let artifact_id = ArtifactId::new(format!("artifact.replay.{idx}"));
        app.put_artifact_content(
            &ArtifactRecord::builder(artifact_id.clone())
                .content_type("application/octet-stream")
                .size_bytes(128)
                .build(),
            &[9; 128],
        )
        .expect("artifact should persist");

        let mut desired = app.state_snapshot().state.desired;
        desired.put_workload(
            WorkloadRecord::builder(
                WorkloadId::new(format!("workload.replay.{idx}")),
                RuntimeType::new("graph.exec.v1"),
                artifact_id,
            )
            .desired_state(DesiredState::Stopped)
            .assigned_to(NodeId::new("node-bench"))
            .build(),
        );
        app.replace_desired(desired);
    }
    app.persist_state().expect("state should persist");

    if matches!(scenario, ReplayScenario::CompactedHistoryOnly) {
        let storage = orion_node::NodeStorage::new(state_dir.clone());
        std::fs::remove_file(storage.snapshot_manifest_path())
            .expect("snapshot manifest should be removable for history-only replay");
    }

    state_dir
}

fn benchmark_persistence_replay(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_persistence_replay");
    for scenario in [
        ReplayScenario::CurrentSnapshot,
        ReplayScenario::StaleSnapshotHistoryTail,
        ReplayScenario::CompactedHistoryOnly,
    ] {
        for workload_count in [10usize, 100, 500] {
            let state_dir = seed_state_dir(workload_count, scenario);
            group.bench_with_input(
                BenchmarkId::new(scenario.label(), workload_count),
                &state_dir,
                |b, state_dir| {
                    b.iter(|| {
                        let app = NodeApp::builder()
                            .config(
                                NodeConfig::for_local_node(NodeId::new("node-bench"))
                                    .with_http_bind_addr(
                                        "127.0.0.1:0".parse().expect("socket address should parse"),
                                    )
                                    .with_ipc_socket_path(NodeConfig::default_ipc_socket_path_for(
                                        "node-test",
                                    ))
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
                        let replayed = app.replay_state().expect("replay should succeed");
                        assert!(replayed, "persisted state should exist");
                    });
                },
            );
            let _ = std::fs::remove_dir_all(&state_dir);
        }
    }
    group.finish();
}

criterion_group!(benches, benchmark_persistence_replay);
criterion_main!(benches);

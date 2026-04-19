use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use orion::{
    ArtifactId, NodeId, RuntimeType, WorkloadId,
    control_plane::{ArtifactRecord, DesiredClusterState, DesiredState, WorkloadRecord},
};
use orion_node::{NodeApp, NodeConfig};
fn desired_state_with_workloads(workload_count: usize) -> DesiredClusterState {
    let mut desired = DesiredClusterState::default();
    for idx in 0..workload_count {
        let artifact_id = ArtifactId::new(format!("artifact.snapshot.{idx}"));
        desired.put_artifact(ArtifactRecord::builder(artifact_id.clone()).build());
        desired.put_workload(
            WorkloadRecord::builder(
                WorkloadId::new(format!("workload.snapshot.{idx}")),
                RuntimeType::new("graph.exec.v1"),
                artifact_id,
            )
            .desired_state(DesiredState::Stopped)
            .assigned_to(NodeId::new("node-bench"))
            .build(),
        );
    }
    desired
}

fn bench_app(workload_count: usize) -> NodeApp {
    NodeApp::builder()
        .config(
            NodeConfig::for_local_node(NodeId::new("node-bench"))
                .with_http_bind_addr("127.0.0.1:0".parse().expect("socket address should parse")),
        )
        .desired(desired_state_with_workloads(workload_count))
        .try_build()
        .expect("node app should build")
}

fn benchmark_state_snapshot(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_state_snapshot");
    for workload_count in [10usize, 100, 500] {
        let app = bench_app(workload_count);
        group.bench_with_input(
            BenchmarkId::new("state_snapshot", workload_count),
            &workload_count,
            |b, _| {
                b.iter(|| {
                    std::hint::black_box(app.state_snapshot());
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, benchmark_state_snapshot);
criterion_main!(benches);

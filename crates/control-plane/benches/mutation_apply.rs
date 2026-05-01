use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use orion_control_plane::{
    ArtifactRecord, DesiredClusterState, DesiredState, MutationBatch, WorkloadRecord,
};
use orion_core::{ArtifactId, NodeId, Revision, RuntimeType, WorkloadId};

fn desired_state_with_workloads(workload_count: usize) -> DesiredClusterState {
    let mut desired = DesiredClusterState::default();
    for idx in 0..workload_count {
        let artifact_id = ArtifactId::new(format!("artifact.bench.{idx}"));
        desired.put_artifact(ArtifactRecord::builder(artifact_id.clone()).build());
        desired.put_workload(
            WorkloadRecord::builder(
                WorkloadId::new(format!("workload.bench.{idx}")),
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

fn benchmark_mutation_apply(c: &mut Criterion) {
    let mut group = c.benchmark_group("control_plane_mutation_apply");
    for workload_count in [10usize, 100, 500] {
        let desired = desired_state_with_workloads(workload_count);
        let batch = MutationBatch::full_state_replay(Revision::ZERO, &desired);
        group.bench_with_input(
            BenchmarkId::new("full_state_replay_apply_checked", workload_count),
            &batch,
            |b, batch| {
                b.iter(|| {
                    let mut target = DesiredClusterState::default();
                    std::hint::black_box(batch.clone())
                        .apply_to_checked(&mut target)
                        .expect("mutation batch should apply");
                    std::hint::black_box(target);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, benchmark_mutation_apply);
criterion_main!(benches);

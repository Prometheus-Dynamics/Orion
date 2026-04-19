use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use orion_control_plane::{
    AvailabilityState, DesiredClusterState, DesiredState, ExecutorRecord, HealthState, LeaseState,
    ProviderRecord, ResourceRecord, WorkloadRecord,
};
use orion_core::{
    ArtifactId, ExecutorId, NodeId, ProviderId, ResourceId, ResourceType, RuntimeType, WorkloadId,
};
use orion_runtime::{LocalRuntimeStore, Runtime};

fn benchmark_reconcile(c: &mut Criterion) {
    let mut group = c.benchmark_group("runtime_reconcile");
    for workload_count in [50usize, 250, 1000] {
        let mut desired = DesiredClusterState::default();
        desired.put_provider(
            ProviderRecord::builder(ProviderId::new("provider.local"), NodeId::new("node-a"))
                .resource_type(ResourceType::new("imu.sample"))
                .build(),
        );
        desired.put_executor(
            ExecutorRecord::builder(ExecutorId::new("executor.local"), NodeId::new("node-a"))
                .runtime_type(RuntimeType::new("graph.exec.v1"))
                .build(),
        );

        for idx in 0..workload_count {
            desired.put_workload(
                WorkloadRecord::builder(
                    WorkloadId::new(format!("workload.{idx}")),
                    RuntimeType::new("graph.exec.v1"),
                    ArtifactId::new(format!("artifact.{idx}")),
                )
                .desired_state(DesiredState::Running)
                .assigned_to(NodeId::new("node-a"))
                .require_resource(ResourceType::new("imu.sample"), 1)
                .build(),
            );
        }

        let mut store = LocalRuntimeStore::new(NodeId::new("node-a"));
        store.replace_desired(desired);
        for idx in 0..workload_count {
            store.observed.put_resource(
                ResourceRecord::builder(
                    ResourceId::new(format!("resource.{idx}")),
                    ResourceType::new("imu.sample"),
                    ProviderId::new("provider.local"),
                )
                .health(HealthState::Healthy)
                .availability(AvailabilityState::Available)
                .lease_state(LeaseState::Unleased)
                .build(),
            );
        }

        let runtime = Runtime::new(NodeId::new("node-a"));
        group.bench_with_input(
            BenchmarkId::new("workloads", workload_count),
            &workload_count,
            |b, _| {
                b.iter(|| {
                    let _ = runtime.reconcile(&store).expect("reconcile should succeed");
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, benchmark_reconcile);
criterion_main!(benches);

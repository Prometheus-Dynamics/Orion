//! Steady state: an executor that republishes its snapshot on every tick, stamping each resource
//! with a fresh observation time, must not keep rewriting the state directory.

use super::observed_coalescing::{coalescing_app, register_provider_client};
use super::*;
use orion::control_plane::{
    ClientHello, ClientRole, ExecutorRecord, ExecutorStateUpdate, ProviderStateUpdate,
    ResourceConfigState, ResourceOwnershipMode, ResourceState, TypedConfigValue,
    WorkloadObservedState,
};
use std::time::Instant;

const PROVIDER: &str = "provider.steady";
const EXECUTOR: &str = "executor.steady";
const INTERVAL: Duration = Duration::from_millis(2_000);

fn state_writes(app: &NodeApp) -> u64 {
    app.observability_snapshot()
        .persistence
        .state_persist
        .success_count
}

fn dir_bytes(path: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                dir_bytes(&path)
            } else {
                entry.metadata().map(|m| m.len()).unwrap_or(0)
            }
        })
        .sum()
}

fn provider_update(app: &NodeApp) -> ControlMessage {
    ControlMessage::ProviderState(ProviderStateUpdate {
        provider: ProviderRecord::builder(PROVIDER, app.config.node_id.clone())
            .resource_type(ResourceType::new("engine.session"))
            .build(),
        resources: Vec::new(),
    })
}

/// One executor snapshot: a realized session whose observation time is `observed_at_ms`.
fn executor_update(app: &NodeApp, observed_at_ms: u64) -> ControlMessage {
    let session = ResourceRecord::builder("resource.steady.session", "engine.session", PROVIDER)
        .realized_by_executor(EXECUTOR)
        .realized_for_workload("workload.steady")
        .source_workload("workload.steady")
        .ownership_mode(ResourceOwnershipMode::Exclusive)
        .availability(AvailabilityState::Available)
        .state(ResourceState::new(observed_at_ms).with_config(
            ResourceConfigState::new().field("status", TypedConfigValue::String("running".into())),
        ))
        .build();
    ControlMessage::ExecutorState(ExecutorStateUpdate {
        executor: ExecutorRecord::builder(EXECUTOR, app.config.node_id.clone())
            .runtime_type(RuntimeType::new("steady.exec.v1"))
            .build(),
        workloads: vec![
            WorkloadRecord::builder(
                WorkloadId::new("workload.steady"),
                RuntimeType::new("steady.exec.v1"),
                ArtifactId::new("artifact.steady"),
            )
            .desired_state(DesiredState::Running)
            .assigned_to(app.config.node_id.clone())
            .observed_state(WorkloadObservedState::Running)
            .build(),
        ],
        resources: vec![session],
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn steady_executor_republish_does_not_rewrite_state() {
    let state_dir = temp_state_dir("steady-republish");
    let app = coalescing_app("node.steady", state_dir.clone(), INTERVAL);
    let source = LocalAddress::new("steady-client");
    register_provider_client(&app, &source);
    app.apply_local_control_message(
        &source,
        ControlMessage::ClientHello(ClientHello {
            client_name: "steady-executor".into(),
            role: ClientRole::Executor,
        }),
    )
    .expect("executor hello should be accepted");
    let handle = app.spawn_reconcile_loop(Duration::from_millis(5));

    app.apply_local_control_message(&source, provider_update(&app))
        .expect("provider state should apply");
    app.apply_local_control_message(&source, executor_update(&app, 1))
        .expect("first executor state should apply");
    // Let the startup writes drain (the coalescer flushes one interval after them).
    tokio::time::sleep(INTERVAL * 2 + Duration::from_millis(200)).await;

    let writes_before = state_writes(&app);
    let bytes = dir_bytes(&state_dir);
    let started = Instant::now();
    let mut publishes = 0_u64;
    while started.elapsed() < Duration::from_secs(6) {
        publishes += 1;
        app.apply_local_control_message(&source, executor_update(&app, 1_000 + publishes))
            .expect("executor republish should apply");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(INTERVAL).await;
    let writes = state_writes(&app) - writes_before;
    let usage = app
        .observability_snapshot()
        .resource_usage
        .observed_persistence;
    handle.shutdown().await;

    eprintln!(
        "steady republish: publishes={publishes} state_writes={writes} state_dir_bytes={bytes} \
         coalesced_changes={} flushes={}",
        usage.coalesced_changes_total, usage.flushes_total
    );
    assert_eq!(
        writes, 0,
        "timestamp-only executor republishes must not cause {writes} state writes"
    );
}

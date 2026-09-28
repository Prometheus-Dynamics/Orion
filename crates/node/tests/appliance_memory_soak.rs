//! Long-running memory soak for orion-node in the single-node, IPC-only appliance profile.
//!
//! Models a small appliance: one provider publishes observed resource state (capture heartbeats)
//! at high frequency, one executor watches its assigned workloads and reports their observed
//! state, and a control-plane client keeps cycling desired workloads so mutation history churns
//! through its cap. The node's `resource_usage` diagnostics are sampled over IPC and a
//! least-squares slope of process memory after warm-up must stay under a threshold.
//!
//! Run with:
//! `ORION_SOAK_DURATION_SECS=60 cargo test -p orion-node --test appliance_memory_soak -- --ignored --nocapture`
#![cfg(unix)]

mod appliance_soak;

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use appliance_soak::{
    ApplianceNode, NODE_ID, Sample, SoakConfig, assert_within_caps, slope_per_min,
};
use orion::{
    ArtifactId, ExecutorId, NodeId, ProviderId, ResourceId, ResourceType, Revision, RuntimeType,
    WorkloadId,
    client::{
        LocalControlPlaneClient, LocalExecutorEvent, LocalExecutorService, LocalNodeRuntime,
        LocalProviderService,
    },
    control_plane::{
        ArtifactRecord, AvailabilityState, DesiredState, DesiredStateMutation, ExecutorRecord,
        HealthState, LeaseState, MutationBatch, ProviderRecord, ResourceRecord, ResourceState,
        WorkloadObservedState, WorkloadRecord,
    },
};
use tokio::{task::JoinHandle, time::MissedTickBehavior};

const RUNTIME_TYPE: &str = "soak.capture.v1";
const ARTIFACT_ID: &str = "artifact.soak.capture";
const PROVIDER_ID: &str = "provider.soak.camera";
const EXECUTOR_ID: &str = "executor.soak.capture";

#[derive(Default)]
struct LoadCounters {
    provider_publishes: AtomicU64,
    provider_errors: AtomicU64,
    provider_events: AtomicU64,
    executor_publishes: AtomicU64,
    executor_errors: AtomicU64,
    executor_events: AtomicU64,
    mutations: AtomicU64,
    mutation_errors: AtomicU64,
}

struct Load {
    stop: Arc<AtomicBool>,
    counters: Arc<LoadCounters>,
    tasks: Vec<JoinHandle<()>>,
}

impl Load {
    async fn shutdown(self) {
        self.stop.store(true, Ordering::Relaxed);
        for task in self.tasks {
            task.abort();
            let _ = task.await;
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

fn period(hz: u64) -> tokio::time::Interval {
    let mut interval = tokio::time::interval(Duration::from_micros(1_000_000 / hz));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval
}

fn provider_record() -> ProviderRecord {
    ProviderRecord::builder(ProviderId::new(PROVIDER_ID), NodeId::new(NODE_ID))
        .resource_type(ResourceType::new("camera.device"))
        .build()
}

fn executor_record() -> ExecutorRecord {
    ExecutorRecord::builder(ExecutorId::new(EXECUTOR_ID), NodeId::new(NODE_ID))
        .runtime_type(RuntimeType::new(RUNTIME_TYPE))
        .build()
}

fn heartbeat_resources(count: usize, tick: u64) -> Vec<ResourceRecord> {
    let health = if (tick / 500).is_multiple_of(2) {
        HealthState::Healthy
    } else {
        HealthState::Degraded
    };
    (0..count)
        .map(|index| {
            ResourceRecord::builder(
                ResourceId::new(format!("resource.soak.camera.{index}")),
                ResourceType::new("camera.device"),
                ProviderId::new(PROVIDER_ID),
            )
            .health(health)
            .availability(AvailabilityState::Available)
            .lease_state(LeaseState::Unleased)
            .label(format!("capture.slot={index}"))
            .state(ResourceState::new(now_ms()))
            .build()
        })
        .collect()
}

fn workload(slot: usize, desired: DesiredState, generation: u64) -> WorkloadRecord {
    WorkloadRecord::builder(
        WorkloadId::new(format!("workload.soak.capture.{slot}")),
        RuntimeType::new(RUNTIME_TYPE),
        ArtifactId::new(format!("{ARTIFACT_ID}.{}", generation % 2)),
    )
    .desired_state(desired)
    .assigned_to(NodeId::new(NODE_ID))
    .build()
}

async fn start_load(runtime: LocalNodeRuntime, config: &SoakConfig) -> Load {
    let stop = Arc::new(AtomicBool::new(false));
    let counters = Arc::new(LoadCounters::default());
    let assigned: Arc<Mutex<Vec<WorkloadRecord>>> = Arc::default();
    let mut tasks = Vec::new();

    let control = runtime
        .control_plane("soak-control")
        .expect("control-plane client should build");
    let base = control
        .fetch_state_snapshot()
        .await
        .expect("initial snapshot should load")
        .state
        .desired
        .revision;
    control
        .apply_mutations(MutationBatch {
            base_revision: base,
            mutations: (0..2)
                .map(|generation| {
                    DesiredStateMutation::PutArtifact(
                        ArtifactRecord::builder(ArtifactId::new(format!(
                            "{ARTIFACT_ID}.{generation}"
                        )))
                        .build(),
                    )
                })
                .collect(),
        })
        .await
        .expect("artifacts should apply");

    // Provider: register, watch leases/state, and publish capture heartbeats at high rate.
    let provider = LocalProviderService::new(runtime.clone(), "soak-provider", provider_record());
    provider.register().await.expect("provider should register");
    let mut provider_watch = provider
        .subscribe(Revision::ZERO)
        .await
        .expect("provider should subscribe");
    let (stop_c, counters_c) = (stop.clone(), counters.clone());
    tasks.push(tokio::spawn(async move {
        while !stop_c.load(Ordering::Relaxed) {
            match provider_watch.next_event().await {
                Ok(_) => counters_c.provider_events.fetch_add(1, Ordering::Relaxed),
                Err(error) => panic!("provider watch failed: {error:?}"),
            };
        }
    }));
    let publisher = runtime
        .provider("soak-provider-publish", provider_record())
        .expect("provider app should build");
    let (stop_c, counters_c, hz, count) = (
        stop.clone(),
        counters.clone(),
        config.provider_hz,
        config.resources,
    );
    tasks.push(tokio::spawn(async move {
        let mut ticker = period(hz);
        let mut tick = 0_u64;
        while !stop_c.load(Ordering::Relaxed) {
            ticker.tick().await;
            match publisher
                .publish_resources(heartbeat_resources(count, tick))
                .await
            {
                Ok(()) => counters_c
                    .provider_publishes
                    .fetch_add(1, Ordering::Relaxed),
                Err(_) => counters_c.provider_errors.fetch_add(1, Ordering::Relaxed),
            };
            tick += 1;
        }
    }));

    // Executor: register, watch assigned workloads, and report their observed state.
    let executor = LocalExecutorService::new(runtime.clone(), "soak-executor", executor_record());
    executor.register().await.expect("executor should register");
    let mut executor_watch = executor
        .subscribe_workloads()
        .await
        .expect("executor should subscribe");
    let (stop_c, counters_c, assigned_c) = (stop.clone(), counters.clone(), assigned.clone());
    tasks.push(tokio::spawn(async move {
        while !stop_c.load(Ordering::Relaxed) {
            let workloads = match executor_watch.next_event().await {
                Ok(LocalExecutorEvent::Bootstrap(workloads)) => workloads,
                Ok(LocalExecutorEvent::WorkloadsChanged { workloads, .. }) => workloads,
                Err(error) => panic!("executor watch failed: {error:?}"),
            };
            counters_c.executor_events.fetch_add(1, Ordering::Relaxed);
            *assigned_c.lock().expect("assigned lock") = workloads;
        }
    }));
    let reporter = runtime
        .executor("soak-executor-report", executor_record())
        .expect("executor app should build");
    let (stop_c, counters_c, hz) = (stop.clone(), counters.clone(), config.executor_hz);
    tasks.push(tokio::spawn(async move {
        let mut ticker = period(hz);
        while !stop_c.load(Ordering::Relaxed) {
            ticker.tick().await;
            let observed: Vec<WorkloadRecord> = assigned
                .lock()
                .expect("assigned lock")
                .iter()
                .cloned()
                .map(|mut record| {
                    record.observed_state = match record.desired_state {
                        DesiredState::Running => WorkloadObservedState::Running,
                        DesiredState::Stopped => WorkloadObservedState::Stopped,
                    };
                    record
                })
                .collect();
            match reporter.publish_workloads(observed).await {
                Ok(()) => counters_c
                    .executor_publishes
                    .fetch_add(1, Ordering::Relaxed),
                Err(_) => counters_c.executor_errors.fetch_add(1, Ordering::Relaxed),
            };
        }
    }));

    // Control plane: add, update, and remove workloads so mutation history cycles its cap.
    let (stop_c, counters_c) = (stop.clone(), counters.clone());
    let (interval, slots) = (config.mutation_interval, config.workload_slots);
    tasks.push(tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut tick = 0_u64;
        while !stop_c.load(Ordering::Relaxed) {
            ticker.tick().await;
            match mutate_once(&control, tick, slots).await {
                Ok(()) => counters_c.mutations.fetch_add(1, Ordering::Relaxed),
                Err(_) => counters_c.mutation_errors.fetch_add(1, Ordering::Relaxed),
            };
            tick += 1;
        }
    }));

    Load {
        stop,
        counters,
        tasks,
    }
}

async fn mutate_once(
    control: &LocalControlPlaneClient,
    tick: u64,
    slots: usize,
) -> Result<(), orion::client::ClientError> {
    let snapshot = control.fetch_state_snapshot().await?;
    let slot = (tick % slots as u64) as usize;
    let generation = tick / slots as u64;
    let id = WorkloadId::new(format!("workload.soak.capture.{slot}"));
    let mutation = match snapshot.state.desired.workloads.get(&id) {
        None => {
            DesiredStateMutation::PutWorkload(workload(slot, DesiredState::Running, generation))
        }
        Some(_) if generation % 3 == 2 => DesiredStateMutation::RemoveWorkload(id),
        Some(existing) => {
            let desired = match existing.desired_state {
                DesiredState::Running => DesiredState::Stopped,
                DesiredState::Stopped => DesiredState::Running,
            };
            DesiredStateMutation::PutWorkload(workload(slot, desired, generation))
        }
    };
    control
        .apply_mutations(MutationBatch {
            base_revision: snapshot.state.desired.revision,
            mutations: vec![mutation],
        })
        .await
}

fn report_slopes(label: &str, samples: &[&Sample]) -> (f64, f64) {
    let memory = slope_per_min(samples, |sample| sample.memory_kib);
    let anon = slope_per_min(samples, |sample| sample.rss_anon_kib);
    let clients = slope_per_min(samples, |sample| sample.usage.registries.local_clients);
    let history = slope_per_min(samples, |sample| {
        sample.usage.mutation_history.encoded_bytes.unwrap_or(0) / 1024
    });
    println!(
        "{label}: n={} mem_slope={memory:.1} KiB/min anon_slope={anon:.1} KiB/min \
         history_slope={history:.1} KiB/min local_clients_slope={clients:.2}/min",
        samples.len()
    );
    (memory, anon)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "long-running soak; run with --ignored (see docs/testing.md)"]
async fn appliance_memory_soak_stays_flat_under_heartbeat_load() {
    let config = SoakConfig::from_env();
    println!("appliance soak config: {config:?}");
    let mut node = ApplianceNode::spawn(&config);
    let runtime = LocalNodeRuntime::new(&node.ipc_socket, &node.ipc_stream_socket);
    let sampler = runtime
        .control_plane("soak-sampler")
        .expect("sampler client should build");

    let started = Instant::now();
    let first = sampler
        .query_observability()
        .await
        .expect("observability snapshot should load");
    let Some(baseline) = Sample::new(0.0, first.resource_usage) else {
        println!(
            "SKIP appliance memory soak: process memory counters are unavailable on this host \
             (needs Linux /proc/<pid>/status and smaps_rollup)"
        );
        return;
    };
    println!(
        "orion-node pid={} memory metric={}",
        node.pid(),
        if baseline.usage.process.pss_bytes.is_some() {
            "pss"
        } else {
            "vm_rss"
        }
    );

    let load = start_load(runtime, &config).await;
    let mut samples = vec![baseline];
    println!("{}", Sample::header());
    println!("{}", samples[0].row());
    let mut ticker = tokio::time::interval(config.sample_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await;
    while started.elapsed() < config.duration {
        ticker.tick().await;
        node.assert_running();
        let snapshot = sampler
            .query_observability()
            .await
            .expect("observability snapshot should load");
        let sample = Sample::new(started.elapsed().as_secs_f64(), snapshot.resource_usage)
            .expect("memory counters should stay available");
        println!("{}", sample.row());
        assert_within_caps(&sample, config.max_mutation_history);
        samples.push(sample);
    }

    let counters = load.counters.clone();
    load.shutdown().await;
    let load_summary = [
        ("provider_publishes", &counters.provider_publishes),
        ("provider_errors", &counters.provider_errors),
        ("provider_events", &counters.provider_events),
        ("executor_publishes", &counters.executor_publishes),
        ("executor_errors", &counters.executor_errors),
        ("executor_events", &counters.executor_events),
        ("mutations", &counters.mutations),
        ("mutation_errors", &counters.mutation_errors),
    ]
    .map(|(name, value)| format!("{name}={}", value.load(Ordering::Relaxed)))
    .join(" ");
    println!("load: {load_summary}");

    let warm: Vec<&Sample> = samples
        .iter()
        .filter(|sample| sample.t_secs >= config.warmup.as_secs_f64())
        .collect();
    report_slopes("all samples", &samples.iter().collect::<Vec<_>>());
    let (memory_slope, anon_slope) = report_slopes("after warm-up", &warm);
    let (first_warm, last) = (warm.first().expect("warm samples"), samples.last().unwrap());
    println!(
        "memory: start={} KiB warm={} KiB end={} KiB peak={} KiB",
        samples[0].memory_kib,
        first_warm.memory_kib,
        last.memory_kib,
        samples.iter().map(|s| s.memory_kib).max().unwrap_or(0)
    );

    assert!(
        warm.len() >= 5,
        "too few post-warm-up samples ({}); raise ORION_SOAK_DURATION_SECS",
        warm.len()
    );
    let publishes = counters.provider_publishes.load(Ordering::Relaxed);
    let provider_errors = counters.provider_errors.load(Ordering::Relaxed);
    assert!(publishes > 0, "provider never published");
    assert!(
        provider_errors * 100 <= publishes,
        "provider publish errors {provider_errors} exceed 1% of {publishes}"
    );
    assert!(
        counters.mutations.load(Ordering::Relaxed) as f64
            >= config.max_mutation_history as f64 * 1.5,
        "mutation load too low to cycle the mutation history cap"
    );
    let window = last.t_secs - first_warm.t_secs;
    if window < config.min_slope_window.as_secs_f64() {
        // Allocator and cache warm-up keeps PSS rising for the first minutes, so a short window
        // cannot tell warm-up from a leak. Caps are still enforced above.
        println!(
            "NOTE: slope limit not enforced: post-warm-up window {window:.0}s is shorter than \
             {}s (ORION_SOAK_MIN_SLOPE_WINDOW_SECS); run the full duration to judge growth",
            config.min_slope_window.as_secs()
        );
        return;
    }
    assert!(
        memory_slope <= config.max_slope_kib_per_min,
        "orion-node memory grows {memory_slope:.1} KiB/min after warm-up \
         (limit {} KiB/min, ORION_SOAK_MAX_SLOPE_KIB_PER_MIN)",
        config.max_slope_kib_per_min
    );
    // PSS also counts file-backed pages the kernel may reclaim under host memory pressure, which
    // can hide heap growth; anonymous RSS is the heap-dominated signal.
    assert!(
        anon_slope <= config.max_slope_kib_per_min,
        "orion-node anonymous RSS grows {anon_slope:.1} KiB/min after warm-up \
         (limit {} KiB/min, ORION_SOAK_MAX_SLOPE_KIB_PER_MIN)",
        config.max_slope_kib_per_min
    );
}

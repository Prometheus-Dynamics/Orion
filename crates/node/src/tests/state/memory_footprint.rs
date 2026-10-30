//! Heap footprint of pipeline-shaped workloads, measured in-process.
//!
//! Each workload mirrors a HeliOS pipeline: an `ArtifactRecord` plus a `WorkloadRecord` whose
//! config carries the inline Daedalus GraphDocument JSON (`graph.inline`, a string of
//! `HELIOS_MEM_PAYLOAD_BYTES` bytes). Writes go through the same `ControlMessage::Mutations` path
//! the IPC clients use, so persistence and the mutation history run as they do in the node.
//!
//! A counting global allocator tracks live heap bytes (allocations minus frees) and the peak
//! within each phase. glibc's own view (`mallinfo2`: arena, in-use, free) is printed next to it,
//! because freed memory that glibc keeps in its arenas shows up in RSS but not in live bytes.
//!
//! Run with (ignored by default):
//! `HELIOS_MEM_WORKLOADS=1 HELIOS_MEM_UPDATES=300 cargo test -p orion-node --lib \
//!  workload_artifact_memory_footprint -- --ignored --nocapture --test-threads=1`

use super::*;
use orion::control_plane::DesiredStateMutation;
use orion::transport::ipc::UnixControlHandler;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Counts live heap bytes requested through the global allocator, and the peak since the last
/// `reset_peak`. Only the harness binary (`orion-node` lib tests) uses it.
struct CountingAlloc;

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);

fn note_alloc(size: usize) {
    let live = LIVE_BYTES.fetch_add(size, Ordering::Relaxed) + size;
    PEAK_BYTES.fetch_max(live, Ordering::Relaxed);
}

fn note_dealloc(size: usize) {
    LIVE_BYTES.fetch_sub(size, Ordering::Relaxed);
}

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            note_alloc(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            note_alloc(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        note_dealloc(layout.size());
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            note_dealloc(layout.size());
            note_alloc(new_size);
        }
        new_ptr
    }
}

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

fn live_bytes() -> usize {
    LIVE_BYTES.load(Ordering::Relaxed)
}

fn peak_bytes() -> usize {
    PEAK_BYTES.load(Ordering::Relaxed)
}

fn reset_peak() {
    PEAK_BYTES.store(live_bytes(), Ordering::Relaxed);
}

fn env_opt_usize(key: &str) -> Option<usize> {
    std::env::var(key)
        .ok()
        .and_then(|raw| raw.trim().parse().ok())
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|raw| raw.trim().parse().ok())
        .unwrap_or(default)
}

fn proc_status_kib(field: &str) -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status
        .lines()
        .find(|line| line.starts_with(field))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn glibc_heap() -> (usize, usize, usize) {
    // SAFETY: `mallinfo2` only reads allocator bookkeeping.
    let info = unsafe { libc::mallinfo2() };
    (info.arena, info.uordblks, info.fordblks)
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn glibc_heap() -> (usize, usize, usize) {
    (0, 0, 0)
}

/// Graph JSON of about `target_bytes`, shaped like an inline Daedalus GraphDocument.
fn graph_document(revision: usize, target_bytes: usize) -> String {
    let mut out = String::with_capacity(target_bytes + 256);
    out.push_str(&format!(
        r#"{{"format":"daedalus.graph","schema_version":1,"requires":[{{"id":"styx.frames","version":"1.0.0"}}],"metadata":{{"revision":{revision}}},"graph":{{"nodes":["#
    ));
    let mut index = 0usize;
    while out.len() < target_bytes {
        if index > 0 {
            out.push(',');
        }
        out.push_str(&format!(
            r#"{{"id":"stage.{index}","type":"camera.stage.v1","inputs":{{"frame":"stage.{}.out"}},"params":{{"gain":{},"rev":{revision}}}}}"#,
            index.saturating_sub(1),
            index % 97
        ));
        index += 1;
    }
    out.push_str("]}}");
    out
}

fn pipeline_workload(index: usize, revision: usize, payload_bytes: usize) -> WorkloadRecord {
    let config = WorkloadConfig::new("helios.engine.execution.config.v1")
        .field("graph.kind", TypedConfigValue::String("inline".into()))
        .field(
            "graph.inline",
            TypedConfigValue::String(graph_document(revision, payload_bytes)),
        )
        .field("cpu", TypedConfigValue::Int(2))
        .field("nice", TypedConfigValue::Int(0));
    WorkloadRecord::builder(
        WorkloadId::new(format!("workload.pipeline.{index}")),
        RuntimeType::new("helios.engine.v1"),
        ArtifactId::new(format!("artifact.pipeline.{index}")),
    )
    .desired_state(DesiredState::Stopped)
    .assigned_to(NodeId::new("node-mem"))
    .config(config)
    .build()
}

fn pipeline_artifact(index: usize, payload_bytes: usize) -> ArtifactRecord {
    ArtifactRecord::builder(ArtifactId::new(format!("artifact.pipeline.{index}")))
        .content_type("application/json")
        .size_bytes(payload_bytes as u64)
        .build()
}

fn send_mutations(app: &NodeApp, mutations: Vec<DesiredStateMutation>) {
    let base_revision = app.current_desired_revision();
    let response = app
        .unix_control_handler()
        .handle_control(ControlEnvelope {
            source: LocalAddress::new("orionctl.mem"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::Mutations(MutationBatch::new(base_revision, mutations)),
        })
        .expect("mutation batch should be served");
    assert!(
        matches!(response.message, ControlMessage::Accepted),
        "mutation batch rejected: {:?}",
        response.message
    );
}

#[derive(Debug)]
struct Phase {
    name: &'static str,
    live_kib: f64,
    peak_kib: f64,
    arena_kib: f64,
    in_use_kib: f64,
    free_kib: f64,
    rss_kib: u64,
    history_batches: u64,
    history_encoded_kib: f64,
}

fn sample(app: &NodeApp, name: &'static str, base_live: usize) -> Phase {
    let usage = app.observability_snapshot().resource_usage;
    let (arena, in_use, free) = glibc_heap();
    Phase {
        name,
        live_kib: live_bytes().saturating_sub(base_live) as f64 / 1024.0,
        peak_kib: peak_bytes().saturating_sub(base_live) as f64 / 1024.0,
        arena_kib: arena as f64 / 1024.0,
        in_use_kib: in_use as f64 / 1024.0,
        free_kib: free as f64 / 1024.0,
        rss_kib: proc_status_kib("VmRSS:").unwrap_or(0),
        history_batches: usage.mutation_history.batches,
        history_encoded_kib: usage.mutation_history.encoded_bytes.unwrap_or(0) as f64 / 1024.0,
    }
}

fn print_phase(phase: &Phase) {
    eprintln!(
        "MEMHARNESS phase={:<14} live_kib={:>9.1} peak_kib={:>9.1} glibc_arena_kib={:>9.1} \
         glibc_in_use_kib={:>9.1} glibc_free_kib={:>9.1} vmrss_kib={:>8} history_batches={:>4} \
         history_encoded_kib={:>8.1}",
        phase.name,
        phase.live_kib,
        phase.peak_kib,
        phase.arena_kib,
        phase.in_use_kib,
        phase.free_kib,
        phase.rss_kib,
        phase.history_batches,
        phase.history_encoded_kib,
    );
}

/// Heap cost of N pipeline workloads, each updated M times, with a removal phase at the end.
///
/// Reports live heap per workload and per revision after the history has reached its cap, plus
/// glibc's retained arena (what the process keeps even after frees).
#[test]
#[ignore = "memory footprint harness; run explicitly with --ignored"]
fn workload_artifact_memory_footprint() {
    let workloads = env_usize("HELIOS_MEM_WORKLOADS", 1).max(1);
    let updates = env_usize("HELIOS_MEM_UPDATES", 300);
    let payload_bytes = env_usize("HELIOS_MEM_PAYLOAD_BYTES", 40 * 1024);
    let threads_before = proc_status_kib("Threads:").unwrap_or(0);

    let state_dir = temp_state_dir("memory-footprint");
    let base_live = live_bytes();
    reset_peak();
    // The per-client IPC rate limit (256/s) is not what is measured, so it is lifted. Unset env
    // keeps the shipped defaults; the overrides reproduce older settings for before/after runs.
    let mut builder = NodeApp::builder()
        .config(test_node_config_with_state_dir_and_auth(
            "node-mem",
            "node-mem",
            state_dir,
            crate::PeerAuthenticationMode::Disabled,
        ))
        .with_local_rate_limit_max_messages(u32::MAX);
    if let Some(bytes) = env_opt_usize("HELIOS_MEM_HISTORY_BYTES") {
        builder = builder.with_max_mutation_history_bytes(bytes);
    }
    if let Some(capacity) = env_opt_usize("HELIOS_MEM_QUEUE_CAPACITY") {
        builder = builder.with_persistence_worker_queue_capacity(capacity);
    }
    let app = builder.try_build().expect("node app should build");
    let threads_after_build = proc_status_kib("Threads:").unwrap_or(0);
    let client_hello = app.unix_control_handler().handle_control(ControlEnvelope {
        source: LocalAddress::new("orionctl.mem"),
        destination: LocalAddress::new("orion"),
        message: ControlMessage::ClientHello(ClientHello {
            client_name: "orionctl.mem".into(),
            role: ClientRole::ControlPlane,
        }),
    });
    client_hello.expect("client hello should be served");
    let empty = sample(&app, "empty", base_live);
    print_phase(&empty);

    // Creation: one artifact and one workload per pipeline, in one batch each (as HeliOS writes).
    reset_peak();
    for index in 0..workloads {
        send_mutations(
            &app,
            vec![
                DesiredStateMutation::PutArtifact(pipeline_artifact(index, payload_bytes)),
                DesiredStateMutation::PutWorkload(pipeline_workload(index, 0, payload_bytes)),
            ],
        );
    }
    let created = sample(&app, "created", base_live);
    print_phase(&created);

    // Updates: every pipeline is rewritten with a new revision of its graph, round by round.
    reset_peak();
    let mut ramp = Vec::new();
    for revision in 1..=updates {
        for index in 0..workloads {
            send_mutations(
                &app,
                vec![DesiredStateMutation::PutWorkload(pipeline_workload(
                    index,
                    revision,
                    payload_bytes,
                ))],
            );
        }
        if revision == 1 || revision % 50 == 0 || revision == updates {
            ramp.push(sample(&app, "updates", base_live));
        }
    }
    for phase in &ramp {
        print_phase(phase);
    }
    let updated = sample(&app, "updated", base_live);
    print_phase(&updated);

    // Removal: every pipeline is removed; tombstones are kept until retention expires.
    reset_peak();
    let removals = (0..workloads)
        .flat_map(|index| {
            [
                DesiredStateMutation::RemoveWorkload(WorkloadId::new(format!(
                    "workload.pipeline.{index}"
                ))),
                DesiredStateMutation::RemoveArtifact(ArtifactId::new(format!(
                    "artifact.pipeline.{index}"
                ))),
            ]
        })
        .collect();
    send_mutations(&app, removals);
    let removed = sample(&app, "removed", base_live);
    print_phase(&removed);

    let threads_running = proc_status_kib("Threads:").unwrap_or(0);
    drop(app);
    let dropped = Phase {
        name: "dropped",
        live_kib: live_bytes().saturating_sub(base_live) as f64 / 1024.0,
        peak_kib: 0.0,
        arena_kib: glibc_heap().0 as f64 / 1024.0,
        in_use_kib: glibc_heap().1 as f64 / 1024.0,
        free_kib: glibc_heap().2 as f64 / 1024.0,
        rss_kib: proc_status_kib("VmRSS:").unwrap_or(0),
        history_batches: 0,
        history_encoded_kib: 0.0,
    };
    print_phase(&dropped);

    let per_workload_kib = updated.live_kib / workloads as f64;
    let revisions = (workloads * updates).max(1) as f64;
    eprintln!(
        "MEMHARNESS SUMMARY workloads={workloads} updates={updates} payload_bytes={payload_bytes} \
         threads_before={threads_before} threads_after_build={threads_after_build} \
         threads_running={threads_running} live_after_build_kib={:.1} \
         live_updated_kib={:.1} live_per_workload_kib={per_workload_kib:.1} \
         live_per_revision_bytes={:.0} glibc_arena_updated_kib={:.1} vmrss_updated_kib={}",
        empty.live_kib,
        updated.live_kib,
        updated.live_kib * 1024.0 / revisions,
        updated.arena_kib,
        updated.rss_kib,
    );
}

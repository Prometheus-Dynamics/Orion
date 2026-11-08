//! What one message costs the node on a `unix:` link: heap allocations and bytes allocated on the
//! node's threads, and bytes on the wire, for a provider's status update, the same update fanned
//! out to a watching client, and a stream batch to a subscriber; plus what the node allocates
//! while idle. Visibility for small boards first: the test prints the numbers and fails only on a
//! blow-up (ceilings far above today's), so a noisy machine does not fail it.
//!
//! The clients run on their own threads, which the counting allocator ignores; everything the
//! node does (gateway tasks, the status lane, the stream hub) runs on the runtime's threads and
//! is counted. Instructions are user-space instructions retired on the runtime's worker threads,
//! from one perf counter per thread; they are left out where perf events are not allowed
//! (`kernel.perf_event_paranoid` above 2, most containers). Run with `--nocapture` to see the
//! table.

#![cfg(all(target_os = "linux", feature = "link-gateway"))]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use orion_link::message::{
    Hello, Message, ProviderState, Roles, StatusEntry, StatusRequest, StreamDeclare, StreamPublish,
    StreamSubscribe,
};
use orion_link::{LINK_INTERFACE_REVISION, StreamDecoder, StreamEncoder};
use orion_node::control_plane::{
    AvailabilityState, Backfill, HealthState, ProviderRecord, ResourceRecord, SampleChunkBuilder,
    SeriesDecl, SeriesKind, StatusQuery, StatusSubject, StreamRef, StreamSubscription,
    TypedConfigValue,
};
use orion_node::link_gateway::LinkConfig;
use orion_node::{NodeApp, NodeConfig, ProviderId, ResourceId};

struct Counting;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

thread_local! {
    /// Set on the client threads: their allocations are not the node's.
    static IGNORED: Cell<bool> = const { Cell::new(false) };
}

fn counted(size: usize) {
    if !IGNORED.try_with(Cell::get).unwrap_or(true) {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(size as u64, Ordering::Relaxed);
    }
}

// SAFETY: forwards to the system allocator; the counters do not allocate.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        counted(layout.size());
        // SAFETY: the caller's contract.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        counted(new_size);
        // SAFETY: the caller's contract.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Context switches so far of the runtime's worker threads (`tokio-rt-worker`): each one is a
/// wake-up of the node. Linux only; 0 elsewhere.
fn worker_wakeups() -> u64 {
    // Reading /proc allocates; that is the test's cost, not the node's.
    let was = IGNORED.with(|ignored| ignored.replace(true));
    let wakeups = read_worker_wakeups();
    IGNORED.with(|ignored| ignored.set(was));
    wakeups
}

fn read_worker_wakeups() -> u64 {
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return 0;
    };
    tasks
        .filter_map(Result::ok)
        .filter(|task| {
            std::fs::read_to_string(task.path().join("comm"))
                .is_ok_and(|name| name.trim() == "tokio-rt-worker")
        })
        .filter_map(|task| std::fs::read_to_string(task.path().join("status")).ok())
        .map(|status| {
            status
                .lines()
                .filter_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.ends_with("voluntary_ctxt_switches")
                        .then(|| value.trim().parse::<u64>().ok())
                        .flatten()
                })
                .sum::<u64>()
        })
        .sum()
}

/// The ids of the runtime's worker threads.
fn worker_tids() -> Vec<i32> {
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return Vec::new();
    };
    tasks
        .filter_map(Result::ok)
        .filter(|task| {
            std::fs::read_to_string(task.path().join("comm"))
                .is_ok_and(|name| name.trim() == "tokio-rt-worker")
        })
        .filter_map(|task| task.file_name().to_str()?.parse().ok())
        .collect()
}

/// `perf_event_attr`, first published layout (`PERF_ATTR_SIZE_VER0`, 64 bytes).
#[repr(C)]
struct PerfEventAttr {
    kind: u32,
    size: u32,
    config: u64,
    sample_period: u64,
    sample_type: u64,
    read_format: u64,
    flags: u64,
    wakeup_events: u32,
    bp_type: u32,
    bp_addr: u64,
}

/// One user-space instruction counter per worker thread, opened at the first call; `None` when
/// perf events are not allowed here.
fn instruction_counters() -> Option<&'static [i32]> {
    static COUNTERS: OnceLock<Vec<i32>> = OnceLock::new();
    let counters = COUNTERS.get_or_init(|| {
        let was = IGNORED.with(|ignored| ignored.replace(true));
        let mut fds = Vec::new();
        for tid in worker_tids() {
            let attr = PerfEventAttr {
                kind: 0,   // PERF_TYPE_HARDWARE
                size: 64,  // PERF_ATTR_SIZE_VER0
                config: 1, // PERF_COUNT_HW_INSTRUCTIONS
                sample_period: 0,
                sample_type: 0,
                read_format: 0,
                flags: (1 << 5) | (1 << 6), // exclude_kernel, exclude_hv
                wakeup_events: 0,
                bp_type: 0,
                bp_addr: 0,
            };
            // SAFETY: a valid attribute struct of the size it states; the other arguments select
            // one thread on any CPU, no group, no flags.
            let fd = unsafe {
                libc::syscall(
                    libc::SYS_perf_event_open,
                    &raw const attr,
                    tid,
                    -1i32,
                    -1i32,
                    0u64,
                )
            };
            match i32::try_from(fd) {
                Ok(fd) if fd >= 0 => fds.push(fd),
                _ => {
                    fds.clear();
                    break;
                }
            }
        }
        IGNORED.with(|ignored| ignored.set(was));
        fds
    });
    (!counters.is_empty()).then_some(counters.as_slice())
}

/// User-space instructions retired so far on the worker threads (0 without counters).
fn instructions() -> u64 {
    instruction_counters().map_or(0, |fds| {
        fds.iter()
            .map(|&fd| {
                let mut value = 0u64;
                // SAFETY: reads one u64 counter value into `value`.
                let n = unsafe { libc::read(fd, (&raw mut value).cast(), 8) };
                if n == 8 { value } else { 0 }
            })
            .sum()
    })
}

fn snapshot() -> (u64, u64, u64) {
    (
        ALLOCATIONS.load(Ordering::Relaxed),
        ALLOCATED_BYTES.load(Ordering::Relaxed),
        instructions(),
    )
}

const PROVIDER: &str = "provider.cost-board";
const WAIT: Duration = Duration::from_secs(10);
/// Messages per measured phase.
const MESSAGES: u64 = 2_000;
/// Stream batches, and samples per batch.
const BATCHES: u64 = 200;
const SAMPLES: u64 = 50;

/// A blocking session on its own (ignored) thread's terms: frames are encoded before a phase
/// starts, and reads only count frames until the expected one.
struct Session {
    stream: UnixStream,
    seq: u16,
    decoder: Box<StreamDecoder<4096>>,
    written: u64,
    read: u64,
}

impl Session {
    fn connect(path: &Path, name: &str, roles: Roles) -> Self {
        let deadline = Instant::now() + WAIT;
        let stream = loop {
            match UnixStream::connect(path) {
                Ok(stream) => break stream,
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10))
                }
                Err(error) => panic!("link socket: {error}"),
            }
        };
        stream.set_read_timeout(Some(WAIT)).expect("timeout");
        let mut session = Self {
            stream,
            seq: 0,
            decoder: Box::new(StreamDecoder::new()),
            written: 0,
            read: 0,
        };
        session.send(&Message::Hello(Hello {
            device_name: name.into(),
            roles,
            max_frame: 4096,
            revision: LINK_INTERFACE_REVISION,
        }));
        session.until(|message| matches!(message, Message::Welcome(_)));
        session
    }

    fn encode(&mut self, message: &Message) -> Vec<u8> {
        self.seq = self.seq.wrapping_add(1);
        let mut frame = [0u8; 4096];
        let len = message.encode(self.seq, &mut frame).expect("fits");
        StreamEncoder::for_frame(&frame[..len]).collect()
    }

    fn write(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).expect("writes");
        self.written += bytes.len() as u64;
    }

    fn send(&mut self, message: &Message) {
        let bytes = self.encode(message);
        self.write(&bytes);
    }

    /// Reads until a message `done` accepts; returns how many messages `count` accepted on the
    /// way (that one included).
    fn until_counting(
        &mut self,
        mut count: impl FnMut(&Message) -> bool,
        mut done: impl FnMut(&Message) -> bool,
    ) -> u64 {
        let mut buf = [0u8; 4096];
        let mut counted = 0;
        loop {
            let n = self.stream.read(&mut buf).expect("the node answers");
            assert!(n > 0, "the node closed the session");
            self.read += n as u64;
            let mut at = 0;
            while at < n {
                let (used, frame) = self.decoder.push_slice(&buf[at..n]);
                at += used;
                let Ok(Some(frame)) = frame else { continue };
                let message = Message::decode(&frame).expect("decodes");
                if count(&message) {
                    counted += 1;
                }
                if done(&message) {
                    // Bytes after the frame belong to later messages; this test reads in lock step,
                    // so none are expected.
                    return counted;
                }
            }
        }
    }

    fn until(&mut self, done: impl FnMut(&Message) -> bool) {
        self.until_counting(|_| false, done);
    }

    /// A ping answered: the node has handled everything sent before it.
    fn sync(&mut self) {
        self.send(&Message::Ping { now_ms: 7 });
        self.until(|message| matches!(message, Message::Pong { .. }));
    }
}

/// Runs `body` on a thread whose allocations are not counted.
fn client<T: Send + 'static>(body: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::spawn(move || {
        IGNORED.with(|ignored| ignored.set(true));
        body()
    })
    .join()
    .expect("the client thread runs")
}

fn status(value: u64) -> Message {
    Message::Status(vec![StatusEntry::new(
        "count",
        TypedConfigValue::UInt(value),
    )])
}

/// Per-message numbers of one phase.
struct Cost {
    allocations: f64,
    bytes: f64,
    instructions: Option<f64>,
    wire_in: f64,
    wire_out: f64,
}

fn per(
    n: u64,
    before: (u64, u64, u64),
    after: (u64, u64, u64),
    wire_in: u64,
    wire_out: u64,
) -> Cost {
    let n = n as f64;
    Cost {
        allocations: (after.0 - before.0) as f64 / n,
        bytes: (after.1 - before.1) as f64 / n,
        instructions: instruction_counters().map(|_| (after.2 - before.2) as f64 / n),
        wire_in: wire_in as f64 / n,
        wire_out: wire_out as f64 / n,
    }
}

fn print(name: &str, cost: &Cost) {
    let instructions = cost
        .instructions
        .map_or_else(|| "-".to_owned(), |i| format!("{i:.0}"));
    println!(
        "{name:<34} {:>8.1} allocs {:>9.0} B allocated {instructions:>9} instr {:>6.0} B in {:>6.0} B out",
        cost.allocations, cost.bytes, cost.wire_in, cost.wire_out
    );
}

fn socket_path() -> PathBuf {
    std::env::temp_dir().join(format!("orion-link-costs-{}.sock", std::process::id()))
}

#[test]
fn what_a_message_costs_the_node_on_a_unix_link() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let path = socket_path();
    let _ = std::fs::remove_file(&path);
    let ipc =
        std::env::temp_dir().join(format!("orion-link-costs-ipc-{}.sock", std::process::id()));
    let app = NodeApp::try_new(NodeConfig::for_local_node("node-costs").with_ipc_socket_path(ipc))
        .expect("node app");
    let link = LinkConfig::parse(&format!(
        "unix:{}?heartbeat_ms=5000&missed_heartbeats=3",
        path.display()
    ))
    .expect("link config");
    let gateway = runtime
        .block_on(async { app.start_link_gateway(vec![link]) })
        .expect("gateway");

    // A provider session with one resource.
    let socket = path.clone();
    let mut provider = client(move || {
        let mut provider = Session::connect(&socket, "cost-board", Roles::PROVIDER);
        provider.send(&Message::ProviderState(ProviderState {
            provider: ProviderRecord::builder(ProviderId::new(PROVIDER), "unassigned")
                .resource_type("imu.sample_source")
                .build(),
            resources: vec![
                ResourceRecord::builder(
                    ResourceId::new("cost-board.imu-0"),
                    "imu.sample_source",
                    ProviderId::new(PROVIDER),
                )
                .health(HealthState::Healthy)
                .availability(AvailabilityState::Available)
                .build(),
            ],
        }));
        provider.sync();
        provider.send(&status(0));
        provider.sync();
        provider
    });
    let query = StatusQuery::all();
    let deadline = Instant::now() + WAIT;
    while app.query_status(&query).is_empty() {
        assert!(
            Instant::now() < deadline,
            "the provider's status never arrived"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    // Idle: what the node allocates per second with one quiet session.
    let before = snapshot();
    let wakeups_before = worker_wakeups();
    let idle_for = Duration::from_secs(1);
    std::thread::sleep(idle_for);
    let after = snapshot();
    let idle_wakeups = worker_wakeups() - wakeups_before;
    let idle = (after.0 - before.0, after.1 - before.1);

    // Status updates from the provider, nobody watching.
    let frames: Vec<Vec<u8>> = (1..=MESSAGES)
        .map(|v| provider.encode(&status(v)))
        .collect();
    let (written, read) = (provider.written, provider.read);
    let before = snapshot();
    let mut provider = client(move || {
        for frame in &frames {
            provider.write(frame);
        }
        provider.sync();
        provider
    });
    let after = snapshot();
    let publish = per(
        MESSAGES,
        before,
        after,
        provider.written - written,
        provider.read - read,
    );

    // The same updates with a client watching: each is fanned out to it.
    let socket = path.clone();
    let mut watcher = client(move || {
        let mut watcher = Session::connect(&socket, "cost-console", Roles::CLIENT);
        watcher.send(&Message::StatusWatch(StatusRequest {
            req: 1,
            filter: StatusQuery::subject(StatusSubject::Provider(ProviderId::new(PROVIDER))),
        }));
        watcher.until(|message| matches!(message, Message::StatusChanges(c) if !c.more));
        watcher
    });
    let frames: Vec<Vec<u8>> = (1..=MESSAGES)
        .map(|v| provider.encode(&status(MESSAGES + v)))
        .collect();
    let (written, watcher_read) = (provider.written, watcher.read);
    let last = TypedConfigValue::UInt(2 * MESSAGES);
    let before = snapshot();
    let (provider, watcher, changes) = client(move || {
        let writer = std::thread::spawn(move || {
            IGNORED.with(|ignored| ignored.set(true));
            for frame in &frames {
                provider.write(frame);
            }
            provider
        });
        let changes = watcher.until_counting(
            |message| matches!(message, Message::StatusChanges(_)),
            |message| {
                matches!(message, Message::StatusChanges(c)
                    if c.updated.iter().any(|entry| entry.value == last))
            },
        );
        (writer.join().expect("writer"), watcher, changes)
    });
    let after = snapshot();
    let watched = per(
        MESSAGES,
        before,
        after,
        provider.written - written,
        watcher.read - watcher_read,
    );

    // Stream batches from the provider to a subscriber.
    let mut provider = provider;
    let mut watcher = watcher;
    let (provider, mut watcher) = client(move || {
        provider.send(&Message::StreamDeclare(StreamDeclare {
            req: 9,
            id: "imu".into(),
            series: vec![SeriesDecl::new("imu.accel_x", SeriesKind::F32, 1_000)],
        }));
        provider.until(|message| matches!(message, Message::StreamDemand(_)));
        watcher.send(&Message::StreamSubscribe(Box::new(StreamSubscribe {
            req: 2,
            subscription: StreamSubscription::all(StreamRef {
                owner: StatusSubject::Provider(ProviderId::new(PROVIDER)),
                id: "imu".into(),
            })
            .with_backfill(Backfill::None),
        })));
        watcher.until(|message| matches!(message, Message::StreamCatalog(_)));
        provider.until(|message| matches!(message, Message::StreamDemand(d) if d.subscribers == 1));
        (provider, watcher)
    });
    let mut provider = provider;
    let t0 = 1_700_000_000_000_000u64;
    let frames: Vec<Vec<u8>> = (0..BATCHES)
        .map(|batch| {
            let mut builder = SampleChunkBuilder::new(0, SeriesKind::F32);
            for i in 0..SAMPLES {
                builder.push(t0 + (batch * SAMPLES + i) * 1_000, i as f64);
            }
            provider.encode(&Message::StreamPublish(StreamPublish {
                id: "imu".into(),
                chunks: vec![builder.take()],
            }))
        })
        .collect();
    let (written, sub_read) = (provider.written, watcher.read);
    let before = snapshot();
    let (provider, watcher, batches) = client(move || {
        let writer = std::thread::spawn(move || {
            IGNORED.with(|ignored| ignored.set(true));
            for frame in &frames {
                provider.write(frame);
            }
            provider
        });
        // The hub may merge batches into one message: wait for the last batch's sequence.
        let batches = watcher.until_counting(
            |message| matches!(message, Message::StreamData(_)),
            |message| matches!(message, Message::StreamData(d) if d.cursor.seq >= BATCHES),
        );
        (writer.join().expect("writer"), watcher, batches)
    });
    let after = snapshot();
    let streamed = per(
        BATCHES,
        before,
        after,
        provider.written - written,
        watcher.read - sub_read,
    );

    println!(
        "node idle, one session: {:.0} allocs/s, {:.0} B/s allocated, {:.0} wake-ups/s",
        idle.0 as f64 / idle_for.as_secs_f64(),
        idle.1 as f64 / idle_for.as_secs_f64(),
        idle_wakeups as f64 / idle_for.as_secs_f64()
    );
    print("status update (no watcher)", &publish);
    print("status update fanned out to 1 watch", &watched);
    print(&format!("stream batch ({SAMPLES} f32 samples)"), &streamed);
    println!(
        "watch answers: {changes} for {MESSAGES} updates; stream messages: {batches} for {BATCHES} batches"
    );

    // Blow-up ceilings only (see the module docs).
    assert!(
        publish.allocations < 200.0,
        "status update: {:.1} allocs",
        publish.allocations
    );
    assert!(
        watched.allocations < 400.0,
        "fan-out: {:.1} allocs",
        watched.allocations
    );
    assert!(
        streamed.allocations < 400.0,
        "stream batch: {:.1} allocs",
        streamed.allocations
    );
    // Idle allocates nothing today (link status is updated in place, only when it changed); a
    // few allocations of slack keep the test calm on a busy machine.
    assert!(idle.0 <= 10, "idle: {} allocs in a second", idle.0);
    // An idle link sleeps until its next deadline (the lease check once a second, the session's
    // loss timeout): 1 to 4 wake-ups a second today, against 20 with the old fixed tick. Only
    // voluntary switches are counted, so load does not raise it much; 10 keeps it calm.
    assert!(
        idle_wakeups <= 10,
        "idle: {idle_wakeups} worker wake-ups in a second"
    );

    drop((provider, watcher));
    runtime.block_on(gateway.shutdown());
    let _ = std::fs::remove_file(&path);
}

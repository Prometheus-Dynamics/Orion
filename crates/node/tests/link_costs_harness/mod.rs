//! The link cost tests' harness: a counting allocator (allocations on the node's threads only),
//! wake-up and instruction counters for the runtime's workers, and a blocking link session.
//! Each cost test is its own binary, so its counters see only its own node.

#![allow(dead_code, unused_imports)]

pub(crate) use std::alloc::{GlobalAlloc, Layout, System};
pub(crate) use std::cell::Cell;
pub(crate) use std::io::{Read, Write};
pub(crate) use std::os::unix::net::UnixStream;
pub(crate) use std::path::{Path, PathBuf};
pub(crate) use std::sync::OnceLock;
pub(crate) use std::sync::atomic::{AtomicU64, Ordering};
pub(crate) use std::time::{Duration, Instant};

pub(crate) use orion_link::message::{
    Hello, Message, ProviderState, Roles, StatusEntry, StatusRequest, StreamDeclare, StreamPublish,
    StreamSubscribe,
};
pub(crate) use orion_link::{LINK_INTERFACE_REVISION, StreamDecoder, StreamEncoder};
pub(crate) use orion_node::control_plane::{
    AvailabilityState, Backfill, HealthState, ProviderRecord, ResourceRecord, SampleChunkBuilder,
    SeriesDecl, SeriesKind, StatusQuery, StatusSubject, StreamRef, StreamSubscription,
    TypedConfigValue,
};
pub(crate) use orion_node::link_gateway::LinkConfig;
pub(crate) use orion_node::{NodeApp, NodeConfig, ProviderId, ResourceId};

pub(crate) struct Counting;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

thread_local! {
    /// Set on the client threads: their allocations are not the node's.
    pub(crate) static IGNORED: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn counted(size: usize) {
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
pub(crate) fn worker_wakeups() -> u64 {
    // Reading /proc allocates; that is the test's cost, not the node's.
    let was = IGNORED.with(|ignored| ignored.replace(true));
    let wakeups = read_worker_wakeups();
    IGNORED.with(|ignored| ignored.set(was));
    wakeups
}

pub(crate) fn read_worker_wakeups() -> u64 {
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
pub(crate) fn worker_tids() -> Vec<i32> {
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
pub(crate) struct PerfEventAttr {
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
pub(crate) fn instruction_counters() -> Option<&'static [i32]> {
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
pub(crate) fn instructions() -> u64 {
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

pub(crate) fn snapshot() -> (u64, u64, u64) {
    (
        ALLOCATIONS.load(Ordering::Relaxed),
        ALLOCATED_BYTES.load(Ordering::Relaxed),
        instructions(),
    )
}

pub(crate) const PROVIDER: &str = "provider.cost-board";
pub(crate) const WAIT: Duration = Duration::from_secs(10);
/// Messages per measured phase.
pub(crate) const MESSAGES: u64 = 2_000;
/// Stream batches, and samples per batch.
pub(crate) const BATCHES: u64 = 200;
pub(crate) const SAMPLES: u64 = 50;

/// A blocking session on its own (ignored) thread's terms: frames are encoded before a phase
/// starts, and reads only count frames until the expected one.
pub(crate) struct Session {
    stream: Box<dyn Duplex>,
    seq: u16,
    decoder: Box<StreamDecoder<4096>>,
    pub(crate) written: u64,
    pub(crate) read: u64,
}

/// A session's byte stream: a unix socket, or a pty master for a serial link.
pub(crate) trait Duplex: Read + Write + Send {}

impl<T: Read + Write + Send> Duplex for T {}

impl Session {
    pub(crate) fn connect(path: &Path, name: &str, roles: Roles) -> Self {
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
        Self::over(Box::new(stream), name, roles)
    }

    /// A session over `stream`, which must time out its reads (after [`WAIT`]).
    pub(crate) fn over(stream: Box<dyn Duplex>, name: &str, roles: Roles) -> Self {
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

    pub(crate) fn encode(&mut self, message: &Message) -> Vec<u8> {
        self.seq = self.seq.wrapping_add(1);
        let mut frame = [0u8; 4096];
        let len = message.encode(self.seq, &mut frame).expect("fits");
        StreamEncoder::for_frame(&frame[..len]).collect()
    }

    pub(crate) fn write(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).expect("writes");
        self.written += bytes.len() as u64;
    }

    pub(crate) fn send(&mut self, message: &Message) {
        let bytes = self.encode(message);
        self.write(&bytes);
    }

    /// Reads until a message `done` accepts; returns how many messages `count` accepted on the
    /// way (that one included).
    pub(crate) fn until_counting(
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

    pub(crate) fn until(&mut self, done: impl FnMut(&Message) -> bool) {
        self.until_counting(|_| false, done);
    }

    /// A ping answered: the node has handled everything sent before it.
    pub(crate) fn sync(&mut self) {
        self.send(&Message::Ping { now_ms: 7 });
        self.until(|message| matches!(message, Message::Pong { .. }));
    }
}

/// Runs `body` on a thread whose allocations are not counted.
pub(crate) fn client<T: Send + 'static>(body: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::spawn(move || {
        IGNORED.with(|ignored| ignored.set(true));
        body()
    })
    .join()
    .expect("the client thread runs")
}

pub(crate) fn status(value: u64) -> Message {
    Message::Status(vec![StatusEntry::new(
        "count",
        TypedConfigValue::UInt(value),
    )])
}

/// Per-message numbers of one phase.
pub(crate) struct Cost {
    pub(crate) allocations: f64,
    pub(crate) bytes: f64,
    pub(crate) instructions: Option<f64>,
    pub(crate) wire_in: f64,
    pub(crate) wire_out: f64,
}

pub(crate) fn per(
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

pub(crate) fn print(name: &str, cost: &Cost) {
    let instructions = cost
        .instructions
        .map_or_else(|| "-".to_owned(), |i| format!("{i:.0}"));
    println!(
        "{name:<34} {:>8.1} allocs {:>9.0} B allocated {instructions:>9} instr {:>6.0} B in {:>6.0} B out",
        cost.allocations, cost.bytes, cost.wire_in, cost.wire_out
    );
}

pub(crate) fn socket_path() -> PathBuf {
    std::env::temp_dir().join(format!("orion-link-costs-{}.sock", std::process::id()))
}

//! What one message costs the node on a `serial:` link, over a pseudo-terminal: the same counters
//! as `link_costs.rs` (allocations and instructions on the node's threads, wake-ups when idle),
//! for a provider's status update and for a stream batch nobody subscribes to. A serial link has
//! one device, so fan-out to a watcher is measured on the unix link only. The test prints the
//! numbers and fails only on a blow-up or on a per-message allocation coming back.

#![cfg(all(target_os = "linux", feature = "link-gateway"))]

mod link_costs_harness;

use link_costs_harness::*;
use std::ffi::CStr;
use std::fs::File;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

/// The master side of a pty, with reads that give up after [`WAIT`].
struct PtyMaster(File);

impl Read for PtyMaster {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut poll = libc::pollfd {
            fd: self.0.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let wait_ms = i32::try_from(WAIT.as_millis()).unwrap_or(i32::MAX);
        // SAFETY: one valid pollfd.
        match unsafe { libc::poll(&raw mut poll, 1, wait_ms) } {
            0 => Err(std::io::ErrorKind::TimedOut.into()),
            n if n < 0 => Err(std::io::Error::last_os_error()),
            _ => self.0.read(buf),
        }
    }
}

impl Write for PtyMaster {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}

/// Opens a pty (blocking master) and returns it with the slave's path.
fn open_pty() -> (PtyMaster, PathBuf) {
    // SAFETY: plain libc calls on a fd we own; the name buffer is sized as passed.
    unsafe {
        let master = libc::posix_openpt(libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC);
        assert!(master >= 0, "posix_openpt");
        let fd = OwnedFd::from_raw_fd(master);
        assert_eq!(libc::grantpt(master), 0, "grantpt");
        assert_eq!(libc::unlockpt(master), 0, "unlockpt");
        let mut name = [0 as libc::c_char; 128];
        assert_eq!(libc::ptsname_r(master, name.as_mut_ptr(), name.len()), 0);
        let path = PathBuf::from(CStr::from_ptr(name.as_ptr()).to_str().expect("UTF-8"));
        (PtyMaster(File::from(fd)), path)
    }
}

/// Waits until this process (the gateway) holds `path` open, then a little longer for it to set
/// the line to raw mode.
fn wait_until_opened(path: &Path) {
    let deadline = Instant::now() + WAIT;
    loop {
        let opened = std::fs::read_dir("/proc/self/fd").is_ok_and(|fds| {
            fds.filter_map(Result::ok)
                .any(|fd| std::fs::read_link(fd.path()).is_ok_and(|target| target == path))
        });
        if opened {
            std::thread::sleep(Duration::from_millis(200));
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the gateway never opened {path:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn what_a_message_costs_the_node_on_a_serial_link() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let ipc = std::env::temp_dir().join(format!(
        "orion-link-costs-serial-ipc-{}.sock",
        std::process::id()
    ));
    let app = NodeApp::try_new(NodeConfig::for_local_node("node-costs").with_ipc_socket_path(ipc))
        .expect("node app");
    let (master, slave) = open_pty();
    let link = LinkConfig::parse(&format!(
        "serial:{}?heartbeat_ms=5000&missed_heartbeats=3",
        slave.display()
    ))
    .expect("link config");
    let gateway = runtime
        .block_on(async { app.start_link_gateway(vec![link]) })
        .expect("gateway");
    wait_until_opened(&slave);

    let mut provider = client(move || {
        let mut provider = Session::over(Box::new(master), "cost-board", Roles::PROVIDER);
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
        provider.send(&Message::StreamDeclare(StreamDeclare {
            req: 10,
            id: "temp".into(),
            series: vec![SeriesDecl::new("temp.board", SeriesKind::F32, 1_000)],
        }));
        provider.until(|message| matches!(message, Message::StreamDemand(_)));
        provider.sync();
        provider
    });

    // Idle: one quiet session.
    let before = snapshot();
    let wakeups_before = worker_wakeups();
    let idle_for = Duration::from_secs(1);
    std::thread::sleep(idle_for);
    let after = snapshot();
    let idle_wakeups = worker_wakeups() - wakeups_before;
    let idle = (after.0 - before.0, after.1 - before.1);

    // Status updates, nobody watching; a ping after them marks the last one handled.
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

    // Stream batches nobody subscribes to.
    let t0 = 1_700_000_000_000_000u64;
    let frames: Vec<Vec<u8>> = (0..BATCHES)
        .map(|batch| {
            let mut builder = SampleChunkBuilder::new(0, SeriesKind::F32);
            for i in 0..SAMPLES {
                builder.push(t0 + (batch * SAMPLES + i) * 1_000, i as f64);
            }
            provider.encode(&Message::StreamPublish(StreamPublish {
                id: "temp".into(),
                chunks: vec![builder.take()],
            }))
        })
        .collect();
    let (written, read) = (provider.written, provider.read);
    let before = snapshot();
    let provider = client(move || {
        for frame in &frames {
            provider.write(frame);
        }
        provider.sync();
        provider
    });
    let after = snapshot();
    let streamed = per(
        BATCHES,
        before,
        after,
        provider.written - written,
        provider.read - read,
    );

    println!(
        "serial, idle, one session: {:.0} allocs/s, {:.0} B/s allocated, {:.0} wake-ups/s",
        idle.0 as f64 / idle_for.as_secs_f64(),
        idle.1 as f64 / idle_for.as_secs_f64(),
        idle_wakeups as f64 / idle_for.as_secs_f64()
    );
    print("serial status update (no watcher)", &publish);
    print("serial stream batch, no subscriber", &streamed);

    // As on the unix link: the read buffer, decoded batches and stored entries are reused, so
    // only the pools filling allocate. Ceilings catch a per-message allocation coming back.
    assert!(
        publish.allocations < 1.0,
        "serial status update: {:.1} allocs",
        publish.allocations
    );
    assert!(
        streamed.allocations < 1.0,
        "serial stream batch: {:.1} allocs",
        streamed.allocations
    );
    assert!(idle.0 <= 10, "serial idle: {} allocs in a second", idle.0);
    assert!(
        idle_wakeups <= 10,
        "serial idle: {idle_wakeups} worker wake-ups in a second"
    );

    drop(provider);
    runtime.block_on(gateway.shutdown());
}

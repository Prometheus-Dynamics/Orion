use super::*;
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

/// A fake `NOTIFY_SOCKET`: a bound datagram socket that collects every notification.
struct FakeNotifySocket {
    socket: UnixDatagram,
    path: Option<PathBuf>,
    env_value: OsString,
}

impl FakeNotifySocket {
    fn bind_path(label: &str) -> Self {
        // Keep the path short: sun_path is limited to 108 bytes.
        let path = PathBuf::from(format!(
            "/tmp/orion-sdn-{}-{label}.sock",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let socket = UnixDatagram::bind(&path).expect("fake notify socket should bind");
        socket
            .set_read_timeout(Some(Duration::from_millis(20)))
            .expect("read timeout should apply");
        Self {
            socket,
            env_value: path.clone().into_os_string(),
            path: Some(path),
        }
    }

    fn notifier(&self) -> Notifier {
        Notifier::connect(&self.env_value).expect("notifier should connect")
    }

    /// Returns every message received so far, waiting up to `wait` for the first one.
    fn drain(&self, wait: Duration) -> Vec<String> {
        let deadline = Instant::now() + wait;
        let mut messages = Vec::new();
        let mut buf = [0_u8; 4096];
        loop {
            match self.socket.recv(&mut buf) {
                Ok(len) => messages.push(String::from_utf8_lossy(&buf[..len]).into_owned()),
                Err(_) if messages.is_empty() && Instant::now() < deadline => {}
                Err(_) => return messages,
            }
        }
    }
}

impl Drop for FakeNotifySocket {
    fn drop(&mut self) {
        if let Some(path) = &self.path {
            let _ = std::fs::remove_file(path);
        }
    }
}

fn pings(messages: &[String]) -> usize {
    messages
        .iter()
        .filter(|message| message.lines().any(|line| line == "WATCHDOG=1"))
        .count()
}

/// A probe whose heartbeat advances on every poke until it is wedged.
#[derive(Default)]
struct FakeLoop {
    heartbeat: AtomicU64,
    wedged: AtomicBool,
    pokes: AtomicU64,
}

impl LivenessProbe for FakeLoop {
    fn heartbeat(&self) -> u64 {
        self.heartbeat.load(Ordering::SeqCst)
    }

    fn poke(&self) {
        self.pokes.fetch_add(1, Ordering::SeqCst);
        if !self.wedged.load(Ordering::SeqCst) {
            self.heartbeat.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[test]
fn notifier_sends_ready_status_and_stopping_to_a_path_socket() {
    let fake = FakeNotifySocket::bind_path("msgs");
    let notifier = fake.notifier();
    notifier.ready("serving ipc=/run/orion/control.sock");
    notifier.status("line one\nline two");
    notifier.stopping("shutting down");

    let messages = fake.drain(Duration::from_secs(1));
    assert_eq!(
        messages,
        vec![
            "READY=1\nSTATUS=serving ipc=/run/orion/control.sock".to_owned(),
            "STATUS=line one line two".to_owned(),
            "STOPPING=1\nSTATUS=shutting down".to_owned(),
        ]
    );
}

#[cfg(target_os = "linux")]
#[test]
fn notifier_supports_abstract_sockets() {
    use std::os::linux::net::SocketAddrExt;
    let name = format!("orion-sdn-abstract-{}", std::process::id());
    let addr = SocketAddr::from_abstract_name(name.as_bytes()).expect("abstract address");
    let socket = UnixDatagram::bind_addr(&addr).expect("abstract socket should bind");
    socket
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("read timeout should apply");

    let notifier =
        Notifier::connect(OsStr::new(&format!("@{name}"))).expect("notifier should connect");
    notifier.notify("READY=1").expect("notify should send");

    let mut buf = [0_u8; 64];
    let len = socket.recv(&mut buf).expect("datagram should arrive");
    assert_eq!(&buf[..len], b"READY=1");
}

#[test]
fn notifier_rejects_unsupported_socket_forms() {
    for value in ["relative/path.sock", "vsock:2:1234"] {
        let err = Notifier::connect(OsStr::new(value)).expect_err("value should be rejected");
        assert_eq!(err.kind(), io::ErrorKind::Unsupported, "{value}");
    }
}

#[test]
fn watchdog_interval_follows_sd_watchdog_enabled_rules() {
    let os = |value: &str| OsString::from(value);
    assert_eq!(watchdog_interval(None, None, 42), Ok(None));
    assert_eq!(
        watchdog_interval(Some(&os("30000000")), None, 42),
        Ok(Some(Duration::from_secs(15)))
    );
    assert_eq!(
        watchdog_interval(Some(&os("30000000")), Some(&os("42")), 42),
        Ok(Some(Duration::from_secs(15)))
    );
    // WATCHDOG_PID names another process (for example a wrapper that forked us).
    assert_eq!(
        watchdog_interval(Some(&os("30000000")), Some(&os("41")), 42),
        Ok(None)
    );
    assert!(watchdog_interval(Some(&os("soon")), None, 42).is_err());
    assert!(watchdog_interval(Some(&os("0")), None, 42).is_err());
    assert!(watchdog_interval(Some(&os("1000")), Some(&os("pid")), 42).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watchdog_pings_at_the_requested_cadence_while_the_loop_progresses() {
    let fake = FakeNotifySocket::bind_path("cadence");
    let probe = Arc::new(FakeLoop::default());
    let interval = Duration::from_millis(50);
    let handle = spawn_watchdog(Arc::new(fake.notifier()), interval, probe.clone());

    tokio::time::sleep(Duration::from_millis(530)).await;
    handle.shutdown().await;

    let messages = fake.drain(Duration::from_secs(1));
    let count = pings(&messages);
    // Ten ticks in 530ms; allow for scheduling jitter on a loaded CI host.
    assert!(
        (6..=11).contains(&count),
        "expected about 10 pings at a 50ms interval, got {count}: {messages:?}"
    );
    assert!(messages.iter().all(|message| message == "WATCHDOG=1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watchdog_stops_pinging_while_the_loop_is_wedged_and_resumes_after() {
    let fake = FakeNotifySocket::bind_path("wedged");
    let probe = Arc::new(FakeLoop::default());
    let interval = Duration::from_millis(40);
    let handle = spawn_watchdog(Arc::new(fake.notifier()), interval, probe.clone());

    tokio::time::sleep(Duration::from_millis(200)).await;
    let healthy = fake.drain(Duration::from_secs(1));
    assert!(
        pings(&healthy) >= 2,
        "healthy loop should be pinged: {healthy:?}"
    );

    probe.wedged.store(true, Ordering::SeqCst);
    // At most one tick may still see the progress made by the last poke.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = fake.drain(Duration::from_millis(1));
    let pokes_before = probe.pokes.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let wedged = fake.drain(Duration::from_millis(1));
    assert_eq!(
        pings(&wedged),
        0,
        "wedged loop must not be pinged: {wedged:?}"
    );
    assert!(
        probe.pokes.load(Ordering::SeqCst) > pokes_before,
        "the watchdog keeps poking a stalled loop"
    );

    probe.wedged.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(200)).await;
    handle.shutdown().await;
    let recovered = fake.drain(Duration::from_secs(1));
    assert!(
        recovered
            .iter()
            .any(|message| message == "WATCHDOG=1\nSTATUS=running"),
        "recovery should resume pings and reset the status: {recovered:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watchdog_tracks_the_node_reconcile_loop() {
    let fake = FakeNotifySocket::bind_path("node");
    let app = NodeApp::builder()
        .config(
            crate::NodeConfig::for_local_node(crate::NodeId::new("node.systemd.watchdog"))
                // A backstop far longer than the watchdog interval: only the watchdog's own
                // pokes keep the idle loop's heartbeat moving.
                .with_runtime_tuning_mut(|tuning| {
                    tuning.with_reconcile_backstop_interval(Duration::from_secs(60))
                }),
        )
        .try_build()
        .expect("node app should build");
    let reconcile_loop = app.spawn_reconcile_loop(Duration::from_millis(5));
    let interval = Duration::from_millis(60);
    let watchdog = spawn_watchdog(Arc::new(fake.notifier()), interval, Arc::new(app.clone()));

    tokio::time::sleep(Duration::from_millis(400)).await;
    let healthy = fake.drain(Duration::from_secs(1));
    assert!(
        pings(&healthy) >= 4,
        "an idle but live reconcile loop should be pinged every interval: {healthy:?}"
    );

    // A loop that is gone stops the heartbeat exactly like a wedged one.
    reconcile_loop.shutdown().await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let _ = fake.drain(Duration::from_millis(1));
    tokio::time::sleep(Duration::from_millis(300)).await;
    let stopped = fake.drain(Duration::from_millis(1));
    watchdog.shutdown().await;
    assert_eq!(
        pings(&stopped),
        0,
        "no pings once the reconcile loop stops: {stopped:?}"
    );
}

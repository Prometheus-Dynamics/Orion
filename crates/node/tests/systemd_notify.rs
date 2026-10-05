//! Runs the `orion-node` binary against a fake systemd `NOTIFY_SOCKET` and checks the
//! notification protocol: `READY=1` only once every listener accepts connections, watchdog pings
//! while running, `STOPPING=1` on shutdown (timer or SIGTERM).
#![cfg(unix)]

use std::os::unix::net::{UnixDatagram, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

struct Harness {
    dir: PathBuf,
    notify: UnixDatagram,
    child: Option<Child>,
}

impl Harness {
    fn new(label: &str) -> Self {
        // Short paths: unix socket paths are limited to 108 bytes.
        let dir = PathBuf::from(format!("/tmp/orion-sdt-{}-{label}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("test dir should be created");
        let notify = UnixDatagram::bind(dir.join("notify")).expect("notify socket should bind");
        notify
            .set_read_timeout(Some(Duration::from_millis(50)))
            .expect("read timeout should apply");
        Self {
            dir,
            notify,
            child: None,
        }
    }

    fn ipc_socket(&self) -> PathBuf {
        self.dir.join("control.sock")
    }

    fn ipc_stream_socket(&self) -> PathBuf {
        self.dir.join("stream.sock")
    }

    fn spawn(&mut self, extra_env: &[(&str, &str)]) {
        let mut command = Command::new(env!("CARGO_BIN_EXE_orion-node"));
        command
            .env_clear()
            .env("ORION_NODE_ID", "node.systemd")
            .env("ORION_NODE_HTTP_ADDR", "off")
            .env("ORION_NODE_STATE_DIR", self.dir.join("state"))
            .env("ORION_NODE_IPC_SOCKET", self.ipc_socket())
            .env("ORION_NODE_IPC_STREAM_SOCKET", self.ipc_stream_socket())
            .env("ORION_NODE_RECONCILE_MS", "10")
            .env("NOTIFY_SOCKET", self.dir.join("notify"))
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        #[cfg(feature = "peer-tcp")]
        command.env("ORION_NODE_PEER_ADDR", "127.0.0.1:0");
        for (key, value) in extra_env {
            command.env(key, value);
        }
        self.child = Some(command.spawn().expect("orion-node should start"));
    }

    fn pid(&self) -> i32 {
        i32::try_from(self.child.as_ref().expect("child").id()).expect("pid fits i32")
    }

    fn recv(&self, timeout: Duration) -> Option<String> {
        let deadline = Instant::now() + timeout;
        let mut buf = [0_u8; 4096];
        while Instant::now() < deadline {
            if let Ok(len) = self.notify.recv(&mut buf) {
                return Some(String::from_utf8_lossy(&buf[..len]).into_owned());
            }
        }
        None
    }

    /// Collects notifications until `STOPPING=1` arrives (inclusive).
    fn recv_until_stopping(&self, timeout: Duration) -> Vec<String> {
        let deadline = Instant::now() + timeout;
        let mut messages = Vec::new();
        while Instant::now() < deadline {
            if let Some(message) = self.recv(Duration::from_millis(100)) {
                let stopping = has_line(&message, "STOPPING=1");
                messages.push(message);
                if stopping {
                    return messages;
                }
            }
        }
        panic!("no STOPPING=1 within {timeout:?}; got {messages:?}");
    }

    fn wait(&mut self, timeout: Duration) -> ExitStatus {
        let child = self.child.as_mut().expect("child");
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = child.try_wait().expect("try_wait") {
                return status;
            }
            assert!(Instant::now() < deadline, "orion-node did not exit");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn has_line(message: &str, line: &str) -> bool {
    message.lines().any(|candidate| candidate == line)
}

fn assert_accepts(path: &Path) {
    UnixStream::connect(path)
        .unwrap_or_else(|err| panic!("{} should accept at READY: {err}", path.display()));
}

/// Waits for `READY=1` and checks that every listener named in it already accepts connections.
fn expect_ready(harness: &Harness) -> String {
    let ready = harness
        .recv(Duration::from_secs(20))
        .expect("orion-node should send READY=1");
    assert!(
        ready.starts_with("READY=1\nSTATUS=serving node=node.systemd "),
        "first notification should be READY=1 with a status: {ready:?}"
    );
    assert_accepts(&harness.ipc_socket());
    assert_accepts(&harness.ipc_stream_socket());
    #[cfg(feature = "peer-tcp")]
    {
        let addr = ready
            .split_whitespace()
            .find_map(|field| field.strip_prefix("peer_tcp=orion+tcp://"))
            .unwrap_or_else(|| panic!("READY status should name the peer listener: {ready:?}"));
        std::net::TcpStream::connect(addr).expect("peer-tcp listener should accept at READY");
    }
    ready
}

#[test]
fn ready_follows_listeners_watchdog_pings_and_stopping_on_exit() {
    let mut harness = Harness::new("ready");
    harness.spawn(&[
        ("ORION_NODE_SHUTDOWN_AFTER_INIT_MS", "1500"),
        ("WATCHDOG_USEC", "200000"),
    ]);
    expect_ready(&harness);

    let messages = harness.recv_until_stopping(Duration::from_secs(20));
    let pings = messages
        .iter()
        .filter(|message| has_line(message, "WATCHDOG=1"))
        .count();
    // 1.5s at a 100ms ping interval; a loaded host may drop a few.
    assert!(
        pings >= 5,
        "expected watchdog pings every 100ms: {messages:?}"
    );
    assert!(
        !messages.iter().any(|message| has_line(message, "READY=1")),
        "READY=1 is sent once"
    );
    assert!(harness.wait(Duration::from_secs(20)).success());
    assert!(
        harness.recv(Duration::from_millis(300)).is_none(),
        "nothing is sent after STOPPING=1"
    );
}

#[test]
fn sigterm_stops_gracefully_and_watchdog_pid_mismatch_disables_pings() {
    let mut harness = Harness::new("sigterm");
    // WATCHDOG_PID names some other process, so this node must not ping.
    harness.spawn(&[("WATCHDOG_USEC", "100000"), ("WATCHDOG_PID", "1")]);
    expect_ready(&harness);
    std::thread::sleep(Duration::from_millis(400));

    // SAFETY: plain kill(2) on the child we spawned and still own.
    let rc = unsafe { libc::kill(harness.pid(), libc::SIGTERM) };
    assert_eq!(rc, 0, "SIGTERM should be delivered");
    let messages = harness.recv_until_stopping(Duration::from_secs(20));
    assert!(
        !messages
            .iter()
            .any(|message| has_line(message, "WATCHDOG=1")),
        "WATCHDOG_PID for another process disables pings: {messages:?}"
    );
    assert!(
        harness.wait(Duration::from_secs(20)).success(),
        "SIGTERM should shut down cleanly"
    );
}

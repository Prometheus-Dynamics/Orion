//! systemd service notifications (`sd_notify`) and the service watchdog, without libsystemd.
//!
//! Enabled by the `systemd-notify` feature (Unix only). Everything here is a no-op unless the
//! service manager set the variables it reads:
//!
//! - `NOTIFY_SOCKET`: the datagram socket that receives `READY=1`, `STATUS=...`, `STOPPING=1` and
//!   `WATCHDOG=1`. A path (`/run/systemd/notify`) or, on Linux, an abstract socket (`@name`).
//! - `WATCHDOG_USEC`: the watchdog timeout (`WatchdogSec=` in the unit). The node pets the
//!   watchdog every `WATCHDOG_USEC / 2`, but only while it is healthy (see [`spawn_watchdog`]).
//! - `WATCHDOG_PID`: when set, the watchdog is only enabled in the process with that PID.
//!
//! The protocol is one datagram per message, newline-separated `KEY=VALUE` assignments, as
//! documented in `sd_notify(3)`. See `docs/node-env.md` ("Running under systemd").

use crate::NodeApp;
use std::ffi::OsStr;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::{SocketAddr, UnixDatagram};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, info, warn};

/// Sends `sd_notify` messages to the service manager's notification socket.
#[derive(Debug)]
pub struct Notifier {
    socket: UnixDatagram,
    addr: SocketAddr,
}

impl Notifier {
    /// Builds a notifier from `NOTIFY_SOCKET`. Returns `None` when the variable is unset or empty
    /// (not started by systemd, or `Type=` is not `notify`), and logs a warning and returns `None`
    /// when it names a socket this implementation cannot use.
    pub fn from_env() -> Option<Self> {
        let value = std::env::var_os("NOTIFY_SOCKET")?;
        if value.is_empty() {
            return None;
        }
        match Self::connect(&value) {
            Ok(notifier) => Some(notifier),
            Err(err) => {
                warn!(notify_socket = ?value, error = %err, "ignoring unusable NOTIFY_SOCKET");
                None
            }
        }
    }

    /// Builds a notifier for a `NOTIFY_SOCKET` value: an absolute path, or `@name` for a Linux
    /// abstract socket. Other forms (for example `vsock:`) are rejected with `Unsupported`.
    pub fn connect(notify_socket: &OsStr) -> io::Result<Self> {
        let bytes = notify_socket.as_bytes();
        let addr = match bytes.first() {
            Some(b'/') => SocketAddr::from_pathname(notify_socket)?,
            Some(b'@') => abstract_addr(&bytes[1..])?,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "NOTIFY_SOCKET must be an absolute path or an @abstract socket name",
                ));
            }
        };
        let socket = UnixDatagram::unbound()?;
        // Never stall a runtime worker on a full notification queue; a dropped message is logged.
        socket.set_nonblocking(true)?;
        Ok(Self { socket, addr })
    }

    /// Sends one notification datagram, for example `"READY=1\nSTATUS=serving"`.
    pub fn notify(&self, state: &str) -> io::Result<()> {
        let sent = self.socket.send_to_addr(state.as_bytes(), &self.addr)?;
        if sent != state.len() {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "short write to NOTIFY_SOCKET",
            ));
        }
        Ok(())
    }

    /// Like [`Notifier::notify`], but logs failures instead of returning them. Notifications are
    /// advisory: a missing message only delays systemd's view of the service.
    pub fn notify_or_log(&self, state: &str) {
        if let Err(err) = self.notify(state) {
            warn!(error = %err, message = state.lines().next().unwrap_or(""), "sd_notify failed");
        }
    }

    /// Reports start-up complete (`READY=1`) together with a status line.
    pub fn ready(&self, status: &str) {
        self.notify_or_log(&format!("READY=1\nSTATUS={}", one_line(status)));
    }

    /// Updates the free-form status shown by `systemctl status`.
    pub fn status(&self, status: &str) {
        self.notify_or_log(&format!("STATUS={}", one_line(status)));
    }

    /// Reports that the service is shutting down (`STOPPING=1`).
    pub fn stopping(&self, status: &str) {
        self.notify_or_log(&format!("STOPPING=1\nSTATUS={}", one_line(status)));
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn abstract_addr(name: &[u8]) -> io::Result<SocketAddr> {
    #[cfg(target_os = "android")]
    use std::os::android::net::SocketAddrExt;
    #[cfg(target_os = "linux")]
    use std::os::linux::net::SocketAddrExt;
    SocketAddr::from_abstract_name(name)
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn abstract_addr(_name: &[u8]) -> io::Result<SocketAddr> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "abstract NOTIFY_SOCKET names are only supported on Linux",
    ))
}

/// Status values are a single line in the protocol; fold any newlines into spaces.
fn one_line(status: &str) -> String {
    status.replace(['\n', '\r'], " ")
}

/// Returns the watchdog ping interval (`WATCHDOG_USEC / 2`) from the environment, or `None`
/// when the watchdog is not enabled for this process. Invalid values are logged and ignored.
pub fn watchdog_interval_from_env() -> Option<Duration> {
    let usec = std::env::var_os("WATCHDOG_USEC");
    let pid = std::env::var_os("WATCHDOG_PID");
    match watchdog_interval(usec.as_deref(), pid.as_deref(), std::process::id()) {
        Ok(interval) => interval,
        Err(err) => {
            warn!(error = %err, "ignoring invalid systemd watchdog settings");
            None
        }
    }
}

/// Computes the watchdog ping interval from `WATCHDOG_USEC` and `WATCHDOG_PID` values, following
/// `sd_watchdog_enabled(3)`: `None` when `WATCHDOG_USEC` is unset, or when `WATCHDOG_PID` is set
/// and is not `own_pid`; an error when either value does not parse or the timeout is zero.
pub fn watchdog_interval(
    watchdog_usec: Option<&OsStr>,
    watchdog_pid: Option<&OsStr>,
    own_pid: u32,
) -> Result<Option<Duration>, String> {
    let Some(usec) = watchdog_usec else {
        return Ok(None);
    };
    let usec =
        parse_u64(usec).ok_or_else(|| format!("WATCHDOG_USEC={usec:?} is not an integer"))?;
    if usec == 0 {
        return Err("WATCHDOG_USEC=0 is not a valid timeout".to_owned());
    }
    if let Some(pid) = watchdog_pid {
        let pid = parse_u64(pid).ok_or_else(|| format!("WATCHDOG_PID={pid:?} is not a PID"))?;
        if pid != u64::from(own_pid) {
            return Ok(None);
        }
    }
    // Ping at half the timeout, as sd_watchdog_enabled(3) recommends, so one late ping is
    // tolerated.
    Ok(Some(
        Duration::from_micros(usec / 2).max(Duration::from_millis(1)),
    ))
}

fn parse_u64(value: &OsStr) -> Option<u64> {
    value.to_str()?.trim().parse().ok()
}

/// What the watchdog checks before each ping.
pub trait LivenessProbe: Send + Sync + 'static {
    /// A counter that advances whenever the supervised work makes progress.
    fn heartbeat(&self) -> u64;
    /// Asks the supervised work to make progress before the next check, so an idle but healthy
    /// loop still advances the heartbeat.
    fn poke(&self);
}

/// The node is live while its background reconcile loop keeps completing passes: the heartbeat
/// is [`NodeApp::reconcile_loop_heartbeat`] and a poke is [`NodeApp::request_reconcile`].
impl LivenessProbe for NodeApp {
    fn heartbeat(&self) -> u64 {
        self.reconcile_loop_heartbeat()
    }

    fn poke(&self) {
        self.request_reconcile();
    }
}

/// Stops the task started by [`spawn_watchdog`].
#[derive(Debug)]
pub struct WatchdogHandle {
    shutdown_tx: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl WatchdogHandle {
    /// Stops pinging and waits for the task to exit.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.task.await;
    }
}

/// Pets the systemd watchdog (`WATCHDOG=1`) every `interval` while `probe` shows progress.
///
/// At every tick the task compares the probe's heartbeat with the value seen at the previous tick
/// (or at spawn). It sends `WATCHDOG=1` only if the heartbeat advanced, then pokes the probe so an
/// idle loop runs a pass before the next tick. When the heartbeat stops advancing (a reconcile pass
/// that never returns, a loop that exited, or a runtime too starved to run this task) the pings
/// stop, the status line says so, and systemd restarts the service once `WatchdogSec=` elapses
/// since the last ping. If progress resumes first, pinging resumes too.
///
/// With `interval = WATCHDOG_USEC / 2` a reconcile pass must therefore finish within roughly one
/// interval of being poked. The poke costs one extra reconcile pass per interval at most.
pub fn spawn_watchdog(
    notifier: Arc<Notifier>,
    interval: Duration,
    probe: Arc<dyn LivenessProbe>,
) -> WatchdogHandle {
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(async move {
        let mut last = probe.heartbeat();
        probe.poke();
        let mut stalled = false;
        let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                    continue;
                }
                _ = ticker.tick() => {}
            }
            let heartbeat = probe.heartbeat();
            if heartbeat != last {
                last = heartbeat;
                if stalled {
                    stalled = false;
                    info!("reconcile loop made progress again; resuming systemd watchdog pings");
                    notifier.notify_or_log("WATCHDOG=1\nSTATUS=running");
                } else {
                    debug!(heartbeat, "systemd watchdog ping");
                    notifier.notify_or_log("WATCHDOG=1");
                }
            } else if !stalled {
                stalled = true;
                warn!(
                    heartbeat,
                    interval_ms = interval.as_millis(),
                    "reconcile loop made no progress since the last watchdog check; withholding systemd watchdog pings"
                );
                notifier.status("reconcile loop stalled; withholding watchdog pings");
            }
            probe.poke();
        }
    });
    WatchdogHandle { shutdown_tx, task }
}

#[cfg(test)]
mod tests;

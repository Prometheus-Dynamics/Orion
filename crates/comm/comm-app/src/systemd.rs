//! Telling systemd the comm core is ready and alive (`sd_notify`), without libsystemd.
//!
//! Nothing here does anything unless the service manager set the variables it reads:
//!
//! - `NOTIFY_SOCKET`: the datagram socket that takes `READY=1`, `STATUS=...`, `STOPPING=1` and
//!   `WATCHDOG=1`. A path, or `@name` for an abstract socket.
//! - `WATCHDOG_USEC`: the watchdog timeout (`WatchdogSec=` in the unit). The comm core says it
//!   is alive every half of it.
//! - `WATCHDOG_PID`: when set, the watchdog is only for the process with that number.

use std::ffi::OsStr;
use std::io;
use std::os::linux::net::SocketAddrExt;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::{SocketAddr, UnixDatagram};
use std::time::Duration;

use tracing::warn;

/// Sends `sd_notify` messages to the service manager.
#[derive(Debug)]
pub struct Notifier {
    socket: UnixDatagram,
    addr: SocketAddr,
}

impl Notifier {
    /// A notifier from `NOTIFY_SOCKET`. `None` when it is unset or empty (not started by
    /// systemd, or the unit is not `Type=notify`), or names a socket that can't be used.
    #[must_use]
    pub fn from_env() -> Option<Self> {
        let value = std::env::var_os("NOTIFY_SOCKET")?;
        if value.is_empty() {
            return None;
        }
        match Self::connect(&value) {
            Ok(notifier) => Some(notifier),
            Err(error) => {
                warn!(notify_socket = ?value, %error, "ignoring NOTIFY_SOCKET: it cannot be used");
                None
            }
        }
    }

    /// A notifier for a `NOTIFY_SOCKET` value: an absolute path, or `@name` for an abstract
    /// socket.
    ///
    /// # Errors
    ///
    /// `Unsupported` for any other form.
    pub fn connect(notify_socket: &OsStr) -> io::Result<Self> {
        let bytes = notify_socket.as_bytes();
        let addr = match bytes.first() {
            Some(b'/') => SocketAddr::from_pathname(notify_socket)?,
            Some(b'@') => SocketAddr::from_abstract_name(&bytes[1..])?,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "NOTIFY_SOCKET must be an absolute path or an @abstract name",
                ));
            }
        };
        let socket = UnixDatagram::unbound()?;
        // A full queue at the service manager must never hold the comm core up.
        socket.set_nonblocking(true)?;
        Ok(Self { socket, addr })
    }

    /// Sends one message. Failures are logged: a missing message only delays what systemd
    /// knows.
    pub fn notify(&self, state: &str) {
        match self.socket.send_to_addr(state.as_bytes(), &self.addr) {
            Ok(sent) if sent == state.len() => {}
            Ok(_) => warn!(
                message = state.lines().next().unwrap_or(""),
                "sd_notify was cut short"
            ),
            Err(error) => {
                warn!(%error, message = state.lines().next().unwrap_or(""), "sd_notify failed")
            }
        }
    }

    /// Start-up is done.
    pub fn ready(&self, status: &str) {
        self.notify(&format!("READY=1\nSTATUS={}", one_line(status)));
    }

    /// Still alive.
    pub fn alive(&self) {
        self.notify("WATCHDOG=1");
    }

    /// Shutting down.
    pub fn stopping(&self, status: &str) {
        self.notify(&format!("STOPPING=1\nSTATUS={}", one_line(status)));
    }
}

/// A status is one line in the protocol.
fn one_line(status: &str) -> String {
    status.replace(['\n', '\r'], " ")
}

/// How often to say "alive", from the environment: half of `WATCHDOG_USEC`. `None` when the
/// watchdog is not on for this process.
#[must_use]
pub fn watchdog_interval_from_env() -> Option<Duration> {
    let usec = std::env::var_os("WATCHDOG_USEC");
    let pid = std::env::var_os("WATCHDOG_PID");
    match watchdog_interval(usec.as_deref(), pid.as_deref(), std::process::id()) {
        Ok(interval) => interval,
        Err(error) => {
            warn!(%error, "ignoring the systemd watchdog settings");
            None
        }
    }
}

/// How often to say "alive", as `sd_watchdog_enabled(3)` has it: `None` when `WATCHDOG_USEC` is
/// unset, or `WATCHDOG_PID` is set and is not `own_pid`.
///
/// # Errors
///
/// A value that is not a number, or a timeout of zero.
pub fn watchdog_interval(
    watchdog_usec: Option<&OsStr>,
    watchdog_pid: Option<&OsStr>,
    own_pid: u32,
) -> Result<Option<Duration>, String> {
    let parse = |value: &OsStr| {
        value
            .to_str()
            .and_then(|text| text.trim().parse::<u64>().ok())
    };
    let Some(usec) = watchdog_usec else {
        return Ok(None);
    };
    let usec = parse(usec).ok_or_else(|| format!("WATCHDOG_USEC={usec:?} is not a number"))?;
    if usec == 0 {
        return Err("WATCHDOG_USEC=0 is not a timeout".to_owned());
    }
    if let Some(pid) = watchdog_pid {
        let pid =
            parse(pid).ok_or_else(|| format!("WATCHDOG_PID={pid:?} is not a process number"))?;
        if pid != u64::from(own_pid) {
            return Ok(None);
        }
    }
    // Half the timeout, so one late message is forgiven.
    Ok(Some(
        Duration::from_micros(usec / 2).max(Duration::from_millis(1)),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_watchdog_is_on_only_when_systemd_says_so_for_this_process() {
        let os = OsStr::new;
        assert_eq!(watchdog_interval(None, None, 7), Ok(None));
        assert_eq!(
            watchdog_interval(Some(os("30000000")), None, 7),
            Ok(Some(Duration::from_secs(15)))
        );
        assert_eq!(
            watchdog_interval(Some(os("30000000")), Some(os("7")), 7),
            Ok(Some(Duration::from_secs(15)))
        );
        assert_eq!(
            watchdog_interval(Some(os("30000000")), Some(os("8")), 7),
            Ok(None)
        );
        assert_eq!(
            watchdog_interval(Some(os("1")), None, 7),
            Ok(Some(Duration::from_millis(1)))
        );
        assert!(watchdog_interval(Some(os("0")), None, 7).is_err());
        assert!(watchdog_interval(Some(os("soon")), None, 7).is_err());
        assert!(watchdog_interval(Some(os("30000000")), Some(os("me")), 7).is_err());
    }

    #[test]
    fn messages_reach_a_path_and_an_abstract_socket_and_nothing_else_is_taken() {
        let dir = std::env::temp_dir().join(format!("orion-notify-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("dir");
        let path = dir.join("notify");
        let manager = UnixDatagram::bind(&path).expect("bind");
        let notifier = Notifier::connect(path.as_os_str()).expect("connect");
        notifier.ready("serving\non two lines");
        notifier.alive();
        notifier.stopping("asked to stop");
        let mut buf = [0_u8; 128];
        let mut next = || {
            let len = manager.recv(&mut buf).expect("recv");
            String::from_utf8(buf[..len].to_vec()).expect("text")
        };
        assert_eq!(next(), "READY=1\nSTATUS=serving on two lines");
        assert_eq!(next(), "WATCHDOG=1");
        assert_eq!(next(), "STOPPING=1\nSTATUS=asked to stop");

        let name = format!("orion-notify-test-{}", std::process::id());
        let addr = SocketAddr::from_abstract_name(name.as_bytes()).expect("addr");
        let manager = UnixDatagram::bind_addr(&addr).expect("bind");
        Notifier::connect(OsStr::new(&format!("@{name}")))
            .expect("connect")
            .alive();
        let len = manager.recv(&mut buf).expect("recv");
        assert_eq!(&buf[..len], b"WATCHDOG=1");

        assert_eq!(
            Notifier::connect(OsStr::new("vsock:1:2"))
                .expect_err("refused")
                .kind(),
            io::ErrorKind::Unsupported
        );
        // A manager that is gone is not the comm core's problem.
        drop(manager);
        Notifier::connect(OsStr::new(&format!("@{name}")))
            .expect("connect")
            .alive();
        let _ = std::fs::remove_dir_all(dir);
    }
}

//! A board for a drill: a directory, settings, and the programs as processes.

#![allow(dead_code)] // Each drill uses part of it.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream};
use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use orion_scratch::Scratch;

pub const DRILL: &str = env!("CARGO_BIN_EXE_orion-drill");

/// A board's directory and settings. The programs are started from it.
pub struct Board {
    /// Removed when the board is dropped.
    pub dir: Scratch,
    pub socket: PathBuf,
    pub settings: PathBuf,
    pub ssh: SocketAddr,
    pub peer: SocketAddr,
    /// The two ports, held for as long as the board is.
    _held: [OwnedFd; 2],
}

/// A port held for the board: bound and not listening, for as long as the socket lives. A port
/// merely found free can be handed to another socket before the board's program listens there.
/// Bound with SO_REUSEADDR, as the comm core binds its listeners, the hold does not stop the
/// program listening there.
pub fn held_port() -> (OwnedFd, SocketAddr) {
    use rustix::net::{AddressFamily, SocketType, bind, getsockname, socket, sockopt};
    let held = socket(AddressFamily::INET, SocketType::STREAM, None).expect("socket");
    sockopt::set_socket_reuseaddr(&held, true).expect("reuse");
    bind(&held, &SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).expect("bind");
    let addr = SocketAddrV4::try_from(getsockname(&held).expect("addr")).expect("an address");
    (held, SocketAddr::V4(addr))
}

impl Board {
    /// A board named `drill` in a directory of its own. `extra` is added to its settings.
    pub fn new(name: &str, extra: &str) -> Self {
        Self::named(name, "drill", extra)
    }

    /// A board called `node`.
    pub fn named(name: &str, node: &str, extra: &str) -> Self {
        let dir = Scratch::new(&format!("drill-{name}"));
        let socket = dir.join("orion.sock");
        let ((ssh_held, ssh), (peer_held, peer)) = (held_port(), held_port());
        let uid = std::fs::metadata(&dir)
            .map(|meta| std::os::unix::fs::MetadataExt::uid(&meta))
            .expect("uid");
        let settings = dir.join("orion.conf");
        std::fs::write(
            &settings,
            format!(
                "node.name = {node}\nsocket = {}\nstate.dir = {}\nsocket.node_uids = {uid}\n\
                 ssh.listen = {ssh}\npeer.listen = {peer}\nrecords.save_after_ms = 10\n\
                 workloads.stop_grace_ms = 500\n{extra}",
                socket.display(),
                dir.join("comm").display(),
            ),
        )
        .expect("settings");
        Self {
            dir,
            socket,
            settings,
            ssh,
            peer,
            _held: [ssh_held, peer_held],
        }
    }

    /// The comm core, as `orion comm` runs it.
    pub fn comm(&self) -> Proc {
        Proc::start("comm", self.command(&["comm", path(&self.settings)]))
    }

    /// Node logic, as `orion node` runs it, with a state directory of its own.
    pub fn node(&self) -> Proc {
        let mut command = self.command(&["node", path(&self.settings)]);
        command.env("ORION_STATE_DIR", self.dir.join("node"));
        Proc::start("node", command)
    }

    /// A drill role.
    pub fn role(&self, role: &str, more: &[&str]) -> Proc {
        let mut args = vec![role, path(&self.socket)];
        args.extend_from_slice(more);
        Proc::start(role, self.command(&args))
    }

    /// The command line, run to its end: whether it worked, and what it printed.
    pub fn ctl(&self, args: &[&str]) -> (bool, String) {
        let mut all = vec!["--address", path(&self.socket), "--timeout", "2000"];
        all.extend_from_slice(args);
        let out = self.command(&all).output().expect("ctl");
        let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
        text.push_str(&String::from_utf8_lossy(&out.stderr));
        (out.status.success(), text)
    }

    /// Waits up to `limit` for the command line to work, and returns how long it took.
    pub fn ctl_until(&self, args: &[&str], limit: Duration) -> Duration {
        let started = Instant::now();
        loop {
            let (ok, text) = self.ctl(args);
            if ok {
                return started.elapsed();
            }
            assert!(started.elapsed() < limit, "{args:?} still fails: {text}");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(DRILL);
        command
            .args(args)
            .env(
                "ORION_LOG",
                std::env::var("DRILL_LOG").unwrap_or_else(|_| "warn".into()),
            )
            .stdin(Stdio::null());
        command
    }

    /// The SSH server's first line, if it answers.
    pub fn ssh_banner(&self) -> Option<String> {
        let mut stream = TcpStream::connect_timeout(&self.ssh, Duration::from_secs(1)).ok()?;
        stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
        let mut line = [0_u8; 64];
        let read = stream.read(&mut line).ok()?;
        let _ = stream.write_all(b"");
        Some(String::from_utf8_lossy(&line[..read]).trim().to_owned())
    }
}

fn path(path: &Path) -> &str {
    path.to_str().expect("a path in UTF-8")
}

/// A program of the drill, with what it printed. Killed when dropped.
pub struct Proc {
    pub name: String,
    child: Child,
    lines: Arc<Mutex<Vec<String>>>,
}

impl Proc {
    fn start(name: &str, mut command: Command) -> Self {
        let mut child = command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start");
        let lines = Arc::new(Mutex::new(Vec::new()));
        for stream in [
            Box::new(child.stdout.take().expect("stdout")) as Box<dyn Read + Send>,
            Box::new(child.stderr.take().expect("stderr")),
        ] {
            let (lines, name) = (lines.clone(), name.to_owned());
            std::thread::spawn(move || {
                for line in BufReader::new(stream).lines().map_while(Result::ok) {
                    if std::env::var_os("DRILL_VERBOSE").is_some() {
                        eprintln!("[{name}] {line}");
                    }
                    lines.lock().expect("lines").push(line);
                }
            });
        }
        Self {
            name: name.to_owned(),
            child,
            lines,
        }
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// `SIGKILL`, and waits for it to be gone.
    pub fn kill(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    pub fn lines(&self) -> Vec<String> {
        self.lines.lock().expect("lines").clone()
    }

    /// Every line that starts with `word `.
    pub fn said(&self, word: &str) -> Vec<String> {
        let prefix = format!("{word} ");
        self.lines()
            .into_iter()
            .filter(|line| line.starts_with(&prefix) || line == word)
            .collect()
    }

    /// The number after the last `word N` line, or 0.
    pub fn last(&self, word: &str) -> u64 {
        self.said(word)
            .last()
            .and_then(|line| line.rsplit(' ').next()?.parse().ok())
            .unwrap_or(0)
    }

    /// Waits up to `limit` for `word N` with N at least `at_least`.
    pub fn wait_for(&self, word: &str, at_least: u64, limit: Duration) -> u64 {
        let started = Instant::now();
        loop {
            let last = self.last(word);
            if last >= at_least {
                return last;
            }
            assert!(
                started.elapsed() < limit,
                "{}: no `{word} {at_least}` in {limit:?}; it said {:?}",
                self.name,
                tail(&self.lines())
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Waits up to `limit` for a line that is `word`, or starts with it.
    pub fn wait_line(&self, word: &str, limit: Duration) -> Duration {
        let started = Instant::now();
        loop {
            if !self.said(word).is_empty() {
                return started.elapsed();
            }
            assert!(
                started.elapsed() < limit,
                "{}: no `{word}` in {limit:?}; it said {:?}",
                self.name,
                tail(&self.lines())
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// How many times it has said `word`.
    pub fn count(&self, word: &str) -> usize {
        self.said(word).len()
    }

    /// Whether it is still running.
    pub fn running(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    /// Whether it exits within `within`. A process's sockets close as it exits, a moment
    /// before it can be waited for, so what it was connected to can hear it go first.
    pub fn exits_within(&mut self, within: Duration) -> bool {
        let until = Instant::now() + within;
        while self.running() {
            if Instant::now() >= until {
                return false;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        true
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn tail(lines: &[String]) -> &[String] {
    &lines[lines.len().saturating_sub(12)..]
}

/// Whether a process with this number is alive.
pub fn alive(pid: u32) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .map(|stat| !stat.contains(") Z "))
        .unwrap_or(false)
}

/// Asserts `proc` saw no hole: no gap in counts, no torn frame, nothing it failed on.
pub fn clean(proc: &Proc) {
    for word in ["gap", "torn", "failed", "skipped"] {
        let said = proc.said(word);
        assert!(said.is_empty(), "{}: {said:?}", proc.name);
    }
}

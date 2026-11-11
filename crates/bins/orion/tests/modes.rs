//! The binary run as a board runs it: the comm core and node logic, each a run of the same
//! file from the same settings, and the command line reaching them. The two services run
//! their code from the same file.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

const BINARY: &str = env!("CARGO_BIN_EXE_orion");
const WAIT: Duration = Duration::from_secs(20);

fn temp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("orion-bin-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    dir
}

/// Who runs the tests: node logic must be let in as this board's.
fn uid() -> String {
    let out = Command::new("id").arg("-u").output().expect("id");
    String::from_utf8_lossy(&out.stdout).trim().to_owned()
}

/// A service, stopped with `SIGTERM` when dropped.
struct Service(Child);

impl Service {
    fn start(mode: &str, settings: &Path) -> Self {
        let child = Command::new(BINARY)
            .arg(mode)
            .arg(settings)
            .env("ORION_LOG", "warn")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("the service starts");
        Self(child)
    }

    fn pid(&self) -> u32 {
        self.0.id()
    }

    /// Asks it to stop and says whether it ended well.
    fn stop(mut self) -> bool {
        let stopped = Command::new("kill")
            .args(["-TERM", &self.pid().to_string()])
            .status()
            .is_ok_and(|status| status.success());
        let deadline = Instant::now() + WAIT;
        while stopped && Instant::now() < deadline {
            if let Ok(Some(status)) = self.0.try_wait() {
                return status.success();
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        false
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn run(program: &Path, args: &[&str]) -> Output {
    Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .expect("the command line runs")
}

fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// The file the binary's code is mapped from in `pid`: its device and inode.
fn code(pid: u32) -> String {
    let maps = std::fs::read_to_string(format!("/proc/{pid}/maps")).expect("maps");
    maps.lines()
        .map(|line| line.split_whitespace().collect::<Vec<_>>())
        .find(|fields| {
            fields.len() >= 6
                && fields[1] == "r-xp"
                && Path::new(fields[5]).file_name() == Path::new(BINARY).file_name()
        })
        .map(|fields| format!("{} {}", fields[3], fields[4]))
        .unwrap_or_else(|| panic!("no code mapping of the binary in {pid}"))
}

#[test]
fn both_services_from_one_file_and_the_command_line_reaches_them() {
    let dir = temp("modes");
    let socket = dir.join("orion.sock");
    let settings = dir.join("orion.conf");
    std::fs::write(
        &settings,
        format!(
            "node.name = board\nsocket = {}\nsocket.node_uids = {}\nstate.dir = {}\n",
            socket.display(),
            uid(),
            dir.join("state").display()
        ),
    )
    .expect("settings");

    let comm = Service::start("comm", &settings);
    until("the comm core's socket", || socket.exists());
    let node = Service::start("node", &settings);

    let address = socket.to_str().expect("path");
    let binary = Path::new(BINARY);
    let mut shown = None;
    until("node logic to answer", || {
        let out = run(binary, &["--address", address, "node", "show"]);
        let ok = out.status.success();
        shown = Some(out);
        ok
    });
    let shown = shown.expect("ran");
    let text = String::from_utf8_lossy(&shown.stdout);
    assert!(text.contains("board"), "{text}");

    // The same command line by its other names.
    // Node logic keeps the records: the list is there once it is connected.
    until("node logic's records", || {
        run(binary, &["ctl", "--address", address, "resource", "list"])
            .status
            .success()
    });
    let link = dir.join("orionctl");
    std::os::unix::fs::symlink(binary, &link).expect("link");
    let linked = run(&link, &["--address", address, "node", "show"]);
    assert!(linked.status.success());
    assert!(String::from_utf8_lossy(&linked.stdout).contains("board"));

    // Both run their code from the same file, so its pages are in the page cache once for
    // the two. (How `smaps` counts a page as shared depends on the kernel's folios, so the
    // file is what is checked.)
    assert_eq!(code(comm.pid()), code(node.pid()), "both map the same file");

    assert!(node.stop(), "node logic stops cleanly");
    assert!(comm.stop(), "the comm core stops cleanly");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn the_version_and_a_bad_service_line_are_answered() {
    let binary = Path::new(BINARY);
    let version = run(binary, &["--version"]);
    assert!(version.status.success());
    assert!(String::from_utf8_lossy(&version.stdout).starts_with("orion "));
    let bad = run(binary, &["node", "a.conf", "b.conf"]);
    assert_eq!(bad.status.code(), Some(2));
    let help = run(binary, &["help"]);
    assert!(String::from_utf8_lossy(&help.stdout).contains("topic"));
}

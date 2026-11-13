//! The binary itself, run as a board would run it: it is the comm core and its own
//! helpers, it is reached with `ssh` and `scp`, and with a broken settings file it stays
//! up in recovery. If OpenSSH is not installed the tests that need it say so and pass.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::time::Duration;

const BINARY: &str = env!("CARGO_BIN_EXE_orion-recovery");
const WAIT: Duration = Duration::from_secs(20);

fn installed() -> bool {
    let there = |program: &str| {
        Command::new(program)
            .arg("-V")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok()
    };
    let all = there("ssh") && there("scp") && there("ssh-keygen");
    if !all {
        println!("OpenSSH is not installed: the binary was not tried with it");
    }
    all
}

fn temp(name: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("orion-recovery-bin-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    dir
}

fn me() -> String {
    let uid = Command::new("id").arg("-u").output().expect("id");
    let uid = String::from_utf8_lossy(&uid.stdout).trim().to_owned();
    std::fs::read_to_string("/etc/passwd")
        .expect("passwd")
        .lines()
        .find(|line| line.split(':').nth(2) == Some(uid.as_str()))
        .and_then(|line| line.split(':').next())
        .expect("the user running the tests has an account")
        .to_owned()
}

/// The binary running, with what it logs read for the port it says SSH is open on.
struct Running {
    child: Child,
    port: u16,
    log: mpsc::Receiver<String>,
    /// Every line it has logged so far.
    seen: std::cell::RefCell<Vec<String>>,
}

impl Running {
    fn start(settings: &Path, env: &[(&str, String)]) -> Self {
        let mut command = Command::new(BINARY);
        command
            .arg(settings)
            .stderr(Stdio::piped())
            .stdout(Stdio::null());
        // Nothing of the test runner's own is taken for a setting.
        for (name, _) in std::env::vars().filter(|(name, _)| name.starts_with("ORION_")) {
            command.env_remove(name);
        }
        for (name, value) in env {
            command.env(name, value);
        }
        let mut child = command.spawn().expect("the binary starts");
        let stderr = BufReader::new(child.stderr.take().expect("stderr"));
        let (lines, log) = mpsc::channel();
        std::thread::spawn(move || {
            for line in stderr.lines().map_while(Result::ok) {
                let _ = lines.send(line);
            }
        });
        let mut running = Self {
            child,
            port: 0,
            log,
            seen: std::cell::RefCell::default(),
        };
        // "ssh: open on 127.0.0.1:PORT" or "recovery: SSH is open on 127.0.0.1:PORT".
        let line = running.logged("open on 127.0.0.1:");
        running.port = line
            .rsplit(':')
            .next()
            .and_then(|port| port.trim().parse().ok())
            .unwrap_or_else(|| panic!("no port in {line:?}"));
        running
    }

    /// The next line it logs that has `what` in it.
    fn logged(&self, what: &str) -> String {
        let began = std::time::Instant::now();
        loop {
            let left = WAIT.saturating_sub(began.elapsed());
            match self.log.recv_timeout(left) {
                Ok(line) => {
                    self.seen.borrow_mut().push(line.clone());
                    if line.contains(what) {
                        return line;
                    }
                }
                Err(_) => panic!("it never logged {what:?}"),
            }
        }
    }

    fn options(&self, command: &mut Command, dir: &Path, port_flag: &str, key: Option<&Path>) {
        command
            .args(["-F", "/dev/null", port_flag, &self.port.to_string()])
            .args(["-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes"])
            .args(["-o", "ConnectTimeout=10", "-o", "LogLevel=ERROR"])
            .args(["-o", "IdentitiesOnly=yes", "-o", "IdentityAgent=none"])
            .arg("-o")
            .arg(format!(
                "UserKnownHostsFile={}",
                dir.join("known_hosts").display()
            ))
            .env_remove("SSH_AUTH_SOCK")
            .stdin(Stdio::null());
        match key {
            Some(key) => command.arg("-i").arg(key),
            None => command.args(["-o", "PubkeyAuthentication=no"]),
        };
    }

    fn ssh(&self, dir: &Path, key: Option<&Path>, run: &str) -> Output {
        let mut command = Command::new("ssh");
        self.options(&mut command, dir, "-p", key);
        command
            .arg(format!("{}@127.0.0.1", me()))
            .arg(run)
            .output()
            .expect("ssh")
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        // Asked to stop, as a service manager asks.
        let _ = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status();
        let _ = self.child.wait();
    }
}

fn said(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

#[test]
fn it_says_what_it_is_and_is_its_own_helpers() {
    let version = Command::new(BINARY).arg("--version").output().expect("run");
    assert!(
        said(&version).starts_with("orion-recovery "),
        "{}",
        said(&version)
    );
    // A helper asked for with nothing to do says why and does not go on to be a comm core.
    let helper = Command::new(BINARY)
        .arg("ssh-session")
        .output()
        .expect("run");
    assert_eq!(helper.status.code(), Some(126));
    assert!(String::from_utf8_lossy(&helper.stderr).contains("--uid is missing"));
}

#[test]
fn the_binary_is_a_comm_core_reached_with_ssh_and_scp() {
    if !installed() {
        return;
    }
    let dir = temp("runs");
    let settings = dir.join("orion.conf");
    std::fs::write(
        &settings,
        format!(
            "node.name = board\nsocket = {0}/orion.sock\nstate.dir = {0}/state\nssh.listen = 127.0.0.1:0\n",
            dir.display()
        ),
    )
    .expect("settings");
    let running = Running::start(&settings, &[]);
    // No helper is named: the binary starts itself as the child that reads the
    // connection, as the session and as the sftp server.
    assert_eq!(said(&running.ssh(&dir, None, "echo in")), "in\n");
    std::fs::write(dir.join("up.txt"), "from the desk\n").expect("write");
    let mut scp = Command::new("scp");
    running.options(&mut scp, &dir, "-P", None);
    let copied = scp
        .arg(dir.join("up.txt"))
        .arg(format!("{}@127.0.0.1:{}/arrived.txt", me(), dir.display()))
        .output()
        .expect("scp");
    assert!(
        copied.status.success(),
        "{}",
        String::from_utf8_lossy(&copied.stderr)
    );
    assert_eq!(
        std::fs::read_to_string(dir.join("arrived.txt")).expect("arrived"),
        "from the desk\n"
    );
    assert!(dir.join("orion.sock").exists());
    let port = running.port;
    drop(running);
    // Stopped when asked: nothing answers there any more.
    assert!(std::net::TcpStream::connect(("127.0.0.1", port)).is_err());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn with_a_broken_settings_file_it_stays_up_in_recovery() {
    if !installed() {
        return;
    }
    let dir = temp("broken");
    let key = dir.join("repairer");
    let made = Command::new("ssh-keygen")
        .args(["-q", "-t", "ed25519", "-N", "", "-f"])
        .arg(&key)
        .status()
        .expect("ssh-keygen");
    assert!(made.success());
    std::fs::copy(key.with_extension("pub"), dir.join("recovery_keys")).expect("keys");
    let settings = dir.join("orion.conf");
    std::fs::write(&settings, "this is not a settings file\n").expect("settings");
    let running = Running::start(
        &settings,
        &[
            ("ORION_RECOVERY_LISTEN", "127.0.0.1:0".to_owned()),
            (
                "ORION_RECOVERY_KEYS",
                dir.join("recovery_keys").display().to_string(),
            ),
        ],
    );
    // It did not end; who the recovery keys name gets a shell, and nobody else does.
    assert_eq!(said(&running.ssh(&dir, Some(&key), "echo in")), "in\n");
    assert_eq!(said(&running.ssh(&dir, None, "echo in")), "");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn the_flasher_example_is_read_whole_and_starts() {
    if !installed() {
        return;
    }
    let dir = temp("flasher");
    let example = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../packaging/busybox/orion-recovery.conf.example"
    );
    // As shipped, but for where it keeps things and listens: those come from the environment.
    let running = Running::start(
        Path::new(example),
        &[
            ("ORION_SOCKET", dir.join("orion.sock").display().to_string()),
            ("ORION_STATE_DIR", dir.join("state").display().to_string()),
            ("ORION_SSH_LISTEN", "127.0.0.1:0".to_owned()),
        ],
    );
    let seen = running.seen.borrow().join("\n");
    assert!(!seen.contains("unknown"), "{seen}");
    assert!(
        !seen.contains("recovery:"),
        "it started as the settings say: {seen}"
    );
    // Security off, as the example says: in without a key.
    assert_eq!(said(&running.ssh(&dir, None, "echo in")), "in\n");
    drop(running);
    let _ = std::fs::remove_dir_all(dir);
}

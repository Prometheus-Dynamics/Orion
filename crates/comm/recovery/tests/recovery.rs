//! The recovery way in with OpenSSH's `ssh` as the client: each broken state still lets
//! you in, only who the recovery keys say, and it never slows down trying. If `ssh` is not
//! installed the tests say so and pass.

use std::io::Write;
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use orion_check::Security;
use orion_recovery::{Recovery, RecoveryConfig};
use orion_ssh::{Helper, users};

const WAIT: Duration = Duration::from_secs(15);

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
    let both = there("ssh") && there("ssh-keygen");
    if !both {
        println!("OpenSSH is not installed: the recovery way in was not tried with it");
    }
    both
}

fn temp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("orion-recovery-{name}-{}", std::process::id()));
    if dir.exists() {
        // What an earlier run made unwritable.
        let _ = Command::new("chmod")
            .args(["-R", "u+rwx"])
            .arg(&dir)
            .status();
        let _ = std::fs::remove_dir_all(&dir);
    }
    std::fs::create_dir_all(&dir).expect("dir");
    dir
}

fn me() -> users::User {
    users::lookup_uid(Path::new("/etc"), rustix::process::getuid().as_raw())
        .expect("the user running the tests has an account")
}

/// A key pair: the private key's file, and its public key as a line of a keys file.
fn keygen(dir: &Path, name: &str) -> (PathBuf, String) {
    let path = dir.join(name);
    let made = Command::new("ssh-keygen")
        .args(["-q", "-t", "ed25519", "-N", "", "-C", name, "-f"])
        .arg(&path)
        .status()
        .expect("ssh-keygen");
    assert!(made.success());
    (
        path.clone(),
        std::fs::read_to_string(path.with_extension("pub")).expect("public key"),
    )
}

fn config(dir: &Path, why: &str) -> RecoveryConfig {
    RecoveryConfig {
        listen: vec!["127.0.0.1:0".parse().expect("addr")],
        keys_file: dir.join("recovery_keys"),
        host_key: dir.join("state/ssh_host_ed25519_key"),
        helper: Helper {
            program: env!("CARGO_BIN_EXE_orion-recovery-helper").into(),
            args: Vec::new(),
        },
        subsystems: Vec::new(),
        etc: "/etc".into(),
        security: Security::On,
        retry: Duration::from_millis(100),
        why: why.to_owned(),
    }
}

/// The recovery way in, in a thread of its own, until dropped.
struct Board {
    recovery: Arc<Mutex<Option<Recovery>>>,
    dir: PathBuf,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Board {
    fn start(dir: &Path, config: RecoveryConfig) -> Self {
        let recovery = Arc::new(Mutex::new(None));
        let (held, ready) = (recovery.clone(), std::sync::mpsc::channel());
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            runtime.block_on(async move {
                *held.lock().expect("lock") = Some(Recovery::start(config));
                ready.0.send(()).expect("ready");
                let _ = stopped.await;
                held.lock().expect("lock").take();
            });
        });
        ready.1.recv().expect("started");
        Self {
            recovery,
            dir: dir.to_owned(),
            stop: Some(stop),
            thread: Some(thread),
        }
    }

    fn addr(&self) -> Option<SocketAddr> {
        self.recovery
            .lock()
            .expect("lock")
            .as_ref()
            .and_then(Recovery::local_addr)
    }

    fn attempts(&self) -> u64 {
        self.recovery
            .lock()
            .expect("lock")
            .as_ref()
            .map_or(0, Recovery::attempts)
    }

    /// Waits until it listens somewhere.
    fn open(&self) -> SocketAddr {
        let began = Instant::now();
        loop {
            if let Some(addr) = self.addr() {
                return addr;
            }
            assert!(began.elapsed() < WAIT, "the recovery way in never opened");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn ssh(&self, user: &str, key: Option<&Path>, command: &str) -> Output {
        let mut ssh = Command::new("ssh");
        ssh.args(["-F", "/dev/null", "-p", &self.open().port().to_string()])
            .args(["-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes"])
            .args(["-o", "ConnectTimeout=10", "-o", "LogLevel=ERROR"])
            .args(["-o", "IdentitiesOnly=yes", "-o", "IdentityAgent=none"])
            .arg("-o")
            .arg(format!(
                "UserKnownHostsFile={}",
                self.dir.join("known_hosts").display()
            ));
        match key {
            Some(key) => ssh.arg("-i").arg(key),
            None => ssh.args(["-o", "PubkeyAuthentication=no"]),
        };
        let mut child = ssh
            .arg(format!("{user}@127.0.0.1"))
            .arg(command)
            .env_remove("SSH_AUTH_SOCK")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("ssh");
        child
            .stdin
            .take()
            .expect("stdin")
            .write_all(b"")
            .expect("write");
        child.wait_with_output().expect("ssh ends")
    }

    /// Whether `key` gets a shell as the user running the tests.
    fn lets_in(&self, key: Option<&Path>) -> bool {
        let ran = self.ssh(&me().name, key, "echo in");
        String::from_utf8_lossy(&ran.stdout) == "in\n"
    }
}

impl Drop for Board {
    fn drop(&mut self) {
        drop(self.stop.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[test]
fn each_broken_state_still_lets_you_in() {
    if !installed() {
        return;
    }
    let dir = temp("broken");
    let (repairer, public) = keygen(&dir, "repairer");
    std::fs::write(dir.join("recovery_keys"), &public).expect("keys");

    // A disk nothing can be written to: the state directory is there and takes no file.
    std::fs::create_dir(dir.join("state")).expect("dir");
    std::fs::set_permissions(dir.join("state"), std::fs::Permissions::from_mode(0o555))
        .expect("mode");
    let board = Board::start(&dir, config(&dir, "the disk is full"));
    assert!(board.lets_in(Some(&repairer)));
    assert!(
        std::fs::read_dir(dir.join("state"))
            .expect("dir")
            .next()
            .is_none()
    );
    drop(board);
    std::fs::set_permissions(dir.join("state"), std::fs::Permissions::from_mode(0o755))
        .expect("mode");

    // A state directory that is not a directory at all.
    std::fs::remove_dir(dir.join("state")).expect("remove");
    std::fs::write(dir.join("state"), "this was a directory once").expect("write");
    let board = Board::start(&dir, config(&dir, "the state directory is broken"));
    assert!(board.lets_in(Some(&repairer)));
    drop(board);
    std::fs::remove_file(dir.join("state")).expect("remove");

    // A host key that is rubbish.
    std::fs::create_dir(dir.join("state")).expect("dir");
    std::fs::write(dir.join("state/ssh_host_ed25519_key"), [0xFF_u8; 300]).expect("write");
    let board = Board::start(&dir, config(&dir, "the host key is broken"));
    assert!(board.lets_in(Some(&repairer)));
    drop(board);

    // An account whose shell is gone, on a board whose account files are elsewhere: the
    // session gets the plain shell.
    let me = me();
    std::fs::create_dir(dir.join("etc")).expect("dir");
    std::fs::write(
        dir.join("etc/passwd"),
        format!(
            "{}:x:{}:{}::/no/such/home:/no/such/shell\n",
            me.name, me.uid, me.gid
        ),
    )
    .expect("passwd");
    let mut broken = config(&dir, "the shell is gone");
    broken.etc = dir.join("etc");
    let board = Board::start(&dir, broken);
    let ran = board.ssh(&me.name, Some(&repairer), "echo \"in $0 at $PWD\"");
    assert_eq!(String::from_utf8_lossy(&ran.stdout), "in /bin/sh at /\n");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn only_who_the_recovery_keys_say_and_never_wider() {
    if !installed() {
        return;
    }
    let dir = temp("who");
    let (repairer, public) = keygen(&dir, "repairer");
    let (stranger, strangers) = keygen(&dir, "stranger");

    // No keys file: with security on, nobody. Not the stranger, not someone with no key.
    let board = Board::start(&dir, config(&dir, "the config is broken"));
    assert!(!board.lets_in(Some(&repairer)));
    assert!(!board.lets_in(None));
    // The file appears, as someone at the console would write it: the repairer is let
    // in without a restart, and the stranger still is not.
    std::fs::write(dir.join("recovery_keys"), &public).expect("keys");
    let began = Instant::now();
    while !board.lets_in(Some(&repairer)) {
        assert!(began.elapsed() < WAIT, "the keys file was never read again");
    }
    assert!(!board.lets_in(Some(&stranger)));
    assert!(!board.lets_in(None));
    // A line with options in front of the key is not understood, so it lets nobody in.
    std::fs::write(
        dir.join("recovery_keys"),
        format!("{public}restrict {strangers}"),
    )
    .expect("keys");
    std::thread::sleep(Duration::from_millis(600));
    assert!(!board.lets_in(Some(&stranger)));
    assert!(board.lets_in(Some(&repairer)));
    // Taken out of the file, the repairer is out too.
    std::fs::write(dir.join("recovery_keys"), "# nobody\n").expect("keys");
    let began = Instant::now();
    while board.lets_in(Some(&repairer)) {
        assert!(
            began.elapsed() < WAIT,
            "a key taken out of the file still got in"
        );
    }
    drop(board);

    // A board whose settings were read and say security is off is as open as it always is.
    let mut open = config(&dir, "the trust store is broken");
    open.security = Security::Off;
    let board = Board::start(&dir, open);
    assert!(board.lets_in(Some(&stranger)));
    assert!(board.lets_in(None));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn it_tries_again_at_the_same_pace_for_ever() {
    if !installed() {
        return;
    }
    let dir = temp("pace");
    let (repairer, public) = keygen(&dir, "repairer");
    std::fs::write(dir.join("recovery_keys"), &public).expect("keys");

    // Both addresses it is told are taken.
    let taken: Vec<std::net::TcpListener> = (0..2)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").expect("listen"))
        .collect();
    let mut told = config(&dir, "the port is taken");
    told.listen = taken
        .iter()
        .map(|listener| listener.local_addr().expect("addr"))
        .collect();
    let second = told.listen[1];
    let board = Board::start(&dir, told);

    // It goes on trying, as often in the fourth second as in the first.
    let mut counts = Vec::new();
    for _ in 0..4 {
        std::thread::sleep(Duration::from_secs(1));
        counts.push(board.attempts());
    }
    assert_eq!(board.addr(), None);
    let paces: Vec<u64> = counts.windows(2).map(|pair| pair[1] - pair[0]).collect();
    assert!(counts[0] >= 6, "{counts:?}");
    assert!(
        paces.iter().all(|pace| (6..=11).contains(pace)),
        "{counts:?}"
    );

    // The second address comes free: it is open there within a moment, and lets in.
    let mut taken = taken;
    drop(taken.pop());
    let began = Instant::now();
    assert_eq!(board.open(), second);
    assert!(
        began.elapsed() < Duration::from_secs(2),
        "{:?}",
        began.elapsed()
    );
    assert!(board.lets_in(Some(&repairer)));
    let _ = std::fs::remove_dir_all(dir);
}

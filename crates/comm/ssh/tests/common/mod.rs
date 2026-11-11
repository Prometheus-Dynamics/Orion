//! What the tests with OpenSSH share: keys, a trust store, a server, and `ssh` run against it.

#![allow(dead_code)]

use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::Duration;

use orion_check::Security;
use orion_keys::{Keys, KeysConfig, SharedKeys};
use orion_ssh::{Helper, Ssh, SshConfig, users};
use orion_types::KeyId;

pub fn installed() -> bool {
    let there = |program: &str| {
        Command::new(program)
            .arg("-V")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok()
    };
    let both = there("ssh") && Path::new("/usr/bin/ssh-keygen").exists() || there("ssh-keygen");
    if !both {
        println!("OpenSSH is not installed: the SSH server was not tried with it");
    }
    both
}

pub fn temp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("orion-ssh-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    dir
}

/// A key pair made by `ssh-keygen`: the private key's file and the public key's bytes.
pub fn keygen(dir: &Path, name: &str) -> (PathBuf, [u8; 32]) {
    let path = dir.join(name);
    let made = Command::new("ssh-keygen")
        .args(["-q", "-t", "ed25519", "-N", "", "-C", name, "-f"])
        .arg(&path)
        .status()
        .expect("ssh-keygen");
    assert!(made.success());
    let public = std::fs::read_to_string(path.with_extension("pub")).expect("public key");
    let public = russh::keys::PublicKey::from_openssh(&public).expect("a public key");
    (path, public.key_data().ed25519().expect("ed25519").0)
}

/// The account the tests run as: the only one a server that is not root can let in.
pub fn me() -> users::User {
    users::lookup_uid(Path::new("/etc"), rustix::process::getuid().as_raw())
        .expect("the user running the tests has an account")
}

/// An SSH server in a thread of its own, until dropped.
pub struct Server {
    pub addr: SocketAddr,
    pub dir: PathBuf,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Server {
    pub fn start(
        dir: &Path,
        security: Security,
        keys: SharedKeys,
        change: impl FnOnce(&mut SshConfig),
    ) -> Self {
        let mut config = SshConfig {
            listen: "127.0.0.1:0".parse().expect("addr"),
            host_key: dir.join("host_key"),
            helper: Helper {
                program: env!("CARGO_BIN_EXE_orion-ssh-helper").into(),
                args: Vec::new(),
            },
            subsystems: Vec::new(),
            forward: true,
            etc: "/etc".into(),
            max_connections: 64,
            max_unauthenticated: 10,
            login: Duration::from_secs(60),
        };
        change(&mut config);
        let (ready, started) = std::sync::mpsc::channel();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            runtime.block_on(async move {
                let ssh = Ssh::start(config, security, keys)
                    .await
                    .expect("the server starts");
                ready.send(ssh.local_addr()).expect("ready");
                let _ = stopped.await;
            });
        });
        Self {
            addr: started.recv().expect("the server starts"),
            dir: dir.to_owned(),
            stop: Some(stop),
            thread: Some(thread),
        }
    }

    /// Runs OpenSSH's `ssh` against the server as `user`, with `key` if one is given,
    /// `extra` options, and `input` on its standard input.
    pub fn ssh(&self, user: &str, key: Option<&Path>, extra: &[&str], input: &str) -> Output {
        let mut command = Command::new("ssh");
        command
            .args(["-F", "/dev/null", "-p", &self.addr.port().to_string()])
            .args([
                "-o",
                "StrictHostKeyChecking=accept-new",
                "-o",
                "BatchMode=yes",
            ])
            .args(["-o", "ConnectTimeout=10", "-o", "LogLevel=ERROR"])
            .args(["-o", "IdentitiesOnly=yes", "-o", "IdentityAgent=none"])
            .arg("-o")
            .arg(format!(
                "UserKnownHostsFile={}",
                self.dir.join("known_hosts").display()
            ));
        match key {
            Some(key) => command.arg("-i").arg(key),
            None => command.args(["-o", "PubkeyAuthentication=no"]),
        };
        // Options go before the board's name, a command after it.
        let split = extra
            .iter()
            .position(|arg| *arg == "--")
            .unwrap_or(extra.len());
        command
            .args(&extra[..split])
            .arg(format!("{user}@127.0.0.1"))
            .args(extra.get(split + 1..).unwrap_or_default())
            .env_remove("SSH_AUTH_SOCK")
            .env("TERM", "xterm-256color")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().expect("ssh");
        child
            .stdin
            .take()
            .expect("stdin")
            .write_all(input.as_bytes())
            .expect("write");
        child.wait_with_output().expect("ssh ends")
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        drop(self.stop.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

pub fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// A trust store with security on, one operator trusted with everything and one that may
/// only read.
pub fn trust(dir: &Path) -> (SharedKeys, PathBuf, PathBuf, PathBuf) {
    let (admin, admin_key) = keygen(dir, "admin");
    let (reader, reader_key) = keygen(dir, "reader");
    let (stranger, _) = keygen(dir, "stranger");
    let mut keys = Keys::open(KeysConfig::in_memory("lab"), Security::On).expect("keys");
    keys.approve_operator(admin_key, "admin").expect("enroll");
    keys.grant(&KeyId(admin_key), true, Vec::new(), true)
        .expect("grant");
    keys.approve_operator(reader_key, "reader").expect("enroll");
    keys.grant(&KeyId(reader_key), true, Vec::new(), false)
        .expect("grant");
    (SharedKeys::new(keys), admin, reader, stranger)
}

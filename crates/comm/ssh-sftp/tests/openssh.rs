//! The sftp subsystem with OpenSSH's own `sftp` and `scp` as the clients, through the SSH
//! server: files go both ways whole, the usual operations work, and what the user may not
//! do is refused. If OpenSSH is not installed the tests say so and pass.

use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::Duration;

use orion_check::Security;
use orion_keys::{Keys, KeysConfig, SharedKeys};
use orion_ssh::{Helper, Ssh, SshConfig, Subsystem, users};
use orion_types::KeyId;

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
    let all = there("ssh") && there("sftp") && there("scp") && there("ssh-keygen");
    if !all {
        println!("OpenSSH is not installed: sftp and scp were not tried");
    }
    all
}

fn temp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("orion-sftp-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("board")).expect("dir");
    std::fs::create_dir_all(dir.join("desk")).expect("dir");
    dir
}

fn me() -> users::User {
    users::lookup_uid(Path::new("/etc"), rustix::process::getuid().as_raw())
        .expect("the user running the tests has an account")
}

/// An SSH server with the sftp subsystem, and one operator who may log in.
struct Server {
    addr: SocketAddr,
    dir: PathBuf,
    key: PathBuf,
    user: String,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Server {
    fn start(dir: &Path, with_sftp: bool) -> Self {
        let key = dir.join("admin");
        let made = Command::new("ssh-keygen")
            .args(["-q", "-t", "ed25519", "-N", "", "-f"])
            .arg(&key)
            .status()
            .expect("ssh-keygen");
        assert!(made.success());
        let public = std::fs::read_to_string(key.with_extension("pub")).expect("public key");
        let public = russh::keys::PublicKey::from_openssh(&public).expect("a public key");
        let public = public.key_data().ed25519().expect("ed25519").0;
        let mut keys = Keys::open(KeysConfig::in_memory("lab"), Security::On).expect("keys");
        keys.approve_operator(public, "admin").expect("enroll");
        keys.grant(&KeyId(public), true, Vec::new(), true)
            .expect("grant");

        let helper: PathBuf = env!("CARGO_BIN_EXE_orion-sftp-helper").into();
        let config = SshConfig {
            listen: "127.0.0.1:0".parse().expect("addr"),
            host_key: dir.join("host_key"),
            helper: Helper {
                program: helper.clone(),
                args: Vec::new(),
            },
            subsystems: if with_sftp {
                vec![Subsystem {
                    name: "sftp".to_owned(),
                    program: vec![helper.into(), orion_ssh_sftp::MODE.into()],
                }]
            } else {
                Vec::new()
            },
            etc: "/etc".into(),
            max_connections: 64,
            max_unauthenticated: 10,
            login: Duration::from_secs(60),
        };
        let (ready, started) = std::sync::mpsc::channel();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let keys = SharedKeys::new(keys);
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            runtime.block_on(async move {
                let ssh = Ssh::start(config, Security::On, keys)
                    .await
                    .expect("the server starts");
                ready.send(ssh.local_addr()).expect("ready");
                let _ = stopped.await;
            });
        });
        Self {
            addr: started.recv().expect("the server starts"),
            dir: dir.to_owned(),
            key,
            user: me().name,
            stop: Some(stop),
            thread: Some(thread),
        }
    }

    fn options(&self, command: &mut Command) {
        command
            .args(["-F", "/dev/null", "-P", &self.addr.port().to_string()])
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
            ))
            .arg("-i")
            .arg(&self.key)
            .env_remove("SSH_AUTH_SOCK")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
    }

    /// Runs `sftp` with `batch` as its commands, one a line.
    fn sftp(&self, batch: &str) -> Output {
        let file = self.dir.join("batch");
        std::fs::write(&file, batch).expect("batch");
        let mut command = Command::new("sftp");
        self.options(&mut command);
        command
            .arg("-b")
            .arg(&file)
            .arg(format!("{}@127.0.0.1", self.user))
            .output()
            .expect("sftp")
    }

    /// Runs `scp` with `args`, where `BOARD:` stands for the board.
    fn scp(&self, args: &[&str]) -> Output {
        let mut command = Command::new("scp");
        self.options(&mut command);
        for arg in args {
            match arg.strip_prefix("BOARD:") {
                Some(path) => command.arg(format!("{}@127.0.0.1:{path}", self.user)),
                None => command.arg(arg),
            };
        }
        command.output().expect("scp")
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

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// Bytes that are not the same anywhere, so a piece in the wrong place shows.
fn pattern(len: usize) -> Vec<u8> {
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    (0..len)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state >> 56) as u8
        })
        .collect()
}

#[test]
fn sftp_puts_gets_lists_and_changes_files_as_the_user() {
    if !installed() {
        return;
    }
    let dir = temp("sftp");
    let server = Server::start(&dir, true);
    let (board, desk) = (dir.join("board"), dir.join("desk"));
    let data = pattern(3 * 1024 * 1024 + 17);
    std::fs::write(desk.join("up.bin"), &data).expect("write");
    std::fs::write(board.join("there.txt"), "on the board\n").expect("write");

    let batch = format!(
        "cd {board}\nlcd {desk}\npwd\nput up.bin\nget there.txt\nmkdir made\nput up.bin made/again.bin\n\
         rename made/again.bin made/renamed.bin\nchmod 640 up.bin\nln -s up.bin link\nls -la\nls made\n\
         get link via-link.bin\nrm made/renamed.bin\nrmdir made\n",
        board = board.display(),
        desk = desk.display()
    );
    let ran = server.sftp(&batch);
    let out = text(&ran.stdout);
    assert!(ran.status.success(), "{out}\n{}", text(&ran.stderr));

    // Up whole, down whole, and through the link.
    assert!(std::fs::read(board.join("up.bin")).expect("up") == data);
    assert_eq!(
        std::fs::read_to_string(desk.join("there.txt")).expect("down"),
        "on the board\n"
    );
    assert!(std::fs::read(desk.join("via-link.bin")).expect("via the link") == data);
    // The mode that was set, the link that was made, and what was removed is gone.
    let mode = std::fs::metadata(board.join("up.bin"))
        .expect("metadata")
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o640);
    assert_eq!(
        std::fs::read_link(board.join("link")).expect("a link"),
        PathBuf::from("up.bin")
    );
    assert!(!board.join("made").exists());
    // What was listed: where it was, the files with their sizes, and the renamed one.
    assert!(
        out.contains(&format!("Remote working directory: {}", board.display())),
        "{out}"
    );
    assert!(
        out.contains(&data.len().to_string()) && out.contains("there.txt"),
        "{out}"
    );
    assert!(out.contains("renamed.bin"), "{out}");
    // The files are the user's.
    use std::os::unix::fs::MetadataExt;
    assert_eq!(
        std::fs::metadata(board.join("up.bin"))
            .expect("metadata")
            .uid(),
        me().uid
    );

    // What is not there, and what the user may not do, is refused and said.
    let missing = server.sftp(&format!(
        "get {}/no-such-file {}/x\n",
        board.display(),
        desk.display()
    ));
    assert!(!missing.status.success());
    assert!(
        text(&missing.stderr).contains("not found")
            || text(&missing.stderr).contains("No such file")
    );
    if !rustix::process::getuid().is_root() {
        std::fs::write(board.join("secret"), "x").expect("write");
        std::fs::set_permissions(board.join("secret"), std::fs::Permissions::from_mode(0o000))
            .expect("mode");
        let refused = server.sftp(&format!(
            "get {}/secret {}/secret\n",
            board.display(),
            desk.display()
        ));
        assert!(!refused.status.success());
        assert!(
            text(&refused.stderr).contains("Permission denied"),
            "{}",
            text(&refused.stderr)
        );
        std::fs::create_dir(board.join("closed")).expect("dir");
        std::fs::set_permissions(board.join("closed"), std::fs::Permissions::from_mode(0o555))
            .expect("mode");
        let refused = server.sftp(&format!(
            "put {}/up.bin {}/closed/up.bin\n",
            desk.display(),
            board.display()
        ));
        assert!(!refused.status.success());
        assert!(!board.join("closed/up.bin").exists());
        std::fs::set_permissions(board.join("closed"), std::fs::Permissions::from_mode(0o755))
            .expect("mode");
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn scp_copies_files_and_directories_both_ways() {
    if !installed() {
        return;
    }
    let dir = temp("scp");
    let server = Server::start(&dir, true);
    let (board, desk) = (dir.join("board"), dir.join("desk"));
    let data = pattern(5 * 1024 * 1024 + 3);
    std::fs::write(desk.join("big.bin"), &data).expect("write");

    // Up, and back down under another name.
    let up = server.scp(&[
        desk.join("big.bin").to_str().expect("a path"),
        &format!("BOARD:{}/big.bin", board.display()),
    ]);
    assert!(up.status.success(), "{}", text(&up.stderr));
    assert!(std::fs::read(board.join("big.bin")).expect("up") == data);
    let down = server.scp(&[
        &format!("BOARD:{}/big.bin", board.display()),
        desk.join("back.bin").to_str().expect("a path"),
    ]);
    assert!(down.status.success(), "{}", text(&down.stderr));
    assert!(std::fs::read(desk.join("back.bin")).expect("down") == data);

    // A directory with what is in it, and the mode and time of a file kept when asked.
    std::fs::create_dir_all(desk.join("tree/inner")).expect("dirs");
    std::fs::write(desk.join("tree/a.txt"), "a").expect("write");
    std::fs::write(desk.join("tree/inner/b.txt"), "b").expect("write");
    std::fs::set_permissions(
        desk.join("tree/a.txt"),
        std::fs::Permissions::from_mode(0o600),
    )
    .expect("mode");
    let tree = server.scp(&[
        "-r",
        "-p",
        desk.join("tree").to_str().expect("a path"),
        &format!("BOARD:{}/", board.display()),
    ]);
    assert!(tree.status.success(), "{}", text(&tree.stderr));
    assert_eq!(
        std::fs::read_to_string(board.join("tree/a.txt")).expect("a"),
        "a"
    );
    assert_eq!(
        std::fs::read_to_string(board.join("tree/inner/b.txt")).expect("b"),
        "b"
    );
    let (there, here) = (
        std::fs::metadata(board.join("tree/a.txt")).expect("metadata"),
        std::fs::metadata(desk.join("tree/a.txt")).expect("metadata"),
    );
    assert_eq!(there.permissions().mode() & 0o777, 0o600);
    use std::os::unix::fs::MetadataExt;
    assert_eq!(there.mtime(), here.mtime());

    // What is not there is said, and nothing is made of it.
    let missing = server.scp(&[
        &format!("BOARD:{}/no-such-file", board.display()),
        desk.join("nothing").to_str().expect("a path"),
    ]);
    assert!(!missing.status.success());
    assert!(!desk.join("nothing").exists());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_board_without_the_subsystem_says_so() {
    if !installed() {
        return;
    }
    let dir = temp("without");
    let server = Server::start(&dir, false);
    let ran = server.sftp("pwd\n");
    assert!(!ran.status.success());
    let _ = std::fs::remove_dir_all(dir);
}

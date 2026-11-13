//! The sandboxed server with OpenSSH's `ssh` as the client: everything works through a
//! child, a child that dies takes only its own connection, and the keeper answers only
//! what a child in that state may ask. If `ssh` is not installed those tests say so and
//! pass.

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use orion_check::Security;
use orion_keys::{Keys, KeysConfig, SharedKeys};
use orion_ssh::{Helper, Keeper, SshConfig, users};
use orion_ssh_sandbox::protocol::{Answer, Ask, Want, recv, send};
use orion_ssh_sandbox::{SandboxConfig, SandboxedSsh, monitor};
use orion_types::KeyId;

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
        println!("OpenSSH is not installed: the sandboxed SSH server was not tried with it");
    }
    both
}

fn temp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("orion-sandbox-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    dir
}

fn me() -> users::User {
    users::lookup_uid(Path::new("/etc"), rustix::process::getuid().as_raw())
        .expect("the user running the tests has an account")
}

fn config(dir: &Path) -> SshConfig {
    SshConfig {
        listen: "127.0.0.1:0".parse().expect("addr"),
        host_key: dir.join("host_key"),
        helper: Helper {
            program: env!("CARGO_BIN_EXE_orion-sandbox-helper").into(),
            args: Vec::new(),
        },
        subsystems: Vec::new(),
        forward: true,
        etc: "/etc".into(),
        recovery: false,
        max_connections: 64,
        max_unauthenticated: 10,
        login: Duration::from_secs(60),
    }
}

/// A trust store with security on and one operator, `key`, trusted with everything.
fn trust(key: [u8; 32]) -> SharedKeys {
    let mut keys = Keys::open(KeysConfig::in_memory("lab"), Security::On).expect("keys");
    keys.approve_operator(key, "admin").expect("enroll");
    keys.grant(&KeyId(key), true, Vec::new(), true)
        .expect("grant");
    SharedKeys::new(keys)
}

fn keygen(dir: &Path, name: &str) -> (PathBuf, [u8; 32]) {
    let path = dir.join(name);
    let made = Command::new("ssh-keygen")
        .args(["-q", "-t", "ed25519", "-N", "", "-f"])
        .arg(&path)
        .status()
        .expect("ssh-keygen");
    assert!(made.success());
    let public = std::fs::read_to_string(path.with_extension("pub")).expect("public key");
    let public = russh::keys::PublicKey::from_openssh(&public).expect("a public key");
    (path, public.key_data().ed25519().expect("ed25519").0)
}

/// The sandboxed server in a thread of its own, until dropped.
struct Server {
    addr: SocketAddr,
    dir: PathBuf,
    key: PathBuf,
    user: String,
    ssh: Arc<Mutex<Option<SandboxedSsh>>>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Server {
    fn start(dir: &Path) -> Self {
        let (key, public) = keygen(dir, "admin");
        let (config, keys) = (config(dir), trust(public));
        let (ready, started) = std::sync::mpsc::channel();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let ssh = Arc::new(Mutex::new(None));
        let held = ssh.clone();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            runtime.block_on(async move {
                let server =
                    SandboxedSsh::start(config, SandboxConfig::default(), Security::On, keys)
                        .await
                        .expect("the server starts");
                ready.send(server.local_addr()).expect("ready");
                *held.lock().expect("lock") = Some(server);
                let _ = stopped.await;
                held.lock().expect("lock").take();
            });
        });
        Self {
            addr: started.recv().expect("the server starts"),
            dir: dir.to_owned(),
            key,
            user: me().name,
            ssh,
            stop: Some(stop),
            thread: Some(thread),
        }
    }

    fn children(&self) -> Vec<u32> {
        self.ssh
            .lock()
            .expect("lock")
            .as_ref()
            .map_or_else(Vec::new, SandboxedSsh::children)
    }

    fn connections(&self) -> usize {
        self.ssh
            .lock()
            .expect("lock")
            .as_ref()
            .map_or(0, SandboxedSsh::connections)
    }

    fn ssh(&self, key: &Path, options: &[&str], command: &[&str], input: &str) -> Output {
        let mut ssh = Command::new("ssh");
        ssh.args(["-F", "/dev/null", "-p", &self.addr.port().to_string()])
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
            .arg(key)
            .args(options)
            .arg(format!("{}@127.0.0.1", self.user))
            .args(command)
            .env_remove("SSH_AUTH_SOCK")
            .env("TERM", "xterm")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = ssh.spawn().expect("ssh");
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

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn until(what: &str, mut done: impl FnMut() -> bool) {
    let began = Instant::now();
    while !done() {
        assert!(began.elapsed() < WAIT, "{what} never happened");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn everything_works_through_a_child_that_cannot_gain_rights() {
    if !installed() {
        return;
    }
    let dir = temp("works");
    let server = Server::start(&dir);
    let (stranger, _) = keygen(&dir, "stranger");
    let me = me();

    // A command, with its exit code and who it ran as.
    let ran = server.ssh(&server.key, &[], &["echo out; id -u; exit 7"], "");
    assert_eq!(ran.status.code(), Some(7), "{}", text(&ran.stderr));
    assert_eq!(text(&ran.stdout), format!("out\n{}\n", me.uid));
    // A shell on a terminal.
    let ran = server.ssh(&server.key, &["-tt"], &[], "tty; exit 3\n");
    assert_eq!(ran.status.code(), Some(3), "{}", text(&ran.stderr));
    assert!(text(&ran.stdout).contains("/dev/pts/"));
    // A lot of output, whole.
    let ran = server.ssh(&server.key, &[], &["seq 1 100000"], "");
    assert_eq!(text(&ran.stdout).lines().count(), 100_000);
    // A stranger is refused by the keeper, through the child.
    let refused = server.ssh(&stranger, &[], &["echo in"], "");
    assert_eq!(
        (refused.status.code(), text(&refused.stdout).as_str()),
        (Some(255), "")
    );

    // A connection from the board, through the child, which never makes it itself.
    #[cfg(feature = "forward")]
    {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("listen");
        let port = listener.local_addr().expect("addr").port();
        std::thread::spawn(move || {
            for mut stream in listener.incoming().flatten() {
                let _ = stream.write_all(b"from the service\n");
            }
        });
        let ran = server.ssh(&server.key, &["-W", &format!("127.0.0.1:{port}")], &[], "");
        assert_eq!(
            text(&ran.stdout),
            "from the service\n",
            "{}",
            text(&ran.stderr)
        );
    }

    // While a connection is open there is a child for it, and that child can gain no
    // rights by any means; when it is over the child is gone.
    // The children of the connections before are gone first, so the one counted is this one's.
    until("the earlier children are gone", || {
        server.children().is_empty() && server.connections() == 0
    });
    std::thread::scope(|scope| {
        let open = scope.spawn(|| server.ssh(&server.key, &[], &["sleep 2; echo done"], ""));
        // The child sets it on itself once it starts, so it is looked at until then.
        until("a child for the connection that can gain no rights", || {
            server.children().iter().any(|pid| {
                std::fs::read_to_string(format!("/proc/{pid}/status")).is_ok_and(|status| {
                    status
                        .lines()
                        .any(|line| line.replace('\t', " ") == "NoNewPrivs: 1")
                })
            })
        });
        assert_eq!(text(&open.join().expect("ssh").stdout), "done\n");
    });
    until("the child is gone", || {
        server.children().is_empty() && server.connections() == 0
    });
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_child_that_dies_takes_only_its_own_connection() {
    if !installed() {
        return;
    }
    let dir = temp("dies");
    let server = Server::start(&dir);
    let mark = dir.join("session.pid");
    std::thread::scope(|scope| {
        // One connection with a session that would run for five minutes.
        let doomed = scope.spawn(|| {
            let command = format!("echo $$ > {}; exec sleep 300", mark.display());
            server.ssh(&server.key, &[], &[&command], "")
        });
        until("the first child", || {
            server.children().len() == 1 && mark.exists()
        });
        let child = server.children()[0];
        // And another beside it.
        let beside = scope.spawn(|| server.ssh(&server.key, &[], &["sleep 3; echo survived"], ""));
        until("the second child", || server.children().len() == 2);

        // The first connection's child dies, as a fault in it would kill it.
        let killed = Command::new("kill")
            .args(["-KILL", &child.to_string()])
            .status()
            .expect("kill");
        assert!(killed.success());

        // Its client is cut off, and the session it carried is ended, not left running.
        assert_eq!(doomed.join().expect("ssh").status.code(), Some(255));
        let session: u32 = std::fs::read_to_string(&mark)
            .expect("pid")
            .trim()
            .parse()
            .expect("a pid");
        until("the session is ended", || {
            !Path::new(&format!("/proc/{session}")).exists()
        });
        // The connection beside it never noticed.
        assert_eq!(text(&beside.join().expect("ssh").stdout), "survived\n");
    });
    // And the server takes the next one as if nothing had happened.
    let ran = server.ssh(&server.key, &[], &["echo in"], "");
    assert_eq!(text(&ran.stdout), "in\n");
    until("no connections", || {
        server.connections() == 0 && server.children().is_empty()
    });
    let _ = std::fs::remove_dir_all(dir);
}

/// A child's end of a keeper that is answering it, for a test that plays the child.
fn keeper(dir: &Path, key: [u8; 32], login: Duration) -> (UnixStream, std::thread::JoinHandle<()>) {
    let (ours, theirs) = UnixStream::pair().expect("pair");
    let keeper = Keeper::new(
        config(dir),
        Security::On,
        trust(key),
        "127.0.0.1:22".parse().expect("addr"),
    );
    let answering =
        std::thread::spawn(move || monitor::serve(&keeper, &theirs, None, login, || {}));
    (ours, answering)
}

fn ask(socket: &UnixStream, ask: &Ask) -> (Answer, Vec<std::os::fd::OwnedFd>) {
    send(socket, ask, &[]).expect("ask");
    recv(socket)
        .expect("an answer")
        .expect("the keeper is there")
}

#[test]
fn the_keeper_answers_only_what_a_child_in_that_state_may_ask() {
    let dir = temp("keeper");
    let (admin, stranger) = ([7_u8; 32], [9_u8; 32]);
    let (child, answering) = keeper(&dir, admin, Duration::from_secs(30));
    let me = me();

    // Before a login: no session, no connection, whatever the child says it is.
    let refused = Answer::Failed("not logged in".to_owned());
    assert_eq!(ask(&child, &Ask::Start(Want::Shell, None)).0, refused);
    assert_eq!(
        ask(&child, &Ask::Start(Want::Command(b"id".to_vec()), None)).0,
        refused
    );
    assert_eq!(
        ask(&child, &Ask::Connect("127.0.0.1".to_owned(), 22)).0,
        refused
    );
    // A key nobody enrolled, no key, and an account that is not there.
    assert_eq!(ask(&child, &Ask::Offered(Some(stranger))).0, Answer::No);
    assert_eq!(
        ask(&child, &Ask::Login(me.name.clone(), Some(stranger))).0,
        Answer::No
    );
    assert_eq!(
        ask(&child, &Ask::Login(me.name.clone(), None)).0,
        Answer::No
    );
    assert_eq!(
        ask(
            &child,
            &Ask::Login("no-such-account".to_owned(), Some(admin))
        )
        .0,
        Answer::No
    );
    assert_eq!(ask(&child, &Ask::Start(Want::Shell, None)).0, refused);

    // The operator's key, as an account the keeper can become.
    assert_eq!(ask(&child, &Ask::Offered(Some(admin))).0, Answer::Yes);
    assert_eq!(
        ask(&child, &Ask::Login(me.name.clone(), Some(admin))).0,
        Answer::Yes
    );
    // One login for a connection: it cannot become someone else afterwards.
    assert_eq!(
        ask(&child, &Ask::Login(me.name.clone(), Some(admin))).0,
        Answer::No
    );
    // A subsystem there is none of.
    assert!(matches!(
        ask(
            &child,
            &Ask::Start(Want::Subsystem("sftp".to_owned()), None)
        )
        .0,
        Answer::Failed(_)
    ));

    // Now a session: its pipes and its status socket come with the answer.
    let (answer, fds) = ask(
        &child,
        &Ask::Start(Want::Command(b"echo hi; exit 4".to_vec()), None),
    );
    assert_eq!((answer, fds.len()), (Answer::Pipes, 4));
    let mut fds = fds.into_iter();
    let (_input, output, _errors, status) = (
        fds.next().expect("input"),
        std::fs::File::from(fds.next().expect("output")),
        fds.next().expect("errors"),
        UnixStream::from(fds.next().expect("status")),
    );
    let mut said = String::new();
    (&output).read_to_string(&mut said).expect("output");
    assert_eq!(said, "hi\n");
    let mut code = [0_u8; 4];
    (&status).read_exact(&mut code).expect("status");
    assert_eq!(u32::from_le_bytes(code), 4);

    // Something that is not a question ends it.
    shm_fdpass::send_on(&child, &[&[0xFF; 9]], &[]).expect("send");
    assert!(recv::<Answer>(&child).expect("an end").is_none());
    answering.join().expect("the keeper ends");

    // A child whose client never logs in is not waited for, whatever the child does.
    let began = Instant::now();
    let (silent, answering) = keeper(&dir, admin, Duration::from_millis(600));
    assert_eq!(ask(&silent, &Ask::Offered(Some(stranger))).0, Answer::No);
    answering.join().expect("the keeper ends");
    let took = began.elapsed();
    assert!(
        took >= Duration::from_millis(550) && took < Duration::from_secs(5),
        "{took:?}"
    );
    let _ = std::fs::remove_dir_all(dir);
}

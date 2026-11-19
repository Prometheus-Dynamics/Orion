//! A comm core that cannot start as it was told to ends in recovery, and each broken
//! state still lets you in; one that can start is reached with `ssh` and `scp`, through a
//! child with no rights. If OpenSSH is not installed the tests say so and pass.

use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use orion_comm_app::{CommSettings, RecoverWith, Started, start_or_recover};
use orion_config::{ConfigError, Source};
use orion_scratch::Scratch;

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
    let all = there("ssh") && there("scp") && there("ssh-keygen");
    if !all {
        println!("OpenSSH is not installed: the comm core's SSH was not tried with it");
    }
    all
}

fn temp(name: &str) -> Scratch {
    Scratch::new(&format!("comm-recovery-{name}"))
}

fn helper() -> PathBuf {
    env!("CARGO_BIN_EXE_orion-comm-helper").into()
}

/// The name of the account the tests run as.
fn me() -> String {
    let uid = rustix::process::getuid().as_raw().to_string();
    std::fs::read_to_string("/etc/passwd")
        .expect("passwd")
        .lines()
        .find(|line| line.split(':').nth(2) == Some(uid.as_str()))
        .and_then(|line| line.split(':').next())
        .expect("the user running the tests has an account")
        .to_owned()
}

fn keygen(dir: &Path, name: &str) -> (PathBuf, String) {
    let path = dir.join(name);
    let made = Command::new("ssh-keygen")
        .args(["-q", "-t", "ed25519", "-N", "", "-f"])
        .arg(&path)
        .status()
        .expect("ssh-keygen");
    assert!(made.success());
    (
        path.clone(),
        std::fs::read_to_string(path.with_extension("pub")).expect("public key"),
    )
}

fn settings(text: &str) -> Result<CommSettings, ConfigError> {
    let mut source = Source::parse(text, Vec::new())?;
    CommSettings::from_source_with_view(&mut source, Path::new("/nonexistent/orion-test/security"))
}

/// As [`settings`], on a board whose security record, in `dir`, says on.
fn settings_on(dir: &Path, text: &str) -> Result<CommSettings, ConfigError> {
    let mut source = Source::parse(text, Vec::new())?;
    CommSettings::from_source_with_view(&mut source, &secured(dir))
}

/// The board's security record, as a board with security on has it, in `dir`.
fn secured(dir: &Path) -> PathBuf {
    let path = dir.join("security");
    std::fs::write(&path, "on\ngeneration 1\n").expect("security record");
    path
}

/// Makes a directory writable again when dropped, so that it can be removed with the test's
/// directory, whether the test passed or panicked.
struct Writable(PathBuf);

impl Drop for Writable {
    fn drop(&mut self) {
        let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o755));
    }
}

fn options(
    command: &mut Command,
    dir: &Path,
    port_flag: &str,
    addr: SocketAddr,
    key: Option<&Path>,
) {
    command
        .args(["-F", "/dev/null", port_flag, &addr.port().to_string()])
        .args(["-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes"])
        .args(["-o", "ConnectTimeout=10", "-o", "LogLevel=ERROR"])
        .args(["-o", "IdentitiesOnly=yes", "-o", "IdentityAgent=none"])
        .arg("-o")
        .arg(format!(
            "UserKnownHostsFile={}",
            dir.join("known_hosts").display()
        ))
        .env_remove("SSH_AUTH_SOCK")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    match key {
        Some(key) => command.arg("-i").arg(key),
        None => command.args(["-o", "PubkeyAuthentication=no"]),
    };
}

/// Runs `ssh` on a thread that may block, so the comm core on this one goes on.
async fn ssh(dir: &Path, addr: SocketAddr, key: Option<&Path>, run: &str) -> Output {
    let mut command = Command::new("ssh");
    options(&mut command, dir, "-p", addr, key);
    command.arg(format!("{}@127.0.0.1", me())).arg(run);
    tokio::task::spawn_blocking(move || command.output().expect("ssh"))
        .await
        .expect("ssh ends")
}

fn said(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// What `started` is, as recovery: where it listens, once it does, and why.
async fn in_recovery(started: &Started) -> (SocketAddr, String) {
    let Started::Recovery(recovery) = started else {
        panic!("the comm core started: {started:?}");
    };
    let began = Instant::now();
    loop {
        if let Some(addr) = recovery.local_addr() {
            return (addr, recovery.why().to_owned());
        }
        assert!(began.elapsed() < WAIT, "the recovery way in never opened");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn run(test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, test);
}

#[test]
fn each_broken_state_ends_in_recovery_and_still_lets_you_in() {
    if !installed() {
        return;
    }
    run(async {
        let dir = temp("broken");
        let (repairer, public) = keygen(&dir, "repairer");
        let (stranger, _) = keygen(&dir, "stranger");
        std::fs::write(dir.join("recovery_keys"), &public).expect("keys");
        let with_view = |security_view: PathBuf| RecoverWith {
            listen: vec!["127.0.0.1:0".parse().expect("addr")],
            keys_file: dir.join("recovery_keys"),
            helper: Some(helper()),
            retry: Duration::from_millis(100),
            last_good: dir.join("no-record"),
            security_view,
            #[cfg(feature = "login-helper")]
            login: Default::default(),
        };
        let with = || with_view(secured(&dir));
        let good = |state: &Path| {
            format!(
                "node.name = board\nsocket = {0}/orion.sock\nstate.dir = {1}\n\
                 recovery.keys = {0}/recovery_keys\nssh.helper = {2}\n",
                dir.display(),
                state.display(),
                helper().display()
            )
        };

        // A broken config: a setting that is not one. Nothing in the file is believed,
        // so security is on, and only the repairer gets in.
        let broken = settings(&format!(
            "{}comm.max_sessions = many\n",
            good(&dir.join("state"))
        ));
        assert!(broken.is_err());
        let started = start_or_recover(broken, with()).await;
        let (addr, why) = in_recovery(&started).await;
        assert!(
            why.contains("the settings cannot be read") && why.contains("comm.max_sessions"),
            "{why}"
        );
        assert_eq!(
            said(&ssh(&dir, addr, Some(&repairer), "echo in").await),
            "in\n"
        );
        assert_eq!(said(&ssh(&dir, addr, Some(&stranger), "echo in").await), "");
        assert_eq!(said(&ssh(&dir, addr, None, "echo in").await), "");
        drop(started);

        // The broken file decides nothing: the switch is the board's record. Off, and recovery
        // is as open as the board always is.
        let off = dir.join("security-off");
        std::fs::write(&off, "off\ngeneration 2\n").expect("record");
        let broken = || {
            settings(&format!(
                "{}comm.max_sessions = many\n",
                good(&dir.join("state"))
            ))
        };
        let started = start_or_recover(broken(), with_view(off)).await;
        let (addr, _) = in_recovery(&started).await;
        assert_eq!(said(&ssh(&dir, addr, None, "echo in").await), "in\n");
        drop(started);
        // A record that is damaged is on, whatever it was meant to say.
        let damaged = dir.join("security-damaged");
        std::fs::write(&damaged, "of\n").expect("record");
        let started = start_or_recover(broken(), with_view(damaged)).await;
        let (addr, _) = in_recovery(&started).await;
        assert_eq!(said(&ssh(&dir, addr, None, "echo in").await), "");
        assert_eq!(
            said(&ssh(&dir, addr, Some(&repairer), "echo in").await),
            "in\n"
        );
        drop(started);

        // A config that is not even text of the right shape.
        let started = start_or_recover(settings("node.name board\n[[[\n"), with()).await;
        let (addr, _) = in_recovery(&started).await;
        assert_eq!(
            said(&ssh(&dir, addr, Some(&repairer), "echo in").await),
            "in\n"
        );
        drop(started);

        // A broken state directory: it is a file.
        std::fs::write(dir.join("not-a-directory"), "x").expect("write");
        let started = start_or_recover(
            settings_on(&dir, &good(&dir.join("not-a-directory"))),
            with(),
        )
        .await;
        let (addr, why) = in_recovery(&started).await;
        assert!(
            why.starts_with("state.dir ") && why.contains("cannot be made"),
            "{why}"
        );
        assert_eq!(
            said(&ssh(&dir, addr, Some(&repairer), "echo in").await),
            "in\n"
        );
        assert_eq!(said(&ssh(&dir, addr, Some(&stranger), "echo in").await), "");
        drop(started);

        // A trust store that is rubbish.
        std::fs::create_dir(dir.join("rubbish")).expect("dir");
        std::fs::write(dir.join("rubbish/node-identity.ed25519"), [1_u8; 7]).expect("write");
        let started =
            start_or_recover(settings_on(&dir, &good(&dir.join("rubbish"))), with()).await;
        let (addr, _) = in_recovery(&started).await;
        assert_eq!(
            said(&ssh(&dir, addr, Some(&repairer), "echo in").await),
            "in\n"
        );
        drop(started);

        // A disk that takes no more: nothing can be written where the state goes.
        if !rustix::process::getuid().is_root() {
            std::fs::create_dir(dir.join("full")).expect("dir");
            std::fs::set_permissions(dir.join("full"), std::fs::Permissions::from_mode(0o555))
                .expect("mode");
            let _writable = Writable(dir.join("full"));
            let started =
                start_or_recover(settings_on(&dir, &good(&dir.join("full"))), with()).await;
            let (addr, why) = in_recovery(&started).await;
            assert!(why.contains("cannot be written to"), "{why}");
            assert_eq!(
                said(&ssh(&dir, addr, Some(&repairer), "echo in").await),
                "in\n"
            );
            // A file can still be copied off the board to be looked at.
            std::fs::write(dir.join("evidence.log"), "what went wrong\n").expect("write");
            let mut scp = Command::new("scp");
            options(&mut scp, &dir, "-P", addr, Some(&repairer));
            scp.arg(format!("{}@127.0.0.1:{}/evidence.log", me(), dir.display()))
                .arg(dir.join("copied.log"));
            let copied = tokio::task::spawn_blocking(move || scp.output().expect("scp"))
                .await
                .expect("scp ends");
            assert!(
                copied.status.success(),
                "{}",
                String::from_utf8_lossy(&copied.stderr)
            );
            assert_eq!(
                std::fs::read_to_string(dir.join("copied.log")).expect("copied"),
                "what went wrong\n"
            );
            drop(started);
        }

        // A state partition that is not mounted: the directory would be on the root file
        // system. Only where the test's directory is on it (not where /tmp is its own).
        let device = |path: &Path| {
            std::os::unix::fs::MetadataExt::dev(&std::fs::metadata(path).expect("stat"))
        };
        if device(dir.path()) == device(Path::new("/")) {
            let text = format!(
                "{}state.require_mount = true\n",
                good(&dir.join("data/orion"))
            );
            let started = start_or_recover(settings_on(&dir, &text), with()).await;
            let (addr, why) = in_recovery(&started).await;
            assert!(why.contains("state.require_mount"), "{why}");
            assert!(
                !dir.join("data").exists(),
                "nothing made on the root file system"
            );
            assert_eq!(
                said(&ssh(&dir, addr, Some(&repairer), "echo in").await),
                "in\n"
            );
            drop(started);
        }
    });
}

#[test]
fn a_comm_core_that_can_start_is_reached_with_ssh_through_a_child() {
    if !installed() {
        return;
    }
    run(async {
        let dir = temp("starts");
        let text = format!(
            "node.name = board\nsocket = {0}/orion.sock\nstate.dir = {0}/state\n\
             ssh.listen = 127.0.0.1:0\nssh.helper = {1}\n",
            dir.display(),
            helper().display()
        );
        let started = start_or_recover(settings(&text), RecoverWith::default()).await;
        let Started::Core(core) = &started else {
            panic!("the comm core did not start: {started:?}");
        };
        let addr = core.ssh_addr().expect("ssh");
        // Security is off, as it is unless a board says otherwise: anyone gets a shell,
        // as the user. The connection was read by a child of the comm core's.
        assert_eq!(
            said(&ssh(&dir, addr, None, "echo in; id -u").await),
            format!("in\n{}\n", rustix::process::getuid().as_raw())
        );
        // And a file goes up with scp.
        std::fs::write(dir.join("up.txt"), "from the desk\n").expect("write");
        let mut scp = Command::new("scp");
        options(&mut scp, &dir, "-P", addr, None);
        scp.arg(dir.join("up.txt")).arg(format!(
            "{}@127.0.0.1:{}/arrived.txt",
            me(),
            dir.display()
        ));
        let copied = tokio::task::spawn_blocking(move || scp.output().expect("scp"))
            .await
            .expect("scp ends");
        assert!(
            copied.status.success(),
            "{}",
            String::from_utf8_lossy(&copied.stderr)
        );
        assert_eq!(
            std::fs::read_to_string(dir.join("arrived.txt")).expect("arrived"),
            "from the desk\n"
        );
        drop(started);
    });
}

/// What the board on `socket` says it is doing, and why if it is in recovery.
async fn board_state(socket: &Path) -> (Option<String>, Option<String>) {
    let node = board_node(socket).await;
    (
        node.state().map(str::to_owned),
        node.recovery_why().map(str::to_owned),
    )
}

/// The board's hello document, on its socket.
async fn board_node(socket: &Path) -> orion::node::Node {
    let address: orion::Address = socket.display().to_string().parse().expect("address");
    tokio::task::spawn_blocking(move || {
        // Recovery opens the socket once whatever held it has let go: a moment, at most.
        let began = Instant::now();
        let orion = loop {
            match orion::Orion::connect(&address, "test") {
                Ok(orion) => break orion,
                Err(error) => assert!(began.elapsed() < WAIT, "connect: {error}"),
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        orion.node().expect("hello")
    })
    .await
    .expect("asked")
}

/// A raw hello at `revision` on the board's socket, and the kind and body of the answer.
async fn hello_at(socket: &Path, revision: u16) -> (u8, Vec<u8>) {
    let connection = orion_unix::UnixConnection::connect(socket)
        .await
        .expect("connect");
    let mut out = [0_u8; 128];
    let hello = orion_msg_session::Hello {
        name: "test",
        roles: orion_msg_session::Roles::CLIENT,
        max_frame: 4096,
        revision,
        level: 0,
    };
    let len = orion_wire::encode_body(&hello, 1, &mut out).expect("hello");
    connection.send(&out[..len], &[]).await.expect("send");
    let mut buf = vec![0_u8; 4096];
    let len = connection
        .recv(&mut buf, &mut orion_server::FdSet::default())
        .await
        .expect("recv")
        .expect("an answer");
    let frame = orion_wire::frame::decode(&buf[..len]).expect("frame");
    (frame.kind(), frame.payload().to_vec())
}

#[test]
fn a_good_start_is_recorded_and_recovery_opens_its_ways_in_and_says_why() {
    run(async {
        let dir = temp("last-good");
        let with = || RecoverWith {
            listen: vec!["127.0.0.1:0".parse().expect("addr")],
            keys_file: dir.join("recovery_keys"),
            helper: Some(helper()),
            retry: Duration::from_millis(100),
            last_good: dir.join("state/last-good.carriers"),
            security_view: secured(&dir),
            #[cfg(feature = "login-helper")]
            login: Default::default(),
        };
        let socket = dir.join("board.sock");
        let good = format!(
            "node.name = board\nsocket = {}\nstate.dir = {}/state\n\
             enrollment.key = do-not-copy-me\nrecovery.keys = {}/recovery_keys\n",
            socket.display(),
            dir.display(),
            dir.display()
        );

        // A good start: the board says it is running, and its ways in are recorded.
        let started = start_or_recover(settings_on(&dir, &good), with()).await;
        assert!(matches!(started, Started::Core(_)), "{started:?}");
        assert_eq!(board_state(&socket).await, (Some("running".into()), None));
        // The hello says the switch, as the board's record has it.
        let on = orion::node::Security::On {
            generation: Some(1),
        };
        assert_eq!(board_node(&socket).await.security(), on);
        let record =
            std::fs::read_to_string(dir.join("state/last-good.carriers")).expect("recorded");
        assert!(
            record.contains(&format!("socket = {}", socket.display())),
            "{record}"
        );
        assert!(
            !record.contains("do-not-copy-me"),
            "nothing secret: {record}"
        );
        drop(started);
        // Aborted tasks are dropped, and let go of the socket, when the runtime next runs.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The settings break. Recovery opens the socket of the last good start, and the
        // board says on it that it is in recovery, and why.
        let started = start_or_recover(settings("node.name board\n[[[\n"), with()).await;
        let (_, why) = in_recovery(&started).await;
        assert!(why.contains("the last good start's"), "{why}");
        let (state, said_why) = board_state(&socket).await;
        assert_eq!(state.as_deref(), Some("recovery"));
        // Recovery says the record's switch too.
        assert_eq!(board_node(&socket).await.security(), on);
        let said_why = said_why.expect("why");
        assert!(
            said_why.contains("the settings cannot be read"),
            "{said_why}"
        );
        // Recovery speaks the same wire, and turns another revision away with both numbers.
        let other = orion_wire::INTERFACE_REVISION + 1;
        let (kind, body) = hello_at(&socket, other).await;
        assert_eq!(kind, orion_wire::kind::REJECT);
        let reject =
            <orion_msg_session::Reject as orion_wire::Decode>::decode(&body).expect("reject");
        assert_eq!(
            (reject.reason, reject.node_revision, reject.peer_revision),
            (
                orion_msg_session::RejectReason::RevisionMismatch,
                orion_wire::INTERFACE_REVISION,
                other
            )
        );
        let (kind, body) = hello_at(&socket, orion_wire::INTERFACE_REVISION).await;
        assert_eq!(kind, orion_wire::kind::WELCOME);
        let welcome =
            <orion_msg_session::Welcome as orion_wire::Decode>::decode(&body).expect("welcome");
        assert_eq!(welcome.level, orion_wire::INTERFACE_LEVEL);
        drop(started);
        // Aborted tasks are dropped, and let go of the socket, when the runtime next runs.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Settings that can be read but cannot be started as they say: recovery on their
        // own socket, with the comm core's reason.
        std::fs::write(dir.join("not-a-directory"), "x").expect("write");
        let broken = good.replace(
            &format!("state.dir = {}/state", dir.display()),
            &format!("state.dir = {}/not-a-directory", dir.display()),
        );
        let started = start_or_recover(settings(&broken), with()).await;
        let (_, why) = in_recovery(&started).await;
        assert!(!why.contains("last good"), "{why}");
        let (state, said_why) = board_state(&socket).await;
        assert_eq!(state.as_deref(), Some("recovery"));
        assert!(said_why.expect("why").contains("state.dir"));
        drop(started);
        // Aborted tasks are dropped, and let go of the socket, when the runtime next runs.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // A start that fails after the board's socket is already served: an NT4 gateway told
        // to share a WebSocket listener there is none of. Nothing of the failed start is left
        // serving, and recovery has the socket. (An address that is taken is not such a
        // failure: it is tried again, and the comm core starts.)
        let broken = format!("{good}nt4.listen = ws\n");
        let started = start_or_recover(settings(&broken), with()).await;
        let (_, why) = in_recovery(&started).await;
        assert!(why.contains("nt4.listen"), "{why}");
        let (state, _) = board_state(&socket).await;
        assert_eq!(state.as_deref(), Some("recovery"));
        drop(started);
        // Aborted tasks are dropped, and let go of the socket, when the runtime next runs.
        tokio::time::sleep(Duration::from_millis(50)).await;
    });
}

/// A comm core that is not root, with settings that cannot be read, ends in recovery and lets
/// the repairer in through the login helper, which reads the same broken settings and answers
/// as recovery does: the recovery keys decide. Once the helper is gone, nobody gets in, so it
/// was the helper that started the sessions.
#[cfg(feature = "login-helper")]
#[test]
fn broken_settings_and_a_recovery_key_log_in_through_the_login_helper() {
    if !installed() {
        return;
    }
    run(async {
        let dir = temp("login-helper");
        let (repairer, public) = keygen(&dir, "repairer");
        let (stranger, _) = keygen(&dir, "stranger");
        std::fs::write(dir.join("recovery_keys"), &public).expect("keys");
        let socket = dir.join("login.sock");
        let with = RecoverWith {
            listen: vec!["127.0.0.1:0".parse().expect("addr")],
            keys_file: dir.join("recovery_keys"),
            helper: Some(helper()),
            retry: Duration::from_millis(100),
            last_good: dir.join("no-record"),
            security_view: secured(&dir),
            login: orion_comm_app::login_helper::LoginSettings {
                helper: Some(socket.clone()),
                comm_user: rustix::process::getuid().as_raw().to_string(),
                idle: Duration::from_millis(500),
            },
        };
        let broken = || settings("node.name = board\ncomm.max_sessions = many\n");
        assert!(broken().is_err());
        // The helper, as the system starts it: the same file, read the same way.
        let helper_with = with.clone();
        let login_helper =
            std::thread::spawn(move || orion_comm_app::login_helper::run(broken(), &helper_with));
        let began = Instant::now();
        while !socket.exists() {
            assert!(began.elapsed() < WAIT, "the login helper never listened");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let started = start_or_recover(broken(), with).await;
        let (addr, why) = in_recovery(&started).await;
        assert!(why.contains("the settings cannot be read"), "{why}");
        assert_eq!(
            said(&ssh(&dir, addr, Some(&repairer), "echo in").await),
            "in\n"
        );
        assert_eq!(said(&ssh(&dir, addr, Some(&stranger), "echo in").await), "");
        assert_eq!(said(&ssh(&dir, addr, None, "echo in").await), "");
        // Idle, the helper goes; then a recovery key opens no session.
        let began = Instant::now();
        while !login_helper.is_finished() {
            assert!(began.elapsed() < WAIT, "the login helper stayed");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(login_helper.join().expect("the helper"), 0);
        assert_eq!(said(&ssh(&dir, addr, Some(&repairer), "echo in").await), "");
        drop(started);
    });
}

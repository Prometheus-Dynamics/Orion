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

fn temp(name: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("orion-comm-recovery-{name}-{}", std::process::id()));
    if dir.exists() {
        let _ = Command::new("chmod")
            .args(["-R", "u+rwx"])
            .arg(&dir)
            .status();
        let _ = std::fs::remove_dir_all(&dir);
    }
    std::fs::create_dir_all(&dir).expect("dir");
    dir
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
    CommSettings::from_source(&mut source)
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
        let with = || RecoverWith {
            listen: vec!["127.0.0.1:0".parse().expect("addr")],
            keys_file: dir.join("recovery_keys"),
            helper: Some(helper()),
            retry: Duration::from_millis(100),
        };
        let good = |state: &Path| {
            format!(
                "node.name = board\nsocket = {0}/orion.sock\nstate.dir = {1}\nsecurity = on\n\
                 recovery.keys = {0}/recovery_keys\nssh.helper = {2}\n",
                dir.display(),
                state.display(),
                helper().display()
            )
        };

        // A broken config: a setting that is not one. Nothing in the file is believed,
        // so security is on, and only the repairer gets in.
        let broken = settings(&format!(
            "{}security = sometimes\n",
            good(&dir.join("state"))
        ));
        assert!(broken.is_err());
        let started = start_or_recover(broken, with()).await;
        let (addr, why) = in_recovery(&started).await;
        assert!(
            why.contains("the settings cannot be read") && why.contains("security"),
            "{why}"
        );
        assert_eq!(
            said(&ssh(&dir, addr, Some(&repairer), "echo in").await),
            "in\n"
        );
        assert_eq!(said(&ssh(&dir, addr, Some(&stranger), "echo in").await), "");
        assert_eq!(said(&ssh(&dir, addr, None, "echo in").await), "");
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
        let started = start_or_recover(settings(&good(&dir.join("not-a-directory"))), with()).await;
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
        let started = start_or_recover(settings(&good(&dir.join("rubbish"))), with()).await;
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
            let started = start_or_recover(settings(&good(&dir.join("full"))), with()).await;
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
            std::fs::set_permissions(dir.join("full"), std::fs::Permissions::from_mode(0o755))
                .expect("mode");
        }

        // A state partition that is not mounted: the directory would be on the root file
        // system. Only where the test's directory is on it (not where /tmp is its own).
        let device = |path: &Path| {
            std::os::unix::fs::MetadataExt::dev(&std::fs::metadata(path).expect("stat"))
        };
        if device(&dir) == device(Path::new("/")) {
            let text = format!(
                "{}state.require_mount = true\n",
                good(&dir.join("data/orion"))
            );
            let started = start_or_recover(settings(&text), with()).await;
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
        let _ = std::fs::remove_dir_all(dir);
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
        let _ = std::fs::remove_dir_all(dir);
    });
}

//! The login helper decides every login again, under each state of the security switch, and
//! starts nothing else: who it refuses, what it starts, who may ask, and when it goes.
//!
//! Unprivileged: the helper runs as the user running the tests, so a login it starts is as
//! that user. Becoming another account needs root, which the board run proves.

#![cfg(target_os = "linux")]

use std::io::Read;
use std::os::unix::net::UnixListener;
use std::path::Path;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use orion_check::Security;
use orion_keys::{Keys, KeysConfig};
use orion_scratch::Scratch;
use orion_ssh::login::{Ask, HelperConfig, LoginClient, RecoveryLogin, Want, Who, serve};
use orion_ssh::{Helper, Io, SshConfig, users};
use orion_types::KeyId;

/// The account the tests run as.
fn me() -> users::User {
    users::lookup_uid(Path::new("/etc"), rustix::process::getuid().as_raw())
        .expect("the user running the tests has an account")
}

fn ssh_config() -> SshConfig {
    SshConfig {
        listen: "127.0.0.1:0".parse().expect("addr"),
        interface: None,
        host_key: std::env::temp_dir().join("orion-login-unused-host-key"),
        helper: Helper {
            program: env!("CARGO_BIN_EXE_orion-ssh-helper").into(),
            args: Vec::new(),
        },
        subsystems: Vec::new(),
        forward: false,
        etc: "/etc".into(),
        recovery: false,
        max_connections: 4,
        max_unauthenticated: 4,
        login: Duration::from_secs(60),
    }
}

/// The security switch's record, as the board would have it.
#[derive(Clone, Copy, Debug)]
enum Switch {
    /// No record: off.
    NotTold,
    Off,
    On,
    /// A record that is not one: on.
    Damaged,
}

const SWITCHES: [Switch; 4] = [Switch::NotTold, Switch::Off, Switch::On, Switch::Damaged];

impl Switch {
    fn on(self) -> bool {
        matches!(self, Self::On | Self::Damaged)
    }
}

/// An operator key granted a shell as the test's account, and one the store does not know.
const GRANTED: [u8; 32] = [7; 32];
const UNKNOWN: [u8; 32] = [9; 32];
/// A recovery key: trusted with everything, and only while the comm core is in recovery.
const RECOVERY: [u8; 32] = [5; 32];

/// The recovery way in's reading of its keys file, as this test has it: the file holds one
/// key in hex, trusted with everything.
fn recovery_trust(path: &Path) -> Option<Keys> {
    let text = std::fs::read_to_string(path).ok()?;
    let key: [u8; 32] = (0..32)
        .map(|at| u8::from_str_radix(text.trim().get(at * 2..at * 2 + 2)?, 16).ok())
        .collect::<Option<Vec<u8>>>()?
        .try_into()
        .ok()?;
    let mut store = Keys::open(KeysConfig::in_memory("recovery"), Security::On).ok()?;
    store.approve_if_new(key, "recovery").ok()?;
    store.grant(&KeyId(key), true, Vec::new(), true).ok()?;
    Some(store)
}

fn recovery_login(dir: &Path) -> RecoveryLogin {
    let keys_file = dir.join("recovery_keys");
    let hex: String = RECOVERY.iter().map(|byte| format!("{byte:02x}")).collect();
    std::fs::write(&keys_file, hex).expect("recovery keys");
    RecoveryLogin {
        ssh: SshConfig {
            recovery: true,
            ..ssh_config()
        },
        keys_file,
        trust: recovery_trust,
    }
}

struct Lab {
    client: LoginClient,
    helper: Option<JoinHandle<std::io::Result<()>>>,
    // Last, so that it goes after the helper's thread and the client that use it.
    dir: Scratch,
}

impl Lab {
    /// Waits for the helper to exit, up to `within`; whether it did.
    fn exited_within(&mut self, within: Duration) -> bool {
        let until = Instant::now() + within;
        while Instant::now() < until {
            if self.helper.as_ref().is_some_and(JoinHandle::is_finished) {
                let done = self.helper.take().expect("the helper").join();
                assert!(matches!(done, Ok(Ok(()))), "{done:?}");
                return true;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        false
    }
}

fn lab(name: &str, switch: Switch, comm_uid: u32, idle: Duration) -> Lab {
    let dir = Scratch::new(&format!("login-{name}"));
    std::fs::create_dir_all(dir.join("keys")).expect("directory");
    let view = dir.join("security");
    match switch {
        Switch::NotTold => {}
        Switch::Off => std::fs::write(&view, "off\ngeneration 1\n").expect("record"),
        Switch::On => std::fs::write(&view, "on\ngeneration 1\n").expect("record"),
        Switch::Damaged => std::fs::write(&view, "maybe\n").expect("record"),
    }
    let keys = KeysConfig {
        state_dir: Some(dir.join("keys")),
        ..KeysConfig::in_memory("lab")
    };
    // The comm core's store, as it would have kept it: one operator granted a shell as the
    // test's account and nothing else.
    let mut store = Keys::open(keys.clone(), Security::On).expect("keys");
    store.approve_if_new(GRANTED, "pat").expect("enrolled");
    let granted = store
        .grant(
            &KeyId(GRANTED),
            false,
            vec![format!("shell:{}", me().name)],
            false,
        )
        .expect("granted");
    assert!(granted);
    drop(store);
    let path = dir.join("login.sock");
    let listener = UnixListener::bind(&path).expect("listen");
    let config = HelperConfig {
        ssh: Some(ssh_config()),
        shell: Some(ssh_config()),
        sandbox_user: me().name,
        keys,
        security_view: view,
        comm_uid,
        idle,
        recovery: recovery_login(&dir),
    };
    let helper = std::thread::spawn(move || serve(&config, &listener));
    Lab {
        client: LoginClient::new(path),
        helper: Some(helper),
        dir,
    }
}

fn shell(account: &str, who: Who) -> Ask {
    Ask::Shell {
        account: account.to_owned(),
        who,
        want: Want::Command(b"echo started".to_vec()),
        term: None,
        recovery: false,
    }
}

fn local(uid: u32) -> Who {
    Who::Local { uid, gid: me().gid }
}

/// Asks, and gives what the session printed and its code, or the refusal's words.
fn outcome(lab: &Lab, ask: &Ask) -> Result<(String, u32), String> {
    let started = lab.client.session(ask).map_err(|error| error.to_string())?;
    let Io::Pipes { output, .. } = started.io else {
        panic!("a command runs on pipes");
    };
    let mut printed = String::new();
    std::fs::File::from(output)
        .read_to_string(&mut printed)
        .expect("output");
    let mut code = [0_u8; 4];
    std::fs::File::from(started.status)
        .read_exact(&mut code)
        .expect("status");
    Ok((printed, u32::from_le_bytes(code)))
}

fn started(lab: &Lab, ask: &Ask) {
    assert_eq!(
        outcome(lab, ask),
        Ok(("started\n".to_owned(), 0)),
        "{ask:?} should have started"
    );
}

fn refused(lab: &Lab, ask: &Ask, words: &str) {
    match outcome(lab, ask) {
        Err(why) => assert!(
            why.contains("the login helper refused") && why.contains(words),
            "{ask:?}: {why}"
        ),
        Ok(done) => panic!("{ask:?} should have been refused, and ran: {done:?}"),
    }
}

/// Under every state of the switch, the helper reaches the comm core's decision: with it off
/// (or not told), what the board lets in with security off; with it on (or its record
/// damaged), only who the check admits.
#[test]
fn the_helper_decides_as_the_comm_core_does_under_each_state_of_the_switch() {
    let me = me();
    let uid = rustix::process::getuid().as_raw();
    for switch in SWITCHES {
        let lab = lab(&format!("{switch:?}"), switch, uid, Duration::from_secs(30));
        // A program on the board, as itself: always.
        started(&lab, &shell(&me.name, local(me.uid)));
        // An operator granted a shell as this account: always.
        started(&lab, &shell(&me.name, Who::Operator(GRANTED)));
        if switch.on() {
            // Nobody known, and a key the store does not hold: refused by the check.
            refused(&lab, &shell(&me.name, Who::Anonymous), "a shell as");
            refused(&lab, &shell(&me.name, Who::Operator(UNKNOWN)), "a shell as");
            // An SSH login with no key: refused; logins are for operators.
            let ssh = Ask::Ssh {
                account: me.name.clone(),
                key: None,
                peer: None,
                local: "127.0.0.1:22".parse().expect("addr"),
                want: Want::Command(b"echo started".to_vec()),
                term: None,
                recovery: false,
            };
            refused(&lab, &ssh, "an SSH login");
        } else {
            // With security off the check admits anyone, as it does everywhere.
            started(&lab, &shell(&me.name, Who::Anonymous));
            started(&lab, &shell(&me.name, Who::Operator(UNKNOWN)));
        }
        // The wrong user: a program on the board asking to be someone else, under any switch.
        refused(&lab, &shell(&me.name, local(me.uid + 1)), "is not let in");
        // Root, when it is not allowed: a program that is not root asking for root, and an
        // operator granted only this account.
        refused(&lab, &shell("root", local(me.uid)), "is not let in");
        if switch.on() {
            refused(
                &lab,
                &shell("root", Who::Operator(GRANTED)),
                "a shell as root",
            );
        }
    }
}

/// Only the comm core's user may ask: anyone else is refused before what they asked is read.
#[test]
fn only_the_comm_cores_user_may_ask() {
    let uid = rustix::process::getuid().as_raw();
    let lab = lab("asker", Switch::Off, uid + 1, Duration::from_secs(30));
    refused(
        &lab,
        &shell(&me().name, local(uid)),
        "only the comm core's user",
    );
}

/// A connection's child is started as `ssh.sandbox_user` and nobody else: the question names
/// no account, and a sandbox user with rights is refused.
#[test]
fn a_connections_child_is_never_an_account_with_rights() {
    let uid = rustix::process::getuid().as_raw();
    let lab = lab("sandbox", Switch::Off, uid, Duration::from_secs(30));
    let root = HelperConfig {
        sandbox_user: "root".to_owned(),
        ..helper_config(&lab)
    };
    let path = lab.dir.join("root.sock");
    let listener = UnixListener::bind(&path).expect("listen");
    std::thread::spawn(move || serve(&root, &listener));
    let (client, _) = std::os::unix::net::UnixStream::pair().expect("pair");
    let (socket, _) = std::os::unix::net::UnixStream::pair().expect("pair");
    let asked = LoginClient::new(&path).sandbox(client.into(), socket.into());
    assert!(
        asked
            .as_ref()
            .is_err_and(|error| error.to_string().contains("not an account without rights")),
        "{asked:?}"
    );
}

fn helper_config(lab: &Lab) -> HelperConfig {
    HelperConfig {
        ssh: Some(ssh_config()),
        shell: Some(ssh_config()),
        sandbox_user: me().name,
        keys: KeysConfig {
            state_dir: Some(lab.dir.join("keys")),
            ..KeysConfig::in_memory("lab")
        },
        security_view: lab.dir.join("security"),
        comm_uid: rustix::process::getuid().as_raw(),
        idle: Duration::from_secs(30),
        recovery: recovery_login(&lab.dir),
    }
}

/// Idle, it exits: nothing running and nothing asked for `idle`. A session that runs holds it.
#[test]
fn the_helper_exits_when_idle_and_not_while_a_session_runs() {
    let uid = rustix::process::getuid().as_raw();
    let me = me();
    let mut lab = lab("idle", Switch::Off, uid, Duration::from_millis(300));
    // A session that runs until it is closed.
    let running = lab
        .client
        .session(&Ask::Shell {
            account: me.name.clone(),
            who: local(me.uid),
            want: Want::Command(b"sleep 30".to_vec()),
            term: None,
            recovery: false,
        })
        .expect("started");
    assert!(
        !lab.exited_within(Duration::from_secs(1)),
        "the helper exited while a session ran"
    );
    // Closing its status socket ends it, and then the helper goes.
    drop(running);
    assert!(
        lab.exited_within(Duration::from_secs(5)),
        "the helper stayed with nothing to do"
    );
}

fn ssh_as(account: &str, key: [u8; 32], recovery: bool) -> Ask {
    Ask::Ssh {
        account: account.to_owned(),
        key: Some(key),
        peer: None,
        local: "127.0.0.1:22".parse().expect("addr"),
        want: Want::Command(b"echo started".to_vec()),
        term: None,
        recovery,
    }
}

/// A comm core in recovery is answered by the recovery keys, whatever the switch: a recovery
/// key logs in, a key the recovery keys do not name does not, and outside recovery the
/// recovery key is nobody.
#[test]
fn in_recovery_the_recovery_keys_decide() {
    let uid = rustix::process::getuid().as_raw();
    let me = me();
    for switch in SWITCHES {
        let lab = lab(
            &format!("recovery-{switch:?}"),
            switch,
            uid,
            Duration::from_secs(30),
        );
        started(&lab, &ssh_as(&me.name, RECOVERY, true));
        started(
            &lab,
            &Ask::Shell {
                account: me.name.clone(),
                who: Who::Operator(RECOVERY),
                want: Want::Command(b"echo started".to_vec()),
                term: None,
                recovery: true,
            },
        );
        if switch.on() {
            refused(&lab, &ssh_as(&me.name, UNKNOWN, true), "an SSH login");
            // The operator the board's own store trusts is not a recovery key.
            refused(&lab, &ssh_as(&me.name, GRANTED, true), "an SSH login");
            refused(&lab, &ssh_as(&me.name, RECOVERY, false), "an SSH login");
        }
    }
}

//! A shell over an Orion session, end to end: the comm core as a board runs it, the client
//! as a tool uses it, and the same in recovery.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use orion::shell::{Event, Terminal};
use orion_comm_app::{CommSettings, RecoverWith, Started, start_or_recover};
use orion_config::{ConfigError, Source};

const WAIT: Duration = Duration::from_secs(10);

fn temp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("orion-comm-shell-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    dir
}

fn helper() -> PathBuf {
    env!("CARGO_BIN_EXE_orion-comm-helper").into()
}

fn me() -> String {
    let uid = rustix::process::getuid().as_raw();
    orion_ssh_users(uid)
}

/// The test's own account name, from the account file.
fn orion_ssh_users(uid: u32) -> String {
    std::fs::read_to_string("/etc/passwd")
        .expect("passwd")
        .lines()
        .find_map(|line| {
            let mut parts = line.split(':');
            let name = parts.next()?;
            let found = parts.nth(1)?.parse::<u32>().ok()? == uid;
            found.then(|| name.to_owned())
        })
        .expect("the user running the tests has an account")
}

fn settings(text: &str) -> Result<CommSettings, ConfigError> {
    let mut source = Source::parse(text, Vec::new())?;
    CommSettings::from_source(&mut source)
}

fn run(test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, test);
}

/// Runs `command` in a shell on the board at `socket`, as this account, writing `input`
/// first: its output, its errors and its exit code.
async fn shell(
    socket: &Path,
    command: &'static str,
    terminal: Option<Terminal>,
    input: &'static [u8],
) -> (String, String, u32) {
    let address: orion::Address = socket.display().to_string().parse().expect("address");
    tokio::task::spawn_blocking(move || {
        let began = Instant::now();
        let orion = loop {
            match orion::Orion::connect(&address, "test") {
                Ok(orion) => break orion,
                Err(error) => assert!(began.elapsed() < WAIT, "connect: {error}"),
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        let mut shell = orion
            .shell(&me(), Some(command.as_bytes()), terminal.as_ref())
            .expect("a shell");
        shell.write(input).expect("input");
        if terminal.is_none() {
            shell.end_input().expect("end");
        }
        let (mut out, mut errors) = (Vec::new(), Vec::new());
        let began = Instant::now();
        loop {
            assert!(began.elapsed() < WAIT, "the shell never ended");
            match shell.next(Duration::from_millis(200)).expect("the session") {
                Some(Event::Output(bytes)) => out.extend(bytes),
                Some(Event::Errors(bytes)) => errors.extend(bytes),
                Some(Event::Exit(code)) => {
                    return (
                        String::from_utf8_lossy(&out).into_owned(),
                        String::from_utf8_lossy(&errors).into_owned(),
                        code,
                    );
                }
                None => {}
            }
        }
    })
    .await
    .expect("asked")
}

#[test]
fn a_tool_gets_a_shell_on_the_board_and_in_recovery() {
    run(async {
        let dir = temp("e2e");
        let socket = dir.join("board.sock");
        let good = format!(
            "node.name = board\nsocket = {}\nstate.dir = {}/state\nsecurity = on\n\
             ssh.helper = {}\nrecovery.keys = {}/recovery_keys\n",
            socket.display(),
            dir.display(),
            helper().display(),
            dir.display()
        );
        let with = || RecoverWith {
            listen: vec!["127.0.0.1:0".parse().expect("addr")],
            keys_file: dir.join("recovery_keys"),
            helper: Some(helper()),
            retry: Duration::from_millis(100),
            last_good: dir.join("state/last-good.carriers"),
        };

        // Up as told: a command with its output, errors and code, input and its end.
        let started = start_or_recover(settings(&good), with()).await;
        assert!(matches!(started, Started::Core(_)), "{started:?}");
        let (out, errors, code) =
            shell(&socket, "cat; echo bad >&2; exit 4", None, b"typed\n").await;
        assert_eq!(
            (out.as_str(), errors.as_str(), code),
            ("typed\n", "bad\n", 4)
        );
        // On a terminal: what is typed is read by the program.
        let terminal = Terminal {
            term: "xterm".into(),
            cols: 100,
            rows: 30,
        };
        let (out, _, code) = shell(
            &socket,
            "read line; echo got:$line; stty size",
            Some(terminal.clone()),
            b"abc\n",
        )
        .await;
        assert!(out.contains("got:abc") && out.contains("30 100"), "{out:?}");
        assert_eq!(code, 0);
        drop(started);
        tokio::time::sleep(Duration::from_millis(50)).await;

        // `shell = off`: no shell.
        let off = start_or_recover(settings(&format!("{good}shell = off\n")), with()).await;
        assert!(matches!(off, Started::Core(_)), "{off:?}");
        let address: orion::Address = socket.display().to_string().parse().expect("address");
        let refused = tokio::task::spawn_blocking(move || {
            let orion = orion::Orion::connect(&address, "test").expect("connect");
            orion.shell(&me(), Some(b"true"), None).map(drop)
        })
        .await
        .expect("asked");
        // Told at once, not left to wait.
        assert!(
            format!("{refused:?}").contains("Refused(Unsupported)"),
            "{refused:?}"
        );
        drop(off);
        tokio::time::sleep(Duration::from_millis(50)).await;

        // In recovery, on the socket of the last good start: a shell still.
        let started = start_or_recover(settings("node.name board\n[[[\n"), with()).await;
        assert!(matches!(started, Started::Recovery(_)), "{started:?}");
        let (out, _, code) = shell(&socket, "echo in recovery", None, b"").await;
        assert_eq!((out.as_str(), code), ("in recovery\n", 0));
        drop(started);
        let _ = std::fs::remove_dir_all(dir);
    });
}

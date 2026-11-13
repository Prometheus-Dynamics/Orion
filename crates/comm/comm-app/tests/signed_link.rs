//! A signed session on a USB link, end to end: a tool that holds an operator's key gets a shell
//! on a board with security on, over the link and nothing else, and in recovery with a recovery
//! key. Without a key it is the link's device, and gets none.

use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use orion::{Identity, Options};
use orion_comm_app::{CommSettings, RecoverWith, Started, start_or_recover};
use orion_config::{ConfigError, Source};
use orion_connect::Session;
use orion_msg_session::{LinkOffer, LinkTransport};
use orion_wire::kind;

const WAIT: Duration = Duration::from_secs(10);

fn temp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("orion-signed-link-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    dir
}

fn helper() -> PathBuf {
    env!("CARGO_BIN_EXE_orion-comm-helper").into()
}

/// The test's own account name.
fn me() -> String {
    let uid = rustix::process::getuid().as_raw();
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

/// `key` as a line of an OpenSSH public keys file.
fn openssh(key: [u8; 32]) -> String {
    let mut blob = Vec::new();
    for part in [&b"ssh-ed25519"[..], &key[..]] {
        blob.extend_from_slice(&(part.len() as u32).to_be_bytes());
        blob.extend_from_slice(part);
    }
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut text = String::new();
    for chunk in blob.chunks(3) {
        let bytes = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let word = u32::from_be_bytes([0, bytes[0], bytes[1], bytes[2]]);
        for at in 0..4 {
            if at <= chunk.len() {
                text.push(ALPHABET[(word >> (18 - 6 * at) & 63) as usize] as char);
            } else {
                text.push('=');
            }
        }
    }
    format!("ssh-ed25519 {text} me@bench\n")
}

/// A cable plugged in: the board's end of a new stream handed to the comm core on `socket` as
/// the USB link `usb0`, as the program that owns the gadget does. The tool's end comes back.
async fn plug_in(socket: &Path) -> OwnedFd {
    let address: orion::Address = socket.display().to_string().parse().expect("address");
    tokio::task::spawn_blocking(move || {
        let (board, tool) = UnixStream::pair().expect("pair");
        let began = Instant::now();
        let mut session = loop {
            match Session::connect(&address, "gadget", Options::default()) {
                Ok(session) => break session,
                Err(error) => assert!(began.elapsed() < WAIT, "connect: {error}"),
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        let offer = LinkOffer {
            link: "usb0",
            transport: LinkTransport::Stream,
            interface: 0,
            endpoint_in: 0,
            endpoint_out: 0,
        };
        let seq = session
            .send_with_fds(&offer, &[std::os::fd::AsFd::as_fd(&board)])
            .expect("offer");
        session.answer(seq, kind::LINK_TAKEN).expect("taken");
        OwnedFd::from(tool)
    })
    .await
    .expect("plugged in")
}

/// Over the link at `tool`, with `identity` if any: enrolls with `enrollment` if given, then
/// runs `command` in a shell as this account. Its output and code, or why there was none.
async fn over_link(
    tool: OwnedFd,
    identity: Option<&Identity>,
    enrollment: Option<&'static [u8]>,
    command: &'static str,
) -> Result<(String, u32), String> {
    let identity = identity.map(|identity| Identity::from_secret(&identity.secret()));
    tokio::task::spawn_blocking(move || {
        let public = identity.as_ref().map(Identity::public);
        let options = Options {
            identity,
            ..Options::default()
        };
        let session =
            Session::over_link(tool, "tool", options).map_err(|error| error.to_string())?;
        let orion = orion::Orion::over(session);
        if let (Some(key), Some(enrollment)) = (public, enrollment) {
            orion
                .enroll(key, "me", Some(enrollment))
                .map_err(|error| error.to_string())?;
        }
        let mut shell = orion
            .shell(&me(), Some(command.as_bytes()), None)
            .map_err(|error| format!("{error:?}"))?;
        shell.end_input().map_err(|error| error.to_string())?;
        let mut out = Vec::new();
        let began = Instant::now();
        loop {
            assert!(began.elapsed() < WAIT, "the shell never ended");
            match shell
                .next(Duration::from_millis(200))
                .map_err(|error| error.to_string())?
            {
                Some(orion::shell::Event::Output(bytes)) => out.extend(bytes),
                Some(orion::shell::Event::Exit(code)) => {
                    return Ok((String::from_utf8_lossy(&out).into_owned(), code));
                }
                _ => {}
            }
        }
    })
    .await
    .expect("asked")
}

#[test]
fn a_tool_with_a_key_gets_a_shell_over_a_usb_link_with_security_on_and_in_recovery() {
    run(async {
        let dir = temp("e2e");
        let socket = dir.join("board.sock");
        let good = format!(
            "node.name = board\nsocket = {}\nstate.dir = {}/state\nsecurity = on\n\
             ssh.helper = {}\nrecovery.keys = {}/recovery_keys\nlinks.usb = usb0\n\
             enrollment.key = sesame\noperators.actions = login\n",
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
        let operator = Identity::generate();
        let stranger = Identity::generate();

        let started = start_or_recover(settings(&good), with()).await;
        assert!(matches!(started, Started::Core(_)), "{started:?}");

        // No key: the device on the link, which no shell is for.
        let refused = over_link(plug_in(&socket).await, None, None, "true").await;
        assert!(
            refused.as_ref().is_err_and(|why| why.contains("Denied")),
            "{refused:?}"
        );
        // A key, enrolled over the link itself with the enrollment key: the operator, who may
        // log in, so a shell.
        let signed = over_link(
            plug_in(&socket).await,
            Some(&operator),
            Some(b"sesame"),
            "echo signed; id -un",
        )
        .await;
        assert_eq!(signed, Ok((format!("signed\n{}\n", me()), 0)));
        // Again, enrolled already.
        let again = over_link(plug_in(&socket).await, Some(&operator), None, "echo again").await;
        assert_eq!(again, Ok(("again\n".to_owned(), 0)));
        // A key nobody granted anything.
        let refused = over_link(plug_in(&socket).await, Some(&stranger), None, "true").await;
        assert!(
            refused.as_ref().is_err_and(|why| why.contains("Denied")),
            "{refused:?}"
        );
        drop(started);
        tokio::time::sleep(Duration::from_millis(50)).await;

        // In recovery, on the link of the last good start: the recovery key may, the trust
        // store is not read, and another key may not.
        std::fs::write(dir.join("recovery_keys"), openssh(operator.public())).expect("keys");
        let started = start_or_recover(settings("node.name board\n[[[\n"), with()).await;
        assert!(matches!(started, Started::Recovery(_)), "{started:?}");
        let rescued = over_link(
            plug_in(&socket).await,
            Some(&operator),
            None,
            "echo in recovery",
        )
        .await;
        assert_eq!(rescued, Ok(("in recovery\n".to_owned(), 0)));
        let refused = over_link(plug_in(&socket).await, Some(&stranger), None, "true").await;
        assert!(
            refused.as_ref().is_err_and(|why| why.contains("Denied")),
            "{refused:?}"
        );
        drop(started);
        let _ = std::fs::remove_dir_all(dir);
    });
}

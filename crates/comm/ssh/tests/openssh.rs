//! The server with OpenSSH's own `ssh` as the client: logins through the check, commands,
//! a shell on a terminal, and the limits before a login. If `ssh` is not installed the
//! tests say so and pass.
mod common;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use common::{Server, installed, keygen, me, temp, text, trust};
use orion_check::Security;
use orion_keys::{Keys, KeysConfig, SharedKeys};

#[test]
fn an_operator_trusted_with_everything_logs_in_and_runs_commands_as_the_user() {
    if !installed() {
        return;
    }
    let dir = temp("commands");
    let (keys, admin, reader, stranger) = trust(&dir);
    let server = Server::start(&dir, Security::On, keys, |_| {});
    let me = me();

    // No command and no terminal: a login shell that reads its commands from its input.
    // What the account's own start-up files print or leave as a status is theirs.
    let ran = server.ssh(&me.name, Some(&admin), &[], "echo from-input\nexit 4\n");
    assert_eq!(ran.status.code(), Some(4), "{}", text(&ran.stderr));
    assert!(
        text(&ran.stdout).contains("from-input"),
        "{}",
        text(&ran.stdout)
    );

    // A command: its output, its errors apart, its exit code, and who it ran as.
    let ran = server.ssh(
        &me.name,
        Some(&admin),
        &[
            "--",
            "echo out; echo err >&2; id -u; echo \"$USER $HOME $SHELL\"; pwd; exit 7",
        ],
        "",
    );
    assert_eq!(ran.status.code(), Some(7), "{}", text(&ran.stderr));
    assert_eq!(
        text(&ran.stdout),
        format!(
            "out\n{}\n{} {} {}\n{}\n",
            me.uid,
            me.name,
            me.home.display(),
            me.shell.display(),
            me.home.display()
        )
    );
    assert_eq!(text(&ran.stderr), "err\n");

    // Its input reaches it, and its end is the command's end.
    let ran = server.ssh(
        &me.name,
        Some(&admin),
        &["--", "wc -c; cat"],
        &"x".repeat(100_000),
    );
    assert_eq!(text(&ran.stdout).trim(), "100000");
    // A lot of output, all of it and in order.
    let ran = server.ssh(&me.name, Some(&admin), &["--", "seq 1 200000"], "");
    let lines: Vec<String> = text(&ran.stdout).lines().map(str::to_owned).collect();
    assert_eq!(lines.len(), 200_000);
    assert!(
        lines
            .iter()
            .enumerate()
            .all(|(at, line)| *line == (at + 1).to_string())
    );
    // Ended by a signal: what a shell reports.
    let ran = server.ssh(&me.name, Some(&admin), &["--", "kill -TERM $$"], "");
    assert_eq!(ran.status.code(), Some(128 + 15));
    // The groups of the account, all of them.
    let ran = server.ssh(&me.name, Some(&admin), &["--", "id -G"], "");
    let mut groups: Vec<u32> = text(&ran.stdout)
        .split_whitespace()
        .map(|group| group.parse().expect("a group"))
        .collect();
    groups.sort_unstable();
    let mut mine = me.groups.clone();
    mine.sort_unstable();
    if rustix::process::getuid().is_root() {
        assert_eq!(groups, mine);
    } else {
        // Not root: the session has the groups the server has, which contain the account's.
        assert!(groups.contains(&me.gid), "{groups:?}");
    }

    // Who is not let in: a key nobody enrolled, an operator that may only read, and nobody.
    for (key, who) in [
        (Some(&stranger), "a stranger"),
        (Some(&reader), "a reader"),
        (None, "nobody"),
    ] {
        let refused = server.ssh(&me.name, key.map(PathBuf::as_path), &["--", "echo in"], "");
        assert_eq!(refused.status.code(), Some(255), "{who}");
        assert_eq!(text(&refused.stdout), "", "{who}");
        assert!(
            text(&refused.stderr).contains("Permission denied"),
            "{who}: {}",
            text(&refused.stderr)
        );
    }
    // An account that is not there, and one the server cannot become.
    let refused = server.ssh("no-such-account", Some(&admin), &["--", "echo in"], "");
    assert_eq!(
        (refused.status.code(), text(&refused.stdout).as_str()),
        (Some(255), "")
    );
    if !rustix::process::getuid().is_root() {
        let refused = server.ssh("root", Some(&admin), &["--", "echo in"], "");
        assert_eq!(
            (refused.status.code(), text(&refused.stdout).as_str()),
            (Some(255), "")
        );
    }
}

#[test]
fn a_shell_runs_on_a_terminal() {
    if !installed() {
        return;
    }
    let dir = temp("terminal");
    let (keys, admin, _, _) = trust(&dir);
    let server = Server::start(&dir, Security::On, keys, |_| {});
    let me = me();
    // A login shell on a terminal, typed at.
    let typed =
        "echo term=$TERM; tty; stty size; test -t 0 && echo a-terminal; echo \"$0\"; exit 3\n";
    let ran = server.ssh(&me.name, Some(&admin), &["-tt"], typed);
    let out = text(&ran.stdout);
    assert_eq!(ran.status.code(), Some(3), "{out}\n{}", text(&ran.stderr));
    assert!(out.contains("term=xterm-256color"), "{out}");
    assert!(out.contains("/dev/pts/"), "{out}");
    assert!(out.contains("a-terminal"), "{out}");
    // A login shell: its name starts with a dash.
    let shell = me
        .shell
        .file_name()
        .expect("a shell")
        .to_string_lossy()
        .into_owned();
    assert!(out.contains(&format!("-{shell}")), "{out}");
    // A command on a terminal.
    let ran = server.ssh(&me.name, Some(&admin), &["-tt", "--", "tty; exit 5"], "");
    assert_eq!(ran.status.code(), Some(5));
    assert!(text(&ran.stdout).contains("/dev/pts/"));
}

#[test]
fn with_security_off_anyone_is_let_in() {
    if !installed() {
        return;
    }
    let dir = temp("off");
    let (stranger, _) = keygen(&dir, "stranger");
    let keys =
        SharedKeys::new(Keys::open(KeysConfig::in_memory("lab"), Security::Off).expect("keys"));
    let server = Server::start(&dir, Security::Off, keys, |_| {});
    let me = me();
    // With a key nobody knows, and with no key at all.
    for key in [Some(stranger.as_path()), None] {
        let ran = server.ssh(&me.name, key, &["--", "echo in"], "");
        assert_eq!(text(&ran.stdout), "in\n", "{}", text(&ran.stderr));
    }
    // The account still has to be one, and one the server can become.
    let refused = server.ssh("no-such-account", None, &["--", "echo in"], "");
    assert_eq!(refused.status.code(), Some(255));
}

#[test]
fn the_host_key_is_kept_and_a_broken_one_is_replaced() {
    if !installed() {
        return;
    }
    let dir = temp("hostkey");
    let (keys, admin, _, _) = trust(&dir);
    let me = me();
    let first = Server::start(&dir, Security::On, keys.clone(), |_| {});
    assert_eq!(
        text(
            &first
                .ssh(&me.name, Some(&admin), &["--", "echo in"], "")
                .stdout
        ),
        "in\n"
    );
    let kept = std::fs::read(dir.join("host_key")).expect("the host key");
    {
        use std::os::unix::fs::MetadataExt;
        let mode = std::fs::metadata(dir.join("host_key"))
            .expect("metadata")
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }
    let port = first.addr.port();
    drop(first);

    // The same key after a restart: a client that insists on the key it knows gets in.
    let again = Server::start(&dir, Security::On, keys.clone(), |config| {
        config.listen = format!("127.0.0.1:{port}").parse().expect("addr");
    });
    assert_eq!(
        std::fs::read(dir.join("host_key")).expect("the host key"),
        kept
    );
    let strict = ["-o", "StrictHostKeyChecking=yes", "--", "echo in"];
    assert_eq!(
        text(&again.ssh(&me.name, Some(&admin), &strict, "").stdout),
        "in\n"
    );
    drop(again);

    // A key that is not a key: moved aside, another made, and the board still lets in.
    std::fs::write(dir.join("host_key"), "not a key").expect("write");
    let broken = Server::start(&dir, Security::On, keys.clone(), |config| {
        config.listen = format!("127.0.0.1:{port}").parse().expect("addr");
    });
    let aside = std::fs::read_dir(&dir)
        .expect("dir")
        .filter_map(Result::ok)
        .any(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("host_key.corrupt-")
        });
    assert!(aside);
    assert_ne!(
        std::fs::read(dir.join("host_key")).expect("the host key"),
        kept
    );
    // The client that insists is told the key changed; one that does not gets in.
    let refused = broken.ssh(&me.name, Some(&admin), &strict, "");
    assert_eq!(refused.status.code(), Some(255));
    std::fs::remove_file(dir.join("known_hosts")).expect("forget");
    assert_eq!(
        text(
            &broken
                .ssh(&me.name, Some(&admin), &["--", "echo in"], "")
                .stdout
        ),
        "in\n"
    );
    drop(broken);

    // A key that cannot be kept, because where it goes cannot be written: one for the run.
    let nowhere = Server::start(&dir, Security::On, keys, |config| {
        config.host_key = "/proc/orion-no-such-place/host_key".into();
    });
    std::fs::remove_file(dir.join("known_hosts")).expect("forget");
    assert_eq!(
        text(
            &nowhere
                .ssh(&me.name, Some(&admin), &["--", "echo in"], "")
                .stdout
        ),
        "in\n"
    );
}

#[test]
fn connections_that_do_not_log_in_are_few_and_do_not_have_long() {
    if !installed() {
        return;
    }
    let dir = temp("limits");
    let (keys, admin, _, _) = trust(&dir);
    let server = Server::start(&dir, Security::On, keys, |config| {
        config.max_unauthenticated = 3;
        config.login = Duration::from_secs(1);
    });
    let me = me();
    // Three connections that say nothing take every place there is before a login.
    let idle: Vec<std::net::TcpStream> = (0..3)
        .map(|_| std::net::TcpStream::connect(server.addr).expect("connect"))
        .collect();
    std::thread::sleep(Duration::from_millis(200));
    let refused = server.ssh(&me.name, Some(&admin), &["--", "echo in"], "");
    assert_eq!(
        (refused.status.code(), text(&refused.stdout).as_str()),
        (Some(255), "")
    );
    // They do not have long: each is closed, and there is room again.
    let began = Instant::now();
    for mut stream in idle {
        use std::io::Read;
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .expect("timeout");
        let mut rest = Vec::new();
        // The server's greeting, and then the end.
        let _ = stream.read_to_end(&mut rest);
        assert!(rest.starts_with(b"SSH-2.0-"), "{}", text(&rest));
    }
    assert!(
        began.elapsed() < Duration::from_secs(8),
        "{:?}",
        began.elapsed()
    );
    assert_eq!(
        text(
            &server
                .ssh(&me.name, Some(&admin), &["--", "echo in"], "")
                .stdout
        ),
        "in\n"
    );
    // One that is logged in stays past the time a login has.
    let ran = server.ssh(&me.name, Some(&admin), &["--", "sleep 2; echo still"], "");
    assert_eq!(text(&ran.stdout), "still\n");
}

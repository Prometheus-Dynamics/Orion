//! Local forwarding with OpenSSH's own `ssh` as the client: `-W` and `-L` reach a service
//! through the board, whole and both ways, and only for someone who is logged in and only
//! when it is on. If `ssh` is not installed the tests say so and pass.

mod common;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::{Server, installed, me, temp, text, trust};
use orion_check::Security;

/// A service only the board reaches: it says who it is, then says back what it is sent,
/// in capitals, until its client is done.
fn service() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listen");
    let port = listener.local_addr().expect("addr").port();
    std::thread::spawn(move || {
        for mut stream in listener.incoming().flatten() {
            std::thread::spawn(move || {
                let _ = stream.write_all(b"service\n");
                let mut bytes = [0_u8; 4096];
                while let Ok(count @ 1..) = stream.read(&mut bytes) {
                    bytes[..count].make_ascii_uppercase();
                    if stream.write_all(&bytes[..count]).is_err() {
                        break;
                    }
                }
            });
        }
    });
    port
}

#[test]
fn a_connection_from_the_board_is_carried_both_ways() {
    if !installed() {
        return;
    }
    let dir = temp("forward");
    let (keys, admin, _, stranger) = trust(&dir);
    let server = Server::start(&dir, Security::On, keys, |_| {});
    let me = me();
    let port = service();

    // `-W`: the client's input and output are the connection.
    let target = format!("127.0.0.1:{port}");
    let ran = server.ssh(
        &me.name,
        Some(&admin),
        &["-W", &target],
        "hello through the board\n",
    );
    assert!(ran.status.success(), "{}", text(&ran.stderr));
    assert_eq!(text(&ran.stdout), "service\nHELLO THROUGH THE BOARD\n");
    // A lot, and all of it.
    let much = "abcdefghij".repeat(100_000);
    let ran = server.ssh(&me.name, Some(&admin), &["-W", &target], &much);
    assert_eq!(text(&ran.stdout).len(), "service\n".len() + much.len());
    assert!(text(&ran.stdout).ends_with(&much.to_ascii_uppercase()[much.len() - 1000..]));

    // Nothing listening there: the client is told, and nothing is carried.
    let closed = TcpListener::bind("127.0.0.1:0")
        .expect("listen")
        .local_addr()
        .expect("addr")
        .port();
    let ran = server.ssh(
        &me.name,
        Some(&admin),
        &["-W", &format!("127.0.0.1:{closed}")],
        "x",
    );
    assert!(!ran.status.success());
    assert_eq!(text(&ran.stdout), "");
    // Someone who is not let in gets no connection from the board either.
    let ran = server.ssh(&me.name, Some(&stranger), &["-W", &target], "x");
    assert_eq!(
        (ran.status.code(), text(&ran.stdout).as_str()),
        (Some(255), "")
    );

    // `-L`: a port on the client's machine that is the service, for any program there.
    let local = TcpListener::bind("127.0.0.1:0")
        .expect("listen")
        .local_addr()
        .expect("addr")
        .port();
    let mut tunnel = Command::new("ssh")
        .args(["-F", "/dev/null", "-p", &server.addr.port().to_string()])
        .args([
            "-o",
            "StrictHostKeyChecking=accept-new",
            "-o",
            "BatchMode=yes",
        ])
        .args([
            "-o",
            "IdentitiesOnly=yes",
            "-o",
            "IdentityAgent=none",
            "-o",
            "LogLevel=ERROR",
        ])
        .args(["-o", "ExitOnForwardFailure=yes"])
        .arg("-o")
        .arg(format!(
            "UserKnownHostsFile={}",
            dir.join("known_hosts").display()
        ))
        .arg("-i")
        .arg(&admin)
        .args(["-N", "-L", &format!("127.0.0.1:{local}:127.0.0.1:{port}")])
        .arg(format!("{}@127.0.0.1", me.name))
        .env_remove("SSH_AUTH_SOCK")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("ssh");
    let began = Instant::now();
    let mut through = loop {
        match TcpStream::connect(("127.0.0.1", local)) {
            Ok(stream) => break stream,
            Err(error) => {
                assert!(
                    began.elapsed() < Duration::from_secs(10),
                    "the tunnel never opened: {error}"
                );
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    };
    through
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("timeout");
    let mut greeting = [0_u8; 8];
    through.read_exact(&mut greeting).expect("a greeting");
    assert_eq!(&greeting, b"service\n");
    // Two connections at once through the one tunnel, each its own.
    let mut second = TcpStream::connect(("127.0.0.1", local)).expect("connect");
    second
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("timeout");
    second.read_exact(&mut greeting).expect("a greeting");
    through.write_all(b"first").expect("write");
    second.write_all(b"second").expect("write");
    let (mut one, mut two) = ([0_u8; 5], [0_u8; 6]);
    through.read_exact(&mut one).expect("read");
    second.read_exact(&mut two).expect("read");
    assert_eq!((&one, &two), (b"FIRST", b"SECOND"));
    let _ = tunnel.kill();
    let _ = tunnel.wait();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn forwarding_that_is_off_is_refused() {
    if !installed() {
        return;
    }
    let dir = temp("forward-off");
    let (keys, admin, _, _) = trust(&dir);
    let server = Server::start(&dir, Security::On, keys, |config| config.forward = false);
    let me = me();
    let port = service();
    let ran = server.ssh(
        &me.name,
        Some(&admin),
        &["-W", &format!("127.0.0.1:{port}")],
        "x",
    );
    assert!(!ran.status.success());
    assert_eq!(text(&ran.stdout), "");
    assert!(
        text(&ran.stderr).contains("forwarding failed"),
        "{}",
        text(&ran.stderr)
    );
    // A shell is still to be had.
    let ran = server.ssh(&me.name, Some(&admin), &["--", "echo in"], "");
    assert_eq!(text(&ran.stdout), "in\n");
    let _ = std::fs::remove_dir_all(dir);
}

//! The network and SSH ways in kept to several interfaces (`peer.interface`, `ssh.interface`):
//! one listener on each, nothing on an interface not named, an interface that is not there
//! holding up nothing and said once by its name, one that is there with no address yet
//! listened on from the start, and one that is taken away and made again under the same name
//! listened on again, with no restart.
//!
//! Interfaces are made and taken away, so the test runs itself again in a network namespace
//! of its own, as that namespace's root, and makes dummy interfaces there. Where that cannot
//! be done it says so and passes, unless `NAMESPACE_TESTS=required` is set, as the gate sets
//! it: there it fails, saying why.

use std::net::SocketAddr;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use orion_comm_app::{CommCore, CommSettings};
use orion_config::Source;
use orion_scratch::Scratch;
use orion_server::Retry;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

/// Set when the test runs again inside its own namespace.
const INSIDE: &str = "ORION_INTERFACES_INSIDE";

/// What the run inside says when it got to the end.
const DONE: &str = "interfaces: every way in followed its interfaces";

const PEER: u16 = 9200;
const SSH: u16 = 2222;
const TIME: u16 = 4123;

fn own_test(name: &str) -> String {
    match module_path!().split_once("::") {
        Some((_, module)) => format!("{module}::{name}"),
        None => name.to_owned(),
    }
}

fn ip(args: &str) {
    let status = Command::new("ip")
        .args(args.split_whitespace())
        .status()
        .expect("ip");
    assert!(status.success(), "ip {args}");
}

/// A dummy interface named `name`, up, at 10.77.`net`.1.
fn make(name: &str, net: u8) {
    ip(&format!("link add {name} type dummy"));
    ip(&format!("addr add 10.77.{net}.1/24 dev {name}"));
    ip(&format!("link set {name} up"));
}

fn at(net: u8, port: u16) -> SocketAddr {
    SocketAddr::from(([10, 77, net, 1], port))
}

/// Whether something listening at `addr` takes a connection.
async fn takes(addr: SocketAddr) -> bool {
    matches!(
        tokio::time::timeout(Duration::from_millis(500), TcpStream::connect(addr)).await,
        Ok(Ok(_))
    )
}

/// Waits until `addr` takes a connection, or fails the test after `within`.
async fn taken_within(addr: SocketAddr, within: Duration) {
    let deadline = Instant::now() + within;
    while !takes(addr).await {
        assert!(
            Instant::now() < deadline,
            "nothing took a connection at {addr}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Whether the clock is served at `addr`: an SNTP question there is answered as a server
/// answers.
async fn tells_the_time(addr: SocketAddr) -> bool {
    let socket = tokio::net::UdpSocket::bind("0.0.0.0:0")
        .await
        .expect("bind");
    // Version 4, a client's question.
    let mut question = [0_u8; 48];
    question[0] = (4 << 3) | 3;
    if socket.send_to(&question, addr).await.is_err() {
        return false;
    }
    let mut answer = [0_u8; 64];
    matches!(
        tokio::time::timeout(Duration::from_millis(500), socket.recv_from(&mut answer)).await,
        Ok(Ok((48, _))) if answer[0] & 7 == 4
    )
}

/// Waits until the clock is served at `addr`, or fails the test after `within`.
async fn told_within(addr: SocketAddr, within: Duration) {
    if !cfg!(feature = "time") {
        return;
    }
    let deadline = Instant::now() + within;
    while !tells_the_time(addr).await {
        assert!(Instant::now() < deadline, "no clock served at {addr}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// What SSH says first at `addr`.
async fn banner(addr: SocketAddr) -> String {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut buf = [0_u8; 64];
    let len = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
        .await
        .expect("a banner in time")
        .expect("read");
    String::from_utf8_lossy(&buf[..len]).into_owned()
}

fn settings(dir: &Path) -> CommSettings {
    // A build with no time sync does not read its keys.
    let time = if cfg!(feature = "time") {
        format!("time.serve = 0.0.0.0:{TIME}\ntime.interface = orion0, onever\n")
    } else {
        String::new()
    };
    let text = format!(
        "node.name = board\nsocket = {0}/orion.sock\nstate.dir = {0}/state\n\
         peer.listen = 0.0.0.0:{PEER}\npeer.interface = orion0, orion1, onever, obare\n\
         ssh.listen = 0.0.0.0:{SSH}\nssh.interface = orion0, orion1, onever, obare\n\
         ssh.sandbox = false\n{time}",
        dir.display()
    );
    let mut source = Source::parse(&text, Vec::new()).expect("source");
    let mut settings = CommSettings::from_source(&mut source).expect("settings");
    assert!(source.warnings().is_empty(), "{:?}", source.warnings());
    settings.listen_retry = Retry {
        first: Duration::from_millis(50),
        most: Duration::from_millis(100),
    };
    settings
}

/// The lines of the log that name `name`.
fn naming(log: &orion_log::Window, name: &str) -> Vec<String> {
    String::from_utf8_lossy(&log.read())
        .lines()
        .filter(|line| line.contains(name))
        .map(str::to_owned)
        .collect()
}

fn inside() {
    orion_log::install();
    let log = orion_log::Window::new(orion_log::WINDOW_BYTES);
    ip("link set lo up");
    make("orion0", 0);
    make("orion1", 1);
    // Up, and not named: nothing listens on it.
    make("orion2", 2);
    // Named, there and up, with no address yet: a link whose address comes later.
    ip("link add obare type dummy");
    ip("link set obare up");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, async {
        let dir = Scratch::new("interfaces");
        let core = CommCore::start(settings(dir.path()))
            .await
            .expect("starts, with an interface that is not there");
        for net in [0, 1] {
            assert!(
                takes(at(net, PEER)).await,
                "the network way in on orion{net}"
            );
            let said = banner(at(net, SSH)).await;
            assert!(said.starts_with("SSH-2.0-"), "ssh on orion{net}: {said}");
        }
        assert!(
            !takes(at(2, PEER)).await,
            "the network way in on orion2, not named"
        );
        assert!(!takes(at(2, SSH)).await, "ssh on orion2, not named");
        // The clock is served where `time.interface` says, and nowhere else.
        told_within(at(0, TIME), Duration::from_secs(5)).await;
        assert!(!tells_the_time(at(1, TIME)).await, "the clock on orion1");

        // The one that is not there is tried again many times, and said once each.
        tokio::time::sleep(Duration::from_millis(2500)).await;
        let said = naming(&log, "onever");
        let ways = if cfg!(feature = "time") { 3 } else { 2 };
        assert_eq!(said.len(), ways, "once for each way in: {said:#?}");

        // The one with no address was listened on from the start, with nothing tried again
        // (a try that failed would be said, by its name): its address comes, and the listener
        // already there takes the connection.
        let said = naming(&log, "obare");
        assert!(
            said.iter().all(|line| line.contains("obare: open on ")),
            "opened at once, and nothing else said: {said:#?}"
        );
        ip("addr add 10.77.4.1/24 dev obare");
        taken_within(at(4, PEER), Duration::from_millis(500)).await;
        taken_within(at(4, SSH), Duration::from_millis(500)).await;

        // Taken away and made again under the same name: a new interface to the kernel.
        ip("link del orion0");
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!takes(at(0, PEER)).await, "orion0 is gone");
        make("orion0", 0);
        taken_within(at(0, PEER), Duration::from_secs(10)).await;
        taken_within(at(0, SSH), Duration::from_secs(10)).await;
        let said = banner(at(0, SSH)).await;
        assert!(said.starts_with("SSH-2.0-"), "ssh on orion0 again: {said}");
        told_within(at(0, TIME), Duration::from_secs(10)).await;
        // The other went on meanwhile.
        assert!(takes(at(1, PEER)).await, "orion1 went on");

        // The one that was not there comes: it is listened on.
        make("onever", 3);
        taken_within(at(3, PEER), Duration::from_secs(10)).await;
        taken_within(at(3, SSH), Duration::from_secs(10)).await;
        told_within(at(3, TIME), Duration::from_secs(10)).await;
        drop(core);
    });
    println!("{DONE}");
}

/// Why a network namespace with interfaces of its own cannot be made here, if it cannot.
fn cannot() -> Option<String> {
    let made = Command::new("unshare")
        .args(["--map-root-user", "--net", "--"])
        .args(["ip", "link", "add", "orioncheck0", "type", "dummy"])
        .stdout(Stdio::null())
        .output();
    match made {
        Err(error) => Some(format!("unshare could not be run: {error}")),
        Ok(out) if !out.status.success() => Some(format!(
            "no network namespace with a dummy interface here: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        )),
        Ok(_) => None,
    }
}

#[test]
fn each_interface_named_gets_a_listener_that_follows_it_when_it_is_made_again() {
    if std::env::var_os(INSIDE).is_some() {
        inside();
        return;
    }
    if let Some(why) = cannot() {
        assert!(
            std::env::var_os("NAMESPACE_TESTS").is_none_or(|said| said != "required"),
            "this test must run here, and cannot: {why}"
        );
        println!("{why}: the interfaces were not tried");
        return;
    }
    let ran = Command::new("unshare")
        .args(["--map-root-user", "--net", "--"])
        .arg(std::env::current_exe().expect("this test"))
        .args([
            "--exact",
            own_test("each_interface_named_gets_a_listener_that_follows_it_when_it_is_made_again")
                .as_str(),
            "--nocapture",
        ])
        .env(INSIDE, "1")
        .output()
        .expect("the test again, inside");
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&ran.stdout),
        String::from_utf8_lossy(&ran.stderr)
    );
    assert!(ran.status.success(), "{said}");
    assert!(said.contains(DONE), "{said}");
}

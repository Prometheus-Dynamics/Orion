//! The board announced by name (`mdns`): what the comm core says of itself, `_orion._tcp`
//! with its TXT record and `_ssh._tcp` and `_sftp-ssh._tcp` at the SSH way in; nothing with
//! `mdns = false`; and, with a cluster name no announcement can carry, the name and SSH still
//! announced and `_orion._tcp` not.
//!
//! It runs itself again in a network namespace of its own, as `orion-mdns`'s test does, with a
//! veth pair: the comm core on `ana`, asked through `anb`. Where that cannot be done it says
//! so and passes, unless `NAMESPACE_TESTS=required` is set, as the gate sets it.

use std::net::{Ipv4Addr, SocketAddr};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use orion_comm_app::{CommCore, CommSettings};
use orion_config::Source;
use orion_mdns::packet::{A, Data, IN, Message, Name, PTR, Question};
use orion_mdns::{GROUP, PORT};
use orion_scratch::Scratch;
use rustix::net::sockopt;
use tokio::net::UdpSocket;
use tokio::time::{Instant, sleep, timeout};

const INSIDE: &str = "ORION_ANNOUNCE_INSIDE";
const DONE: &str = "announce: said what it is, kept quiet, and withheld what it could not carry";

fn own_test(name: &str) -> String {
    match module_path!().split_once("::") {
        Some((_, module)) => format!("{module}::{name}"),
        None => name.to_owned(),
    }
}

fn sh(line: &str) {
    let status = Command::new("sh").args(["-c", line]).status().expect("sh");
    assert!(status.success(), "{line}");
}

fn settings(dir: &Path, more: &str) -> CommSettings {
    let view = dir.join("security");
    std::fs::write(&view, "off\ngeneration 1\n").expect("security record");
    let text = format!(
        "node.name = cam\nsocket = {0}/orion.sock\nstate.dir = {0}/state\ncluster = lab\n\
         peer.listen = 0.0.0.0:9200\nssh.listen = 0.0.0.0:2222\nssh.sandbox = false\n\
         ssh.host_key = {0}/host_key\n{more}",
        dir.display()
    );
    let mut source = Source::parse(&text, Vec::new()).expect("source");
    let settings = CommSettings::from_source_with_view(&mut source, &view).expect("settings");
    assert!(source.warnings().is_empty(), "{:?}", source.warnings());
    settings
}

fn question(name: &str, kind: u16) -> Message {
    Message {
        questions: vec![Question {
            name: Name::dotted(name).expect("name"),
            kind,
            class: IN,
        }],
        ..Message::default()
    }
}

/// Asks the group through `anb`, from a port that is not mDNS's.
async fn ask(message: &Message) -> Option<Message> {
    let socket = UdpSocket::bind("0.0.0.0:0").await.expect("bind");
    let index = rustix::net::netdevice::name_to_index(&socket, "anb").expect("index");
    sockopt::set_ip_multicast_if_with_ifindex(&socket, &GROUP, &Ipv4Addr::UNSPECIFIED, index)
        .expect("multicast if");
    socket
        .send_to(&message.write(), SocketAddr::from((GROUP, PORT)))
        .await
        .expect("send");
    let mut buffer = vec![0; 9000];
    let (read, _) = timeout(Duration::from_millis(700), socket.recv_from(&mut buffer))
        .await
        .ok()?
        .ok()?;
    Message::read(&buffer[..read]).ok()
}

async fn answered(message: &Message) -> Message {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(answer) = ask(message).await {
            return answer;
        }
        assert!(Instant::now() < deadline, "no answer to {message:?}");
    }
}

/// Whether nothing answers `message` for as long as a claimed name would take to.
async fn unanswered(message: &Message) -> bool {
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if ask(message).await.is_some() {
            return false;
        }
    }
    true
}

async fn claimed(core: &CommCore, name: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while core.mdns_name().as_deref() != Some(name) {
        assert!(
            Instant::now() < deadline,
            "not {name}: {:?}",
            core.mdns_name()
        );
        sleep(Duration::from_millis(20)).await;
    }
}

/// The TXT strings of `instance` in `message`, as text.
fn txt(message: &Message, instance: &str) -> Vec<String> {
    let instance = Name::dotted(instance).expect("name");
    message
        .answers
        .iter()
        .chain(&message.additional)
        .find_map(|r| match &r.data {
            Data::Txt(strings) if r.name == instance => Some(
                strings
                    .iter()
                    .map(|s| String::from_utf8_lossy(s).into_owned())
                    .collect(),
            ),
            _ => None,
        })
        .unwrap_or_default()
}

fn port(message: &Message, instance: &str) -> Option<u16> {
    let instance = Name::dotted(instance).expect("name");
    message
        .answers
        .iter()
        .chain(&message.additional)
        .find_map(|r| match &r.data {
            Data::Srv { port, .. } if r.name == instance => Some(*port),
            _ => None,
        })
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

async fn scenario() {
    sh("ip link set lo up");
    sh("ip link add ana type veth peer name anb");
    sh("ip addr add 10.79.1.1/24 dev ana && ip addr add 10.79.1.2/24 dev anb");
    sh("ip link set ana up && ip link set anb up");
    for interface in ["all", "ana", "anb"] {
        sh(&format!(
            "echo 1 > /proc/sys/net/ipv4/conf/{interface}/accept_local && \
             echo 0 > /proc/sys/net/ipv4/conf/{interface}/rp_filter"
        ));
    }
    let only = "mdns.interface = ana\n";

    // As it is: all it says of itself.
    let dir = Scratch::new("announce-on");
    let core = CommCore::start(settings(dir.path(), only))
        .await
        .expect("start");
    claimed(&core, "cam").await;
    let browse = answered(&question("_orion._tcp.local", PTR)).await;
    let key = hex(&core.keys().lock().identity().public());
    assert_eq!(
        txt(&browse, "cam._orion._tcp.local"),
        [
            "v=2".to_owned(),
            "id=cam".to_owned(),
            format!("pk={key}"),
            format!("wv={}", orion_wire::PROTOCOL_VERSION),
            format!("rev={}", orion_wire::INTERFACE_REVISION),
            "cl=lab".to_owned(),
            "tcp=9200".to_owned(),
            format!("level={}", orion_wire::INTERFACE_LEVEL),
            "state=running".to_owned(),
            "security=off".to_owned(),
        ]
    );
    assert_eq!(port(&browse, "cam._orion._tcp.local"), Some(9200));
    let ssh = answered(&question("_ssh._tcp.local", PTR)).await;
    assert_eq!(port(&ssh, "cam._ssh._tcp.local"), Some(2222));
    let sftp = answered(&question("_sftp-ssh._tcp.local", PTR)).await;
    assert_eq!(port(&sftp, "cam._sftp-ssh._tcp.local"), Some(2222));
    drop(core);
    sleep(Duration::from_millis(300)).await;

    // `mdns = false`: nothing.
    let dir = Scratch::new("announce-off");
    let core = CommCore::start(settings(dir.path(), "mdns = false\n"))
        .await
        .expect("start");
    assert!(unanswered(&question("cam.local", A)).await);
    assert_eq!(core.mdns_name(), None);
    drop(core);

    // A cluster name too long for an announcement: the name and SSH, not `_orion._tcp`.
    let dir = Scratch::new("announce-long");
    let long = format!(
        "{only}cluster = {}\n",
        "c".repeat(orion_mdns::MAX_CLUSTER + 1)
    );
    let core = CommCore::start(settings(dir.path(), &long))
        .await
        .expect("start");
    claimed(&core, "cam").await;
    let host = answered(&question("cam.local", A)).await;
    assert!(
        host.answers
            .iter()
            .any(|r| r.data == Data::A([10, 79, 1, 1]))
    );
    let ssh = answered(&question("_ssh._tcp.local", PTR)).await;
    assert_eq!(port(&ssh, "cam._ssh._tcp.local"), Some(2222));
    assert!(unanswered(&question("_orion._tcp.local", PTR)).await);
    drop(core);
    println!("{DONE}");
}

fn inside() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, scenario());
}

/// Why a network namespace with a veth pair cannot be made here, if it cannot.
fn cannot() -> Option<String> {
    let made = Command::new("unshare")
        .args(["--map-root-user", "--net", "--"])
        .args([
            "ip", "link", "add", "ancheck0", "type", "veth", "peer", "name", "ancheck1",
        ])
        .stdout(Stdio::null())
        .output();
    match made {
        Err(error) => Some(format!("unshare could not be run: {error}")),
        Ok(out) if !out.status.success() => Some(format!(
            "no network namespace with a veth pair here: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        )),
        Ok(_) => None,
    }
}

#[test]
fn the_board_says_what_it_is_keeps_quiet_when_told_and_withholds_what_it_cannot_carry() {
    if std::env::var_os(INSIDE).is_some() {
        inside();
        return;
    }
    if let Some(why) = cannot() {
        assert!(
            std::env::var_os("NAMESPACE_TESTS").is_none_or(|said| said != "required"),
            "this test must run here, and cannot: {why}"
        );
        println!("{why}: the announcement was not tried");
        return;
    }
    let name = "the_board_says_what_it_is_keeps_quiet_when_told_and_withholds_what_it_cannot_carry";
    let ran = Command::new("unshare")
        .args(["--map-root-user", "--net", "--"])
        .arg(std::env::current_exe().expect("this test"))
        .args(["--exact", own_test(name).as_str(), "--nocapture"])
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

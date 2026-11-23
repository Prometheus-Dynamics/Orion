//! The responder on real sockets: answering on the interface it is given and not on another,
//! defending its name against a second responder that then takes the tagged one, following
//! an interface taken away and made again, and saying goodbye.
//!
//! It makes interfaces, so it runs itself again in a network namespace of its own, as that
//! namespace's root: a veth pair, `mda` and `mdb`, each end an address of its own and both
//! taking packets from the other's (`accept_local`), and a dummy, `mdc`. Where that cannot be
//! done it says so and passes, unless `NAMESPACE_TESTS=required` is set, as the gate sets it.

use std::cell::RefCell;
use std::net::{Ipv4Addr, SocketAddr};
use std::process::{Command, Stdio};
use std::rc::Rc;
use std::time::Duration;

use orion_mdns::packet::{A, Data, IN, Message, Name, PTR, Question, SRV, TXT};
use orion_mdns::{Announce, GROUP, Named, ORION, PORT, Reach, Retell, SSH, Service, Settings};
use rustix::net::sockopt;
use tokio::net::UdpSocket;
use tokio::sync::oneshot;
use tokio::time::{Instant, sleep, timeout};

const INSIDE: &str = "ORION_MDNS_INSIDE";
const DONE: &str = "mdns: answered, defended, followed and said goodbye";

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

/// The veth pair, each end taking what comes from the other's address.
fn pair() {
    sh("ip link add mda type veth peer name mdb");
    sh("ip addr add 10.78.1.1/24 dev mda && ip addr add 10.78.1.2/24 dev mdb");
    sh("ip link set mda up && ip link set mdb up");
    for interface in ["all", "mda", "mdb"] {
        sh(&format!(
            "echo 1 > /proc/sys/net/ipv4/conf/{interface}/accept_local && \
             echo 0 > /proc/sys/net/ipv4/conf/{interface}/rp_filter"
        ));
    }
}

fn announce(tag: &str) -> Announce {
    Announce {
        name: "cam".to_owned(),
        tag: tag.to_owned(),
        services: vec![
            Service {
                kind: ORION,
                port: 9200,
                txt: vec![b"v=2".to_vec(), b"state=running".to_vec()],
                reach: Reach::everywhere(),
            },
            Service {
                kind: SSH,
                port: 22,
                txt: Vec::new(),
                reach: Reach::everywhere(),
            },
        ],
    }
}

fn question(name: &str, kind: u16) -> Message {
    Message {
        id: 7,
        questions: vec![Question {
            name: Name::dotted(name).expect("name"),
            kind,
            class: IN,
        }],
        ..Message::default()
    }
}

/// Asks the group through `on`, from a port that is not mDNS's: the answer comes back to it.
async fn ask(on: &str, message: &Message) -> Option<Message> {
    let socket = UdpSocket::bind("0.0.0.0:0").await.expect("bind");
    let index = rustix::net::netdevice::name_to_index(&socket, on).expect("index");
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

/// Asks until it is answered, or `within` passes.
async fn answered(on: &str, message: &Message, within: Duration) -> Message {
    let deadline = Instant::now() + within;
    loop {
        if let Some(answer) = ask(on, message).await {
            return answer;
        }
        assert!(
            Instant::now() < deadline,
            "no answer on {on} to {message:?}"
        );
    }
}

async fn named(named: &Named, name: &str, within: Duration) {
    let deadline = Instant::now() + within;
    while named.get().as_deref() != Some(name) {
        assert!(
            Instant::now() < deadline,
            "not {name}, but {:?}",
            named.get()
        );
        sleep(Duration::from_millis(20)).await;
    }
}

/// A socket on mDNS's port on `on`, with the responders: it hears the group.
fn listener(on: &str) -> UdpSocket {
    use rustix::net::{AddressFamily, SocketType, ipproto};
    let fd = rustix::net::socket(AddressFamily::INET, SocketType::DGRAM, Some(ipproto::UDP))
        .expect("socket");
    sockopt::set_socket_reuseaddr(&fd, true).expect("reuseaddr");
    sockopt::set_socket_reuseport(&fd, true).expect("reuseport");
    rustix::net::bind(
        &fd,
        &std::net::SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, PORT),
    )
    .expect("bind");
    let index = rustix::net::netdevice::name_to_index(&fd, on).expect("index");
    sockopt::set_ip_add_membership_with_ifindex(&fd, &GROUP, &Ipv4Addr::UNSPECIFIED, index)
        .expect("join");
    let socket = std::net::UdpSocket::from(fd);
    socket.set_nonblocking(true).expect("nonblocking");
    let socket = UdpSocket::from_std(socket).expect("tokio");
    socket.bind_device(Some(on.as_bytes())).expect("device");
    socket
}

fn has_a(message: &Message, last: u8) -> bool {
    message
        .answers
        .iter()
        .chain(&message.additional)
        .any(|r| r.data == Data::A([10, 78, 1, last]))
}

#[allow(clippy::too_many_lines)]
async fn scenario() {
    pair();
    sh("ip link set lo up");
    sh("ip link add mdc type dummy && ip addr add 10.78.3.1/24 dev mdc && ip link set mdc up");
    let only = |name: &str| Settings {
        interfaces: vec![name.to_owned()],
    };
    let a = Named::default();
    let (stop_a, stopped_a) = oneshot::channel::<()>();
    let retold: Rc<RefCell<Option<Vec<Vec<u8>>>>> = Rc::default();
    let asked = retold.clone();
    let a_ran = tokio::task::spawn_local(orion_mdns::run(
        announce("aaaa"),
        only("mda"),
        a.clone(),
        Some(Retell {
            kind: ORION,
            txt: Box::new(move || asked.borrow_mut().take()),
        }),
        async move {
            let _ = stopped_a.await;
        },
    ));
    named(&a, "cam", Duration::from_secs(5)).await;

    // Asked through the other end, it answers with the address of its own.
    let answer = answered("mdb", &question("cam.local", A), Duration::from_secs(3)).await;
    assert_eq!(answer.id, 7, "a plain client's id comes back");
    assert!(has_a(&answer, 1), "{answer:?}");
    let browse = answered(
        "mdb",
        &question("_orion._tcp.local", PTR),
        Duration::from_secs(3),
    )
    .await;
    assert!(browse.answers.iter().any(|r| r.data.kind() == PTR));
    assert!(browse.additional.iter().any(|r| r.data.kind() == SRV));
    assert!(browse.additional.iter().any(|r| r.data.kind() == TXT));
    // Its TXT strings changed (the clock set): a browse after says the new ones.
    let set = vec![b"v=2".to_vec(), b"clock=synced".to_vec()];
    *retold.borrow_mut() = Some(set.clone());
    sleep(orion_mdns::FOLLOW + Duration::from_millis(200)).await;
    let browse = answered(
        "mdb",
        &question("_orion._tcp.local", PTR),
        Duration::from_secs(3),
    )
    .await;
    assert!(
        browse
            .additional
            .iter()
            .any(|r| matches!(&r.data, Data::Txt(t) if *t == set)),
        "{browse:?}"
    );
    // On an interface it was not given, nothing: even with another there in the group, which
    // has the kernel take the group's packets on that interface (and hand them to any socket
    // in the group that is not kept to its own).
    let _there = listener("mdc");
    assert!(ask("mdc", &question("cam.local", A)).await.is_none());

    // A second responder for the same name, on the other end: it is answered, and takes
    // its tagged name; the first keeps its own.
    let b = Named::default();
    let (stop_b, stopped_b) = oneshot::channel::<()>();
    let b_ran = tokio::task::spawn_local(orion_mdns::run(
        announce("bbbb"),
        only("mdb"),
        b.clone(),
        None,
        async move {
            let _ = stopped_b.await;
        },
    ));
    named(&b, "cam-bbbb", Duration::from_secs(10)).await;
    assert_eq!(a.get().as_deref(), Some("cam"));
    let answer = answered(
        "mda",
        &question("cam-bbbb.local", A),
        Duration::from_secs(3),
    )
    .await;
    assert!(has_a(&answer, 2), "{answer:?}");
    stop_b.send(()).expect("stop b");
    b_ran.await.expect("b stopped");

    // The pair taken away and made again: answered again, with no restart.
    sh("ip link del mda");
    sleep(orion_mdns::FOLLOW + Duration::from_millis(500)).await;
    pair();
    let answer = answered("mdb", &question("cam.local", A), Duration::from_secs(8)).await;
    assert!(has_a(&answer, 1), "{answer:?}");

    // Stopped: every record said to be gone.
    let hears = listener("mdb");
    stop_a.send(()).expect("stop a");
    a_ran.await.expect("a stopped");
    let mut buffer = vec![0; 9000];
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let (read, _) = timeout(left, hears.recv_from(&mut buffer))
            .await
            .expect("a goodbye")
            .expect("read");
        let Ok(message) = Message::read(&buffer[..read]) else {
            continue;
        };
        if message.response && !message.answers.is_empty() {
            assert!(message.answers.iter().all(|r| r.ttl == 0), "{message:?}");
            break;
        }
    }
    println!("{DONE}");
}

fn inside() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, scenario());
}

/// Why a network namespace with interfaces of its own cannot be made here, if it cannot.
fn cannot() -> Option<String> {
    let made = Command::new("unshare")
        .args(["--map-root-user", "--net", "--"])
        .args([
            "ip", "link", "add", "mdcheck0", "type", "veth", "peer", "name", "mdcheck1",
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
fn it_answers_defends_follows_its_interface_and_says_goodbye() {
    if std::env::var_os(INSIDE).is_some() {
        inside();
        return;
    }
    if let Some(why) = cannot() {
        assert!(
            std::env::var_os("NAMESPACE_TESTS").is_none_or(|said| said != "required"),
            "this test must run here, and cannot: {why}"
        );
        println!("{why}: mDNS was not tried");
        return;
    }
    let ran = Command::new("unshare")
        .args(["--map-root-user", "--net", "--"])
        .arg(std::env::current_exe().expect("this test"))
        .args([
            "--exact",
            own_test("it_answers_defends_follows_its_interface_and_says_goodbye").as_str(),
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

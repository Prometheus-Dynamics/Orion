//! The other boards heard on the local network: two comm cores on the two ends of a veth
//! pair each list the other in `orion/boards`, read through the client, with what it
//! announces and how the reader's keys know it; neither lists itself; `orion.boards` moves
//! as the list does; and a board that stops is gone from the list.
//!
//! It runs itself again in a network namespace of its own, as the announcement's test does.
//! Where that cannot be done it says so and passes, unless `NAMESPACE_TESTS=required` is
//! set, as the gate sets it.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use orion::heard::{HeardBoard, HeardBoards, Trust};
use orion::topics::FieldId;
use orion::{Address, Orion};
use orion_comm_app::{CommCore, CommSettings};
use orion_config::Source;
use orion_mdns::packet::{Data, IN, Message, Name, Record};
use orion_mdns::{GROUP, MOST, PORT};
use orion_scratch::Scratch;
use rustix::net::sockopt;
use tokio::net::UdpSocket;

const INSIDE: &str = "ORION_BOARDS_INSIDE";
const DONE: &str = "boards: each heard the other, as its keys know it, and heard it go";

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

fn settings(dir: &Path, name: &str, security: &str, on: &str, port: u16) -> CommSettings {
    let view = dir.join("security");
    std::fs::write(&view, format!("{security}\ngeneration 1\n")).expect("security record");
    let text = format!(
        "node.name = {name}\nsocket = {0}/orion.sock\nstate.dir = {0}/state\ncluster = bench\n\
         peer.listen = 0.0.0.0:{port}\nmdns.interface = {on}\n",
        dir.display()
    );
    let mut source = Source::parse(&text, Vec::new()).expect("source");
    let settings = CommSettings::from_source_with_view(&mut source, &view).expect("settings");
    assert!(source.warnings().is_empty(), "{:?}", source.warnings());
    settings
}

/// What a client on the board's socket does, on a thread that may block.
async fn on<T: Send + 'static>(socket: &Path, ask: impl FnOnce(&Orion) -> T + Send + 'static) -> T {
    let socket: PathBuf = socket.to_owned();
    tokio::task::spawn_blocking(move || {
        let deadline = Instant::now() + Duration::from_secs(10);
        let orion = loop {
            match Orion::connect(&Address::Local(socket.clone()), "tool") {
                Ok(orion) => break orion,
                Err(error) => assert!(Instant::now() < deadline, "connect: {error}"),
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        ask(&orion)
    })
    .await
    .expect("asked")
}

/// The boards the core on `socket` hears, once they are as `wanted` says.
async fn heard(socket: &Path, what: &str, wanted: impl Fn(&HeardBoards) -> bool) -> HeardBoards {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let heard = on(socket, |orion| orion.boards().expect("boards")).await;
        if wanted(&heard) {
            return heard;
        }
        assert!(Instant::now() < deadline, "never {what}: {heard:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// `orion.boards` on `socket`: how many are listed, and the count of changes.
async fn counts(socket: &Path) -> (u64, u64) {
    on(socket, |orion| {
        let mut boards = orion.subscribe("orion.boards").expect("subscribe");
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            boards.wait(Duration::from_millis(20)).expect("wait");
            let mut field = |id| {
                boards
                    .latest(FieldId(id))
                    .and_then(|sample| sample.payload.first_chunk().copied())
                    .map(u64::from_le_bytes)
            };
            if let (Some(listed), Some(changes)) = (field(0), field(1)) {
                return (listed, changes);
            }
            assert!(Instant::now() < deadline, "orion.boards says nothing");
        }
    })
    .await
}

/// An announcement of a board `id` with `key`, at 10.80.1.`last`, as a responder makes one.
fn announcement(id: &str, key: &[u8; 32], last: u8) -> Message {
    let service = Name::dotted("_orion._tcp.local").expect("a name");
    let instance = service.under(id.as_bytes()).expect("a name");
    let host = Name::dotted(&format!("{id}.local")).expect("a name");
    let hex: String = key.iter().map(|byte| format!("{byte:02x}")).collect();
    let txt = [
        "v=2".to_owned(),
        format!("id={id}"),
        format!("pk={hex}"),
        format!("wv={}", orion_wire::PROTOCOL_VERSION),
        "cl=bench".to_owned(),
        "tcp=9300".to_owned(),
    ];
    let record = |name: &Name, data| Record {
        name: name.clone(),
        class: IN,
        ttl: 4500,
        data,
    };
    Message {
        response: true,
        answers: vec![
            record(&service, Data::Ptr(instance.clone())),
            record(
                &instance,
                Data::Srv {
                    priority: 0,
                    weight: 0,
                    port: 9300,
                    target: host.clone(),
                },
            ),
            record(
                &instance,
                Data::Txt(txt.into_iter().map(String::into_bytes).collect()),
            ),
            record(&host, Data::A([10, 80, 1, last])),
        ],
        ..Message::default()
    }
}

/// Says `messages` to the group out of `bda`, from `port`, so that the board on `bdb` hears
/// them and the one on `bda` does not.
async fn say(messages: &[Message], port: u16) {
    use rustix::net::{AddressFamily, SocketType, ipproto};
    let fd = rustix::net::socket(AddressFamily::INET, SocketType::DGRAM, Some(ipproto::UDP))
        .expect("socket");
    sockopt::set_socket_reuseaddr(&fd, true).expect("reuse");
    sockopt::set_socket_reuseport(&fd, true).expect("reuse");
    rustix::net::bind(&fd, &SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port)).expect("bind");
    let socket = std::net::UdpSocket::from(fd);
    socket.set_nonblocking(true).expect("nonblocking");
    let socket = UdpSocket::from_std(socket).expect("socket");
    let index = rustix::net::netdevice::name_to_index(&socket, "bda").expect("index");
    sockopt::set_ip_multicast_if_with_ifindex(&socket, &GROUP, &Ipv4Addr::UNSPECIFIED, index)
        .expect("multicast if");
    sockopt::set_ip_multicast_loop(&socket, false).expect("no loop");
    for message in messages {
        socket
            .send_to(&message.write(), SocketAddr::from((GROUP, PORT)))
            .await
            .expect("send");
        // The board reads them one at a time: none is dropped for coming all at once.
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn only(heard: &HeardBoards) -> &HeardBoard {
    assert_eq!(heard.boards.len(), 1, "{heard:?}");
    assert_eq!(heard.not_listed, 0);
    &heard.boards[0]
}

const V6_CAM: Ipv6Addr = Ipv6Addr::new(0xfd80, 1, 0, 0, 0, 0, 0, 1);
const V6_ARM: Ipv6Addr = Ipv6Addr::new(0xfd80, 1, 0, 0, 0, 0, 0, 2);

async fn scenario() {
    sh("ip link set lo up");
    sh("ip link add bda type veth peer name bdb");
    sh("ip addr add 10.80.1.1/24 dev bda && ip addr add 10.80.1.2/24 dev bdb");
    // IPv6 crosses too: one fixed address each, usable at once. The kernel makes each a
    // link-local address as well, which is never announced, whatever the build's features.
    sh("ip -6 addr add fd80:1::1/64 dev bda nodad && ip -6 addr add fd80:1::2/64 dev bdb nodad");
    sh("ip link set bda up && ip link set bdb up");
    for interface in ["all", "bda", "bdb"] {
        sh(&format!(
            "echo 1 > /proc/sys/net/ipv4/conf/{interface}/accept_local && \
             echo 0 > /proc/sys/net/ipv4/conf/{interface}/rp_filter"
        ));
    }

    // One board, security off: it hears nobody, and does not hear itself.
    let cam_dir = Scratch::new("boards-cam");
    let cam = CommCore::start(settings(cam_dir.path(), "cam", "off", "bda", 9200))
        .await
        .expect("start cam");
    tokio::time::sleep(Duration::from_secs(2)).await;
    let alone = heard(cam.socket(), "alone", |_| true).await;
    assert_eq!(
        alone,
        HeardBoards::default(),
        "its own announcement is not heard"
    );
    assert_eq!(counts(cam.socket()).await, (0, 0));

    // A second, security on. Each lists the other, with what it announces.
    let arm_dir = Scratch::new("boards-arm");
    let arm = CommCore::start(settings(arm_dir.path(), "arm", "on", "bdb", 9201))
        .await
        .expect("start arm");
    let key_of = |orion: &Orion| orion.node().expect("node").fingerprint;
    let (cam_key, arm_key) = (
        on(cam.socket(), key_of).await,
        on(arm.socket(), key_of).await,
    );
    let whole = |heard: &HeardBoards| heard.boards.iter().any(|b| !b.addresses.is_empty());
    let on_cam = heard(cam.socket(), "heard arm", whole).await;
    let listed = only(&on_cam);
    assert_eq!(
        *listed,
        HeardBoard {
            name: "arm".into(),
            addresses: vec![IpAddr::from([10, 80, 1, 2]), V6_ARM.into()],
            port: 9201,
            key: Some(arm_key.clone()),
            cluster: Some("bench".into()),
            state: Some("running".into()),
            security: Some("on".into()),
            clock: None,
            // This board's security is off: its keys are not asked.
            trust: Trust::Open,
            heard_ms_ago: listed.heard_ms_ago,
        }
    );
    assert!(listed.heard_ms_ago < 15_000, "{listed:?}");
    let (now_listed, changes) = counts(cam.socket()).await;
    assert_eq!(now_listed, 1);
    assert!(changes > 0);

    // As a person asks: one line a board.
    let address = cam.socket().display().to_string();
    let said = tokio::task::spawn_blocking(move || {
        let args = ["--address", &address, "node", "boards"].map(str::to_owned);
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let code = orion_ctl::run(&args, &mut out, &mut err);
        assert_eq!((code, String::from_utf8_lossy(&err).as_ref()), (0, ""));
        String::from_utf8(out).expect("text")
    })
    .await
    .expect("asked");
    let lines: Vec<&str> = said.lines().collect();
    assert_eq!(lines.len(), 2, "{said}");
    assert_eq!(
        lines[0],
        "# name addresses port key cluster state security clock trust heard_ms_ago"
    );
    let line = format!("arm 10.80.1.2,fd80:1::2 9201 {arm_key} bench running on - open ");
    assert!(lines[1].starts_with(&line), "{said}");

    // On the board with security on: as its keys know the other.
    let trusted = |trust: Trust| {
        move |heard: &HeardBoards| {
            let cam = heard.boards.iter().find(|board| board.name == "cam");
            cam.is_some_and(|cam| !cam.addresses.is_empty() && cam.trust == trust)
        }
    };
    let on_arm = heard(arm.socket(), "discovered", trusted(Trust::Discovered)).await;
    let listed = only(&on_arm);
    assert_eq!(
        (
            listed.name.as_str(),
            &listed.key,
            listed.security.as_deref(),
            listed.port
        ),
        ("cam", &Some(cam_key), Some("off"), 9200)
    );
    assert_eq!(
        listed.addresses,
        [IpAddr::from([10, 80, 1, 1]), V6_CAM.into()]
    );
    let cam_name = "cam".parse().expect("a name");
    let real = cam.keys().lock().identity().public();
    let pin = |key| arm.keys().lock().trust_peer(&cam_name, key).expect("saved");
    // Another key pinned for its name.
    assert!(pin([9; 32]));
    heard(arm.socket(), "key-mismatch", trusted(Trust::KeyMismatch)).await;
    arm.keys().lock().revoke([9; 32]).expect("saved");
    heard(arm.socket(), "discovered again", trusted(Trust::Discovered)).await;
    // The key it announces pinned.
    assert!(pin(real));
    heard(arm.socket(), "enrolled", trusted(Trust::Enrolled)).await;
    // And revoked.
    arm.keys().lock().revoke(real).expect("saved");
    heard(arm.socket(), "revoked", trusted(Trust::Revoked)).await;

    // An announcement that does not come from the mDNS port is not one (RFC 6762 §6): it
    // is said first, and never listed.
    say(&[announcement("stray", &[1; 32], 98)], 0).await;
    // Names nobody knows fill the list: no more are kept, and the board says there are more.
    let fakes: Vec<Message> = (0..MOST as u8)
        .map(|each| announcement(&format!("fake{each}"), &[each; 32], 100 + each))
        .collect();
    say(&fakes, PORT).await;
    let full = heard(arm.socket(), "full", |heard| heard.not_listed > 0).await;
    assert_eq!(full.boards.len(), MOST, "{full:?}");
    assert!(full.boards.iter().any(|board| board.name == "cam"));
    assert!(full.boards.iter().all(|board| board.name != "stray"));
    // A board the keys pin always has a place: it takes one of theirs.
    let ghost = [7; 32];
    assert!(
        arm.keys()
            .lock()
            .trust_peer(&"ghost".parse().expect("a name"), ghost)
            .expect("saved")
    );
    say(&[announcement("ghost", &ghost, 99)], PORT).await;
    let placed = |heard: &HeardBoards| heard.boards.iter().any(|board| board.name == "ghost");
    let with = heard(arm.socket(), "a place for the pinned board", placed).await;
    assert_eq!(with.boards.len(), MOST, "{with:?}");
    let listed = with.boards.iter().find(|b| b.name == "ghost").expect("it");
    assert_eq!(listed.trust, Trust::Enrolled);
    assert_eq!(listed.addresses, [IpAddr::from([10, 80, 1, 99])]);
    // The board on the other end heard none of that.
    only(&heard(cam.socket(), "still one", |_| true).await);

    // It stops: gone from the other's list at its goodbye, and the topic says so.
    drop(arm);
    heard(cam.socket(), "heard arm go", |heard| {
        heard.boards.is_empty()
    })
    .await;
    let (now_listed, later) = counts(cam.socket()).await;
    assert_eq!(now_listed, 0);
    assert!(later > changes);
    drop(cam);
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
            "ip", "link", "add", "bdcheck0", "type", "veth", "peer", "name", "bdcheck1",
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
fn two_boards_each_list_the_other_as_their_keys_know_it_and_hear_it_go() {
    if std::env::var_os(INSIDE).is_some() {
        inside();
        return;
    }
    if let Some(why) = cannot() {
        assert!(
            std::env::var_os("NAMESPACE_TESTS").is_none_or(|said| said != "required"),
            "this test must run here, and cannot: {why}"
        );
        println!("{why}: the boards were not heard");
        return;
    }
    let name = "two_boards_each_list_the_other_as_their_keys_know_it_and_hear_it_go";
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

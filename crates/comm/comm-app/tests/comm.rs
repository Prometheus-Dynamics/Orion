//! The comm core on its own: it starts with nothing else on the board and serves hello and
//! topics, and it tells the service manager how it is.

#[path = "../../broker/tests/common/mod.rs"]
mod common;

use std::os::unix::net::UnixDatagram;
use std::path::Path;
use std::time::Duration;

use common::{Client, Event, Wire};
use orion_comm_app::systemd::Notifier;
use orion_comm_app::{CommCore, CommSettings, run_until};
use orion_config::Source;
use orion_msg_session::{Hello, Identify};
use orion_scratch::Scratch;
use orion_server::{FdSet, Roles};
use orion_unix::UnixConnection;
use orion_wire::{Body, Bytes, encode_body, frame, kind};
use shm_ring::{FieldId, Flags, Reader, SampleTime};
use tokio::io::AsyncReadExt;

fn run(test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, test);
}

fn temp(name: &str) -> Scratch {
    Scratch::new(&format!("comm-{name}"))
}

fn settings(dir: &Path, extra: &str) -> CommSettings {
    let text = format!(
        "node.name = board\nsocket = {0}/orion.sock\nstate.dir = {0}/state\nstatus.interval_ms = 50\n{extra}",
        dir.display()
    );
    let mut source = Source::parse(&text, Vec::new()).expect("source");
    let settings = CommSettings::from_source(&mut source).expect("settings");
    assert!(source.warnings().is_empty(), "{:?}", source.warnings());
    settings
}

async fn send<B: Body>(connection: &mut UnixConnection, body: &B, seq: u16) {
    let mut out = [0_u8; 512];
    let len = encode_body(body, seq, &mut out).expect("encode");
    connection.send(&out[..len], &[]).await.expect("send");
}

/// The payload of the next frame of kind `want`.
async fn expect(connection: &mut UnixConnection, want: u8) -> Vec<u8> {
    let mut buf = vec![0_u8; 16 * 1024];
    loop {
        let len = connection
            .recv(&mut buf, &mut FdSet::default())
            .await
            .expect("recv")
            .expect("open");
        let view = frame::decode(&buf[..len]).expect("frame");
        if view.kind() == want {
            return view.payload().to_vec();
        }
        assert!(
            matches!(view.kind(), kind::PING | kind::PONG),
            "kind {:#x}",
            view.kind()
        );
    }
}

/// Subscribes and waits for the topic to open: a topic that never opens fails the test, not hangs it.
async fn watch(client: &mut Client, topic: &str) -> Reader {
    tokio::time::timeout(Duration::from_secs(10), subscribe(client, topic))
        .await
        .unwrap_or_else(|_| panic!("{topic} never opened"))
}

async fn subscribe(client: &mut Client, topic: &str) -> Reader {
    let seq = client.subscribe(topic, None, &[]).await;
    loop {
        match client.next().await {
            Event::Opened {
                request_seq,
                shared: true,
                mut fds,
                ..
            } if request_seq == seq => {
                return Reader::attach(fds.pop().expect("read end")).expect("reader");
            }
            Event::Catalog(_) | Event::Other(_) | Event::State { .. } => {}
            other => panic!("subscribe to {topic}: {other:?}"),
        }
    }
}

/// Only the status topics (the `self-status` feature) are read field by field.
#[cfg(feature = "self-status")]
fn int(reader: &mut Reader, field: u16) -> Option<i64> {
    reader
        .latest(FieldId(field))
        .and_then(|sample| sample.payload.try_into().ok())
        .map(i64::from_le_bytes)
}

/// Waits a few seconds for a status topic (the `self-status` feature) to show something.
#[cfg(feature = "self-status")]
async fn until(what: &str, mut done: impl FnMut() -> bool) {
    for _ in 0..2000 {
        if done() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("never happened: {what}");
}

#[test]
fn the_comm_core_starts_alone_and_serves_hello_and_topics() {
    run(async {
        let dir = temp("alone");
        // No node logic, no other program, nothing on the links it is told about: those built
        // in (a link that is not is warned about, in `a_link_not_built_in_is_warned_about`).
        let mut extra = String::from(
            "peer.listen = 127.0.0.1:0\nssh.listen = 127.0.0.1:0\nssh.sandbox = false\n",
        );
        let mut parts = vec!["orion.sock", "127.0.0.1:", "topics", "hello", "network"];
        if cfg!(feature = "self-status") {
            parts.push("status");
        }
        if cfg!(feature = "link-serial") {
            extra += &format!("links.serial = uart0={}/no-such-port\n", dir.display());
            parts.push("serial link");
        }
        if cfg!(feature = "link-usb") {
            extra += "links.usb = usb0\n";
            parts.push("usb link");
        }
        if cfg!(feature = "link-can") {
            extra += "links.can = can9=no-such-can:0x100:0x180:1-2\n";
            parts.push("can bus");
        }
        let core = CommCore::start(settings(&dir, &extra))
            .await
            .expect("start");
        let summary = core.summary();
        for part in parts {
            assert!(summary.contains(part), "{summary}");
        }

        // Hello: who the board is, to anyone who connects and asks.
        let mut asker = UnixConnection::connect(core.socket())
            .await
            .expect("connect");
        let hello = Hello {
            name: "tool",
            roles: Roles::CLIENT,
            max_frame: 16 * 1024,
            revision: 1,
            level: 0,
        };
        send(&mut asker, &hello, 1).await;
        expect(&mut asker, kind::WELCOME).await;
        let nonce = [7_u8; 16];
        send(
            &mut asker,
            &Identify {
                nonce: Bytes(&nonce),
            },
            2,
        )
        .await;
        let answer = expect(&mut asker, kind::IDENTITY).await;
        let identity = orion_hello::verify(&answer, &nonce).expect("a genuine answer");
        assert_eq!(identity.node.as_str(), "board");
        assert_eq!(
            identity.public_key.0,
            core.keys().lock().identity().public()
        );

        // Topics: one program publishes, another reads, through the broker and then without it.
        let mut publisher = Client::open(
            Wire::Unix(
                UnixConnection::connect(core.socket())
                    .await
                    .expect("connect"),
            ),
            "camera",
        )
        .await;
        let (_, mut writer, _keep) = publisher.declare("camera.pose", 8).await;
        let mut subscriber = Client::open(
            Wire::Unix(
                UnixConnection::connect(core.socket())
                    .await
                    .expect("connect"),
            ),
            "dashboard",
        )
        .await;
        let mut pose = watch(&mut subscriber, "camera.pose").await;
        writer
            .publish(
                FieldId(0),
                SampleTime::from_nanos(5),
                Flags::NONE,
                &2.5_f64.to_le_bytes(),
            )
            .expect("publish");
        let read = pose.latest(FieldId(0)).expect("the sample");
        assert_eq!(
            (read.payload, read.time),
            (2.5_f64.to_le_bytes().as_slice(), SampleTime::from_nanos(5))
        );

        // The board's vital signs and the comm core's own counters are topics too.
        #[cfg(feature = "self-status")]
        {
            let mut host = watch(&mut subscriber, "orion.host").await;
            until("the host is read", || int(&mut host, 0).is_some()).await;
            assert!(
                int(&mut host, 0).expect("uptime") > 0 && int(&mut host, 1).expect("memory") > 0
            );
            let mut counters = watch(&mut subscriber, "orion.comm").await;
            until("the comm core counts itself", || {
                int(&mut counters, 0) == Some(3)
            })
            .await;
        }

        // A link with nothing on it keeps trying, and says so (on its status topic), without
        // disturbing anything.
        #[cfg(all(feature = "self-status", feature = "link-serial"))]
        {
            let mut link = watch(&mut subscriber, "orion.state.link.uart0").await;
            until("the serial link is retrying", || {
                int(&mut link, 0) == Some(2)
            })
            .await;
            assert!(int(&mut link, 1).expect("failures") >= 1);
        }
        #[cfg(all(feature = "self-status", feature = "link-can"))]
        {
            let mut bus = watch(&mut subscriber, "orion.state.link.can9").await;
            until("the CAN bus is retrying", || int(&mut bus, 0) == Some(2)).await;
        }

        // The network way in answers as well.
        let addr = core.peer_addr().expect("listening");
        let options = orion_tcp_options(&core);
        let mut remote = orion_tcp::connect(addr, &options, None)
            .await
            .expect("connect");
        let mut out = [0_u8; 256];
        let len = encode_body(&hello, 1, &mut out).expect("encode");
        remote.send(&out[..len]).await.expect("send");
        let mut buf = [0_u8; 4096];
        let len = remote.recv(&mut buf).await.expect("recv").expect("open");
        assert_eq!(
            frame::decode(&buf[..len]).expect("frame").kind(),
            kind::WELCOME
        );

        // And the SSH way in says which SSH it speaks, and has made its host key.
        let ssh = core.ssh_addr().expect("ssh");
        let mut greeting = [0_u8; 8];
        tokio::net::TcpStream::connect(ssh)
            .await
            .expect("connect")
            .read_exact(&mut greeting)
            .await
            .expect("a greeting");
        assert_eq!(&greeting, b"SSH-2.0-");
        assert!(dir.join("state/ssh_host_ed25519_key").exists());

        // Stopping it closes every way in.
        let socket = core.socket().to_owned();
        drop(core);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(UnixConnection::connect(&socket).await.is_err());
        assert!(tokio::net::TcpStream::connect(addr).await.is_err());
        assert!(tokio::net::TcpStream::connect(ssh).await.is_err());
    });
}

/// A link this build does not have is warned about, not taken; one it has is taken without a
/// word. Each build checks the links it leaves out, whatever its features.
#[test]
fn a_link_not_built_in_is_warned_about() {
    let links = [
        (
            "links.serial",
            "uart0=/dev/null",
            cfg!(feature = "link-serial"),
        ),
        ("links.usb", "usb0", cfg!(feature = "link-usb")),
        (
            "links.can",
            "can9=no-such-can:0x100:0x180:1-2",
            cfg!(feature = "link-can"),
        ),
        (
            "links.sealed",
            "far=127.0.0.1:1",
            cfg!(feature = "link-sealed"),
        ),
    ];
    for (key, value, built) in links {
        let text = format!("node.name = board\n{key} = {value}\n");
        let mut source = Source::parse(&text, Vec::new()).expect("source");
        CommSettings::from_source(&mut source).expect(key);
        let warned = source
            .warnings()
            .iter()
            .any(|warning| warning.to_string().contains(key));
        assert_eq!(warned, !built, "{key}: {:?}", source.warnings());
    }
}

fn orion_tcp_options(core: &CommCore) -> orion_tcp::TcpOptions {
    orion_tcp::TcpOptions {
        security: orion_server::Security::Off,
        keys: core.keys().clone(),
        max_frame: 64 * 1024,
        handshake: Duration::from_secs(5),
    }
}

#[test]
fn the_service_manager_is_told_ready_alive_and_stopping() {
    run(async {
        let dir = temp("systemd");
        let manager = UnixDatagram::bind(dir.join("notify")).expect("bind");
        manager.set_nonblocking(true).expect("non-blocking");
        let notifier = Notifier::connect(dir.join("notify").as_os_str()).expect("notifier");
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let running = tokio::task::spawn_local(run_until(
            settings(&dir, ""),
            Some(notifier),
            Some(Duration::from_millis(20)),
            async {
                let _ = stopped.await;
            },
        ));
        let mut heard: Vec<String> = Vec::new();
        let listen = |heard: &mut Vec<String>| {
            let mut buf = [0_u8; 512];
            while let Ok(len) = manager.recv(&mut buf) {
                heard.push(String::from_utf8(buf[..len].to_vec()).expect("text"));
            }
        };
        for _ in 0..100 {
            listen(&mut heard);
            if heard
                .iter()
                .filter(|message| *message == "WATCHDOG=1")
                .count()
                >= 3
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Ready is said once, when every way in is up, with what is being served.
        assert!(
            heard[0].starts_with("READY=1\nSTATUS=serving "),
            "{heard:?}"
        );
        assert!(heard[0].contains("orion.sock"), "{heard:?}");
        assert_eq!(
            heard
                .iter()
                .filter(|message| message.starts_with("READY=1"))
                .count(),
            1
        );
        assert!(
            heard
                .iter()
                .filter(|message| *message == "WATCHDOG=1")
                .count()
                >= 3,
            "{heard:?}"
        );
        // And it was true: the socket takes a connection.
        UnixConnection::connect(&dir.join("orion.sock"))
            .await
            .expect("connect");

        stop.send(()).expect("stop");
        running.await.expect("joined").expect("ran");
        listen(&mut heard);
        assert_eq!(
            heard.last().map(String::as_str),
            Some("STOPPING=1\nSTATUS=stopping")
        );
        // Nothing is said after it has stopped.
        tokio::time::sleep(Duration::from_millis(80)).await;
        let before = heard.len();
        listen(&mut heard);
        assert_eq!(heard.len(), before);
    });
}

#[test]
fn a_comm_core_that_cannot_start_says_which_part_and_one_whose_network_address_is_taken_starts() {
    run(async {
        let dir = temp("twice");
        let first = CommCore::start(settings(&dir, "peer.listen = 127.0.0.1:0\n"))
            .await
            .expect("start");
        // The same socket again: another comm core is already there.
        let error = CommCore::start(settings(&dir, ""))
            .await
            .expect_err("refused");
        assert!(error.to_string().starts_with("socket "), "{error}");
        // A network address that is taken: the comm core starts all the same, serves its
        // socket, and its network way in opens once the address is free.
        let taken = first.peer_addr().expect("addr");
        let other = temp("twice-b");
        let mut later = settings(&other, &format!("peer.listen = {taken}\n"));
        later.listen_retry = orion_server::Retry {
            first: Duration::from_millis(50),
            most: Duration::from_millis(100),
        };
        let second = CommCore::start(later)
            .await
            .expect("starts without its network way in");
        assert_eq!(second.peer_addr(), Some(taken));
        let mut on_second = Client::open(
            Wire::Unix(
                UnixConnection::connect(second.socket())
                    .await
                    .expect("connect"),
            ),
            "tool",
        )
        .await;
        let _topic = on_second.declare("served.meanwhile", 2).await;
        // A bus whose identifiers cannot work.
        #[cfg(feature = "link-can")]
        {
            let third = temp("twice-c");
            let error = CommCore::start(settings(&third, "links.can = can0=can0:0x7FF:0x100:1\n"))
                .await
                .expect_err("refused");
            assert!(error.to_string().starts_with("links.can"), "{error}");
        }

        // The first is still serving.
        let mut client = Client::open(
            Wire::Unix(
                UnixConnection::connect(first.socket())
                    .await
                    .expect("connect"),
            ),
            "tool",
        )
        .await;
        let _topic = client.declare("still.here", 2).await;

        // The first goes, and with it the address: the second's network way in opens there.
        drop(client);
        drop(first);
        // A hello over it is welcomed, once the first's listener has gone and the second's
        // has opened.
        let options = orion_tcp_options(&second);
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut remote = loop {
            match orion_tcp::connect(taken, &options, None).await {
                Ok(remote) => break remote,
                Err(error) => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "never opened: {error}"
                    );
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        };
        let hello = Hello {
            name: "tool",
            roles: Roles::CLIENT,
            max_frame: 4096,
            revision: 1,
            level: 0,
        };
        let mut out = [0_u8; 256];
        let len = encode_body(&hello, 1, &mut out).expect("encode");
        remote.send(&out[..len]).await.expect("send");
        let mut buf = [0_u8; 4096];
        let len = remote.recv(&mut buf).await.expect("recv").expect("open");
        assert_eq!(
            frame::decode(&buf[..len]).expect("frame").kind(),
            kind::WELCOME
        );
        drop(second);
    });
}

#[test]
fn a_network_way_in_kept_to_an_interface_not_there_yet_waits_and_the_rest_runs() {
    run(async {
        let dir = temp("interface");
        let core = CommCore::start(settings(
            &dir,
            "peer.listen = 127.0.0.1:0\npeer.interface = orion-none0\n",
        ))
        .await
        .expect("starts");
        // Not open: the address is as it was given, with no port chosen yet.
        assert_eq!(core.peer_addr(), Some("127.0.0.1:0".parse().expect("addr")));
        let mut client = Client::open(
            Wire::Unix(
                UnixConnection::connect(core.socket())
                    .await
                    .expect("connect"),
            ),
            "tool",
        )
        .await;
        let _topic = client.declare("served.meanwhile", 2).await;
        drop((client, core));
    });
}

#[test]
fn a_program_on_the_socket_that_goes_quiet_is_kept_by_the_comm_core() {
    run(async {
        let dir = temp("quiet");
        // The idle time is the floor, a second: three heartbeats are less.
        let core = CommCore::start(settings(&dir, "comm.heartbeat_ms = 100\n"))
            .await
            .expect("start");
        let mut program = UnixConnection::connect(core.socket())
            .await
            .expect("connect");
        let hello = Hello {
            name: "debugged",
            roles: Roles::CLIENT,
            max_frame: 16 * 1024,
            revision: orion_wire::INTERFACE_REVISION,
            level: 0,
        };
        send(&mut program, &hello, 1).await;
        expect(&mut program, kind::WELCOME).await;
        // Stopped in a debugger for more than twice the idle time: it says nothing.
        tokio::time::sleep(Duration::from_millis(2_500)).await;
        // Its session is still there, and answers when it speaks again.
        let nonce = [9_u8; 16];
        send(
            &mut program,
            &Identify {
                nonce: Bytes(&nonce),
            },
            2,
        )
        .await;
        let answer = expect(&mut program, kind::IDENTITY).await;
        assert!(orion_hello::verify(&answer, &nonce).is_some());
    });
}

//! The client form in a comm core, against an NT4 server that runs somewhere else: the
//! board's mapped fields appear on the server, the server's accepted topics are published
//! on the board, a value that expires is withdrawn, and a server that goes away and comes
//! back is joined again with everything in place.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use orion::topics::{FieldId, SampleTime, TopicDecl};
use orion::{Address, Orion, fields};
use orion_comm_app::{CommCore, CommSettings};
use orion_config::Source;
use orion_nt4::{Properties, Server, ServerConfig, ServerHandle, Value};
use orion_scratch::Scratch;

const WAIT: Duration = Duration::from_secs(15);

/// A comm core that joins the NT4 server on `port`, in a thread of its own, until dropped.
struct Board {
    socket: PathBuf,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
    /// Last: the thread is joined before it goes, so nothing uses the directory after.
    _dir: Scratch,
}

impl Board {
    fn start(name: &str, port: u16) -> Self {
        Self::start_with(name, port, "")
    }

    /// The same, with `more` settings.
    fn start_with(name: &str, port: u16, more: &str) -> Self {
        let dir = Scratch::new(&format!("nt4-cgw-{name}"));
        let text = format!(
            "node.name = board\nsocket = {0}/orion.sock\nstate.dir = {0}/state\n\
             nt4.server = 127.0.0.1:{port}\nnt4.name = coprocessor\nnt4.period_ms = 10\n\
             nt4.publish = tank.level:litres=double@/Tank/level, tank.alarm:text=string@/Tank/alarm\n\
             nt4.accept = robot.state:enabled=boolean@/Robot/enabled, robot.state:mode=string@/Robot/mode\n{more}",
            dir.display()
        );
        let mut source = Source::parse(&text, Vec::new()).expect("source");
        let settings = CommSettings::from_source(&mut source).expect("settings");
        assert!(source.warnings().is_empty(), "{:?}", source.warnings());
        let (ready, started) = mpsc::channel();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            tokio::task::LocalSet::new().block_on(&runtime, async move {
                let core = CommCore::start(settings)
                    .await
                    .expect("the comm core starts");
                assert!(core.nt4_client().is_some() && core.nt4().is_none());
                ready.send(core.socket().to_owned()).expect("ready");
                let _ = stopped.await;
            });
        });
        Self {
            socket: started.recv().expect("the board starts"),
            stop: Some(stop),
            thread: Some(thread),
            _dir: dir,
        }
    }
}

impl Drop for Board {
    fn drop(&mut self) {
        drop(self.stop.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[derive(Default)]
struct Told {
    stop: AtomicBool,
    alarms: AtomicU32,
}

/// The programs on the board: a tank that publishes its level all the time and an alarm
/// when told, and a reader of `robot.state` that reports what it reads.
fn programs(socket: &Path, told: &Told, read: &mpsc::Sender<(u16, Vec<u8>)>) {
    let orion = Orion::connect(&Address::Local(socket.to_owned()), "tank").expect("connect");
    let mut level = orion
        .declare(&TopicDecl {
            name: "tank.level",
            fields: &fields![litres: Float "l"],
            history: 8,
            expiry_ms: 0,
            max_payload: 8,
            on_subscribe: Default::default(),
        })
        .expect("declare");
    let mut alarm = orion
        .declare(&TopicDecl {
            name: "tank.alarm",
            fields: &fields![text: Text],
            history: 2,
            expiry_ms: 300,
            max_payload: 32,
            on_subscribe: Default::default(),
        })
        .expect("declare");
    let mut robot = orion.subscribe("robot.state").expect("subscribe");
    let (mut step, mut alarms) = (0_u64, 0);
    let mut last: [Option<Vec<u8>>; 2] = [None, None];
    while !told.stop.load(Ordering::Relaxed) {
        step += 1;
        let time = SampleTime::from_nanos(step);
        level
            .publish(FieldId(0), time, &(step as f64 * 0.5).to_le_bytes())
            .expect("publish");
        if told.alarms.load(Ordering::Relaxed) != alarms {
            alarms += 1;
            alarm
                .publish(FieldId(0), time, b"level high")
                .expect("publish");
        }
        // The state as it is now, whenever it was published: it may have been before this
        // program subscribed.
        for field in 0..2_u16 {
            if let Some(sample) = robot.latest(FieldId(field))
                && last[usize::from(field)].as_deref() != Some(sample.payload)
            {
                last[usize::from(field)] = Some(sample.payload.to_vec());
                let _ = read.send((field, sample.payload.to_vec()));
            }
        }
        orion.pump(Duration::from_millis(5)).expect("pump");
    }
}

async fn server(port: u16) -> Server {
    let config = ServerConfig {
        bind: format!("127.0.0.1:{port}").parse().expect("addr"),
        ..ServerConfig::default()
    };
    // The port may still be closing from the server before.
    let tried = Instant::now();
    loop {
        match Server::start(config.clone()).await {
            Ok(server) => return server,
            Err(error) if tried.elapsed() < WAIT => {
                let _ = error;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => panic!("the server cannot start: {error}"),
        }
    }
}

async fn until(what: &str, mut done: impl FnMut() -> bool) {
    let tried = Instant::now();
    while !done() {
        assert!(tried.elapsed() < WAIT, "{what} never happened");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// The level the server holds, once it holds one above `above`.
async fn level_above(host: &ServerHandle, above: f64) -> f64 {
    let mut found = 0.0;
    until("the level on the server", || {
        match host.value("/Tank/level") {
            Some(Value::Double(litres)) if litres > above => {
                found = litres;
                true
            }
            _ => false,
        }
    })
    .await;
    found
}

/// Everything the board has read as `robot.state`.
struct OnBoard {
    read: mpsc::Receiver<(u16, Vec<u8>)>,
    all: Vec<(u16, Vec<u8>)>,
}

impl OnBoard {
    /// Waits until the board has read `wanted`, now or before.
    async fn has(&mut self, wanted: (u16, &[u8])) {
        until("the sample on the board", || {
            self.all.extend(self.read.try_iter());
            self.all
                .iter()
                .any(|(field, payload)| (*field, payload.as_slice()) == wanted)
        })
        .await;
    }
}

#[test]
fn a_board_joins_a_server_and_the_mapping_works_both_ways() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        // The server somewhere else, with two topics its own programs hold.
        let first = server(0).await;
        let port = first.local_addr().port();
        let host = first.handle();
        host.publish("/Robot/enabled", "boolean", Properties::new())
            .expect("publish");
        host.set_value("/Robot/enabled", Value::Boolean(false))
            .expect("set");
        host.publish("/Robot/mode", "string", Properties::new())
            .expect("publish");
        host.set_value("/Robot/mode", Value::String("disabled".into()))
            .expect("set");
        // One the mapping does not name.
        host.set_value("/Robot/secret", Value::Int(7)).expect("set");

        let board = Board::start("joins", port);
        let told = Arc::new(Told::default());
        let (read, on_board) = mpsc::channel();
        let mut on_board = OnBoard {
            read: on_board,
            all: Vec::new(),
        };
        let (socket, telling) = (board.socket.clone(), told.clone());
        let running = std::thread::spawn(move || programs(&socket, &telling, &read));

        // The board is a client there, under its name, and its field is a topic there
        // with the type the mapping gives and values that follow.
        until("the board joins", || {
            host.clients()
                .iter()
                .any(|client| client.name == "coprocessor")
        })
        .await;
        let litres = level_above(&host, 0.0).await;
        assert_eq!(
            host.topic("/Tank/level").expect("the topic").type_name,
            "double"
        );
        assert!(level_above(&host, litres + 2.0).await % 0.5 == 0.0);
        // The tank counts its own sample times, which are not on the board's clock: its
        // values are stamped as they are sent on.
        let stamped = host
            .topic("/Tank/level")
            .and_then(|topic| topic.timestamp_us)
            .expect("stamped");
        assert!(
            (orion_nt4::now_micros() - stamped).abs() < 5_000_000,
            "{stamped}"
        );

        // The server's topics are published on the board, as they are and as they change.
        on_board.has((0, &[0])).await;
        on_board.has((1, b"disabled")).await;
        host.set_value("/Robot/enabled", Value::Boolean(true))
            .expect("set");
        host.set_value("/Robot/mode", Value::String("teleop".into()))
            .expect("set");
        on_board.has((0, &[1])).await;
        on_board.has((1, b"teleop")).await;

        // A value that expires: on the server while it stands, gone when it has stood
        // longer than its expiry, back with the next sample.
        for round in 0..2 {
            told.alarms.fetch_add(1, Ordering::Relaxed);
            until("the alarm on the server", || {
                host.value("/Tank/alarm") == Some(Value::String("level high".into()))
            })
            .await;
            let raised = Instant::now();
            until("the alarm withdrawn", || {
                host.topic("/Tank/alarm").is_none()
            })
            .await;
            let stood = raised.elapsed();
            assert!(
                stood >= Duration::from_millis(250) && stood < Duration::from_secs(3),
                "round {round}: the alarm stood for {stood:?}"
            );
        }

        // The server goes away and another comes up in its place, with other values.
        // The board joins it: its field is there again, and the new values reach it.
        drop(first);
        let second = server(port).await;
        let host = second.handle();
        host.set_value("/Robot/mode", Value::String("autonomous".into()))
            .expect("set");
        until("the board joins again", || {
            host.clients()
                .iter()
                .any(|client| client.name == "coprocessor")
        })
        .await;
        level_above(&host, litres).await;
        on_board.has((1, b"autonomous")).await;
        assert!(host.topic("/Robot/secret").is_none());

        told.stop.store(true, Ordering::Relaxed);
        running.join().expect("the programs end");
    });
}

/// The server's clock less this board's sample clock, in microseconds, as this process
/// reads both: the server here is in this process.
fn true_offset_us() -> i64 {
    orion_nt4::now_micros() - (SampleTime::now().as_nanos() / 1_000) as i64
}

#[test]
fn the_servers_clock_is_a_shared_base_said_in_orion_time_and_values_carry_their_own_time() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let first = server(0).await;
        let port = first.local_addr().port();
        let host = first.handle();
        let board = Board::start_with(
            "clock",
            port,
            "time.sync = report\ntime.servers = operator\ntime.synced_file = off\n",
        );
        let orion = Orion::connect(&Address::Local(board.socket.clone()), "tank").expect("connect");
        let mut time = orion.subscribe("orion.time").expect("subscribe");
        let mut said = |orion: &Orion| -> (Option<Vec<u8>>, Option<Vec<u8>>) {
            orion.pump(Duration::from_millis(5)).expect("pump");
            let field = |time: &mut orion::topics::Subscriber, at: u16| {
                time.latest(FieldId(at))
                    .map(|sample| sample.payload.to_vec())
            };
            (field(&mut time, 9), field(&mut time, 10))
        };
        let number = |bytes: &[u8]| i64::from_le_bytes(bytes.try_into().expect("eight bytes"));

        // Joined: the offset to the server's clock, and the round trip it was measured with.
        let tried = Instant::now();
        let (offset, rtt) = loop {
            if let (Some(offset), Some(rtt)) = said(&orion)
                && offset.len() == 8
            {
                break (number(&offset), number(&rtt));
            }
            assert!(
                tried.elapsed() < WAIT,
                "orion.time never said the offset: {:?}, clients {:?}",
                said(&orion),
                host.clients()
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        assert!((offset - true_offset_us()).abs() < 20_000, "{offset}");
        assert!((0..1_000_000).contains(&rtt), "{rtt}");

        // A value measured a while before it is published is stamped, on the server's
        // clock, with when it was measured: not with when the gateway sent it on.
        let mut level = orion
            .declare(&TopicDecl {
                name: "tank.level",
                fields: &fields![litres: Float "l"],
                history: 8,
                expiry_ms: 0,
                max_payload: 8,
                on_subscribe: Default::default(),
            })
            .expect("declare");
        // Measured 300 ms ago, which the server's clock, started with the server, must reach
        // back to.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let measured = SampleTime::now().as_nanos() - 300_000_000;
        level
            .publish(
                FieldId(0),
                SampleTime::from_nanos(measured),
                &4.5_f64.to_le_bytes(),
            )
            .expect("publish");
        let tried = Instant::now();
        let stamped = loop {
            orion.pump(Duration::from_millis(5)).expect("pump");
            if let Some(topic) = host.topic("/Tank/level")
                && topic.value == Some(Value::Double(4.5))
            {
                break topic.timestamp_us.expect("stamped");
            }
            assert!(tried.elapsed() < WAIT, "the level never reached the server");
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        let expected = (measured / 1_000) as i64 + true_offset_us();
        assert!(
            (stamped - expected).abs() < 20_000,
            "stamped {stamped}, measured at {expected} on the server's clock"
        );

        // The server lost: the two fields are said with nothing in them.
        drop(first);
        let tried = Instant::now();
        loop {
            if let (Some(offset), Some(rtt)) = said(&orion)
                && offset.is_empty()
                && rtt.is_empty()
            {
                break;
            }
            assert!(
                tried.elapsed() < WAIT,
                "orion.time never cleared the offset"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // And said again when another comes up in its place.
        let _second = server(port).await;
        let tried = Instant::now();
        loop {
            if said(&orion).0.is_some_and(|offset| offset.len() == 8) {
                break;
            }
            assert!(
                tried.elapsed() < WAIT,
                "orion.time never said the offset again"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
}

#[test]
fn a_board_is_a_server_or_a_client_of_one() {
    let parse = |text: &str| {
        let mut source = Source::parse(text, Vec::new()).expect("source");
        orion_nt4_client_gateway::Nt4ClientConfig::from_source(&mut source)
            .map_err(|error| error.to_string())
    };
    let config = parse("nt4.server = controller.local\nnt4.publish = a.b:c=double@/A/b")
        .expect("good")
        .expect("some");
    assert_eq!(
        (config.host.as_str(), config.port, config.name.as_str()),
        ("controller.local", 5810, "orion")
    );
    assert_eq!(config.mapping.publish.len(), 1);
    assert_eq!(parse("nt4.publish = a.b:c=double@/A/b"), Ok(None));
    let both = parse("nt4.server = 10.0.0.2\nnt4.listen = 0.0.0.0:5810").expect_err("both");
    assert!(both.contains("one form or the other"), "{both}");
    assert!(parse("nt4.server = host:port").is_err());
    assert!(parse("nt4.server = 10.0.0.2\nnt4.publish = nonsense").is_err());
}

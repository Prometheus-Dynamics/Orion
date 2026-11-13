//! The gateway in a comm core, with programs on the board on one side and an NT4 client on
//! the other: the client sees the mapped topics under their NT4 names and types, a value
//! that expires is withdrawn, and what the client writes back is published on the board.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use orion::topics::{FieldId, Next, SampleTime, TopicDecl};
use orion::{Address, Orion, fields};
use orion_comm_app::{CommCore, CommSettings};
use orion_config::Source;
use orion_nt4::{
    Client, ClientConfig, ClientEvent, SubscribeOptions, Subscription, TopicEvent, Value,
};
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(10);

const MAPPING: &str = "\
nt4.listen = 127.0.0.1:0
nt4.period_ms = 10
nt4.publish = tank.level:litres=double@/Tank/level, tank.level:full=boolean@/Tank/full, \
              tank.alarm:text=string@/Tank/alarm, tank.level:litres_wrong=int@/Tank/wrong, \
              tank.absent:value=double@/Tank/absent, tank.level:shape=struct:Shape@/Tank/shape
nt4.accept = panel.setpoint:value=double@/Panel/setpoint, panel.setpoint:mode=string@/Panel/mode
";

/// A comm core with the gateway, in a thread of its own, until dropped.
struct Board {
    socket: PathBuf,
    nt4: SocketAddr,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
    dir: PathBuf,
}

impl Board {
    fn start(name: &str) -> Self {
        Self::start_on(name, false)
    }

    /// With `on_ws`, the gateway shares the WebSocket listener instead of a port of its own.
    fn start_on(name: &str, on_ws: bool) -> Self {
        let dir = std::env::temp_dir().join(format!("orion-nt4-gw-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("dir");
        let text = format!(
            "node.name = board\nsocket = {0}/orion.sock\nstate.dir = {0}/state\n{1}",
            dir.display(),
            if on_ws {
                MAPPING.replace(
                    "nt4.listen = 127.0.0.1:0",
                    "ws.listen = 127.0.0.1:0\nnt4.listen = ws",
                )
            } else {
                MAPPING.to_owned()
            }
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
                let gateway = core.nt4().expect("the gateway runs").local_addr();
                let nt4 = if on_ws {
                    core.ws_addr().expect("the websocket listens")
                } else {
                    gateway
                };
                ready.send((core.socket().to_owned(), nt4)).expect("ready");
                let _ = stopped.await;
            });
        });
        let (socket, nt4) = started.recv().expect("the board starts");
        Self {
            socket,
            nt4,
            stop: Some(stop),
            thread: Some(thread),
            dir,
        }
    }
}

impl Drop for Board {
    fn drop(&mut self) {
        drop(self.stop.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// What the programs on the board are told to do, and what they read.
#[derive(Default)]
struct Told {
    stop: AtomicBool,
    /// Each time this goes up the alarm is published once.
    alarms: AtomicU32,
}

/// The programs on the board: a tank that publishes its level all the time and an alarm
/// when told, and a panel reader that reports what is published as `panel.setpoint`.
fn programs(socket: &Path, told: &Told, read: &mpsc::Sender<(u16, Vec<u8>)>) {
    let orion = Orion::connect(&Address::Local(socket.to_owned()), "tank").expect("connect");
    let mut level = orion
        .declare(&TopicDecl {
            name: "tank.level",
            fields: &fields![litres: Float "l", full: Bool, litres_wrong: Float, shape: Bytes],
            history: 8,
            expiry_ms: 0,
            max_payload: 16,
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
    // Not mapped: NT4 never hears of it.
    let mut secret = orion
        .declare(&TopicDecl {
            name: "tank.secret",
            fields: &fields![code: Int],
            history: 2,
            expiry_ms: 0,
            max_payload: 8,
            on_subscribe: Default::default(),
        })
        .expect("declare");
    let mut panel = orion.subscribe("panel.setpoint").expect("subscribe");
    let (mut step, mut alarms) = (0_u64, 0);
    while !told.stop.load(Ordering::Relaxed) {
        step += 1;
        let time = SampleTime::from_nanos(step);
        let litres = step as f64 * 0.5;
        level
            .publish(FieldId(0), time, &litres.to_le_bytes())
            .expect("publish");
        level
            .publish(FieldId(1), time, &[u8::from(litres > 5.0)])
            .expect("publish");
        level
            .publish(FieldId(2), time, &litres.to_le_bytes())
            .expect("publish");
        level
            .publish(FieldId(3), time, &[1, 2, 3, 4])
            .expect("publish");
        secret
            .publish(FieldId(0), time, &7_i64.to_le_bytes())
            .expect("publish");
        if told.alarms.load(Ordering::Relaxed) != alarms {
            alarms += 1;
            alarm
                .publish(FieldId(0), time, b"level high")
                .expect("publish");
        }
        while let Next::Sample(sample) = panel.next() {
            let _ = read.send((sample.field.0, sample.payload.to_vec()));
        }
        orion.pump(Duration::from_millis(5)).expect("pump");
    }
}

async fn connect(addr: SocketAddr, name: &str) -> Client {
    let mut config = ClientConfig::new("127.0.0.1", name);
    config.port = addr.port();
    let mut client = Client::start(config);
    loop {
        match timeout(WAIT, client.next_event())
            .await
            .expect("an event")
            .expect("a client")
        {
            ClientEvent::Connected => return client,
            ClientEvent::Disconnected { reason } => panic!("connect failed: {reason}"),
            _ => {}
        }
    }
}

/// The next event that `wanted` takes.
async fn until<T>(
    topics: &mut Subscription,
    mut wanted: impl FnMut(&TopicEvent) -> Option<T>,
) -> T {
    timeout(WAIT, async {
        loop {
            let event = topics.next().await.expect("the client runs");
            if let Some(found) = wanted(&event) {
                return found;
            }
        }
    })
    .await
    .expect("the event came")
}

/// The next sample read on the board. Waits without holding up the NT4 client, which runs
/// on this thread.
async fn on_the_board(read: &mpsc::Receiver<(u16, Vec<u8>)>) -> (u16, Vec<u8>) {
    timeout(WAIT, async {
        loop {
            if let Ok(sample) = read.try_recv() {
                return sample;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("a sample on the board")
}

fn value_of<'a>(event: &'a TopicEvent, topic: &str) -> Option<&'a Value> {
    match event {
        TopicEvent::Value { name, value, .. } if name == topic => Some(value),
        _ => None,
    }
}

#[test]
fn an_nt4_client_sees_mapped_topics_and_writes_one_back() {
    let board = Board::start("maps");
    let told = Arc::new(Told::default());
    let (read, on_board) = mpsc::channel();
    let (socket, telling) = (board.socket.clone(), told.clone());
    let running = std::thread::spawn(move || programs(&socket, &telling, &read));

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let client = connect(board.nt4, "robot").await;
        let everything = SubscribeOptions {
            prefix: true,
            ..Default::default()
        };
        let mut topics = client
            .handle()
            .subscribe(&["/"], everything)
            .expect("subscribe");

        // The mapped fields are NT4 topics, under the names and types the mapping gives.
        let mut announced: Vec<(String, String)> = Vec::new();
        until(&mut topics, |event| {
            if let TopicEvent::Announced(info) = event {
                announced.push((info.name.clone(), info.type_name.clone()));
            }
            (announced.len() == 3).then_some(())
        })
        .await;
        announced.sort();
        assert_eq!(
            announced,
            [
                ("/Tank/full".to_owned(), "boolean".to_owned()),
                ("/Tank/level".to_owned(), "double".to_owned()),
                ("/Tank/shape".to_owned(), "struct:Shape".to_owned()),
            ]
        );

        // Their values follow the board's: the level goes up half a litre at a time, the
        // flag turns when it passes five, and bytes arrive as they were.
        let first = until(&mut topics, |event| match value_of(event, "/Tank/level") {
            Some(Value::Double(litres)) => Some(*litres),
            _ => None,
        })
        .await;
        let later = until(&mut topics, |event| match value_of(event, "/Tank/level") {
            Some(Value::Double(litres)) if *litres > first + 2.0 => Some(*litres),
            _ => None,
        })
        .await;
        assert_eq!(later % 0.5, 0.0, "{later}");
        until(&mut topics, |event| {
            (value_of(event, "/Tank/full") == Some(&Value::Boolean(true))).then_some(())
        })
        .await;
        until(&mut topics, |event| {
            (value_of(event, "/Tank/shape") == Some(&Value::Raw(vec![1, 2, 3, 4]))).then_some(())
        })
        .await;

        // A value that expires: announced with its value, withdrawn when it has stood
        // longer than the topic's expiry, and back with the next sample.
        for round in 0..2 {
            told.alarms.fetch_add(1, Ordering::Relaxed);
            let raised = std::time::Instant::now();
            until(&mut topics, |event| {
                (value_of(event, "/Tank/alarm") == Some(&Value::String("level high".into())))
                    .then_some(())
            })
            .await;
            until(&mut topics, |event| {
                matches!(event, TopicEvent::Unannounced { name, .. } if name == "/Tank/alarm")
                    .then_some(())
            })
            .await;
            let stood = raised.elapsed();
            assert!(
                stood >= Duration::from_millis(290) && stood < Duration::from_secs(3),
                "round {round}: the alarm stood for {stood:?}"
            );
        }

        // Writing back: what the client publishes under the accepted names is published
        // on the board, as the fields the mapping names.
        let setpoint = client
            .handle()
            .publish("/Panel/setpoint", "double")
            .expect("publish");
        let mode = client
            .handle()
            .publish("/Panel/mode", "string")
            .expect("publish");
        setpoint.set(Value::Double(7.5)).expect("set");
        mode.set(Value::String("auto".into())).expect("set");
        let mut got = Vec::new();
        while got.len() < 2 {
            got.push(on_the_board(&on_board).await);
        }
        got.sort();
        assert_eq!(
            got,
            [(0, 7.5_f64.to_le_bytes().to_vec()), (1, b"auto".to_vec())]
        );

        // A second client writes a value of another type under the same name. It is not
        // published; the next one that fits is.
        let other = connect(board.nt4, "panel").await;
        let wrong = other
            .handle()
            .publish("/Panel/setpoint", "string")
            .expect("publish");
        wrong
            .set(Value::String("seven and a half".into()))
            .expect("set");
        tokio::time::sleep(Duration::from_millis(200)).await;
        let again = client
            .handle()
            .publish("/Panel/setpoint", "double")
            .expect("publish");
        again.set(Value::Double(8.5)).expect("set");
        assert_eq!(
            on_the_board(&on_board).await,
            (0, 8.5_f64.to_le_bytes().to_vec())
        );

        // Never announced: a topic that is not mapped, a mapped one nobody publishes, and a
        // field whose type does not go with the NT4 type it was mapped to.
        let names: Vec<String> = announced.into_iter().map(|(name, _)| name).collect();
        for absent in [
            "/Tank/wrong",
            "/Tank/absent",
            "/tank.secret",
            "/Tank/secret",
        ] {
            assert!(!names.iter().any(|name| name == absent), "{absent}");
        }
    });
    told.stop.store(true, Ordering::Relaxed);
    running.join().expect("the programs end");
}

#[test]
fn an_nt4_client_reaches_the_gateway_on_the_websocket_port() {
    let board = Board::start_on("on-ws", true);
    let told = Arc::new(Told::default());
    let (read, _on_board) = mpsc::channel();
    let (socket, telling) = (board.socket.clone(), told.clone());
    let running = std::thread::spawn(move || programs(&socket, &telling, &read));

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let client = connect(board.nt4, "robot").await;
        let mut topics = client
            .handle()
            .subscribe(&["/Tank/level"], SubscribeOptions::default())
            .expect("subscribe");
        let first = until(&mut topics, |event| match value_of(event, "/Tank/level") {
            Some(Value::Double(litres)) => Some(*litres),
            _ => None,
        })
        .await;
        until(&mut topics, |event| match value_of(event, "/Tank/level") {
            Some(Value::Double(litres)) if *litres > first => Some(()),
            _ => None,
        })
        .await;
    });
    told.stop.store(true, Ordering::Relaxed);
    running.join().expect("the programs end");
}

#[test]
fn nt4_on_the_websocket_needs_a_websocket_listener() {
    let dir = std::env::temp_dir().join(format!("orion-nt4-gw-no-ws-{}", std::process::id()));
    let text = format!(
        "node.name = board\nsocket = {0}/orion.sock\nstate.dir = {0}/state\n{1}",
        dir.display(),
        MAPPING.replace("nt4.listen = 127.0.0.1:0", "nt4.listen = ws")
    );
    let mut source = Source::parse(&text, Vec::new()).expect("source");
    let settings = CommSettings::from_source(&mut source).expect("settings");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let started = tokio::task::LocalSet::new()
        .block_on(&runtime, CommCore::start(settings))
        .map(|_| ());
    let error = started.expect_err("nothing to share").to_string();
    assert!(error.contains("no ws.listen to share"), "{error}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_mapping_that_does_not_hold_together_is_refused() {
    let parse = |extra: &str| {
        let text = format!("nt4.listen = 127.0.0.1:0\n{extra}");
        let mut source = Source::parse(&text, Vec::new()).expect("source");
        orion_nt4_gateway::Nt4Config::from_source(&mut source).map_err(|error| error.to_string())
    };
    assert!(
        parse("nt4.publish = a.b:c=double@/A/b")
            .expect("good")
            .is_some()
    );
    // An NT4 name with spaces and colons, and a type with a colon.
    let config = parse("nt4.publish = a.b:c=struct:Pose2d@/Smart Dashboard/Field:Robot")
        .expect("good")
        .expect("some");
    assert_eq!(
        (
            config.mapping.publish[0].nt_type.as_str(),
            config.mapping.publish[0].nt_name.as_str()
        ),
        ("struct:Pose2d", "/Smart Dashboard/Field:Robot")
    );
    for (bad, why) in [
        ("nt4.publish = a.b=double@/A/b", "not topic:field=type@name"),
        ("nt4.publish = a.b:c=/A/b", "not topic:field=type@name"),
        ("nt4.publish = a.b:c=number@/A/b", "not an NT4 type"),
        (
            "nt4.publish = a.b:c=double@/A, a.b:d=double@/A",
            "mapped twice",
        ),
        (
            "nt4.publish = a.b:c=double@/A\nnt4.accept = x.y:z=double@/A",
            "mapped twice",
        ),
        ("nt4.accept = x.y:z=string[]@/A", "no type here"),
        (
            "nt4.accept = x.y:z=double@/A, x.y:z=double@/B",
            "written by two",
        ),
        (
            "nt4.publish = a.b:c=double@/A\nnt4.accept = a.b:d=double@/B",
            "both goes to NT4 and comes from it",
        ),
    ] {
        let error = parse(bad).expect_err(bad);
        assert!(error.contains(why), "{bad}: {error}");
    }
    // Without `nt4.listen` there is no gateway, whatever else is said.
    let mut source = Source::parse("nt4.publish = a.b:c=double@/A/b", Vec::new()).expect("source");
    assert_eq!(
        orion_nt4_gateway::Nt4Config::from_source(&mut source),
        Ok(None)
    );
}

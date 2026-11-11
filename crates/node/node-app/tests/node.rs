//! Node logic as the comm core's first client: it answers for the records, it can die without
//! anything else on the board noticing, and it takes up where it was when it is back.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use orion::records::{Bytes, Record, RecordClass, RecordEntry, Records};
use orion::topics::{FieldId, FrameTime, Next, TopicDecl};
use orion::types::{Error, HlcTimestamp};
use orion::{Address, ClientError, Orion, fields};
use orion_node_app::{Node, NodeSettings};
use orion_stubs::Board;

use tokio::sync::oneshot;

fn dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("orion-node-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("directory");
    dir
}

fn settings(dir: &Path, save_after: Duration) -> NodeSettings {
    let mut source = orion_config::Source::parse("", []).expect("no settings");
    let mut settings = NodeSettings::from_source(&mut source).expect("defaults");
    settings.config.socket = dir.join("orion.sock");
    settings.config.state_dir = dir.join("state");
    settings.config.node_name = "board".parse().expect("name");
    settings.save_after = save_after;
    settings
}

/// Node logic in a thread of its own.
struct Logic {
    kill: Option<oneshot::Sender<()>>,
    ask: Option<oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Logic {
    fn start(settings: NodeSettings) -> Self {
        let (kill, killed) = oneshot::channel::<()>();
        let (ask, asked) = oneshot::channel::<()>();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            runtime.block_on(async move {
                let mut node = Node::load(settings).expect("node logic starts");
                tokio::select! {
                    () = node.run(async { let _ = asked.await; }) => {}
                    // Dropped where it stands: nothing is saved on the way out.
                    _ = killed => {}
                }
            });
        });
        Self {
            kill: Some(kill),
            ask: Some(ask),
            thread: Some(thread),
        }
    }

    /// Ends node logic as a crash would: at once, saving nothing.
    fn kill(mut self) {
        let _ = self.kill.take().expect("alive").send(());
        let _ = self.thread.take().expect("alive").join();
    }

    /// Asks node logic to stop, as the service manager would.
    fn stop(mut self) {
        let _ = self.ask.take().expect("alive").send(());
        let _ = self.thread.take().expect("alive").join();
    }
}

impl Drop for Logic {
    fn drop(&mut self) {
        if let (Some(kill), Some(thread)) = (self.kill.take(), self.thread.take()) {
            let _ = kill.send(());
            let _ = thread.join();
        }
    }
}

fn unavailable<T: std::fmt::Debug>(result: Result<T, ClientError>) -> bool {
    matches!(result, Err(ClientError::Refused(Error::Unavailable)))
}

/// Reads until node logic answers.
fn answered(records: &Records, class: RecordClass, prefix: &str) -> Vec<Record> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match records.get(class, prefix) {
            Ok(found) => return found,
            Err(ClientError::Refused(Error::Unavailable)) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(error) => panic!("records: {error}"),
        }
    }
}

fn entry<'a>(key: &'a str, value: &'a [u8]) -> RecordEntry<'a> {
    RecordEntry {
        class: RecordClass::Resource,
        key,
        stamp: HlcTimestamp::ZERO,
        deleted: false,
        value: Bytes(value),
    }
}

#[test]
fn node_logic_can_die_and_links_stay_up_and_it_resumes() {
    let dir = dir("kill");
    let board = Board::start_for_node(&dir);
    let address = Address::Local(board.socket().to_owned());
    let quick = Duration::from_millis(10);
    let logic = Logic::start(settings(&dir, quick));

    let sensor = Orion::connect(&address, "sensor").expect("connect");
    let dashboard = Orion::connect(&address, "dashboard").expect("connect");
    let mut level = sensor
        .declare(&TopicDecl {
            name: "tank.level",
            fields: &fields![litres: Float "l"],
            history: 64,
            expiry_ms: 0,
            max_payload: 8,
        })
        .expect("declare");
    let mut reading = dashboard.subscribe("tank.level").expect("subscribe");
    let mut publish_and_read = |value: u64| {
        level
            .publish(
                FieldId(0),
                FrameTime::from_nanos(value),
                &(value as f64).to_le_bytes(),
            )
            .expect("publish");
        assert!(reading.wait(Duration::from_secs(2)).expect("wait"));
        match reading.next() {
            Next::Sample(sample) => {
                assert_eq!(
                    f64::from_le_bytes(sample.payload.try_into().expect("float")),
                    value as f64
                );
            }
            other => panic!("{other:?}"),
        }
    };

    // Node logic answers for the records.
    let records = sensor.records();
    assert!(answered(records, RecordClass::Resource, "").is_empty());
    let mut watch = dashboard
        .records()
        .watch(RecordClass::Resource, "fan.", HlcTimestamp::ZERO)
        .expect("watch");
    let stored = records.put(&[entry("fan.left", b"one")]).expect("put");
    assert_eq!(stored.len(), 1);
    assert!(stored[0].stamp > HlcTimestamp::ZERO);
    let seen = watch.wait(Duration::from_secs(2)).expect("watch");
    assert_eq!(seen.as_ref(), Some(&stored[0]));
    records.put(&[entry("pump", b"p")]).expect("put");
    publish_and_read(1);
    // Long enough for the save, which is a tenth of this.
    std::thread::sleep(Duration::from_millis(300));

    // Node logic is killed. Topics flow, sessions stay, and records say "restarting".
    logic.kill();
    publish_and_read(2);
    assert!(unavailable(records.get(RecordClass::Resource, "")));
    assert!(unavailable(records.put(&[entry("fan.right", b"two")])));
    publish_and_read(3);
    dashboard
        .pump(Duration::from_millis(20))
        .expect("still connected");

    // It comes back with what it had, stamps and all.
    let logic = Logic::start(settings(&dir, quick));
    let found = answered(records, RecordClass::Resource, "fan.");
    assert_eq!(found, stored);
    assert_eq!(answered(records, RecordClass::Resource, "").len(), 2);

    // The watch made before it died is still told, though the program never asked again.
    let right = records.put(&[entry("fan.right", b"two")]).expect("put");
    assert!(right[0].stamp > stored[0].stamp, "the clock went on");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let seen = watch
            .wait(Duration::from_secs(2))
            .expect("watch")
            .expect("a change");
        assert!(Instant::now() < deadline);
        if seen.key == "fan.right" {
            assert_eq!(seen, right[0]);
            break;
        }
        // What it was passed again: a version it already had.
        assert_eq!(seen, stored[0]);
    }

    // A delete is a record too.
    let gone = records
        .put(&[RecordEntry {
            deleted: true,
            ..entry("fan.left", b"")
        }])
        .expect("delete");
    assert!(gone[0].deleted);
    let seen = watch
        .wait(Duration::from_secs(2))
        .expect("watch")
        .expect("a change");
    assert_eq!(seen, gone[0]);
    assert_eq!(answered(records, RecordClass::Resource, "fan.").len(), 1);
    publish_and_read(4);
    drop(logic);
}

#[test]
fn a_comm_core_that_restarts_finds_node_logic_again() {
    let dir = dir("comm-restart");
    let mut board = Board::start_for_node(&dir);
    let address = Address::Local(board.socket().to_owned());
    let _logic = Logic::start(settings(&dir, Duration::from_millis(10)));
    let program = Orion::connect(&address, "program").expect("connect");
    assert!(answered(program.records(), RecordClass::Node, "").is_empty());
    let stored = program
        .records()
        .put(&[RecordEntry {
            class: RecordClass::Node,
            ..entry("board", b"here")
        }])
        .expect("put");

    board.stop();
    let _board = Board::start_for_node(&dir);
    program.reconnect(|_| true).expect("the program reconnects");
    // Node logic never stopped: it has the record whether or not it was saved.
    assert_eq!(answered(program.records(), RecordClass::Node, ""), stored);
}

#[test]
fn asked_to_stop_it_saves_what_it_has_not_yet() {
    let dir = dir("stop");
    let board = Board::start_for_node(&dir);
    let address = Address::Local(board.socket().to_owned());
    // A save that is never due on its own.
    let never = Duration::from_secs(3600);
    let logic = Logic::start(settings(&dir, never));
    let program = Orion::connect(&address, "program").expect("connect");
    assert!(answered(program.records(), RecordClass::Resource, "").is_empty());
    let stored = program
        .records()
        .put(&[entry("valve", b"open")])
        .expect("put");
    logic.stop();

    let logic = Logic::start(settings(&dir, never));
    assert_eq!(
        answered(program.records(), RecordClass::Resource, ""),
        stored
    );
    drop(logic);
}

#[test]
fn a_read_larger_than_one_message_comes_in_several() {
    let dir = dir("pages");
    let board = Board::start_for_node(&dir);
    let address = Address::Local(board.socket().to_owned());
    let _logic = Logic::start(settings(&dir, Duration::from_secs(3600)));
    let program = Orion::connect(&address, "program").expect("connect");
    let records = program.records();
    assert!(answered(records, RecordClass::Resource, "").is_empty());
    let value = vec![7_u8; 1000];
    for index in 0..200 {
        let key = format!("part.{index:03}");
        records.put(&[entry(&key, &value)]).expect("put");
    }
    let found = records.get(RecordClass::Resource, "part.").expect("get");
    assert_eq!(found.len(), 200);
    assert!(found.iter().all(|record| record.value == value));
    assert!(found.windows(2).all(|pair| pair[0].key < pair[1].key));
}

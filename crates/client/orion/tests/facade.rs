//! The facade: each role through `orion` alone, and a comm core that restarts under it.

#[path = "../examples/roles.rs"]
mod roles;

use std::time::Duration;

use orion::records::{RecordClass, RecordEntry};
use orion::topics::{FieldId, FrameTime, Next, Status, TopicDecl};
use orion::types::HlcTimestamp;
use orion::{Address, Orion, fields};
use orion_stubs::Board;

#[test]
fn the_example_uses_each_role_through_the_facade_alone() {
    let board = Board::start("facade-roles");
    let story = roles::run(board.socket()).expect("the example runs");
    assert_eq!(
        story,
        [
            "found fan.left (fan), Ready",
            "fan.left.speed reads 900",
            "fan.left.set ended Succeeded",
            "fan.left.speed reads 1500",
            "workload nightly is Running on board",
            "workload nightly is Gone",
        ]
    );
}

#[test]
fn a_comm_core_that_restarts_stops_nothing_that_was_flowing() {
    let mut board = Board::start("facade-restart");
    let (dir, port) = (board.dir().to_owned(), board.network().port());
    let address = Address::Local(board.socket().to_owned());
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
    let mut watch = dashboard
        .records()
        .watch(RecordClass::Resource, "", HlcTimestamp::ZERO)
        .expect("watch");
    let publish = |level: &mut orion::topics::Publisher, value: u64| {
        level
            .publish(
                FieldId(0),
                FrameTime::from_nanos(value),
                &(value as f64).to_le_bytes(),
            )
            .expect("publish");
    };
    let read_all = |reading: &mut orion::topics::Subscriber| -> Vec<f64> {
        let mut values = Vec::new();
        loop {
            match reading.next() {
                Next::Sample(sample) => values.push(f64::from_le_bytes(
                    sample.payload.try_into().expect("float"),
                )),
                Next::Empty => return values,
                Next::Skipped(lost) => panic!("lost {lost}"),
            }
        }
    };
    publish(&mut level, 1);
    assert!(reading.wait(Duration::from_secs(2)).expect("wait"));
    assert_eq!(read_all(&mut reading), [1.0]);

    // The comm core goes away. The ring is the publisher's and the subscriber's: values keep
    // going from one to the other with nothing in between.
    board.stop();
    for value in 2..=5 {
        publish(&mut level, value);
    }
    assert_eq!(
        read_all(&mut reading),
        [2.0, 3.0, 4.0, 5.0],
        "a ring needs no comm core"
    );

    // Both find the session lost when they next look at it.
    let lost = loop {
        match dashboard.pump(Duration::from_millis(20)) {
            Ok(_) => {}
            Err(error) => break error,
        }
    };
    assert!(lost.is_lost(), "{lost}");
    publish(&mut level, 6);

    // It comes back, with no memory of either. Each says everything again.
    board = Board::start_in(&dir, orion_stubs::Security::Off, Some(port));
    sensor.reconnect(|_| true).expect("the sensor is back");
    dashboard
        .reconnect(|_| true)
        .expect("the dashboard is back");
    assert_eq!(reading.status(), Status::Live);

    // What was published while it was away is still there to read, in order, and what is
    // published now arrives as before: it is the same ring, handed back.
    publish(&mut level, 7);
    assert!(reading.wait(Duration::from_secs(2)).expect("wait"));
    assert_eq!(read_all(&mut reading), [6.0, 7.0]);
    let mut woken = 0;
    for value in 8..=10 {
        publish(&mut level, value);
        assert!(
            reading.wait(Duration::from_secs(2)).expect("wait"),
            "the doorbell is wired again"
        );
        woken += read_all(&mut reading).len();
    }
    assert_eq!(woken, 3);
    assert_eq!(level.subscribers(), 1);

    // The watch was asked for again too: a record written now reaches it.
    let entry = RecordEntry {
        class: RecordClass::Resource,
        key: "pump.main",
        stamp: HlcTimestamp::ZERO,
        deleted: false,
        value: orion_wire_bytes(b"x"),
    };
    sensor.records().put(&[entry]).expect("put");
    let change = watch
        .wait(Duration::from_secs(2))
        .expect("wait")
        .expect("the change");
    assert_eq!(change.key, "pump.main");
    drop(board);
    let _ = std::fs::remove_dir_all(dir);
}

fn orion_wire_bytes(bytes: &[u8]) -> orion::records::Bytes<'_> {
    orion::records::Bytes(bytes)
}

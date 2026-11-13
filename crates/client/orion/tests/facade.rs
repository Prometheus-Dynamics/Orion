//! The facade: each role through `orion` alone, and a comm core that restarts under it.

#[path = "../examples/roles.rs"]
mod roles;

use std::time::Duration;

use orion::node::OperatorState;
use orion::records::{RecordClass, RecordEntry};
use orion::topics::{FieldId, Next, SampleTime, Status, TopicDecl};
use orion::types::HlcTimestamp;
use orion::{Address, Orion, fields};
use orion_stubs::{Board, Options};

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
            on_subscribe: Default::default(),
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
                SampleTime::from_nanos(value),
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

#[test]
fn a_resource_is_said_again_only_once_its_actions_are_back() {
    use orion::actions::{ActionDecl, Admits, Policy, Target};
    use orion::provider::ResourceState;

    let mut board = Board::start("facade-order");
    let (dir, port) = (board.dir().to_owned(), board.network().port());
    let address = Address::Local(board.socket().to_owned());
    let options = orion::Options {
        roles: orion::Roles::CLIENT.and(orion::Roles::PROVIDER),
        ..orion::Options::default()
    };
    let driver = Orion::connect_with(&address, "fan-driver", options).expect("connect");
    let mut describing = driver
        .provider()
        .resource("fan.left", "fan")
        .expect("resource");
    describing
        .action("set", "Sets the fan's speed", Policy::new(Admits::LOCAL))
        .expect("action");
    let _fan = describing.publish(ResourceState::Ready).expect("publish");

    // The comm core starts again, and another program takes the action's name first.
    board.stop();
    board = Board::start_in(&dir, orion_stubs::Security::Off, Some(port));
    let other = Orion::connect(&address, "other").expect("connect");
    let _taken = other
        .register_action(
            &ActionDecl {
                name: "fan.left.set",
                target: Target::Resource("fan.left".parse().expect("a name")),
                description: "Not the fan's",
            },
            Policy::new(Admits::LOCAL),
        )
        .expect("register");

    // The driver's actions are refused, so its resource is not said again: nobody finds a
    // fan whose actions belong to someone else.
    let again = driver.reconnect(|_| true);
    assert!(again.is_err(), "the name is taken");
    let found = other
        .records()
        .get(RecordClass::Resource, "fan.left")
        .expect("get");
    assert!(found.is_empty(), "{found:?}");
    drop(board);
    let _ = std::fs::remove_dir_all(dir);
}

fn orion_wire_bytes(bytes: &[u8]) -> orion::records::Bytes<'_> {
    orion::records::Bytes(bytes)
}

#[cfg(feature = "buffers")]
#[test]
fn a_camera_offers_its_pool_again_to_a_comm_core_that_restarts() {
    use orion::buffers::{HoldLimits, NextBatch, Pool};

    let mut board = Board::start("facade-pool");
    let (dir, port) = (board.dir().to_owned(), board.network().port());
    let address = Address::Local(board.socket().to_owned());
    let camera = Orion::connect(&address, "camera").expect("connect");
    let pool = Pool::in_memory(1, 4, 64).expect("pool");
    let mut frames = camera
        .declare_buffers("camera.front", 4, 0, pool)
        .expect("declare");

    // The comm core starts again. The camera's topic is the only one there, so it goes by
    // the same number as before: that must not be taken for "nothing changed".
    board.stop();
    board = Board::start_in(&dir, orion_stubs::Security::Off, Some(port));
    camera.reconnect(|_| true).expect("the camera is back");
    frames.poll().expect("poll");

    // A viewer new to this comm core is handed the pool and sees the camera's frames.
    let viewer = Orion::connect(&address, "viewer").expect("connect");
    let limits = HoldLimits {
        max_held: 1,
        max_hold_ms: 0,
    };
    let mut reading = viewer
        .subscribe_buffers("camera.front", &[], limits)
        .expect("subscribe");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut fill = 0_u8;
    loop {
        frames.poll().expect("poll");
        fill = fill.wrapping_add(1);
        if let Ok(buffer) = frames.take() {
            frames.buffer_mut(buffer).expect("memory").fill(fill);
            frames
                .publish(&[buffer], SampleTime::from_nanos(u64::from(fill)), &[])
                .expect("publish");
        }
        let _ = viewer.pump(Duration::from_millis(5));
        if let NextBatch::Batch(frame) = reading.next() {
            let filled = frame.time.as_nanos() as u8;
            assert!(
                frame
                    .held
                    .bytes(0)
                    .expect("bytes")
                    .iter()
                    .all(|&b| b == filled)
            );
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the viewer was never given the pool"
        );
    }
    drop(board);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_comm_core_set_to_wait_longer_is_waited_for_as_long() {
    let dir = std::env::temp_dir().join(format!("orion-facade-idle-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    let options = Options {
        heartbeat_ms: 5000,
        idle_ms: 60_000,
        ..Options::default()
    };
    let board = Board::start_with(&dir, options);
    let orion =
        Orion::connect(&Address::Local(board.socket().to_owned()), "slow").expect("connect");
    let welcomed = orion.welcomed();
    assert_eq!(welcomed.heartbeat, Duration::from_secs(5));
    assert_eq!(welcomed.idle, Duration::from_secs(60));
    drop(orion);
    drop(board);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn an_operator_asks_to_be_enrolled_and_proves_the_enrollment_key() {
    let dir = std::env::temp_dir().join(format!("orion-facade-enroll-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    let board = Board::start_with(
        &dir,
        Options {
            security: orion_stubs::Security::On,
            enrollment_key: Some(b"correct horse".to_vec()),
            ..Options::default()
        },
    );
    let me = orion::Identity::generate();
    let public = me.public();
    let address = Address::Network {
        host: board.network().to_string(),
        key: None,
    };
    let options = orion::Options {
        identity: Some(me),
        ..orion::Options::default()
    };
    let orion = Orion::connect_with(&address, "desk", options).expect("a signed session");

    // Without the enrollment key: waiting for someone on the board.
    let asked = orion.enroll(public, "desk", None).expect("asked");
    assert_eq!(asked.state, OperatorState::Pending);
    assert_eq!(asked.node, "board");
    assert_eq!(asked.cluster, "lab");
    assert_eq!(
        asked.fingerprint.len(),
        orion.node().expect("node").fingerprint.len()
    );
    // A wrong key proves nothing; the right one enrolls it.
    let wrong = orion
        .enroll(public, "desk", Some(b"battery staple"))
        .expect("asked");
    assert_eq!(wrong.state, OperatorState::Pending);
    let enrolled = orion
        .enroll(public, "desk", Some(b"correct horse"))
        .expect("enrolled");
    assert_eq!(enrolled.state, OperatorState::Enrolled);
    assert_eq!(enrolled.node_key, orion.node().expect("node").public_key);

    // Nobody speaks for another key.
    let other = orion.enroll([7; 32], "someone", Some(b"correct horse"));
    assert!(
        matches!(
            other,
            Err(orion::Error::Client(orion::ClientError::Refused(
                orion::types::Error::Denied
            )))
        ),
        "{other:?}"
    );
    drop(orion);
    drop(board);
    let _ = std::fs::remove_dir_all(&dir);
}

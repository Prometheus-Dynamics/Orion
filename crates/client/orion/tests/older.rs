//! A node from before the additive level is asked for nothing whose meaning it would change by
//! not knowing it: each such call says so, naming the feature and both levels, and sends nothing.

#![cfg(target_os = "linux")]

use std::os::unix::net::UnixStream;
use std::time::Duration;

use orion::actions::{ActionCall, Target};
use orion::topics::{OnSubscribe, TopicDecl};
use orion::{ClientError, Error, Feature, Options, Orion, Session, fields};
use orion_stubs::older::OlderNode;
use orion_wire::{INTERFACE_REVISION, kind};

struct Half(UnixStream);

impl orion::LinkRead for Half {
    fn read(&mut self, into: &mut [u8]) -> std::io::Result<usize> {
        std::io::Read::read(&mut self.0, into)
    }
}

impl orion::LinkWrite for Half {
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        std::io::Write::write_all(&mut self.0, bytes)
    }
}

fn older() -> (OlderNode, Orion) {
    let (node, link) = OlderNode::start(INTERFACE_REVISION);
    let write = Half(link.try_clone().expect("the link again"));
    let options = Options {
        timeout: Duration::from_millis(500),
        ..Options::default()
    };
    let session = Session::over(Half(link), write, None, "tool", options).expect("older node");
    (node, Orion::over(session))
}

fn names(error: &Error, feature: Feature) -> bool {
    let level = |error: &ClientError| matches!(error, ClientError::Level { feature: f, needs: 1, node: 0 } if *f == feature);
    match error {
        Error::Client(error) => level(error),
        Error::Topic(orion::topics::TopicError::Client(error)) => level(error),
        Error::Topic(_) => false,
    }
}

#[test]
fn an_older_node_is_asked_for_nothing_it_would_get_wrong() {
    let (node, orion) = older();
    let decl = |on_subscribe| TopicDecl {
        name: "tank.level",
        fields: &fields![litres: Float "l"],
        history: 8,
        expiry_ms: 0,
        max_payload: 8,
        on_subscribe,
    };

    // Subscribers that start from the newest values: an older node would start them from now.
    let error = orion
        .declare(&decl(OnSubscribe::Latest))
        .expect_err("latest");
    assert!(names(&error, Feature::SubscribeFromLatest), "{error:?}");
    assert!(error.to_string().contains("level 1"), "{error}");

    // A shell: an older node would never answer.
    let error = orion.shell("root", None, None).expect_err("shell");
    assert!(names(&error, Feature::Shell), "{error:?}");

    // A detached run: an older node would let it run with no limit, unseen.
    let call = |detached| ActionCall {
        name: "set",
        target: Target::Resource("fan.left".parse().expect("a name")),
        args: &[],
        deadline_ms: 1_000,
        detached,
    };
    let error = orion.run(&call(true)).expect_err("detached");
    assert!(names(&error, Feature::DetachedRun), "{error:?}");
    assert!(node.heard().is_empty(), "sent: {:?}", node.heard());

    // What an older node knows is still asked: the same topic from now, a run that is not
    // detached.
    let _ = orion.declare(&decl(OnSubscribe::FromNow));
    let _ = orion.run(&call(false));
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(node.heard(), [kind::TOPIC_DECLARE, kind::ACTION_RUN]);
}

/// A board one level below writes based on a version: everything else works with it, and a
/// write based on a version is not sent to it, because it would write whatever is there.
#[test]
fn a_level_6_board_takes_plain_writes_and_is_never_sent_one_based_on_a_version() {
    use orion::records::{RecordClass, RecordEntry};
    use orion::types::HlcTimestamp;
    use orion::workloads::{RestartPolicy, Workload};

    let dir = orion_scratch::Scratch::new("older-level-6");
    let board = orion_stubs::Board::start_with(
        &dir,
        orion_stubs::Options {
            level: 6,
            ..orion_stubs::Options::default()
        },
    );
    let orion =
        Orion::connect(&orion::Address::Local(board.socket().to_owned()), "tool").expect("connect");
    let based_on = |error: &ClientError| {
        matches!(
            error,
            ClientError::Level {
                feature: Feature::BasedOn,
                needs: 7,
                node: 6
            }
        )
    };
    let workload = Workload {
        name: "nightly",
        kind: "script",
        spec: b"tidy",
        node: None,
        restart: RestartPolicy::Never,
    };

    // A plain put and a plain remove: as ever. Its status has no stamp: zero.
    orion.workloads().put(&workload).expect("sent");
    let told = orion
        .workloads()
        .wait("nightly", Duration::from_secs(10), |_| true)
        .expect("not refused")
        .expect("told where it is");
    assert_eq!(told.stamp, HlcTimestamp::ZERO);

    // Based on a version: not sent, saying why.
    let stamp = HlcTimestamp::new(5, 0, 1);
    let error = orion
        .workloads()
        .put_based_on(&workload, &[], stamp)
        .expect_err("not sent");
    assert!(based_on(&error), "{error:?}");
    assert!(error.to_string().contains("level 7"), "{error}");
    let error = orion
        .workloads()
        .remove_based_on("nightly", false, stamp)
        .expect_err("not sent");
    assert!(based_on(&error), "{error:?}");

    // The same for a record: a stamp on an entry is not sent; none is.
    let entry = |stamp| RecordEntry {
        class: RecordClass::Node,
        key: "elsewhere",
        stamp,
        deleted: true,
        value: orion::records::Bytes(&[]),
    };
    let error = orion.records().put(&[entry(stamp)]).expect_err("not sent");
    assert!(based_on(&error), "{error:?}");
    // Plain, it is sent: whatever the board says of it, it is not a matter of level.
    if let Err(error) = orion.records().put(&[entry(HlcTimestamp::ZERO)]) {
        assert!(!matches!(error, ClientError::Level { .. }), "{error:?}");
    }
}

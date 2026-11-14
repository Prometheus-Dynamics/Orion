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

//! Device topics against a host played by the test, which reads and writes every body with
//! the general codec: so what the device writes by hand is what any host reads.

use orion_device_core::{Link, SendError};
use orion_device_host::{Drive, Rig, Sent};
use orion_device_topics::{
    Field, FieldType, Heard, PublishError, RETRY_MS, Request, SampleFlags, TopicSpec, TopicState,
    Topics,
};
use orion_msg_session::Refusal;
use orion_msg_topics::{Declare, Demand, Opened, Sample};
use orion_wire::{Bytes, FrameView};

#[global_allocator]
static GLOBAL: orion_measure::CountingAlloc = orion_measure::CountingAlloc::system();

const TX: usize = 320;
type TopicRig = Rig<Topics<2>, 256, TX>;

static SPECS: [TopicSpec; 2] = [
    TopicSpec::latest(
        "imu",
        &[
            Field::new("yaw", FieldType::Float, "deg"),
            Field::new("ok", FieldType::Bool, ""),
        ],
    ),
    TopicSpec {
        name: "log",
        fields: &[
            Field::new("line", FieldType::Text, ""),
            Field::structured("pose", "demo.Pose"),
        ],
        history: 16,
        expiry_ms: 500,
        max_payload: 240,
    },
];

fn poll(topics: &mut Topics<2>, link: &mut Link<TX>, now: u32) {
    topics.poll(link, now);
}

fn handle(topics: &mut Topics<2>, _: &mut Link<TX>, frame: &FrameView<'_>) -> String {
    format!("{:?}", topics.handle(frame))
}

fn rig() -> TopicRig {
    Rig::new(Topics::new(&SPECS), Drive { poll, handle })
}

fn opened(rig: &mut TopicRig, declare: &Sent, handle: u32) {
    rig.send(&Opened {
        request_seq: declare.seq(),
        handle,
        shared_memory: false,
        ring_bytes: 4096,
    });
}

/// A device whose two topics are open as handles 10 and 11.
fn open() -> TopicRig {
    let mut rig = rig();
    let declares = rig.drain();
    assert_eq!(declares.len(), 2);
    opened(&mut rig, &declares[0], 10);
    opened(&mut rig, &declares[1], 11);
    rig
}

/// Publishes with `publish` and returns the sample the host got.
fn published(
    rig: &mut TopicRig,
    publish: impl FnOnce(&mut Topics<2>, &mut Link<TX>) -> Result<u16, PublishError>,
) -> Vec<Sent> {
    publish(&mut rig.layer, &mut rig.device.link).expect("taken");
    rig.drain()
}

#[test]
fn a_session_declares_every_topic_as_the_host_reads_it() {
    let mut rig = rig();
    let sent = rig.drain();
    assert_eq!(sent.len(), 2);
    for (sent, spec) in sent.iter().zip(&SPECS) {
        let declare: Declare<'_> = sent.body();
        assert_eq!(declare.topic.as_str(), spec.name);
        assert_eq!(declare.history, spec.history);
        assert_eq!(declare.expiry_ms, spec.expiry_ms);
        assert_eq!(declare.max_payload, spec.max_payload);
        let fields: Vec<_> = declare.fields.iter().map(|f| f.expect("a field")).collect();
        assert_eq!(fields.len(), spec.fields.len());
        for (got, want) in fields.iter().zip(spec.fields) {
            assert_eq!(
                (got.name, got.ty, got.unit, got.type_name),
                (want.name, want.ty, want.unit, want.type_name)
            );
        }
    }
    assert_eq!(
        rig.layer.state(0),
        Some(TopicState::Declaring(Request::Asked {
            seq: sent[0].seq(),
            at_ms: 1
        }))
    );
}

#[test]
fn a_sample_reaches_the_host_once_the_topic_is_open() {
    let mut rig = rig();
    assert_eq!(
        rig.layer.publish_f64(&mut rig.device.link, 0, 0, 5, 1.0),
        Err(PublishError::NotOpen)
    );
    let declares = rig.drain();
    opened(&mut rig, &declares[0], 10);
    assert_eq!(rig.heard, ["Open { topic: 0 }"]);
    assert_eq!(
        rig.layer.state(0),
        Some(TopicState::Open {
            handle: 10,
            subscribers: 0
        })
    );

    let sent = published(&mut rig, |t, l| t.publish_f64(l, 0, 0, 1_234, 12.5));
    let got: Sample<'_> = sent[0].body();
    assert_eq!((got.handle, got.field, got.time.as_nanos()), (10, 0, 1_234));
    assert_eq!(got.flags, SampleFlags::NONE);
    assert_eq!(got.payload, Bytes(&12.5_f64.to_le_bytes()));

    let sent = published(&mut rig, |t, l| t.publish_bool(l, 0, 1, 1_300, true));
    assert_eq!(sent[0].body::<Sample<'_>>().payload, Bytes(&[1]));
    let sent = published(&mut rig, |t, l| t.publish_i64(l, 0, 1, 1_300, -3));
    assert_eq!(
        sent[0].body::<Sample<'_>>().payload,
        Bytes(&(-3_i64).to_le_bytes())
    );
}

#[test]
fn a_long_payload_and_its_flags_arrive_whole() {
    let mut rig = open();
    // Long enough that its length takes two bytes.
    let line: Vec<u8> = (0..200_u8).collect();
    let sent = published(&mut rig, |t, l| {
        t.publish_with(l, 1, 0, u64::MAX, SampleFlags::OTHERS_DEPEND, |w| {
            w.raw(&line);
        })
    });
    let got: Sample<'_> = sent[0].body();
    assert_eq!((got.handle, got.time.as_nanos()), (11, u64::MAX));
    assert!(got.flags.has(SampleFlags::OTHERS_DEPEND));
    assert_eq!(got.payload, Bytes(&line));
    // Too long for the frame: refused, nothing sent.
    assert_eq!(
        rig.layer.publish(&mut rig.device.link, 1, 0, 0, &[0; 400]),
        Err(PublishError::Link(SendError::TooLarge))
    );
    assert!(rig.drain().is_empty());
}

#[test]
fn one_sample_at_a_time_and_the_newest_wins() {
    let mut rig = open();
    let link = &mut rig.device.link;
    rig.layer.publish_f64(link, 0, 0, 1, 1.0).expect("taken");
    assert_eq!(
        rig.layer.publish_f64(link, 0, 0, 2, 2.0),
        Err(PublishError::Link(SendError::Busy))
    );
    assert_eq!(
        rig.layer.publish_f64(link, 7, 0, 2, 2.0),
        Err(PublishError::NoSuchTopic)
    );
    assert_eq!(rig.drain().len(), 1);
}

#[test]
fn a_declaration_without_an_answer_is_said_again() {
    let mut rig = rig();
    let first = rig.drain();
    assert_eq!(first.len(), 2);
    opened(&mut rig, &first[1], 11);
    // Nothing more until the wait is over, then the unanswered one only, once.
    assert!(rig.run(RETRY_MS - 1).is_empty());
    let again = rig.run(1);
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].body::<Declare<'_>>().topic.as_str(), "imu");
    // An answer to the first saying is not taken for an answer to the second.
    opened(&mut rig, &first[0], 10);
    assert_eq!(rig.last_heard(), Some("Other"));
    opened(&mut rig, &again[0], 10);
    assert_eq!(rig.last_heard(), Some("Open { topic: 0 }"));
}

#[test]
fn a_refused_topic_waits_for_the_next_session() {
    let mut rig = rig();
    let declares = rig.drain();
    rig.send(&Refusal {
        request_seq: declares[0].seq(),
        code: 4,
        detail: "taken",
    });
    assert_eq!(rig.heard, ["Refused { topic: 0, code: 4 }"]);
    assert_eq!(rig.layer.state(0), Some(TopicState::Refused { code: 4 }));
    // A refusal of something else is for another layer.
    rig.send(&Refusal {
        request_seq: 999,
        code: 4,
        detail: "",
    });
    assert_eq!(rig.last_heard(), Some("Other"));
    opened(&mut rig, &declares[1], 11);
    // Not asked again while the session lasts.
    assert!(rig.run(10 * RETRY_MS).is_empty());
    // The next session declares it again.
    rig.quiet = true;
    rig.run(400);
    rig.quiet = false;
    rig.connect();
    assert_eq!(
        rig.layer.state(0),
        Some(TopicState::Declaring(Request::Unsent))
    );
    assert_eq!(rig.drain().len(), 2);
}

#[test]
fn a_new_session_declares_every_topic_again() {
    let mut rig = open();
    // The host goes quiet; hello starts again and a new session is welcomed.
    rig.quiet = true;
    rig.run(400);
    rig.quiet = false;
    // The session that took the topics is over, and so is what it took.
    assert_eq!(rig.device.link.session(), None);
    assert_eq!(
        rig.layer.state(0),
        Some(TopicState::Declaring(Request::Unsent))
    );
    rig.connect();
    assert_eq!(
        rig.layer.publish_f64(&mut rig.device.link, 0, 0, 0, 0.0),
        Err(PublishError::NotOpen)
    );
    assert_eq!(rig.drain().len(), 2);
}

#[test]
fn demand_says_who_listens_and_what_they_ask() {
    let mut rig = open();
    assert_eq!(rig.layer.subscribers(1), 0);
    rig.send(&Demand {
        handle: 11,
        subscribers: 2,
        request: Bytes(b"rate=50"),
    });
    let want = Heard::Demand {
        topic: 1,
        subscribers: 2,
        request: b"rate=50",
    };
    assert_eq!(rig.last_heard(), Some(format!("{want:?}").as_str()));
    assert_eq!(rig.layer.subscribers(1), 2);
    // A handle that is not this device's changes nothing.
    rig.send(&Demand {
        handle: 99,
        subscribers: 5,
        request: Bytes(b""),
    });
    assert_eq!(rig.last_heard(), Some("Quiet"));
}

#[test]
fn publishing_allocates_nothing() {
    let mut rig = open();
    let before = orion_measure::thread_counts().allocs;
    for n in 0..100_u64 {
        let link = &mut rig.device.link;
        rig.layer
            .publish_f64(link, 0, 0, n, n as f64)
            .expect("taken");
        let mut out = [0_u8; 64];
        while rig.device.transmit(&mut out) != 0 {}
        rig.layer.poll(&mut rig.device.link, rig.now);
    }
    assert_eq!(orion_measure::thread_counts().allocs, before);
}

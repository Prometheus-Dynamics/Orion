//! A comm core whose rings are of an older layout: the client says so, with both layouts, at
//! each place it maps one. The rings here are made by this build and given the layout-1 word,
//! as a comm core built against the device library before `ad5bf930` makes them.

#![cfg(target_os = "linux")]

use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::mpsc;
use std::time::Duration;

use orion_connect::{Address, Client, ClientError, Options, Session};
use orion_keys::{Keys, KeysConfig, SharedKeys};
use orion_msg_topics::{Declare, Field, FieldType, Opened};
use orion_scratch::Scratch;
use orion_server::{
    Admits, Checked, Comm, Cx, Incoming, Limits, Policy, Security, Service, SessionId, Sessions,
    What,
};
use orion_topics::{TopicDecl, TopicError, Topics};
use orion_types::Error;
use orion_unix::UnixCarrier;
use orion_wire::{FrameView, List, kind};
use shm_ring::{Ring, Spec};

const FIELDS: [Field<'static>; 1] = [Field {
    name: "level",
    ty: FieldType::Float,
    unit: "",
    type_name: "",
}];

/// A ring as an older comm core shares it: word 1, its layout, is 1.
fn old_ring() -> Ring {
    let ring = Ring::create(&Spec {
        fields: 1,
        history: 8,
        max_payload: 16,
        expiry_ms: 0,
    })
    .expect("ring");
    let file = std::fs::File::from(ring.write_fd().expect("fd"));
    file.write_all_at(&1_u64.to_le_bytes(), 8)
        .expect("layout 1");
    ring
}

/// Answers a declaration with an old ring's write end, a subscription to `mirrored` with a
/// declaration no ring can have and no ring, and any other subscription with an old ring's
/// read end.
struct OldRings;

impl Service for OldRings {
    fn kinds(&self) -> &'static [u8] {
        &[kind::TOPIC_DECLARE, kind::TOPIC_SUBSCRIBE]
    }

    fn asks<'a>(&self, _frame: &FrameView<'a>) -> Result<(What<'a>, Policy), Error> {
        Ok((
            What::Read("topics"),
            Policy::new(Admits::ANYONE).without_grant(),
        ))
    }

    fn handle(&mut self, request: Checked<Incoming<'_>>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let seq = cx.request_seq();
        let opened = |shared| Opened {
            request_seq: seq,
            handle: 1,
            shared_memory: shared,
            ring_bytes: 0,
        };
        let io = |_| Error::Internal;
        if request.get().kind() == kind::TOPIC_DECLARE {
            let ring = old_ring();
            return cx.reply_with_fds(&opened(true), [ring.write_fd().map_err(io)?]);
        }
        let subscribe: orion_msg_topics::Subscribe<'_> = request.get().body()?;
        if subscribe.topic.to_string() == "mirrored" {
            // More history than any ring may keep: a ring of the client's own cannot be made.
            let declaration = Declare {
                topic: "mirrored".parse().expect("name"),
                fields: List::new(&FIELDS),
                history: shm_ring::MAX_HISTORY + 1,
                expiry_ms: 0,
                max_payload: 16,
                on_subscribe: Default::default(),
            };
            cx.reply(&declaration)?;
            return cx.reply(&opened(false));
        }
        let ring = old_ring();
        cx.reply_with_fds(&opened(true), [ring.read_fd().map_err(io)?])
    }

    fn quiet(&mut self, _session: SessionId, _quiet_ms: u64, _sessions: &mut Sessions) {}
}

/// A comm core with only [`OldRings`], in its own thread, on a socket.
fn old_core(name: &str) -> (Scratch, PathBuf, tokio::sync::oneshot::Sender<()>) {
    let dir = Scratch::new(&format!("old-{name}"));
    let socket = dir.join("orion.sock");
    let (ready, started) = mpsc::channel();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let path = socket.clone();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        tokio::task::LocalSet::new().block_on(&runtime, async move {
            let keys = SharedKeys::new(
                Keys::open(KeysConfig::in_memory("lab"), Security::Off).expect("keys"),
            );
            let comm = Comm::new(
                "board".parse().expect("name"),
                Security::Off,
                Some(Rc::new(keys)),
                Limits::default(),
            );
            comm.add_service(OldRings);
            let unix = UnixCarrier::bind(&path, Some(0o600), &[]).expect("socket");
            ready.send(()).expect("ready");
            tokio::task::spawn_local(async move { comm.serve(unix).await });
            let _ = stopped.await;
        });
    });
    started.recv().expect("the comm core starts");
    (dir, socket, stop)
}

fn topics(socket: PathBuf) -> Topics {
    let options = Options {
        timeout: Duration::from_secs(5),
        ..Options::default()
    };
    let session = Session::connect(&Address::Local(socket), "tool", options).expect("connect");
    Topics::new(&Client::new(session))
}

fn layout(error: &TopicError) -> (&'static str, u64, u64) {
    match error {
        TopicError::Client(ClientError::Layout {
            what, found, reads, ..
        }) => (what, *found, *reads),
        other => panic!("not a layout: {other:?}"),
    }
}

#[test]
fn every_place_a_ring_is_mapped_says_its_layout_and_this_clients() {
    let (_dir, socket, _stop) = old_core("layout");
    let topics = topics(socket);

    let error = topics
        .declare(&TopicDecl {
            name: "tank.level",
            fields: &FIELDS,
            history: 8,
            expiry_ms: 0,
            max_payload: 16,
            on_subscribe: Default::default(),
        })
        .expect_err("an old ring");
    assert_eq!(layout(&error), ("a publisher's ring", 1, 2), "{error}");
    let said = error.to_string();
    assert!(said.contains("the comm core is older"), "{said}");
    assert!(said.contains("ad5bf930"), "{said}");

    let error = subscribed(&topics, "tank.level");
    assert_eq!(layout(&error), ("a subscriber's ring", 1, 2), "{error}");

    // No ring shared: the client's own cannot be made, and says why.
    let error = subscribed(&topics, "mirrored");
    assert!(
        matches!(&error, TopicError::Client(ClientError::Memory { what: "a subscriber's ring of its own", why })
            if why.contains("history")),
        "{error:?}"
    );
}

/// What a subscription to `topic` fails with, at once or once its answer arrives.
fn subscribed(topics: &Topics, topic: &str) -> TopicError {
    match topics.subscribe(topic, &[]) {
        Err(error) => error,
        Ok(mut subscriber) => {
            for _ in 0..100 {
                if let Err(error) = subscriber.wait(Duration::from_millis(50)) {
                    return error;
                }
            }
            panic!("{topic}: no error");
        }
    }
}

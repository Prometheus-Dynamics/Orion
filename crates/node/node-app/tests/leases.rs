//! A lease contested through the comm core and node logic: a program on the board that holds a
//! camera stops answering, another takes the camera, and the first is told who took it.

use std::path::Path;
use std::time::{Duration, Instant};

use orion::actions::{Admits, Policy};
use orion::provider::{Resource, ResourceState};
use orion::records::{Bytes, RecordClass, RecordEntry};
use orion::types::{Error, HlcTimestamp};
use orion::{Address, ClientError, Options, Orion, Roles, Session};
use orion_msg_actions::{Run, Target};
use orion_msg_records::{Changed, Get, LeaseRecord, Put};
use orion_msg_session::Refusal;
use orion_node_app::{Node, NodeSettings};
use orion_scratch::Scratch;
use orion_stubs::Board;
use orion_wire::body::encode_fields;
use orion_wire::{Decode, List, kind};
use tokio::sync::oneshot;

const HEARTBEAT_MS: u32 = 200;
const QUIET_MS: u64 = 1_500;

fn dir(name: &str) -> Scratch {
    Scratch::new(&format!("node-{name}"))
}

fn settings(dir: &Path) -> NodeSettings {
    let mut source = orion_config::Source::parse("", []).expect("no settings");
    let mut settings = NodeSettings::from_source(&mut source).expect("defaults");
    settings.config.socket = dir.join("orion.sock");
    settings.config.state_dir = dir.join("state");
    settings.files_dir = dir.join("files");
    settings.audit = None;
    settings.config.node_name = "board".parse().expect("name");
    settings.lease_quiet_ms = QUIET_MS;
    settings
}

/// Node logic in a thread of its own, until dropped.
struct Logic {
    kill: Option<oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Logic {
    fn start(settings: NodeSettings) -> Self {
        let (kill, killed) = oneshot::channel::<()>();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            runtime.block_on(async move {
                let mut node = Node::load(settings).expect("node logic starts");
                tokio::select! {
                    () = node.run(std::future::pending()) => {}
                    _ = killed => {}
                }
            });
        });
        Self {
            kill: Some(kill),
            thread: Some(thread),
        }
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

/// The camera driver: says the camera exists, with an action.
fn provide(orion: &Orion) -> Resource {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut describing = orion
            .provider()
            .resource("cam.front", "camera")
            .expect("name");
        let acted = describing.action("focus", "Focuses the lens", Policy::new(Admits::LOCAL));
        match acted.and_then(|()| {
            describing
                .publish(ResourceState::Ready)
                .map_err(ClientError::from)
        }) {
            Ok(resource) => return resource,
            // Node logic is not there yet.
            Err(ClientError::Refused {
                error: Error::Unavailable,
                ..
            }) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("provide: {error}"),
        }
    }
}

/// How a request was answered: done, or refused with a code and words.
#[derive(Debug, PartialEq, Eq)]
enum Answer {
    Done,
    Refused(u16, String),
}

/// Pumps `session` until the answer to `seq`.
fn answer(session: &mut Session, seq: u16) -> Answer {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let Some(frame) = session.next(Duration::from_millis(50)).expect("session") else {
            continue;
        };
        match frame.kind() {
            kind::REFUSAL => {
                let refusal: Refusal<'_> = frame.body().expect("refusal");
                if refusal.request_seq == seq {
                    return Answer::Refused(refusal.code, refusal.detail.to_owned());
                }
            }
            kind::CHANGED => {
                let changed: Changed<'_> = frame.body().expect("changed");
                if changed.request_seq == seq {
                    return Answer::Done;
                }
            }
            _ => {}
        }
    }
    panic!("no answer to {seq}");
}

/// Writes the camera's lease, held by `holder`, with no end.
fn claim(session: &mut Session, holder: &str) -> Answer {
    let mut value = vec![0_u8; 128];
    let len = encode_fields(
        &LeaseRecord {
            resource: "cam.front".parse().expect("name"),
            holder,
            expires_ms: 0,
            label: "",
        },
        &mut value,
    )
    .expect("encode");
    let entries = [RecordEntry {
        class: RecordClass::Lease,
        key: "cam.front",
        stamp: HlcTimestamp::ZERO,
        deleted: false,
        value: Bytes(&value[..len]),
    }];
    let seq = session
        .send(&Put {
            records: List::new(&entries),
        })
        .expect("send");
    answer(session, seq)
}

fn connect(address: &Address, name: &str) -> Session {
    Session::connect(address, name, orion::Options::default()).expect("connect")
}

#[test]
fn a_program_that_stops_answering_loses_its_lease_to_a_claimant_and_is_told_by_whom() {
    let dir = dir("leases");
    let board = Board::start_with(
        &dir,
        orion_stubs::Options {
            for_node: true,
            heartbeat_ms: HEARTBEAT_MS,
            ..orion_stubs::Options::default()
        },
    );
    let address = Address::Local(board.socket().to_owned());
    let _logic = Logic::start(settings(&dir));
    let driver = Orion::connect_with(
        &address,
        "camera-driver",
        Options {
            roles: Roles::CLIENT.and(Roles::PROVIDER),
            ..Options::default()
        },
    )
    .expect("connect");
    let _camera = provide(&driver);

    // The detector holds the camera, and answers.
    let mut detector = connect(&address, "detector");
    // The detector is last heard after this: its quiet starts no earlier.
    let sent = Instant::now();
    assert_eq!(claim(&mut detector, "workload:detector"), Answer::Done);
    let mut recorder = connect(&address, "recorder");
    let Answer::Refused(code, words) = claim(&mut recorder, "workload:recorder") else {
        panic!("taken from a holder that answers");
    };
    assert_eq!(code, Error::Busy.code());
    assert!(
        words.starts_with("cam.front is held by workload:detector since "),
        "{words}"
    );

    // The detector stops answering: its program no longer pumps the session. Once it has been
    // quiet long enough, the recorder's claim takes the camera, and not before: measured from
    // before its last word was sent, since when it was heard is the comm core's to know.
    let taken = loop {
        assert!(sent.elapsed() < Duration::from_secs(20), "never taken");
        match claim(&mut recorder, "workload:recorder") {
            Answer::Done => break sent.elapsed(),
            Answer::Refused(code, _) if code == Error::Busy.code() => {
                std::thread::sleep(Duration::from_millis(50));
            }
            other => panic!("{other:?}"),
        }
    };
    assert!(taken >= Duration::from_millis(QUIET_MS), "{taken:?}");

    // The detector speaks again. Its claim and its run of the camera's action are refused
    // with who took it.
    let told = "lease lost while quiet: taken by workload:recorder";
    assert_eq!(
        claim(&mut detector, "workload:detector"),
        Answer::Refused(Error::LeaseLost.code(), told.to_owned())
    );
    let seq = detector
        .send(&Run {
            call: 1,
            name: "cam.front.focus".parse().expect("name"),
            target: Target::Resource("cam.front".parse().expect("name")),
            args: Bytes(&[]),
            deadline_ms: 0,
            detached: false,
            on_behalf: 0,
        })
        .expect("send");
    assert_eq!(
        answer(&mut detector, seq),
        Answer::Refused(Error::LeaseLost.code(), told.to_owned())
    );
    // The recorder holds it, and renews it as its own.
    assert_eq!(claim(&mut recorder, "workload:recorder"), Answer::Done);

    // Another program may not free it, and is told who holds it (root may, so not when the
    // test runs as root). The recorder frees its own.
    let mut bystander = connect(&address, "bystander");
    if !as_root() {
        assert_eq!(
            free(&mut bystander),
            Answer::Refused(
                Error::Denied.code(),
                "cam.front is held by workload:recorder: only its holder or root frees it"
                    .to_owned()
            )
        );
    }
    assert_eq!(free(&mut recorder), Answer::Done);
}

/// Whether this test runs as root: `/proc/self` belongs to the process's user.
fn as_root() -> bool {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata("/proc/self").is_ok_and(|meta| meta.uid() == 0)
}

/// Deletes the camera's lease.
fn free(session: &mut Session) -> Answer {
    let entries = [RecordEntry {
        class: RecordClass::Lease,
        key: "cam.front",
        stamp: HlcTimestamp::ZERO,
        deleted: true,
        value: Bytes(&[]),
    }];
    let seq = session
        .send(&Put {
            records: List::new(&entries),
        })
        .expect("send");
    answer(session, seq)
}

/// The camera's lease as stored: its holder and its label.
fn lease(session: &mut Session) -> (String, String) {
    let seq = session
        .send(&Get {
            class: RecordClass::Lease,
            prefix: "cam.front",
        })
        .expect("send");
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let Some(frame) = session.next(Duration::from_millis(50)).expect("session") else {
            continue;
        };
        if frame.kind() != kind::CHANGED {
            continue;
        }
        let changed: Changed<'_> = frame.body().expect("changed");
        if changed.request_seq != seq {
            continue;
        }
        let entry = changed
            .records
            .iter()
            .next()
            .expect("a lease")
            .expect("entry");
        let lease = LeaseRecord::decode(entry.value.0).expect("lease");
        return (lease.holder.to_owned(), lease.label.to_owned());
    }
    panic!("no lease read");
}

#[test]
fn with_security_on_a_lease_is_held_by_its_writers_principal_whatever_it_says() {
    let dir = dir("leases-bound");
    let board = Board::start_with(
        &dir,
        orion_stubs::Options {
            for_node: true,
            security: orion_config::Security::On,
            ..orion_stubs::Options::default()
        },
    );
    let address = Address::Local(board.socket().to_owned());
    let mut settings = settings(&dir);
    settings.config.security = orion_config::Security::On;
    let _logic = Logic::start(settings);
    let driver = Orion::connect_with(
        &address,
        "camera-driver",
        Options {
            roles: Roles::CLIENT.and(Roles::PROVIDER),
            ..Options::default()
        },
    )
    .expect("connect");
    let _camera = provide(&driver);

    // The detector says it is someone else. The node writes who it is, and keeps what it
    // said as the label.
    let mut detector = connect(&address, "detector");
    assert_eq!(claim(&mut detector, "workload:recorder"), Answer::Done);
    let uid = std::os::unix::fs::MetadataExt::uid(&std::fs::metadata("/proc/self").expect("me"));
    let me = format!("local:{uid}");
    assert_eq!(
        lease(&mut detector),
        (me.clone(), "workload:recorder".to_owned())
    );
    // Writing its own principal keeps the record as written.
    assert_eq!(claim(&mut detector, &me), Answer::Done);
    assert_eq!(lease(&mut detector), (me, String::new()));
    // Its principal is the holder, so another session of the same caller frees it.
    let mut other = connect(&address, "other");
    assert_eq!(free(&mut other), Answer::Done);
}

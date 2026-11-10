//! Records against a store that stands in for node logic: read, write, and a watch that
//! delivers every change in order.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::mpsc;
use std::time::Duration;

use orion_connect::{Address, Client, ClientError, Options, Session};
use orion_msg_records::{Changed, Get, Put, Watch as WatchRequest};
use orion_records::{Record, RecordClass, RecordEntry, Records, WATCH_QUEUE};
use orion_server::{
    Admits, Checked, Comm, Cx, Delivery, Error, Incoming, Limits, Policy, Security, Service,
    SessionId, Sessions, What,
};
use orion_types::HlcTimestamp;
use orion_unix::UnixCarrier;
use orion_wire::{Bytes, FrameView, List, kind};

/// How many records the store sends in one message.
const PAGE: usize = 50;

#[derive(Default)]
struct Store {
    records: BTreeMap<(u8, String), Record>,
    clock: u64,
    watchers: Vec<(SessionId, u16, RecordClass, String)>,
}

fn entry(record: &Record) -> RecordEntry<'_> {
    RecordEntry {
        class: record.class,
        key: &record.key,
        stamp: record.stamp,
        deleted: record.deleted,
        value: Bytes(&record.value),
    }
}

/// Sends `records` as the answer to `request_seq`, a page at a time.
fn send(
    sessions: &mut Sessions,
    to: SessionId,
    request_seq: u16,
    records: &[&Record],
) -> Result<(), Error> {
    let mut pages = records.chunks(PAGE).peekable();
    if pages.peek().is_none() {
        let empty = Changed {
            request_seq,
            records: List::new(&[]),
            more: false,
        };
        return sessions.send(to, &empty, Delivery::Must);
    }
    while let Some(page) = pages.next() {
        let entries: Vec<RecordEntry<'_>> = page.iter().map(|record| entry(record)).collect();
        let changed = Changed {
            request_seq,
            records: List::new(&entries),
            more: pages.peek().is_some(),
        };
        sessions.send(to, &changed, Delivery::Must)?;
    }
    Ok(())
}

struct StoreService(Rc<RefCell<Store>>);

impl Service for StoreService {
    fn kinds(&self) -> &'static [u8] {
        &[kind::PUT, kind::GET, kind::WATCH]
    }

    fn asks<'a>(&self, frame: &FrameView<'a>) -> Result<(What<'a>, Policy), Error> {
        let what = if frame.kind() == kind::PUT {
            What::Write("records")
        } else {
            What::Read("records")
        };
        Ok((what, Policy::new(Admits::ANYONE).without_grant()))
    }

    fn handle(&mut self, request: Checked<Incoming<'_>>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let incoming = request.get();
        let store = &mut *self.0.borrow_mut();
        let (from, seq) = (cx.from(), cx.request_seq());
        match incoming.kind() {
            kind::PUT => {
                let put: Put<'_> = incoming.body()?;
                let mut stored = Vec::new();
                for entry in put.records.iter().flatten() {
                    // Leases are not a client's to write here.
                    if entry.class == RecordClass::Lease {
                        return Err(Error::Denied);
                    }
                    store.clock += 1;
                    let record = Record {
                        class: entry.class,
                        key: entry.key.to_owned(),
                        stamp: HlcTimestamp::new(store.clock, 0, 1),
                        deleted: entry.deleted,
                        value: entry.value.0.to_vec(),
                    };
                    store
                        .records
                        .insert((entry.class.code(), record.key.clone()), record.clone());
                    stored.push(record);
                }
                let sessions = cx.sessions();
                send(sessions, from, seq, &stored.iter().collect::<Vec<_>>())?;
                // Every watcher of these records is pushed the change, at once.
                for (watcher, watch_seq, class, prefix) in &store.watchers {
                    let theirs: Vec<&Record> = stored
                        .iter()
                        .filter(|record| {
                            record.class == *class && record.key.starts_with(prefix.as_str())
                        })
                        .collect();
                    if !theirs.is_empty() {
                        let _ = send(sessions, *watcher, *watch_seq, &theirs);
                    }
                }
                Ok(())
            }
            kind::GET => {
                let get: Get<'_> = incoming.body()?;
                let found: Vec<&Record> = store
                    .records
                    .values()
                    .filter(|record| {
                        record.class == get.class
                            && !record.deleted
                            && record.key.starts_with(get.prefix)
                    })
                    .collect();
                send(cx.sessions(), from, seq, &found)
            }
            _ => {
                let watch: WatchRequest<'_> = incoming.body()?;
                if watch.prefix == "forbidden." {
                    return Err(Error::Denied);
                }
                // A session's newer watch on the same records replaces its older one.
                store.watchers.retain(|(session, _, class, prefix)| {
                    !(*session == from && *class == watch.class && prefix == watch.prefix)
                });
                store
                    .watchers
                    .push((from, seq, watch.class, watch.prefix.to_owned()));
                let mut newer: Vec<&Record> = store
                    .records
                    .values()
                    .filter(|record| {
                        record.class == watch.class
                            && record.key.starts_with(watch.prefix)
                            && record.stamp > watch.after
                    })
                    .collect();
                newer.sort_by_key(|record| record.stamp);
                if newer.is_empty() {
                    return Ok(());
                }
                send(cx.sessions(), from, seq, &newer)
            }
        }
    }

    fn closed(&mut self, session: SessionId, _sessions: &mut Sessions) {
        self.0
            .borrow_mut()
            .watchers
            .retain(|(watcher, ..)| *watcher != session);
    }
}

struct Board {
    socket: PathBuf,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
    dir: PathBuf,
}

impl Board {
    fn start(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("orion-records-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("dir");
        let socket = dir.join("orion.sock");
        let (ready, started) = mpsc::channel();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let path = socket.clone();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            tokio::task::LocalSet::new().block_on(&runtime, async move {
                let limits = Limits {
                    outbox_bytes: 8 << 20,
                    ..Limits::default()
                };
                let comm = Comm::new("board".parse().expect("name"), Security::Off, None, limits);
                comm.add_service(StoreService(Rc::default()));
                let unix = UnixCarrier::bind(&path, Some(0o600), &[]).expect("socket");
                tokio::task::spawn_local(async move { comm.serve(unix).await });
                ready.send(()).expect("ready");
                let _ = stopped.await;
            });
        });
        started.recv().expect("the board starts");
        Self {
            socket,
            stop: Some(stop),
            thread: Some(thread),
            dir,
        }
    }

    fn records(&self, name: &str) -> Records {
        let session = Session::connect(
            &Address::Local(self.socket.clone()),
            name,
            Options::default(),
        )
        .expect("connect");
        Records::new(&Client::new(session))
    }
}

impl Drop for Board {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn resource<'a>(key: &'a str, value: &'a [u8]) -> RecordEntry<'a> {
    RecordEntry {
        class: RecordClass::Resource,
        key,
        stamp: HlcTimestamp::ZERO,
        deleted: false,
        value: Bytes(value),
    }
}

#[test]
fn records_are_written_read_and_gone() {
    let board = Board::start("basic");
    let records = board.records("tool");
    assert!(
        records
            .get(RecordClass::Resource, "")
            .expect("get")
            .is_empty()
    );

    // A write comes back as the store kept it: with its stamp.
    let stored = records
        .put(&[
            resource("camera.front", b"one"),
            resource("camera.rear", b"two"),
        ])
        .expect("put");
    assert_eq!(stored.len(), 2);
    assert!(stored[0].stamp < stored[1].stamp && stored[0].stamp > HlcTimestamp::ZERO);
    records.put(&[resource("arm.left", b"three")]).expect("put");

    let cameras = records.get(RecordClass::Resource, "camera.").expect("get");
    assert_eq!(
        cameras
            .iter()
            .map(|record| record.key.as_str())
            .collect::<Vec<_>>(),
        ["camera.front", "camera.rear"]
    );
    assert_eq!(cameras[0].value, b"one");
    assert_eq!(
        records.get(RecordClass::Resource, "").expect("get").len(),
        3
    );
    assert!(records.get(RecordClass::Node, "").expect("get").is_empty());

    // More records than fit one message arrive whole.
    let keys: Vec<String> = (0..180).map(|index| format!("sensor.{index:03}")).collect();
    let many: Vec<RecordEntry<'_>> = keys.iter().map(|key| resource(key, b"x")).collect();
    for chunk in many.chunks(40) {
        records.put(chunk).expect("put");
    }
    assert_eq!(
        records
            .get(RecordClass::Resource, "sensor.")
            .expect("get")
            .len(),
        180
    );

    // Gone is a record too.
    let gone = RecordEntry {
        deleted: true,
        ..resource("camera.rear", &[])
    };
    assert!(records.put(&[gone]).expect("put")[0].deleted);
    assert_eq!(
        records
            .get(RecordClass::Resource, "camera.")
            .expect("get")
            .len(),
        1
    );

    // What the store refuses is an error where it was asked, and the session carries on.
    let lease = RecordEntry {
        class: RecordClass::Lease,
        ..resource("camera.front", b"mine")
    };
    assert!(matches!(
        records.put(&[lease]),
        Err(ClientError::Refused(orion_types::Error::Denied))
    ));
    assert_eq!(
        records
            .get(RecordClass::Resource, "arm.")
            .expect("get")
            .len(),
        1
    );
}

#[test]
fn a_watch_delivers_every_change_in_order() {
    let board = Board::start("watch");
    let writer = board.records("writer");
    let watcher = board.records("watcher");
    writer
        .put(&[
            resource("camera.front", b"before"),
            resource("arm.left", b"other"),
        ])
        .expect("put");

    // A watch starts with what is there, and only what it asked about.
    let mut watch = watcher
        .watch(RecordClass::Resource, "camera.", HlcTimestamp::ZERO)
        .expect("watch");
    let first = watch
        .wait(Duration::from_secs(2))
        .expect("wait")
        .expect("what is there");
    assert_eq!(
        (first.key.as_str(), first.value.as_slice()),
        ("camera.front", b"before".as_slice())
    );
    assert!(
        watch
            .wait(Duration::from_millis(30))
            .expect("wait")
            .is_none(),
        "nothing has changed"
    );

    // Then every change, as it is made, in the order it was made. Nothing is asked again.
    for change in 0..500_u32 {
        let key = if change % 2 == 0 {
            "camera.front"
        } else {
            "camera.rear"
        };
        writer
            .put(&[resource(key, &change.to_le_bytes())])
            .expect("put");
        // Changes to other records are not this watcher's.
        writer
            .put(&[resource("arm.left", &change.to_le_bytes())])
            .expect("put");
    }
    let mut last = first.stamp;
    for change in 0..500_u32 {
        let record = watch
            .wait(Duration::from_secs(2))
            .expect("wait")
            .expect("every change");
        assert_eq!(
            record.value,
            change.to_le_bytes(),
            "change {change} out of order"
        );
        assert_eq!(
            record.key,
            if change % 2 == 0 {
                "camera.front"
            } else {
                "camera.rear"
            }
        );
        assert!(record.stamp > last, "stamps go up");
        last = record.stamp;
    }
    assert!(watch.poll().expect("poll").is_none());
    assert_eq!(watch.newest(), last);

    // A record going is a change like any other.
    let gone = RecordEntry {
        deleted: true,
        ..resource("camera.rear", &[])
    };
    writer.put(&[gone]).expect("put");
    let record = watch
        .wait(Duration::from_secs(2))
        .expect("wait")
        .expect("the deletion");
    assert!(record.deleted && record.key == "camera.rear");

    // A program that kept the last stamp picks up from there and sees nothing twice.
    let kept = watch.newest();
    drop(watch);
    writer
        .put(&[resource("camera.front", b"while away")])
        .expect("put");
    let mut again = watcher
        .watch(RecordClass::Resource, "camera.", kept)
        .expect("watch");
    let record = again
        .wait(Duration::from_secs(2))
        .expect("wait")
        .expect("what was missed");
    assert_eq!(record.value, b"while away");
    assert!(
        again
            .wait(Duration::from_millis(30))
            .expect("wait")
            .is_none()
    );

    // A watch the store refuses says so on its first read.
    let mut refused = watcher
        .watch(RecordClass::Resource, "forbidden.", HlcTimestamp::ZERO)
        .expect("sent");
    let error = loop {
        match refused.wait(Duration::from_millis(50)) {
            Ok(_) => {}
            Err(error) => break error,
        }
    };
    assert!(
        matches!(error, ClientError::Refused(orion_types::Error::Denied)),
        "{error}"
    );
}

#[test]
fn a_watcher_that_falls_behind_catches_up_and_misses_no_record() {
    let board = Board::start("behind");
    let writer = board.records("writer");
    let watcher = board.records("watcher");
    let mut watch = watcher
        .watch(RecordClass::Resource, "", HlcTimestamp::ZERO)
        .expect("watch");

    // Far more changes than the watch holds, to more records than it holds, while the
    // program reads none of them. It does keep its session turning, for other things.
    let total = WATCH_QUEUE * 3;
    let keys: Vec<String> = (0..total).map(|index| format!("item.{index:05}")).collect();
    for (index, key) in keys.iter().enumerate() {
        writer
            .put(&[resource(key, &(index as u32).to_le_bytes())])
            .expect("put");
    }
    // Something else on the same session is answered meanwhile: the watch's changes arrive.
    assert_eq!(
        watcher
            .get(RecordClass::Resource, "item.00000")
            .expect("get")
            .len(),
        1
    );

    // Now it reads: everything, in stamp order, each record once.
    let mut seen = Vec::with_capacity(total);
    let mut last = HlcTimestamp::ZERO;
    while seen.len() < total {
        let record = watch
            .wait(Duration::from_secs(5))
            .expect("wait")
            .unwrap_or_else(|| panic!("stopped at {}", seen.len()));
        assert!(record.stamp > last, "{} after {}", record.stamp, last);
        last = record.stamp;
        seen.push(record.key);
    }
    assert_eq!(seen, keys);
    assert!(
        watch
            .wait(Duration::from_millis(50))
            .expect("wait")
            .is_none(),
        "nothing twice"
    );
}

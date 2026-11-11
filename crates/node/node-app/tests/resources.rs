//! Resources through node logic: a device from a provider is listed and bound, has one
//! provider, and is lost, not gone, when its provider goes.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use orion::actions::{Admits, Policy};
use orion::provider::{Found, Resource, ResourceState};
use orion::records::{Bytes, RecordClass, RecordEntry};
use orion::topics::{Field, FieldType};
use orion::types::{Error, HlcTimestamp};
use orion::{Address, ClientError, Options, Orion, Roles};
use orion_msg_records::LeaseRecord;
use orion_node_app::{Node, NodeSettings};
use orion_stubs::Board;
use orion_wire::Decode;
use orion_wire::body::encode_fields;
use tokio::sync::oneshot;

fn dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("orion-node-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("directory");
    dir
}

fn settings(dir: &Path) -> NodeSettings {
    let mut source = orion_config::Source::parse("", []).expect("no settings");
    let mut settings = NodeSettings::from_source(&mut source).expect("defaults");
    settings.config.socket = dir.join("orion.sock");
    settings.config.state_dir = dir.join("state");
    settings.files_dir = dir.join("files");
    settings.audit = Some(orion_audit::AuditConfig::new(&dir.join("state/audit.log")));
    settings.config.node_name = "board".parse().expect("name");
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

/// The camera driver: says the camera exists, with a topic and an action.
fn provide(orion: &Orion) -> Resource {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut describing = orion
            .provider()
            .resource("cam.front", "camera")
            .expect("name")
            .label("lens", "wide");
        let _frames = describing
            .topic(
                "frames",
                &[Field {
                    name: "count",
                    ty: FieldType::Int,
                    unit: "",
                    type_name: "",
                }],
                8,
                8,
            )
            .expect("topic");
        let acted = describing.action("focus", "Focuses the lens", Policy::new(Admits::LOCAL));
        let published = acted.and_then(|()| describing.publish(ResourceState::Ready));
        match published {
            Ok(resource) => return resource,
            // Node logic is not there yet.
            Err(ClientError::Refused(Error::Unavailable)) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("provide: {error}"),
        }
    }
}

fn lease(holder: &str) -> Vec<u8> {
    let mut value = vec![0_u8; 128];
    let len = encode_fields(
        &LeaseRecord {
            resource: "cam.front".parse().expect("name"),
            holder,
            expires_ms: 0,
        },
        &mut value,
    )
    .expect("encode");
    value.truncate(len);
    value
}

fn put<'a>(class: RecordClass, value: &'a [u8]) -> RecordEntry<'a> {
    RecordEntry {
        class,
        key: "cam.front",
        stamp: HlcTimestamp::ZERO,
        deleted: false,
        value: Bytes(value),
    }
}

fn camera(orion: &Orion) -> Found {
    let found = orion.resources("cam.").expect("list");
    assert_eq!(found.len(), 1, "{found:?}");
    found.into_iter().next().expect("one")
}

fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(Instant::now() < deadline, "{what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn a_device_from_a_provider_is_listed_and_bound() {
    let dir = dir("resources");
    let board = Board::start_for_node(&dir);
    let address = Address::Local(board.socket().to_owned());
    let _logic = Logic::start(settings(&dir));
    let roles = || Options {
        roles: Roles::CLIENT.and(Roles::PROVIDER),
        ..Options::default()
    };
    let driver = Orion::connect_with(&address, "camera-driver", roles()).expect("connect");
    let resource = provide(&driver);

    // Listed: a program that has never heard of cameras finds it and what it offers.
    let dashboard = Orion::connect(&address, "dashboard").expect("connect");
    let found = camera(&dashboard);
    assert_eq!(
        (found.name.as_str(), found.kind.as_str()),
        ("cam.front", "camera")
    );
    assert_eq!(found.node, "board");
    assert_eq!(found.state, ResourceState::Ready);
    assert_eq!(found.labels, [("lens".to_owned(), "wide".to_owned())]);
    assert_eq!(found.topics, ["cam.front.frames"]);
    assert_eq!(found.actions, ["cam.front.focus"]);

    // Bound: one holder, and a second is refused.
    let records = dashboard.records();
    let held = lease("workload:detector");
    records
        .put(&[put(RecordClass::Lease, &held)])
        .expect("bind");
    let other = lease("workload:recorder");
    match records.put(&[put(RecordClass::Lease, &other)]) {
        Err(ClientError::Refused(Error::Busy)) => {}
        result => panic!("expected busy, got {result:?}"),
    }
    let leases = records.get(RecordClass::Lease, "").expect("leases");
    assert_eq!(leases.len(), 1);
    assert_eq!(
        LeaseRecord::decode(&leases[0].value).expect("lease").holder,
        "workload:detector"
    );

    // It has one provider: another program cannot write it or take it away.
    let impostor = Orion::connect(&address, "impostor").expect("connect");
    let theirs = impostor
        .records()
        .get(RecordClass::Resource, "cam.")
        .expect("get");
    match impostor
        .records()
        .put(&[put(RecordClass::Resource, &theirs[0].value)])
    {
        Err(ClientError::Refused(Error::Denied)) => {}
        result => panic!("expected denied, got {result:?}"),
    }
    match impostor.records().put(&[RecordEntry {
        deleted: true,
        ..put(RecordClass::Resource, &[])
    }]) {
        Err(ClientError::Refused(Error::Denied)) => {}
        result => panic!("expected denied, got {result:?}"),
    }

    // Its provider goes: it is lost, still listed, still its holder's. A watcher is told.
    let mut watch = records
        .watch(RecordClass::Resource, "cam.", HlcTimestamp::ZERO)
        .expect("watch");
    let first = watch
        .wait(Duration::from_secs(5))
        .expect("watch")
        .expect("as it is");
    assert_eq!(
        orion::provider::describe(&first).expect("camera").state,
        ResourceState::Ready
    );
    drop(resource);
    drop(driver);
    let told = watch
        .wait(Duration::from_secs(5))
        .expect("watch")
        .expect("a change");
    assert_eq!(
        orion::provider::describe(&told).expect("camera").state,
        ResourceState::Lost
    );
    let lost = camera(&dashboard);
    assert_eq!(lost.state, ResourceState::Lost);
    assert_eq!(lost.topics, ["cam.front.frames"]);
    assert_eq!(
        records.get(RecordClass::Lease, "").expect("leases").len(),
        1
    );

    // It is back, in a new session, and ready again.
    let driver = Orion::connect_with(&address, "camera-driver", roles()).expect("connect");
    let resource = provide(&driver);
    until("the camera is ready again", || {
        camera(&dashboard).state == ResourceState::Ready
    });

    // Its provider takes it away: it is gone, and its lease with it.
    resource.withdraw().expect("withdraw");
    until("the camera is gone", || {
        dashboard.resources("cam.").expect("list").is_empty()
    });
    assert!(
        records
            .get(RecordClass::Lease, "")
            .expect("leases")
            .is_empty()
    );
}

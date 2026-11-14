//! The gap after a restart: what was served before it is restoring, not ready and not missing,
//! until the program that serves it is back or the wait for it is over.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use orion::actions::{ActionCall, Admits, Policy, Target};
use orion::provider::{Resource, ResourceState};
use orion::records::RecordClass;
use orion::types::{Error, HlcTimestamp};
use orion::{Address, ClientError, Options, Orion, Roles};
use orion_node_app::{Node, NodeSettings};
use orion_stubs::Board;
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

fn driver(address: &Address) -> Orion {
    let options = Options {
        roles: Roles::CLIENT.and(Roles::PROVIDER),
        ..Options::default()
    };
    Orion::connect_with(address, "arm-driver", options).expect("connect")
}

/// The arm's driver says the arm is there and ready, with one action.
fn provide(orion: &Orion) -> Resource {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut describing = orion.provider().resource("arm", "arm").expect("name");
        let acted = describing.action("home", "Homes the arm", Policy::new(Admits::ANYONE));
        match acted.and_then(|()| describing.publish(ResourceState::Ready)) {
            Ok(resource) => return resource,
            // Node logic is not there yet.
            Err(ClientError::Refused(Error::Unavailable)) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("provide: {error}"),
        }
    }
}

/// A dashboard, once node logic answers it.
fn dashboard(address: &Address) -> Orion {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(orion) = Orion::connect(address, "dashboard")
            && orion.resources("").is_ok()
        {
            return orion;
        }
        assert!(Instant::now() < deadline, "no dashboard");
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn arm(orion: &Orion) -> Option<ResourceState> {
    let found = orion.resources("arm").expect("list");
    found.first().map(|found| found.state)
}

/// What a run of `name` on the arm is refused with, or `None` if it was taken.
fn run_refused(orion: &Orion, name: &str) -> Option<ClientError> {
    let mut run = orion
        .run(&ActionCall {
            name,
            target: Target::Resource("arm".parse().expect("name")),
            args: b"",
            deadline_ms: 0,
            detached: false,
        })
        .expect("sent");
    match run.finish(Duration::from_millis(200)) {
        Err(ClientError::Refused(error)) => Some(ClientError::Refused(error)),
        // Taken by the driver, which this test never has answer.
        _ => None,
    }
}

fn refused(orion: &Orion, name: &str) -> Error {
    match run_refused(orion, name) {
        Some(ClientError::Refused(error)) => error,
        other => panic!("expected a refusal, got {other:?}"),
    }
}

fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(Instant::now() < deadline, "{what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn what_was_served_is_restoring_after_the_comm_core_restarts_until_its_program_is_back() {
    let dir = dir("restoring-back");
    let board = Board::start_for_node(&dir);
    let address = Address::Local(board.socket().to_owned());
    let _logic = Logic::start(settings(&dir));
    let arm_driver = driver(&address);
    let resource = provide(&arm_driver);
    assert_eq!(arm(&dashboard(&address)), Some(ResourceState::Ready));

    // The comm core restarts. The driver is not back yet.
    drop(board);
    drop(resource);
    drop(arm_driver);
    let _board = Board::start_for_node(&dir);
    let dash = dashboard(&address);

    // The record does not say ready: a program that waits for ready waits.
    assert_eq!(arm(&dash), Some(ResourceState::Restoring));
    // A run of what was served is told to ask again, and one of what never was is not.
    assert_eq!(refused(&dash, "arm.home"), Error::Restoring);
    assert_eq!(refused(&dash, "arm.park"), Error::NotFound);

    // A watcher sees both edges: restoring, then ready when the driver has said so again.
    let mut watch = dash
        .records()
        .watch(RecordClass::Resource, "arm", HlcTimestamp::ZERO)
        .expect("watch");
    let first = watch
        .wait(Duration::from_secs(5))
        .expect("watch")
        .expect("as it is");
    assert_eq!(
        orion::provider::describe(&first).expect("arm").state,
        ResourceState::Restoring
    );
    let arm_driver = driver(&address);
    let _resource = provide(&arm_driver);
    let told = watch
        .wait(Duration::from_secs(5))
        .expect("watch")
        .expect("a change");
    assert_eq!(
        orion::provider::describe(&told).expect("arm").state,
        ResourceState::Ready
    );
    // Ready means it can be run: the run is taken.
    assert_eq!(run_refused(&dash, "arm.home").map(|e| e.to_string()), None);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn what_does_not_come_back_is_lost_and_not_found_when_the_wait_is_over() {
    let dir = dir("restoring-lost");
    let board = Board::start_for_node(&dir);
    let address = Address::Local(board.socket().to_owned());
    let _logic = Logic::start(settings(&dir));
    let arm_driver = driver(&address);
    let resource = provide(&arm_driver);
    drop(board);
    drop(resource);
    drop(arm_driver);
    let _board = Board::start_for_node(&dir);
    let dash = dashboard(&address);
    assert_eq!(refused(&dash, "arm.home"), Error::Restoring);

    until("the arm is lost", || {
        arm(&dash) == Some(ResourceState::Lost)
    });
    assert_eq!(refused(&dash, "arm.home"), Error::NotFound);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_whole_node_that_restarts_knows_what_it_served_from_its_state_directory() {
    let dir = dir("restoring-node");
    let board = Board::start_for_node(&dir);
    let address = Address::Local(board.socket().to_owned());
    let logic = Logic::start(settings(&dir));
    let arm_driver = driver(&address);
    let resource = provide(&arm_driver);
    // The list is written a moment after what is served changed, off the hot path.
    let list = dir.join("state/served");
    until("the list is written", || {
        std::fs::read_to_string(&list).is_ok_and(|text| text.contains("resource arm"))
    });
    // The driver's action and resource, and the one action node logic has of its own.
    assert_eq!(
        std::fs::read_to_string(&list).expect("list"),
        "orion served 1\naction arm.home resource:arm\naction files.send node:board\n\
         resource arm\n"
    );

    // Everything stops, node logic last, as a board being powered off.
    drop(resource);
    drop(arm_driver);
    drop(board);
    drop(logic);
    let _board = Board::start_for_node(&dir);
    let _logic = Logic::start(settings(&dir));
    let dash = dashboard(&address);
    assert_eq!(refused(&dash, "arm.home"), Error::Restoring);
    assert_eq!(refused(&dash, "arm.park"), Error::NotFound);
    // The record, if it was saved before the stop, does not say ready. If it was not, the
    // read of it says restoring.
    match dash.resources("arm") {
        Ok(found) => assert!(
            found.iter().all(|arm| arm.state != ResourceState::Ready),
            "{found:?}"
        ),
        Err(orion::Error::Client(ClientError::Refused(Error::Restoring))) => {}
        Err(other) => panic!("{other}"),
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_first_boot_and_a_list_that_cannot_be_read_restore_nothing() {
    for broken in [None, Some("not a list at all\n")] {
        let dir = dir("restoring-first");
        if let Some(text) = broken {
            std::fs::create_dir_all(dir.join("state")).expect("state");
            std::fs::write(dir.join("state/served"), text).expect("written");
        }
        let board = Board::start_for_node(&dir);
        let address = Address::Local(board.socket().to_owned());
        // It starts all the same, and nothing is restoring: not found, at once.
        let _logic = Logic::start(settings(&dir));
        let dash = dashboard(&address);
        assert_eq!(refused(&dash, "arm.home"), Error::NotFound);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[test]
fn a_read_of_one_resource_served_before_that_has_no_record_yet_is_restoring() {
    // The list is newer than the records: the node went down before the records were saved.
    let dir = dir("restoring-read");
    std::fs::create_dir_all(dir.join("state")).expect("state");
    std::fs::write(dir.join("state/served"), "orion served 1\nresource arm\n").expect("written");
    let board = Board::start_for_node(&dir);
    let address = Address::Local(board.socket().to_owned());
    let _logic = Logic::start(settings(&dir));
    let dash = dashboard(&address);
    let records = dash.records();
    match records.get(RecordClass::Resource, "arm") {
        Err(ClientError::Refused(Error::Restoring)) => {}
        other => panic!("expected restoring, got {other:?}"),
    }
    // A read of many, or of one never served, is answered as it is.
    assert!(
        records
            .get(RecordClass::Resource, "ar")
            .expect("get")
            .is_empty()
    );
    assert!(
        records
            .get(RecordClass::Resource, "leg")
            .expect("get")
            .is_empty()
    );
    // When the wait is over there is nothing to be restoring.
    until("the wait is over", || {
        records
            .get(RecordClass::Resource, "arm")
            .is_ok_and(|found| found.is_empty())
    });
    let _ = std::fs::remove_dir_all(&dir);
}

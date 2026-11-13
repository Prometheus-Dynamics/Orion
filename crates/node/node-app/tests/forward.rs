//! Actions across two nodes: a run on one node for an action that lives on the other is
//! carried there, decided at that node's door, and survives that node restarting.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use orion::actions::{ActionCall, ActionDecl, Admits, Outcome, Policy, Request, Target, Update};
use orion::records::{Bytes, RecordClass, RecordEntry};
use orion::types::{Error, HlcTimestamp};
use orion::{Address, ClientError, Identity, Orion};
use orion_msg_records::NodeRecord;
use orion_node_app::{Node, NodeSettings};
use orion_stubs::{Board, Security};
use orion_wire::List;
use orion_wire::body::encode_fields;
use tokio::sync::oneshot;

fn dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("orion-node-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("directory");
    dir
}

fn settings(dir: &Path, name: &str) -> NodeSettings {
    let mut source = orion_config::Source::parse("", []).expect("no settings");
    let mut settings = NodeSettings::from_source(&mut source).expect("defaults");
    settings.config.socket = dir.join("orion.sock");
    settings.config.state_dir = dir.join("state");
    settings.config.keys_dir = dir.join("state");
    // Node logic checks the node it forwards to as the comm core checks a peer.
    settings.config.security = Security::On;
    settings.files_dir = dir.join("files");
    settings.audit = Some(orion_audit::AuditConfig::new(&dir.join("state/audit.log")));
    settings.config.node_name = name.parse().expect("name");
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

fn right() -> Target {
    Target::Node("right".parse().expect("name"))
}

/// The program on the right node that has the actions: `blink`, which peers may run, and
/// `wipe`, which only programs on its own board may.
struct Driver {
    stop: Arc<AtomicBool>,
    /// Why each run it was told to stop was stopped, in order.
    heard: Arc<std::sync::Mutex<Vec<String>>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Driver {
    fn start(socket: &Path) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let (socket, stopping) = (socket.to_owned(), stop.clone());
        let heard = Arc::new(std::sync::Mutex::new(Vec::new()));
        let hearing = heard.clone();
        let (ready, started) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            let orion = Orion::connect(&Address::Local(socket), "light-driver").expect("connect");
            let register = |name: &str, admits: Admits| {
                let decl = ActionDecl {
                    name,
                    target: right(),
                    description: "",
                };
                let deadline = Instant::now() + Duration::from_secs(10);
                loop {
                    match orion.register_action(&decl, Policy::new(admits)) {
                        Ok(registration) => return registration,
                        Err(orion::Error::Client(ClientError::Refused(Error::Unavailable)))
                            if Instant::now() < deadline =>
                        {
                            std::thread::sleep(Duration::from_millis(10));
                        }
                        Err(error) => panic!("register {name}: {error}"),
                    }
                }
            };
            let mut blink = register("blink", Admits::LOCAL.and(Admits::PEER));
            let _wipe = register("wipe", Admits::LOCAL);
            ready.send(()).expect("ready");
            while !stopping.load(Ordering::Relaxed) {
                // A session that broke is the board going away under the driver.
                let Ok(request) = blink.wait(Duration::from_millis(20)) else {
                    return;
                };
                match request {
                    Some(Request::Run { call, args, .. }) => {
                        let _ = blink.progress(call, 40, "blinking", b"");
                        if args == b"slow" {
                            continue;
                        }
                        let mut output = args.clone();
                        output.make_ascii_uppercase();
                        let _ = blink.finish(call, Outcome::Succeeded, &output, "");
                    }
                    Some(Request::Cancel { call, reason }) => {
                        hearing.lock().expect("heard").push(format!("{reason:?}"));
                        let _ = blink.finish(
                            call,
                            Outcome::Cancelled,
                            b"",
                            &format!("stopped: {reason:?}"),
                        );
                    }
                    None => {}
                }
            }
        });
        started
            .recv_timeout(Duration::from_secs(15))
            .expect("the driver registers its actions");
        Self {
            stop,
            heard,
            thread: Some(thread),
        }
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn call(name: &'static str, args: &'static [u8]) -> ActionCall<'static> {
    ActionCall {
        name,
        target: right(),
        args,
        deadline_ms: 0,
        detached: false,
    }
}

/// Runs until node logic and the other node answer, and returns how the run ended.
fn finish(
    caller: &Orion,
    action: &ActionCall<'_>,
) -> Result<orion::actions::Finished, ClientError> {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let mut run = caller.run(action).expect("sent");
        match run.finish(Duration::from_secs(10)) {
            Err(ClientError::Refused(Error::Unavailable)) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(20));
            }
            other => return other,
        }
    }
}

/// The right node: its comm core, its node logic and the program with the actions.
struct Right {
    driver: Driver,
    _logic: Logic,
    board: Board,
}

impl Right {
    fn start(dir: &Path, port: Option<u16>, left_key: [u8; 32]) -> Self {
        let board = Board::start_named(dir, Security::On, "right", port);
        // The right node knows the left node's key: it is a peer.
        let trusted = board
            .keys()
            .lock()
            .trust_peer(&"left".parse().expect("name"), left_key)
            .expect("trust");
        assert!(trusted);
        let logic = Logic::start(settings(dir, "right"));
        let driver = Driver::start(board.socket());
        Self {
            driver,
            _logic: logic,
            board,
        }
    }
}

#[test]
fn an_action_on_another_node_runs_there_and_the_owner_may_restart() {
    let (left_dir, right_dir) = (dir("forward-left"), dir("forward-right"));
    // The left node's key, as its comm core would have made it.
    std::fs::create_dir_all(left_dir.join("state")).expect("state");
    let left_key = Identity::load_or_create(&left_dir.join("state/node-identity.ed25519"))
        .expect("key")
        .public();
    std::fs::create_dir_all(right_dir.join("state")).expect("state");
    let right_key = Identity::load_or_create(&right_dir.join("state/node-identity.ed25519"))
        .expect("key")
        .public();

    let left = Board::start_named(&left_dir, Security::On, "left", None);
    let _left_logic = Logic::start(settings(&left_dir, "left"));
    let right_node = Right::start(&right_dir, None, left_key);
    let port = right_node.board.network().port();

    // The left node knows where the right node is.
    let caller =
        Orion::connect(&Address::Local(left.socket().to_owned()), "dashboard").expect("connect");
    let address = format!("127.0.0.1:{port}");
    let mut value = vec![0_u8; 128];
    let len = encode_fields(
        &NodeRecord {
            name: "right".parse().expect("name"),
            address: &address,
            labels: List::new(&[]),
        },
        &mut value,
    )
    .expect("encode");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let put = caller.records().put(&[RecordEntry {
            class: RecordClass::Node,
            key: "right",
            stamp: HlcTimestamp::ZERO,
            deleted: false,
            value: Bytes(&value[..len]),
        }]);
        match put {
            Ok(_) => break,
            Err(ClientError::Refused(Error::Unavailable)) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("put: {error}"),
        }
    }

    // The left node forwards only to a node its trust file pins, and only once that node has
    // proved it holds the pinned key.
    match finish(&caller, &call("blink", b"")) {
        Err(ClientError::Refused(Error::Denied)) => {}
        other => panic!("expected denied with no pin, got {other:?}"),
    }
    let wrong = Identity::load_or_create(&left_dir.join("wrong.ed25519"))
        .expect("key")
        .public();
    let right_name = "right".parse().expect("name");
    assert!(
        left.keys()
            .lock()
            .trust_peer(&right_name, wrong)
            .expect("trust")
    );
    match finish(&caller, &call("blink", b"")) {
        Err(ClientError::Refused(Error::Denied)) => {}
        other => panic!("expected denied with the wrong pin, got {other:?}"),
    }
    left.keys().lock().revoke(wrong).expect("revoke");
    assert!(
        left.keys()
            .lock()
            .trust_peer(&right_name, right_key)
            .expect("trust")
    );

    // It runs on the right node, with its progress, for a caller on the left.
    let mut run = caller.run(&call("blink", b"twice")).expect("run");
    let mut steps = Vec::new();
    let ended = loop {
        match run.wait(Duration::from_secs(10)).expect("run") {
            Some(Update::Doing(step)) => steps.push((step.percent, step.message)),
            Some(Update::Finished(finished)) => break finished,
            None => panic!("the run did not end"),
        }
    };
    assert_eq!(
        (ended.outcome, ended.output.as_slice()),
        (Outcome::Succeeded, &b"TWICE"[..])
    );
    assert!(
        steps
            .iter()
            .all(|step| step == &(40, "blinking".to_owned()))
    );

    // The check runs where the action runs: the right node's door refuses a peer what is
    // for programs on its own board, and its node logic knows no such action as `none`.
    match finish(&caller, &call("wipe", b"")) {
        Err(ClientError::Refused(Error::Denied)) => {}
        other => panic!("expected denied, got {other:?}"),
    }
    match finish(&caller, &call("none", b"")) {
        Err(ClientError::Refused(Error::NotFound)) => {}
        other => panic!("expected not found, got {other:?}"),
    }
    // A node nobody knows the address of.
    let nowhere = ActionCall {
        target: Target::Node("nowhere".parse().expect("name")),
        ..call("blink", b"")
    };
    match finish(&caller, &nowhere) {
        Err(ClientError::Refused(Error::NotFound)) => {}
        other => panic!("expected not found, got {other:?}"),
    }

    // Cancel reaches the program on the other node.
    let mut slow = caller.run(&call("blink", b"slow")).expect("run");
    assert!(matches!(
        slow.wait(Duration::from_secs(10)).expect("run"),
        Some(Update::Doing(_))
    ));
    slow.cancel().expect("cancel");
    let cancelled = slow.finish(Duration::from_secs(10)).expect("it ends");
    assert_eq!(
        (cancelled.outcome, cancelled.detail.as_str()),
        (Outcome::Cancelled, "stopped: Asked"),
        "why crosses to the other node"
    );

    // A caller that goes with a run under way on the other node: the program there is told
    // why it is to stop, as the node that carried the run said it.
    let brief =
        Orion::connect(&Address::Local(left.socket().to_owned()), "brief").expect("connect");
    let mut orphan = brief.run(&call("blink", b"slow")).expect("run");
    assert!(matches!(
        orphan.wait(Duration::from_secs(10)).expect("run"),
        Some(Update::Doing(_))
    ));
    drop(orphan);
    drop(brief);
    let deadline = Instant::now() + Duration::from_secs(10);
    while right_node.driver.heard.lock().expect("heard").len() < 2 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        *right_node.driver.heard.lock().expect("heard"),
        ["Asked", "CallerGone"]
    );

    // The owner goes away with a run under way: the caller is told, and does not wait.
    let mut doomed = caller.run(&call("blink", b"slow")).expect("run");
    assert!(matches!(
        doomed.wait(Duration::from_secs(10)).expect("run"),
        Some(Update::Doing(_))
    ));
    let mut right_node = right_node;
    right_node.board.stop();
    let failed = doomed.finish(Duration::from_secs(10)).expect("it ends");
    assert_eq!(failed.outcome, Outcome::Failed);
    assert_eq!(failed.detail, "the node that runs it went away");
    // While it is away, a run is refused and not left waiting.
    let mut away = caller.run(&call("blink", b"x")).expect("sent");
    match away.finish(Duration::from_secs(10)) {
        Err(ClientError::Refused(Error::Unavailable)) => {}
        other => panic!("expected unavailable, got {other:?}"),
    }

    drop(right_node);

    // The owner is back, where it was. Runs go there again, with nothing done on the left.
    let _right_node = Right::start(&right_dir, Some(port), left_key);
    let again = finish(&caller, &call("blink", b"again")).expect("it runs again");
    assert_eq!(again.output, b"AGAIN");
}

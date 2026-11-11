//! Actions through node logic: a program in another process adds its actions, a caller runs
//! one, and the one the caller's policy does not admit is refused at the door.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use orion::actions::{ActionCall, ActionDecl, Admits, Outcome, Policy, Request, Target, Update};
use orion::topics::Next;
use orion::types::Error;
use orion::{Address, ClientError, Orion};
use orion_node_app::{ACTIONS_TOPIC, Node, NodeSettings};
use orion_stubs::{Board, Security};
use tokio::sync::oneshot;

/// Set for the process that is the provider: the board's socket.
const PROVIDER_SOCKET: &str = "ORION_TEST_PROVIDER_SOCKET";

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

fn fan() -> Target {
    Target::Resource("fan.left".parse().expect("name"))
}

/// The provider, when this test binary is started as one: it adds two actions and does the
/// runs it is handed until it is told to quit. As a test of its own it does nothing.
#[test]
fn provider_process() {
    let Ok(socket) = std::env::var(PROVIDER_SOCKET) else {
        return;
    };
    let orion = Orion::connect(&Address::Local(socket.into()), "fan-driver").expect("connect");
    let register = |name: &str, admits: Admits| {
        let decl = ActionDecl {
            name,
            target: fan(),
            description: "",
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match orion.register_action(&decl, Policy::new(admits)) {
                Ok(registration) => return registration,
                // Node logic is not there yet.
                Err(orion::Error::Client(ClientError::Refused(Error::Unavailable)))
                    if Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("register {name}: {error}"),
            }
        }
    };
    // For programs on the board.
    let mut open = register("open", Admits::LOCAL);
    // For operators alone: no program on the board may run it, whoever it runs as.
    let _wipe = register("wipe", Admits::OPERATOR);
    println!("provider ready");
    let mut long = None;
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        match open.wait(Duration::from_millis(100)).expect("connected") {
            Some(Request::Run { call, args, .. }) => {
                if args == b"quit" {
                    open.finish(call, Outcome::Succeeded, b"bye", "")
                        .expect("finish");
                    return;
                }
                if args == b"slow" {
                    // Left running: it ends when it is cancelled.
                    continue;
                }
                if args == b"long" {
                    // Says where it is and goes on until it is released.
                    open.progress(call, 30, "working", b"").expect("progress");
                    long = Some(call);
                    continue;
                }
                if args == b"release" {
                    if let Some(long) = long.take() {
                        open.finish(long, Outcome::Succeeded, b"long done", "")
                            .expect("finish");
                    }
                    open.finish(call, Outcome::Succeeded, b"", "")
                        .expect("finish");
                    continue;
                }
                open.progress(call, 50, "halfway", b"").expect("progress");
                let mut output = args.clone();
                output.reverse();
                open.finish(call, Outcome::Succeeded, &output, "")
                    .expect("finish");
            }
            Some(Request::Cancel { call }) => {
                open.finish(call, Outcome::Cancelled, b"", "stopped")
                    .expect("finish");
            }
            None => {}
        }
    }
}

struct Provider(Child);

impl Provider {
    /// Starts this test binary again as the provider, and waits until its actions are listed.
    fn start(socket: &Path) -> Self {
        let mut child = Command::new(std::env::current_exe().expect("this test"))
            .args([
                "--exact",
                "provider_process",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(PROVIDER_SOCKET, socket)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("the provider starts");
        let stdout = child.stdout.take().expect("stdout");
        let mut lines = BufReader::new(stdout).lines();
        let ready = lines
            .by_ref()
            .map_while(Result::ok)
            .any(|line| line.contains("provider ready"));
        assert!(ready, "the provider never got ready");
        // The rest of what it prints is read and dropped, so it never blocks on a full pipe.
        std::thread::spawn(move || lines.for_each(drop));
        Self(child)
    }
}

impl Drop for Provider {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn call(name: &'static str, args: &'static [u8]) -> ActionCall<'static> {
    ActionCall {
        name,
        target: fan(),
        args,
        deadline_ms: 0,
        detached: false,
    }
}

#[test]
fn an_action_from_another_process_runs_and_is_refused_by_its_policy() {
    let dir = dir("actions");
    let board = Board::start_for_node_with(&dir, Security::On);
    let address = Address::Local(board.socket().to_owned());
    let _logic = Logic::start(settings(&dir));
    let mut provider = Provider::start(board.socket());
    assert_ne!(provider.0.id(), std::process::id());

    let caller = Orion::connect(&address, "dashboard").expect("connect");
    // What happens to runs is a topic anyone on the board can read.
    let mut runs = caller.subscribe(ACTIONS_TOPIC).expect("the topic of runs");

    // It runs: progress, then the result the other process made.
    let mut run = caller.run(&call("open", b"abc")).expect("run");
    let mut doing = Vec::new();
    let finished = loop {
        match run.wait(Duration::from_secs(5)).expect("run") {
            Some(Update::Doing(step)) => doing.push((step.percent, step.message)),
            Some(Update::Finished(finished)) => break finished,
            None => panic!("the run did not end"),
        }
    };
    assert_eq!(finished.outcome, Outcome::Succeeded);
    assert_eq!(finished.output, b"cba");
    assert!(doing.len() <= 1);
    if let Some(step) = doing.first() {
        assert_eq!(step, &(50, "halfway".to_owned()));
    }

    // The one for operators is refused, and the other process never hears of it.
    let mut refused = caller.run(&call("wipe", b"everything")).expect("sent");
    match refused.wait(Duration::from_secs(5)) {
        Err(ClientError::Refused(Error::Denied)) => {}
        other => panic!("expected a refusal, got {other:?}"),
    }
    // One that is not on the list is not found.
    let mut missing = caller.run(&call("explode", b"")).expect("sent");
    match missing.wait(Duration::from_secs(5)) {
        Err(ClientError::Refused(Error::NotFound)) => {}
        other => panic!("expected not found, got {other:?}"),
    }

    // The topic told the run's story, and nothing of the refused ones.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut story = Vec::new();
    while story.len() < 3 && Instant::now() < deadline {
        runs.wait(Duration::from_millis(100)).expect("topic");
        while let Next::Sample(sample) = runs.next() {
            if sample.field.0 == 0 {
                story.push(String::from_utf8_lossy(sample.payload).into_owned());
            }
        }
    }
    let told: Vec<&str> = story
        .iter()
        .map(|line| line.split_once(' ').expect("a run number").1)
        .collect();
    assert_eq!(
        told,
        [
            "open resource:fan.left 255 - claimed",
            "open resource:fan.left 50 - halfway",
            "open resource:fan.left 255 Succeeded ",
        ]
    );

    // A run past its deadline times out, and the caller is told.
    let mut slow = caller
        .run(&ActionCall {
            deadline_ms: 100,
            ..call("open", b"slow")
        })
        .expect("run");
    let ended = slow.finish(Duration::from_secs(5)).expect("it ends");
    assert_eq!(ended.outcome, Outcome::TimedOut);

    // The other process goes away: its actions leave the list.
    drop(provider.0.kill());
    let _ = provider.0.wait();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut gone = caller.run(&call("open", b"abc")).expect("sent");
        match gone.wait(Duration::from_secs(5)) {
            Err(ClientError::Refused(Error::NotFound)) => break,
            _ if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(10)),
            other => panic!("the action stayed on the list: {other:?}"),
        }
    }
}

#[test]
fn actions_are_still_there_after_node_logic_restarts() {
    let dir = dir("actions-restart");
    let board = Board::start_for_node(&dir);
    let address = Address::Local(board.socket().to_owned());
    let logic = Logic::start(settings(&dir));
    let _provider = Provider::start(board.socket());
    let caller = Orion::connect(&address, "dashboard").expect("connect");
    let mut run = caller.run(&call("open", b"one")).expect("run");
    assert_eq!(
        run.finish(Duration::from_secs(5)).expect("ends").output,
        b"eno"
    );

    // Node logic is killed and started again. The provider never notices, and its action is
    // on the new list.
    drop(logic);
    let mut away = caller.run(&call("open", b"two")).expect("sent");
    match away.wait(Duration::from_secs(5)) {
        Err(ClientError::Refused(Error::Unavailable)) => {}
        other => panic!("expected unavailable, got {other:?}"),
    }
    let _logic = Logic::start(settings(&dir));
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut run = caller.run(&call("open", b"two")).expect("sent");
        match run.finish(Duration::from_secs(5)) {
            Ok(finished) => {
                assert_eq!(finished.output, b"owt");
                break;
            }
            Err(ClientError::Refused(Error::Unavailable | Error::NotFound))
                if Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("the run after the restart: {error}"),
        }
    }
}

#[test]
fn a_caller_disconnects_mid_action_and_reattaches_to_the_progress() {
    let dir = dir("actions-detached");
    let board = Board::start_for_node_with(&dir, Security::On);
    let address = Address::Local(board.socket().to_owned());
    let _logic = Logic::start(settings(&dir));
    let _provider = Provider::start(board.socket());
    let detached = |args: &'static [u8]| ActionCall {
        detached: true,
        ..call("open", args)
    };

    // Started detached, seen under way, and then the caller is gone.
    let number = {
        let caller = Orion::connect(&address, "dashboard").expect("connect");
        let mut run = caller.run(&detached(b"long")).expect("run");
        match run.wait(Duration::from_secs(5)).expect("run") {
            Some(Update::Doing(step)) => {
                assert_eq!((step.percent, step.message.as_str()), (30, "working"))
            }
            other => panic!("expected progress, got {other:?}"),
        }
        run.call()
    };

    // Back in a new session: the progress it left at, then the end.
    let caller = Orion::connect(&address, "dashboard").expect("connect");
    let mut back = caller.attach(number).expect("attach");
    match back.wait(Duration::from_secs(5)).expect("attached") {
        Some(Update::Doing(step)) => {
            assert_eq!((step.percent, step.message.as_str()), (30, "working"))
        }
        other => panic!("expected the progress it left at, got {other:?}"),
    }
    let releaser = Orion::connect(&address, "releaser").expect("connect");
    let mut release = releaser.run(&call("open", b"release")).expect("run");
    release.finish(Duration::from_secs(5)).expect("released");
    let ended = back
        .finish(Duration::from_secs(5))
        .expect("the long run ends");
    assert_eq!(
        (ended.outcome, ended.output.as_slice()),
        (Outcome::Succeeded, &b"long done"[..])
    );

    // One that ends while its caller is away is still there to come back to, once.
    let number = {
        let caller = Orion::connect(&address, "dashboard").expect("connect");
        let mut run = caller.run(&detached(b"long")).expect("run");
        assert!(matches!(
            run.wait(Duration::from_secs(5)).expect("run"),
            Some(Update::Doing(_))
        ));
        run.call()
    };
    let mut release = releaser.run(&call("open", b"release")).expect("run");
    release.finish(Duration::from_secs(5)).expect("released");
    let caller = Orion::connect(&address, "dashboard").expect("connect");
    let mut back = caller.attach(number).expect("attach");
    let ended = back.finish(Duration::from_secs(5)).expect("how it ended");
    assert_eq!(ended.output, b"long done");
    let mut again = caller.attach(number).expect("sent");
    match again.wait(Duration::from_secs(5)) {
        Err(ClientError::Refused(Error::NotFound)) => {}
        other => panic!("expected not found, got {other:?}"),
    }

    // A run that was not detached is cancelled when its caller goes: nothing to come back to.
    let number = {
        let caller = Orion::connect(&address, "dashboard").expect("connect");
        let mut run = caller.run(&call("open", b"long")).expect("run");
        assert!(matches!(
            run.wait(Duration::from_secs(5)).expect("run"),
            Some(Update::Doing(_))
        ));
        run.call()
    };
    let mut none = caller.attach(number).expect("sent");
    match none.wait(Duration::from_secs(5)) {
        Err(ClientError::Refused(Error::NotFound)) => {}
        other => panic!("expected not found, got {other:?}"),
    }
}

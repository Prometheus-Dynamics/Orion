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
    settings.files_dir = dir.join("files");
    settings.audit = Some(orion_audit::AuditConfig::new(&dir.join("state/audit.log")));
    settings.config.node_name = "board".parse().expect("name");
    settings
}

/// Node logic in a thread of its own, until dropped: then it is told to stop, as a signal
/// would.
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
                node.run(async {
                    let _ = killed.await;
                })
                .await;
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
                Err(orion::Error::Client(ClientError::Refused {
                    error: Error::Unavailable,
                    ..
                })) if Instant::now() < deadline => {
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
    // Runs held by a tag until `end:<tag>`.
    let mut held: Vec<(Vec<u8>, u64)> = Vec::new();
    // The sessions it was told have ended.
    let mut gone = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        match open.wait(Duration::from_millis(100)).expect("connected") {
            Some(Request::Run {
                call,
                args,
                caller,
                session,
                ..
            }) => {
                if args == b"session" {
                    open.finish(call, Outcome::Succeeded, session.to_string().as_bytes(), "")
                        .expect("finish");
                    continue;
                }
                if args == b"gone" {
                    let said = format!("{gone:?}");
                    open.finish(call, Outcome::Succeeded, said.as_bytes(), "")
                        .expect("finish");
                    continue;
                }
                if args == b"who" {
                    open.finish(call, Outcome::Succeeded, caller.as_bytes(), "")
                        .expect("finish");
                    continue;
                }
                if args == b"quit" {
                    open.finish(call, Outcome::Succeeded, b"bye", "")
                        .expect("finish");
                    return;
                }
                if let Some(tag) = args.strip_prefix(b"hold:") {
                    open.progress(call, 30, "working", b"").expect("progress");
                    held.push((tag.to_vec(), call));
                    continue;
                }
                if let Some(tag) = args.strip_prefix(b"end:") {
                    if let Some(at) = held.iter().position(|(held, _)| held == tag) {
                        let (tag, long) = held.remove(at);
                        let output = [tag.as_slice(), b" done"].concat();
                        open.finish(long, Outcome::Succeeded, &output, "")
                            .expect("finish");
                    }
                    open.finish(call, Outcome::Succeeded, b"", "")
                        .expect("finish");
                    continue;
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
            Some(Request::Cancel { call, .. }) => {
                held.retain(|(_, held)| *held != call);
                open.finish(call, Outcome::Cancelled, b"", "stopped")
                    .expect("finish");
            }
            Some(Request::CallerGone { session }) => gone.push(session),
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
        Err(ClientError::Refused {
            error: Error::Denied,
            ..
        }) => {}
        other => panic!("expected a refusal, got {other:?}"),
    }
    // One that is not on the list is not found.
    let mut missing = caller.run(&call("explode", b"")).expect("sent");
    match missing.wait(Duration::from_secs(5)) {
        Err(ClientError::Refused {
            error: Error::NotFound,
            ..
        }) => {}
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

    // What was done is in the audit log: the registrations, and the runs that passed the
    // check. The refused run never reached node logic.
    let log = std::fs::read_to_string(dir.join("state/audit.log")).unwrap_or_default();
    let did = |what: &str| log.lines().filter(|line| line.contains(what)).count();
    assert_eq!(
        did("\"did\":\"register\",\"what\":\"open resource:fan.left\",\"result\":\"ok\""),
        1,
        "{log}"
    );
    assert_eq!(
        did("\"did\":\"register\",\"what\":\"wipe resource:fan.left\""),
        1,
        "{log}"
    );
    assert_eq!(
        did("\"did\":\"run\",\"what\":\"open resource:fan.left\",\"result\":\"ok\""),
        2,
        "{log}"
    );
    assert_eq!(did("\"did\":\"run\",\"what\":\"wipe"), 0, "{log}");

    // The provider is told who asked, as the audit log names them.
    let who = caller
        .run(&call("open", b"who"))
        .expect("run")
        .finish(Duration::from_secs(5))
        .expect("ends");
    let uid = std::os::unix::fs::MetadataExt::uid(&std::fs::metadata(&dir).expect("dir"));
    assert_eq!(who.output, format!("local:{uid}").as_bytes());

    // The other process goes away: its actions leave the list.
    drop(provider.0.kill());
    let _ = provider.0.wait();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut gone = caller.run(&call("open", b"abc")).expect("sent");
        match gone.wait(Duration::from_secs(5)) {
            Err(ClientError::Refused {
                error: Error::NotFound,
                ..
            }) => break,
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

    // Node logic stops with a run under way: the run is cancelled where it runs, and its
    // caller is told why.
    let mut slow = caller.run(&call("open", b"long")).expect("run");
    assert!(matches!(
        slow.wait(Duration::from_secs(5)).expect("run"),
        Some(Update::Doing(_))
    ));
    drop(logic);
    let stopped = slow.finish(Duration::from_secs(5)).expect("it ends");
    assert_eq!(
        (stopped.outcome, stopped.detail.as_str()),
        (Outcome::Cancelled, "node logic is stopping")
    );

    // It starts again. The provider never notices, and its action is on the new list.
    let mut away = caller.run(&call("open", b"two")).expect("sent");
    match away.wait(Duration::from_secs(5)) {
        Err(ClientError::Refused {
            error: Error::Unavailable,
            ..
        }) => {}
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
            Err(ClientError::Refused {
                error: Error::Unavailable | Error::NotFound,
                ..
            }) if Instant::now() < deadline => {
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
        deadline_ms: 60_000,
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

    // One that ends while its caller is away is still there to come back to, the same each
    // time.
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
    let ended = again
        .finish(Duration::from_secs(5))
        .expect("how it ended, again");
    assert_eq!(ended.output, b"long done");

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
        Err(ClientError::Refused {
            error: Error::NotFound,
            ..
        }) => {}
        other => panic!("expected not found, got {other:?}"),
    }
}

#[test]
fn a_run_handle_names_one_run_across_sessions_and_restarts() {
    let dir = dir("actions-handles");
    let board = Board::start_for_node_with(&dir, Security::On);
    let address = Address::Local(board.socket().to_owned());
    let logic = Logic::start(settings(&dir));
    let _provider = Provider::start(board.socket());
    let detached = |args: &'static [u8]| ActionCall {
        detached: true,
        deadline_ms: 60_000,
        ..call("open", args)
    };
    // Session two, connected now. Every run of the same user takes its number from that user's
    // older runs, so what is held is ended from this session, whose own later numbers are past
    // the one in play.
    let caller = Orion::connect(&address, "dashboard").expect("connect");
    let end = |tag: &'static [u8]| {
        let args: &'static [u8] = Box::leak([b"end:".as_slice(), tag].concat().into_boxed_slice());
        let mut run = caller.run(&call("open", args)).expect("run");
        run.finish(Duration::from_secs(5)).expect("ended");
    };
    let under_way = |run: &mut orion::actions::Run| {
        assert!(matches!(
            run.wait(Duration::from_secs(5)).expect("run"),
            Some(Update::Doing(_))
        ));
        run.handle().expect("the node said its handle")
    };
    let refused = |run: &mut orion::actions::Run| match run.wait(Duration::from_secs(5)) {
        Err(ClientError::Refused { error, .. }) => error,
        other => panic!("expected a refusal, got {other:?}"),
    };

    // Session one starts a detached run and is lost.
    let (number, old) = {
        let caller = Orion::connect(&address, "dashboard").expect("connect");
        let mut run = caller.run(&detached(b"hold:old")).expect("run");
        (run.call(), under_way(&mut run))
    };
    // Session two starts another under the same number while the old one still runs.
    let mut new = caller.run(&detached(b"hold:new")).expect("run");
    assert_eq!(new.call(), number, "a session's numbers start over");
    let new_handle = under_way(&mut new);
    assert_ne!(new_handle, old);
    // The old one ends first. Session two's follow is its own run's end, never the old one's.
    end(b"old");
    end(b"new");
    let ended = new.finish(Duration::from_secs(5)).expect("ends");
    assert_eq!(ended.output, b"new done");
    drop(new);
    // The number reaches the newer run's end, kept for coming back to...
    let mut back = caller.attach(number).expect("attach");
    assert_eq!(
        back.finish(Duration::from_secs(5)).expect("ended").output,
        b"new done"
    );
    assert_eq!(back.handle(), Some(new_handle));
    drop(back);
    // ...again, and never the old one's.
    let mut again = caller.attach(number).expect("sent");
    let ended = again.finish(Duration::from_secs(5)).expect("ended");
    assert_eq!(
        (ended.output, again.handle()),
        (b"new done".to_vec(), Some(new_handle))
    );
    drop(again);
    // The old answer is reached by its handle alone, the same each time.
    for _ in 0..2 {
        let mut old_back = caller.attach_handle(old).expect("attach");
        let ended = old_back.finish(Duration::from_secs(5)).expect("ended");
        assert_eq!(ended.output, b"old done");
    }

    // Cancel after coming back by the handle reaches the run come back to.
    let third = {
        let caller = Orion::connect(&address, "dashboard").expect("connect");
        let mut run = caller.run(&detached(b"hold:third")).expect("run");
        under_way(&mut run)
    };
    let mut third_back = caller.attach_handle(third).expect("attach");
    third_back.cancel().expect("cancel");
    let ended = third_back.finish(Duration::from_secs(5)).expect("ended");
    assert_eq!(
        (ended.outcome, ended.detail.as_str()),
        (Outcome::Cancelled, "stopped")
    );

    // Node logic starts again: a handle from before reaches nothing, and is told so.
    drop(logic);
    let _logic = Logic::start(settings(&dir));
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut gone = caller.attach_handle(new_handle).expect("sent");
        match refused(&mut gone) {
            Error::Unavailable if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            error => {
                assert_eq!(error, Error::RunGone);
                break;
            }
        }
    }
}

#[test]
fn a_program_hears_once_a_callers_session_ends() {
    let dir = dir("actions-caller-gone");
    let board = Board::start_for_node_with(&dir, Security::On);
    let address = Address::Local(board.socket().to_owned());
    let _logic = Logic::start(settings(&dir));
    let _provider = Provider::start(board.socket());
    let token = |orion: &Orion| -> String {
        let finished = orion
            .run(&call("open", b"session"))
            .expect("run")
            .finish(Duration::from_secs(5))
            .expect("ends");
        String::from_utf8(finished.output).expect("text")
    };

    // The same program twice: two sessions, a token each.
    let first = Orion::connect(&address, "dashboard").expect("connect");
    let second = Orion::connect(&address, "dashboard").expect("connect");
    let (one, two) = (token(&first), token(&second));
    assert_ne!(one, two);
    assert_ne!(one, "0");

    // The first goes: the program is told of it by its token, and not of the second.
    drop(first);
    let deadline = Instant::now() + Duration::from_secs(5);
    let told = loop {
        let finished = second
            .run(&call("open", b"gone"))
            .expect("run")
            .finish(Duration::from_secs(5))
            .expect("ends");
        let told = String::from_utf8(finished.output).expect("text");
        if told != "[]" || Instant::now() > deadline {
            break told;
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(told, format!("[{one}]"));

    // A detached run without a deadline, or with one longer than the node takes, is refused
    // by name.
    for (deadline_ms, refused) in [(0, Error::NoDeadline), (u64::MAX, Error::DeadlineTooLong)] {
        let mut run = second
            .run(&ActionCall {
                detached: true,
                deadline_ms,
                ..call("open", b"abc")
            })
            .expect("sent");
        match run.wait(Duration::from_secs(5)) {
            Err(ClientError::Refused { error, .. }) => assert_eq!(error, refused),
            other => panic!("expected {refused:?}, got {other:?}"),
        }
    }
}

//! Every verb of the command line, end to end, against a comm core with stand-ins for node
//! logic.

#![cfg(target_os = "linux")]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use orion::actions::{Admits, Outcome, Policy, Request};
use orion::executor::{ExecError, Executor, WorkloadKind, WorkloadName, WorkloadState};
use orion::provider::ResourceState;
use orion::records::{Bytes, RecordClass, RecordEntry};
use orion::topics::{FieldId, SampleTime, TopicDecl};
use orion::types::HlcTimestamp;
use orion::{Address, Options, Orion, Roles, fields};
use orion_msg_records::{Label, NodeRecord};
use orion_stubs::Board;
use orion_wire::List;
use orion_wire::body::encode_fields;

/// Runs the tool against `board` and returns its exit code and what it printed.
fn ctl(board: &Board, line: &str) -> (i32, String, String) {
    let mut words = vec!["--address".to_owned(), board.socket().display().to_string()];
    words.extend(line.split_whitespace().map(str::to_owned));
    run(&words)
}

fn run(words: &[String]) -> (i32, String, String) {
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let code = orion_ctl::run(words, &mut out, &mut err);
    (
        code,
        String::from_utf8(out).expect("text"),
        String::from_utf8(err).expect("text"),
    )
}

fn connect(socket: &Path, name: &str, roles: Roles) -> Orion {
    let options = Options {
        roles,
        ..Options::default()
    };
    Orion::connect_with(&Address::Local(socket.to_owned()), name, options).expect("connect")
}

/// Something running beside the tool until it is dropped: a provider, an executor.
struct Beside {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Beside {
    fn start(body: impl FnOnce(&AtomicBool) + Send + 'static) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let (flag, ready) = (stop.clone(), Arc::new(AtomicBool::new(false)));
        let started = ready.clone();
        let thread = std::thread::spawn(move || {
            started.store(true, Ordering::Release);
            body(&flag);
        });
        while !ready.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
        Self {
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for Beside {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            thread.join().expect("the helper ends cleanly");
        }
    }
}

/// A fan's provider: a resource with a topic and two actions, served until told to stop.
fn fan(socket: &Path) -> Beside {
    let socket = socket.to_owned();
    let (ready, started) = std::sync::mpsc::channel();
    let beside = Beside::start(move |stop| {
        let orion = connect(&socket, "fan-driver", Roles::CLIENT.and(Roles::PROVIDER));
        let mut describing = orion
            .provider()
            .resource("fan.left", "fan")
            .expect("name")
            .label("mounted", "left intake");
        let mut speed = describing
            .topic("speed", &fields![rpm: Float "rpm", spinning: Bool], 16, 8)
            .expect("topic");
        describing
            .action("set", "Sets the fan's speed", Policy::new(Admits::LOCAL))
            .expect("action");
        describing
            .action(
                "calibrate",
                "Finds the fan's limits",
                Policy::new(Admits::LOCAL),
            )
            .expect("action");
        let mut fan = describing.publish(ResourceState::Ready).expect("publish");
        let mut publish = |rpm: f64, at: u64| {
            speed
                .publish(FieldId(0), SampleTime::from_nanos(at), &rpm.to_le_bytes())
                .expect("publish");
            speed
                .publish(
                    FieldId(1),
                    SampleTime::from_nanos(at),
                    &[u8::from(rpm > 0.0)],
                )
                .expect("publish");
        };
        publish(1200.0, 1);
        ready.send(()).expect("ready");
        let (mut at, mut slow) = (1, None);
        while !stop.load(Ordering::Acquire) {
            let requests = fan.requests().expect("actions");
            if let Some((call, since)) = slow
                && std::time::Instant::now().duration_since(since) > Duration::from_millis(300)
            {
                requests
                    .finish(call, Outcome::Succeeded, b"limits found", "")
                    .expect("finish");
                slow = None;
            }
            match requests.wait(Duration::from_millis(20)).expect("wait") {
                Some(Request::Run {
                    call, name, args, ..
                }) if name == "fan.left.set" => {
                    let Ok(rpm) = String::from_utf8_lossy(&args).parse::<f64>() else {
                        requests
                            .finish(call, Outcome::Failed, &[], "that is not a speed")
                            .expect("finish");
                        continue;
                    };
                    requests
                        .progress(call, 50, "spinning up", &[])
                        .expect("progress");
                    at += 1;
                    publish(rpm, at);
                    requests
                        .finish(
                            call,
                            Outcome::Succeeded,
                            format!("now {rpm} rpm").as_bytes(),
                            "",
                        )
                        .expect("finish");
                }
                Some(Request::Run { call, .. }) => {
                    requests
                        .progress(call, 10, "sweeping", &[])
                        .expect("progress");
                    slow = Some((call, std::time::Instant::now()));
                }
                Some(Request::Cancel { .. } | Request::CallerGone { .. }) | None => {}
            }
        }
    });
    started.recv().expect("the fan is there");
    beside
}

struct Scripts {
    kinds: Vec<WorkloadKind>,
    running: BTreeMap<String, WorkloadState>,
}

impl Executor for Scripts {
    fn kinds(&self) -> &[WorkloadKind] {
        &self.kinds
    }

    fn start(&mut self, name: &WorkloadName, spec: &[u8]) -> Result<(), ExecError> {
        if spec == b"broken" {
            return Err(ExecError("the script does not parse".into()));
        }
        self.running
            .insert(name.to_string(), WorkloadState::Running);
        Ok(())
    }

    fn stop(&mut self, name: &WorkloadName) {
        self.running.remove(name.as_str());
    }

    fn kill(&mut self, name: &WorkloadName) {
        self.running.remove(name.as_str());
    }

    fn inventory(&self) -> Vec<(WorkloadName, WorkloadState)> {
        self.running
            .iter()
            .map(|(name, state)| (name.parse().expect("name"), *state))
            .collect()
    }
}

/// An executor of `script` workloads, serving until told to stop.
fn executor(socket: &Path) -> Beside {
    let socket = socket.to_owned();
    let (ready, started) = std::sync::mpsc::channel();
    let beside = Beside::start(move |stop| {
        let orion = connect(&socket, "script-runner", Roles::EXECUTOR);
        let mut scripts = Scripts {
            kinds: vec!["script".parse().expect("kind")],
            running: BTreeMap::new(),
        };
        let mut serving = orion.executor();
        serving.start(&scripts).expect("start");
        serving
            .turn(&mut scripts, Duration::from_millis(20))
            .expect("turn");
        ready.send(()).expect("ready");
        while !stop.load(Ordering::Acquire) {
            serving
                .turn(&mut scripts, Duration::from_millis(10))
                .expect("turn");
        }
    });
    started.recv().expect("the executor is there");
    beside
}

#[test]
fn node_show_peers_and_time() {
    let board = Board::start("ctl-node");
    // A fact about the board, published by some program on it.
    let facts = connect(board.socket(), "identity", Roles::CLIENT);
    let mut model = facts
        .declare(&TopicDecl {
            name: "hello.model",
            fields: &fields![value: Text],
            history: 2,
            expiry_ms: 0,
            max_payload: 64,
            on_subscribe: Default::default(),
        })
        .expect("declare");
    model
        .publish(FieldId(0), SampleTime::from_nanos(1), b"bench-rig")
        .expect("publish");

    let (code, out, err) = ctl(&board, "node show");
    assert_eq!((code, err.as_str()), (0, ""), "{out}");
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines[0], "node        board");
    assert_eq!(lines[1], "cluster     lab");
    assert!(lines[2].starts_with("key         sha256:"), "{out}");
    assert_eq!(lines[3], "revision    1");
    assert_eq!(lines[4], "model       bench-rig");

    // The nodes the board knows are records.
    let (code, out, _) = ctl(&board, "node peers");
    assert_eq!((code, out.as_str()), (0, ""));
    let labels = [Label {
        key: "rack",
        value: "3",
    }];
    let peer = NodeRecord {
        name: "node-b".parse().expect("name"),
        address: "orion+tcp://10.0.0.2:9200",
        labels: List::new(&labels),
    };
    let mut value = [0_u8; 256];
    let len = encode_fields(&peer, &mut value).expect("encode");
    facts
        .records()
        .put(&[RecordEntry {
            class: RecordClass::Node,
            key: "node-b",
            stamp: HlcTimestamp::ZERO,
            deleted: false,
            value: Bytes(&value[..len]),
        }])
        .expect("put");
    let (code, out, _) = ctl(&board, "node peers");
    assert_eq!(
        (code, out.as_str()),
        (0, "node-b  orion+tcp://10.0.0.2:9200  rack=3\n")
    );

    // The board's clock is this computer's, give or take the trip: what is measured can be
    // off by up to the round trip, however long a loaded machine makes that.
    let (code, out, _) = ctl(&board, "node time");
    assert_eq!(code, 0);
    let ms = |after: &str| -> f64 {
        let at = out.find(after).expect(after) + after.len();
        out[at..]
            .split(' ')
            .next()
            .expect("a number")
            .parse()
            .expect("a number")
    };
    assert!(out.contains("this computer's (round trip "), "{out}");
    assert!(ms("the board's clock is ") <= ms("(round trip "), "{out}");

    // The same tool reaches a board across the network.
    let network = format!("--address orion+tcp://{} node show", board.network());
    let (code, out, err) = run(&network
        .split_whitespace()
        .map(str::to_owned)
        .collect::<Vec<_>>());
    assert_eq!((code, err.as_str()), (0, ""));
    assert!(out.starts_with("node        board\n"), "{out}");
}

#[test]
fn resource_list_and_show() {
    let board = Board::start("ctl-resource");
    assert_eq!(
        ctl(&board, "resource list"),
        (0, String::new(), String::new())
    );
    let _fan = fan(board.socket());
    assert_eq!(
        ctl(&board, "resource list").1,
        "fan.left  fan  Ready  on board\n"
    );
    assert_eq!(ctl(&board, "resource list camera.").1, "");
    let (code, out, _) = ctl(&board, "resource show fan.left");
    assert_eq!(code, 0);
    assert_eq!(
        out,
        "fan.left  fan  Ready  on board\n  mounted = left intake\n  topic   fan.left.speed\n  action  fan.left.set\n  action  fan.left.calibrate\n"
    );
    let (code, out, _) = ctl(&board, "resource show fan.right");
    assert_eq!(
        (code, out.as_str()),
        (1, "there is no resource fan.right\n")
    );
}

#[test]
fn topic_read_watch_and_publish() {
    let board = Board::start("ctl-topic");
    let _fan = fan(board.socket());

    // Read: the newest value of each field, named and typed by the declaration.
    let (code, out, _) = ctl(&board, "topic read fan.left.speed");
    assert_eq!(
        (code, out.as_str()),
        (0, "rpm = 1200 rpm\nspinning = true\n")
    );
    let (code, out, _) = ctl(&board, "topic read fan.right.speed");
    assert_eq!(
        (code, out.as_str()),
        (1, "nobody has declared fan.right.speed\n")
    );

    // Watch: samples as they are published, each with its time.
    let socket = board.socket().to_owned();
    let ticking = Beside::start(move |stop| {
        let orion = connect(&socket, "clock", Roles::CLIENT);
        let mut tick = orion
            .declare(&TopicDecl {
                name: "clock.tick",
                fields: &fields![count: Int],
                history: 64,
                expiry_ms: 0,
                max_payload: 8,
                on_subscribe: Default::default(),
            })
            .expect("declare");
        let mut count = 0_i64;
        while !stop.load(Ordering::Acquire) {
            // Only while someone is listening, as a publisher should.
            orion.pump(Duration::from_millis(10)).expect("pump");
            if tick.subscribers() > 0 {
                count += 1;
                tick.publish(
                    FieldId(0),
                    SampleTime::from_nanos(count as u64 * 10),
                    &count.to_le_bytes(),
                )
                .expect("publish");
            }
        }
    });
    let (code, out, _) = ctl(&board, "topic watch clock.tick --count 3 --for 5000");
    assert_eq!(code, 0, "{out}");
    // Three samples in a row, each with the time it belongs to. Which three depends on when
    // the watcher got its end of the ring: a reader starts at what is published after it.
    let seen: Vec<(u64, u64)> = out
        .lines()
        .map(|line| {
            let (time, rest) = line.split_once(" count = ").expect("a sample's line");
            (
                time.parse().expect("a time"),
                rest.parse().expect("a count"),
            )
        })
        .collect();
    assert_eq!(seen.len(), 3, "{out}");
    assert!(
        seen.iter().all(|(time, count)| *time == count * 10),
        "{out}"
    );
    assert!(
        seen.windows(2).all(|pair| pair[1].1 == pair[0].1 + 1),
        "{out}"
    );
    drop(ticking);
    let (code, out, _) = ctl(&board, "topic watch nobody.here --for 100");
    assert_eq!(
        (code, out.as_str()),
        (1, "nothing was published to nobody.here\n")
    );

    // Publish: a topic made on the spot from what was typed, held while a reader comes.
    let reader = connect(board.socket(), "reader", Roles::CLIENT);
    let mut note = reader.subscribe("bench.note").expect("subscribe");
    let socket = board.socket().display().to_string();
    let publishing = std::thread::spawn(move || {
        let line = format!(
            "--address {socket} topic publish bench.note text:text=hello level:int=7 ok:bool=true --hold 1500"
        );
        run(&line
            .split_whitespace()
            .map(str::to_owned)
            .collect::<Vec<_>>())
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while note.latest(FieldId(2)).is_none() {
        assert!(
            std::time::Instant::now() < deadline,
            "the published values never arrived"
        );
        let _ = note.wait(Duration::from_millis(50)).expect("wait");
        while !matches!(note.next(), orion::topics::Next::Empty) {}
    }
    assert_eq!(
        note.latest(FieldId(0))
            .map(|sample| sample.payload.to_vec()),
        Some(b"hello".to_vec())
    );
    assert_eq!(
        note.latest(FieldId(1))
            .map(|sample| sample.payload.to_vec()),
        Some(7_i64.to_le_bytes().to_vec())
    );
    assert_eq!(
        note.latest(FieldId(2))
            .map(|sample| sample.payload.to_vec()),
        Some(vec![1])
    );
    let (code, out, err) = publishing.join().expect("the tool ends");
    assert_eq!(
        (code, out.as_str(), err.as_str()),
        (0, "published 3 fields to bench.note\n", "")
    );
    // What cannot be a value is said before any board is asked.
    let (code, _, err) = ctl(&board, "topic publish bench.note level:int=high");
    assert_eq!(code, 2);
    assert!(
        err.starts_with("orion: \"high\" is not a whole number"),
        "{err}"
    );
}

#[test]
fn action_run_and_attach() {
    let board = Board::start("ctl-action");
    let _fan = fan(board.socket());

    let (code, out, err) = ctl(
        &board,
        "action run fan.left.set --resource fan.left --args 900",
    );
    assert_eq!(
        (code, out.as_str(), err.as_str()),
        (0, "50% spinning up\nSucceeded: now 900 rpm\n", "")
    );
    assert_eq!(
        ctl(&board, "topic read fan.left.speed").1,
        "rpm = 900 rpm\nspinning = true\n"
    );

    // An action that fails says why, and the tool says so with its exit code.
    let (code, out, _) = ctl(
        &board,
        "action run fan.left.set --resource fan.left --args fast",
    );
    assert_eq!((code, out.as_str()), (1, "Failed: that is not a speed\n"));
    // One that does not exist is refused by the board.
    let (code, out, err) = ctl(&board, "action run fan.left.fly");
    assert_eq!((code, out.as_str()), (1, ""));
    assert!(err.contains("refused"), "{err}");

    // Detached, it needs a deadline: it must end even if nobody comes back.
    let (code, _, err) = ctl(
        &board,
        "action run fan.left.calibrate --resource fan.left --detached",
    );
    assert_eq!(code, 2, "{err}");
    assert!(err.contains("--detached needs --deadline MS"), "{err}");

    // A long one, started detached and not waited for: the tool says how to come back.
    let (code, out, _) = ctl(
        &board,
        "action run fan.left.calibrate --resource fan.left --deadline 60000 --detached --for 100",
    );
    assert_eq!(code, 1);
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines[0], "call 1");
    assert_eq!(lines[1], "10% sweeping");
    assert_eq!(
        lines[2],
        "still running as call 1; `action attach 1` comes back to it"
    );
    // And coming back finds how it ended.
    let (code, out, _) = ctl(&board, "action attach 1 --for 5000");
    assert_eq!(code, 0, "{out}");
    assert_eq!(out.lines().last(), Some("Succeeded: limits found"));
}

#[test]
fn workload_put_and_remove() {
    let board = Board::start("ctl-workload");
    // Nobody runs this kind yet: it waits, and the tool says why and gives up on time.
    let (code, out, _) = ctl(
        &board,
        "workload put nightly --kind script --spec tidy --for 200",
    );
    assert_eq!(code, 1);
    assert_eq!(
        out,
        "nightly is Pending on board: no executor runs this kind\nnightly did not get there in time\n"
    );

    let _executor = executor(board.socket());
    let (code, out, err) = ctl(
        &board,
        "workload put nightly --kind script --spec tidy --restart never",
    );
    assert_eq!((code, err.as_str()), (0, ""), "{out}");
    // Where it is, as it changes; a state that is passed through quickly may not be seen.
    assert_eq!(out.lines().last(), Some("nightly is Running on board"));

    // One the executor cannot start is reported failed, with the executor's reason.
    let (code, out, _) = ctl(&board, "workload put bad --kind script --spec broken");
    assert_eq!(code, 1);
    assert_eq!(
        out.lines().last(),
        Some("bad is Failed on board: the script does not parse")
    );

    // Remove: it stops, and is gone when the executor says so.
    let (code, out, _) = ctl(&board, "workload remove nightly");
    assert_eq!(code, 0, "{out}");
    assert_eq!(out.lines().last(), Some("nightly is Gone on board"));
    let (code, _, err) = ctl(&board, "workload remove nightly");
    assert_eq!(code, 1);
    assert!(err.contains("did not get there") || err.is_empty());
    let (code, _, err) = ctl(&board, "workload put nightly");
    assert_eq!(code, 2);
    assert!(err.starts_with("orion: missing --kind"), "{err}");
}

#[test]
fn file_push_and_pull() {
    let board = Board::start("ctl-file");
    let path = board.dir().join("model.bin");
    let contents: Vec<u8> = (0..200_000_u32).map(|index| (index % 251) as u8).collect();
    std::fs::write(&path, &contents).expect("write");

    let (code, out, err) = ctl(&board, &format!("file push model {}", path.display()));
    assert_eq!(
        (code, out.as_str(), err.as_str()),
        (0, "model is in place, 200000 bytes\n", "")
    );
    assert_eq!(
        board.shelf().committed.lock().expect("lock").get("model"),
        Some(&contents)
    );
    let (code, _, err) = ctl(&board, "file push model /nonexistent/file");
    assert_eq!(code, 2);
    assert!(err.starts_with("orion: /nonexistent/file: "), "{err}");

    // A pull: the board fetches it itself, and checks it.
    let (sum, size) = orion::files::digest(&mut std::io::Cursor::new(&contents)).expect("digest");
    let hex: String = sum.iter().map(|byte| format!("{byte:02x}")).collect();
    board
        .shelf()
        .urls
        .lock()
        .expect("lock")
        .insert("https://updates.example/model".into(), contents.clone());
    let pull = format!("file pull copy https://updates.example/model --size {size} --sha256 {hex}");
    assert_eq!(
        ctl(&board, &pull),
        (
            0,
            "copy is in place, 200000 bytes\n".to_owned(),
            String::new()
        )
    );
    let wrong = format!(
        "file pull other https://updates.example/model --size {size} --sha256 {}",
        "00".repeat(32)
    );
    assert_eq!(ctl(&board, &wrong).0, 1);
    assert_eq!(
        ctl(&board, &wrong).1,
        "other was rejected: the digest does not match\n"
    );
    let (code, _, err) = ctl(
        &board,
        "file pull other https://updates.example/model --size 1 --sha256 abc",
    );
    assert_eq!(code, 2);
    assert!(err.starts_with("orion: --sha256 is 64 hex digits"), "{err}");
}

#[test]
fn what_is_not_understood_is_said_before_any_board_is_asked() {
    // No board at all: a mistake costs no connection.
    let nowhere = "--address /nonexistent/orion.sock";
    for (line, says) in [
        ("fan show", "there is no `fan show`"),
        ("node", "missing a verb"),
        ("", "missing a concept"),
        ("--timeout soon node show", "--timeout needs a number"),
        ("--address nowhere node show", "is not an address"),
    ] {
        let words: Vec<String> = format!("{nowhere} {line}")
            .split_whitespace()
            .map(str::to_owned)
            .collect();
        let words = if line.starts_with("--address") {
            line.split_whitespace().map(str::to_owned).collect()
        } else {
            words
        };
        let (code, out, err) = run(&words);
        assert_eq!((code, out.as_str()), (2, ""), "{line}");
        assert!(
            err.contains(says) && err.contains("usage: orion"),
            "{line}: {err}"
        );
    }
    let (code, out, err) = run(&["help".to_owned()]);
    assert_eq!((code, err.as_str()), (0, ""));
    assert_eq!(out.trim_end(), orion_ctl::USAGE);
    // Understood, and nothing answers there: not done, and why.
    let words: Vec<String> = format!("{nowhere} node show")
        .split_whitespace()
        .map(str::to_owned)
        .collect();
    let (code, out, err) = run(&words);
    assert_eq!((code, out.as_str()), (1, ""));
    assert!(err.starts_with("orion: "), "{err}");
    // Words left over are a mistake too.
    let board = Board::start("ctl-usage");
    let (code, _, err) = ctl(&board, "node show now");
    assert_eq!(code, 2);
    assert!(
        err.starts_with("orion: \"now\" is not understood here"),
        "{err}"
    );
}

#[test]
fn operator_ask_with_and_without_the_enrollment_key() {
    let dir = std::env::temp_dir().join(format!("orion-ctl-operator-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    let board = Board::start_with(
        &dir,
        orion_stubs::Options {
            security: orion_stubs::Security::On,
            enrollment_key: Some(b"correct horse".to_vec()),
            ..orion_stubs::Options::default()
        },
    );
    let key = dir.join("operator.key");
    let secret = dir.join("enrollment.key");
    std::fs::write(&secret, "correct horse\n").expect("write");
    let at = format!("orion+tcp://{}", board.network());
    let line = |rest: &str| -> Vec<String> {
        let mut words = vec![
            "--address".to_owned(),
            at.clone(),
            "--key".to_owned(),
            key.display().to_string(),
        ];
        words.extend(rest.split_whitespace().map(str::to_owned));
        words
    };

    // The key file is made on first use; without the enrollment key the answer is to wait.
    let (code, out, err) = run(&line("operator ask --as desk"));
    assert_eq!(code, 1, "{out}{err}");
    assert!(out.contains("operator    pending"), "{out}");
    assert!(out.contains("node        board"), "{out}");
    assert!(key.exists());
    // Proven: enrolled, and the same key from the same file.
    let with = format!(
        "operator ask --as desk --enrollment-key-file {}",
        secret.display()
    );
    let (code, out, err) = run(&line(&with));
    assert_eq!(code, 0, "{out}{err}");
    assert!(out.contains("operator    enrolled"), "{out}");

    // `ask` without a key, and a key that is not one, are not understood.
    let (code, _, err) = run(&[
        "operator".to_owned(),
        "ask".to_owned(),
        "--as".to_owned(),
        "x".to_owned(),
    ]);
    assert_eq!(code, 2, "{err}");
    assert!(err.contains("--key"), "{err}");
    let (code, _, err) = run(&line("operator enroll abc --as x"));
    assert_eq!(code, 2, "{err}");
    drop(board);
    let _ = std::fs::remove_dir_all(&dir);
}

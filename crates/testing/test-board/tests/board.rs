//! The test board is a board: the real comm core and node logic answer, and refuse, as on one.

use orion::node::Security as Said;
use orion::records::{Record, RecordClass};
use orion::types::Error;
use std::io::{Cursor, ErrorKind};
use std::time::{Duration, Instant};

use orion::files::{FileState, Step, Transfer};
use orion::{Address, ClientError, Identity, Orion, Reply};
use orion_test_board::{Options, Security, State, TestBoard};

/// What node logic answers: records are its to read.
fn records(orion: &Orion) -> Result<Vec<Record>, ClientError> {
    orion.records().get(RecordClass::Resource, "")
}

fn unavailable(result: Result<Vec<Record>, ClientError>) -> bool {
    matches!(
        result,
        Err(ClientError::Refused {
            error: Error::Unavailable,
            ..
        })
    )
}

#[test]
fn a_board_answers_as_a_board_and_node_logic_can_go_and_come_back() {
    let mut board = TestBoard::start("whole");
    assert_eq!(board.state(), &State::Running);
    assert!(board.node_runs());
    let orion = board.connect("tool").expect("connect");
    let node = orion.node().expect("hello");
    assert_eq!(node.name, "board");
    assert_eq!(node.state(), Some("running"));
    assert_eq!(node.security(), Said::Off { generation: None });
    // Node logic answers: records are its to read.
    assert!(records(&orion).expect("node logic answers").is_empty());

    // Without node logic, what is its is refused as unavailable, by the real relay.
    board.stop_node();
    assert!(!board.node_runs());
    let orion = board.connect("tool").expect("connect without node logic");
    assert!(unavailable(records(&orion)));
    board.start_node();
    let orion = board.connect("tool").expect("connect");
    assert!(records(&orion).is_ok());
    let dir = board.dir().to_owned();
    drop(board);
    assert!(!dir.exists(), "the board's directory is removed");
}

#[test]
fn with_its_record_on_the_board_checks_and_refuses_as_a_board_does() {
    let operator = Identity::generate();
    let board = TestBoard::start_with(
        "secured",
        Options {
            security: Security::On,
            network: true,
            operator: Some(operator.public()),
            ..Options::default()
        },
    );
    let orion = board.connect("tool").expect("someone on the board");
    assert_eq!(
        orion.node().expect("hello").security(),
        Said::On {
            generation: Some(1)
        }
    );
    assert!(records(&orion).is_ok());
    // A network session with no key it can prove is not let in with security on.
    let network = Address::Network {
        host: board.network().expect("network way in").to_string(),
        key: None,
    };
    let refused = Orion::connect(&network, "stranger")
        .and_then(|orion| records(&orion).map_err(orion::Error::from));
    assert!(refused.is_err(), "{refused:?}");
    // The operator it was seeded with is let in, over the same way.
    let trusted = Orion::connect_with(
        &network,
        "operator",
        orion::Options {
            identity: Some(operator),
            ..orion::Options::default()
        },
    )
    .expect("the seeded operator");
    assert!(records(&trusted).is_ok());

    // The same with security off lets it in.
    let open = TestBoard::start_with(
        "open",
        Options {
            security: Security::Off,
            network: true,
            ..Options::default()
        },
    );
    let network = Address::Network {
        host: open.network().expect("network way in").to_string(),
        key: None,
    };
    let orion = Orion::connect(&network, "stranger").expect("let in");
    assert!(records(&orion).is_ok());
    assert_eq!(
        orion.node().expect("hello").security(),
        Said::Off {
            generation: Some(1)
        }
    );

    // A damaged record is on, and the hello document says so.
    let damaged = TestBoard::start_with(
        "damaged",
        Options {
            security: Security::Damaged,
            ..Options::default()
        },
    );
    let orion = damaged.connect("tool").expect("someone on the board");
    assert_eq!(orion.node().expect("hello").security(), Said::Damaged);
}

#[test]
fn recovery_is_the_real_recovery_and_a_comm_core_that_cannot_start_goes_there() {
    let board = TestBoard::start_in_recovery("recovery", Options::default(), "a test asked");
    assert_eq!(board.state(), &State::Recovery("a test asked".to_owned()));
    assert!(!board.node_runs());
    assert!(board.recovery_ssh().is_some());
    let orion = board.connect("tool").expect("recovery serves the socket");
    let node = orion.node().expect("hello");
    assert_eq!(node.state(), Some("recovery"));
    assert_eq!(node.recovery_why(), Some("a test asked"));

    // Settings that parse but cannot work: a keys directory that cannot be made. A line of the
    // test's replaces the board's own.
    let broken = TestBoard::start_with(
        "broken",
        Options {
            conf: "keys.dir = /proc/orion-test-board/keys".to_owned(),
            ..Options::default()
        },
    );
    let State::Recovery(why) = broken.state() else {
        panic!("started: {broken:?}");
    };
    assert!(why.starts_with("keys.dir "), "{why}");
    let orion = broken.connect("tool").expect("recovery serves the socket");
    assert_eq!(orion.node().expect("hello").state(), Some("recovery"));
}

#[test]
#[should_panic(expected = "the board's settings do not parse")]
fn settings_that_do_not_parse_are_the_tests_fault_not_a_recovery() {
    let _board = TestBoard::start_with(
        "unparsed",
        Options {
            conf: "comm.max_sessions = many".to_owned(),
            ..Options::default()
        },
    );
}

/// How long the pushing clients wait for each answer: short, so twelve of them pass quickly.
const TIMEOUT: Duration = Duration::from_millis(400);

/// Pushes a small file to `destination` under `reply`. Until every byte is there it waits on a
/// generous bound, so a slow moment on a loaded machine is not what ends it: only the commit's
/// wait is under `reply`.
fn push_late(board: &TestBoard, destination: &str, reply: Reply) -> Result<Transfer, ClientError> {
    let options = orion::Options {
        timeout: TIMEOUT,
        ..orion::Options::default()
    };
    let orion =
        Orion::connect_with(&board.address(), "updater", options).expect("connect the pusher");
    orion.set_reply(Reply::Within(Duration::from_secs(20)));
    let data = vec![7_u8; 50_000];
    let files = orion.files();
    let mut pushing = files.start_push(destination, Cursor::new(&data), None)?;
    loop {
        match pushing.step(Duration::from_millis(50))? {
            Step::Done(transfer) => return Ok(transfer),
            // Every byte is there and the commit is sent: from here the commit's wait is the
            // one under test.
            Step::Progress { received, size } if received == size => orion.set_reply(reply),
            _ => {}
        }
    }
}

#[test]
fn a_commit_answered_after_twelve_timeouts_ends_committed_under_alive_and_timed_out_otherwise() {
    let board = TestBoard::start_with(
        "held",
        Options {
            heartbeat_ms: Some(500),
            ..Options::default()
        },
    );
    // Later than the commit's wait under the default: twelve timeouts.
    let hold = TIMEOUT * 12 + Duration::from_millis(1500);
    let held = board.hold_commits("slots/", hold);
    let began = Instant::now();
    let (timed, alive) = std::thread::scope(|scope| {
        let timed = scope.spawn(|| push_late(&board, "slots/a.img", Reply::Timeout));
        let alive = scope.spawn(|| {
            push_late(
                &board,
                "slots/b.img",
                Reply::Alive {
                    ceiling: Some(Duration::from_secs(30)),
                },
            )
        });
        (timed.join().expect("a push"), alive.join().expect("a push"))
    });
    match timed {
        Err(ClientError::Io(error)) => assert_eq!(error.kind(), ErrorKind::TimedOut, "{error}"),
        other => panic!("under Reply::Timeout the late commit times the push out: {other:?}"),
    }
    let alive = alive.expect("under Reply::Alive the push waits for the late commit");
    assert_eq!(alive.state, FileState::Committed, "{}", alive.detail);
    assert!(
        began.elapsed() >= hold,
        "the commit was answered late: {:?}",
        began.elapsed()
    );
    assert!(held.answered() >= 1);
}

/// Pushes `data` to `destination` on its own session, under the client's defaults, by
/// `deadline`.
fn push_by(
    board: &TestBoard,
    destination: &str,
    data: &[u8],
    deadline: Instant,
) -> Result<Transfer, String> {
    let orion = board.connect("pusher").map_err(|error| error.to_string())?;
    let files = orion.files();
    let mut pushing = files
        .start_push(destination, Cursor::new(data), None)
        .map_err(|error| error.to_string())?;
    let mut had = 0;
    loop {
        match pushing.step(Duration::from_millis(50)) {
            Ok(Step::Done(transfer)) => return Ok(transfer),
            Ok(Step::Progress { received, .. }) => had = received,
            Ok(_) => {}
            Err(error) => return Err(format!("{error} with {had} bytes there")),
        }
        if Instant::now() > deadline {
            return Err(format!("not done in time, with {had} bytes there"));
        }
    }
}

#[test]
fn pushes_through_node_logic_to_a_receiver_all_commit() {
    // A push keeps a window of chunks on their way, and the comm core passes each to node
    // logic, and node logic to the receiver, as a frame that must be sent. A window that
    // did not fit in a session's outbox dropped node logic, or the receiver, whenever either
    // was a moment slow to read, and the push then went nowhere: a quarter to a third of
    // these, before. Twenty, so that no more than one run in a few hundred passes by luck.
    let board = TestBoard::start("pushes");
    let held = board.hold_commits("slots/", Duration::from_millis(300));
    let deadline = Instant::now() + Duration::from_secs(60);
    let sizes = [100_000_usize, 1_000_000, 4_000_000];
    for (i, size) in sizes.iter().cycle().take(20).enumerate() {
        let data = vec![u8::try_from(i).expect("small"); *size];
        let transfer = push_by(&board, &format!("slots/{i}.img"), &data, deadline)
            .unwrap_or_else(|why| panic!("push {i} of {size} bytes: {why}"));
        assert_eq!(
            transfer.state,
            FileState::Committed,
            "push {i}: {}",
            transfer.detail
        );
    }
    assert_eq!(held.answered(), 20);
}

#[test]
fn an_action_left_unanswered_stays_open_while_the_board_answers() {
    use orion::actions::{ActionCall, Outcome, Target};

    let board = TestBoard::start("unanswered");
    let target = Target::Node("board".parse().expect("a name"));
    let held = board.hold_action("arm.call", target.clone());
    let caller = board.connect("caller").expect("connect");
    let mut run = caller
        .run(&ActionCall {
            name: "arm.call",
            target,
            args: b"",
            deadline_ms: 0,
            detached: false,
        })
        .expect("run");
    // Unanswered for a while, and the board answers the caller all along.
    let quiet = Instant::now() + Duration::from_secs(2);
    while Instant::now() < quiet {
        match run
            .wait(Duration::from_millis(100))
            .expect("the run is open")
        {
            None => {}
            Some(update) => panic!("an unanswered run was told {update:?}"),
        }
        assert_eq!(caller.node().expect("the board answers").name, "board");
    }
    assert_eq!(held.asked(), 1);
    // Its program gone, node logic ends the run: nothing is left waiting forever.
    drop(held);
    let finished = run
        .finish(Duration::from_secs(10))
        .expect("ended once its program is gone");
    assert_eq!(finished.outcome, Outcome::Failed);
    assert_eq!(finished.detail, "the program that runs it went away");
}

/// A board whose sessions each have half the default outbox: less than one push's window, so
/// every push is turned away as busy along the way, by the comm core on the way to node logic
/// and by node logic on the way to a receiver.
fn small_outbox(name: &str) -> TestBoard {
    TestBoard::start_with(
        name,
        Options {
            conf: "comm.outbox_bytes = 131072".to_owned(),
            ..Options::default()
        },
    )
}

/// `pushers` pushes of 4 MB at once, under `prefix`, each by the same deadline.
fn at_once(board: &TestBoard, prefix: &str, pushers: usize) {
    let deadline = Instant::now() + Duration::from_secs(60);
    std::thread::scope(|scope| {
        let pushes: Vec<_> = (0..pushers)
            .map(|i| {
                scope.spawn(move || {
                    let data = vec![u8::try_from(i).expect("small"); 4_000_000];
                    push_by(
                        board,
                        &format!("{prefix}{pushers}-{i}.img"),
                        &data,
                        deadline,
                    )
                })
            })
            .collect();
        for (i, push) in pushes.into_iter().enumerate() {
            let transfer = push
                .join()
                .expect("a push")
                .unwrap_or_else(|why| panic!("push {i} of {pushers} to {prefix}: {why}"));
            assert_eq!(transfer.state, FileState::Committed, "{}", transfer.detail);
        }
    });
}

#[test]
fn pushes_at_once_past_a_full_outbox_are_paced_not_lost() {
    // To the node's own store: the comm core turns chunks away while node logic's outbox is
    // full, and node logic is never dropped for it.
    let board = small_outbox("paced");
    at_once(&board, "files/", 2);
    at_once(&board, "files/", 4);
    // To a receiver: node logic turns chunks away while what is on its way to the receiver
    // would not fit in its outbox.
    let held = board.hold_commits("slots/", Duration::from_millis(100));
    at_once(&board, "slots/", 2);
    at_once(&board, "slots/", 4);
    assert_eq!(held.answered(), 6);
    assert!(board.node_runs());
}

/// A resource record's value for `key` on `node`, carrying `attributes`.
fn resource_value(key: &str, node: &str, attributes: &[u8]) -> Vec<u8> {
    let record = orion_msg_records::ResourceRecord {
        name: key.parse().expect("a name"),
        kind: "test",
        node: node.parse().expect("a name"),
        state: orion_msg_records::ResourceState::Ready,
        attributes: orion_wire::Bytes(attributes),
    };
    let mut value = vec![0_u8; attributes.len() + 512];
    let len = orion_wire::body::encode_fields(&record, &mut value).expect("encode");
    value.truncate(len);
    value
}

fn resource<'a>(key: &'a str, value: &'a [u8]) -> orion::records::RecordEntry<'a> {
    orion::records::RecordEntry {
        class: RecordClass::Resource,
        key,
        stamp: orion::types::HlcTimestamp::ZERO,
        deleted: false,
        value: orion::records::Bytes(value),
    }
}

#[test]
fn a_put_while_node_logic_is_behind_is_busy_and_node_logic_stays() {
    use std::sync::atomic::Ordering;

    let board = TestBoard::start_with(
        "slowpoke",
        Options {
            name: "slowpoke".to_owned(),
            conf: "comm.outbox_bytes = 70000".to_owned(),
            ..Options::default()
        },
    );
    let writer = board.connect("writer").expect("connect");
    writer
        .records()
        .put(&[resource(
            "before",
            &resource_value("before", "slowpoke", b""),
        )])
        .expect("node logic takes a put");
    // Node logic slow to read, and a program that sends puts without waiting for their answers
    // until what waits for node logic fills its outbox.
    let delay = orion_node_app::slow::delay("slowpoke");
    delay.store(300, Ordering::Relaxed);
    let filler = board.connect("filler").expect("connect");
    // Small fills, so what is left of the outbox is less than the late put needs.
    let fills: Vec<(String, Vec<u8>)> = (0..800)
        .map(|i| {
            let key = format!("fill-{i}");
            let value = resource_value(&key, "slowpoke", &[7_u8; 1_000]);
            (key, value)
        })
        .collect();
    for (i, (key, value)) in fills.iter().enumerate() {
        filler
            .client()
            .send(&orion_msg_records::Put {
                records: orion_wire::List::new(&[resource(key, value)]),
            })
            .expect("send");
        if i % 16 == 0 {
            // The filler reads what it is told, its refusals among it: it is not the one
            // that is behind.
            filler.client().drain().expect("the filler's session");
        }
    }
    filler.client().drain().expect("the filler's session");
    let busy = writer.records().put(&[resource(
        "late",
        &resource_value("late", "slowpoke", &[1_u8; 16_000]),
    )]);
    // The filler goes before node logic catches up: it would not read the answers.
    drop(filler);
    delay.store(0, Ordering::Relaxed);
    match busy {
        Err(error) => assert!(error.is_busy(), "turned away as busy, not {error:?}"),
        Ok(_) => panic!("a put while node logic's outbox is full is turned away"),
    }
    // Node logic was not dropped for it: it answers at once, and what it was sent is there.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let got = writer
            .records()
            .get(RecordClass::Resource, "fill-0")
            .expect("node logic answers");
        if !got.is_empty() {
            break;
        }
        assert!(Instant::now() < deadline, "fill-0 never written");
        std::thread::sleep(Duration::from_millis(50));
    }
    let late = writer
        .records()
        .get(RecordClass::Resource, "late")
        .expect("node logic answers");
    assert!(late.is_empty(), "a put turned away is not written");
    assert!(board.node_runs());
}

#[test]
fn a_workload_refused_is_found_by_wait() {
    use orion::workloads::{RestartPolicy, Workload};

    let mut board = TestBoard::start("refused-workload");
    board.stop_node();
    let operator = board.connect("operator").expect("connect");
    let workload = Workload {
        name: "sleeper",
        kind: "process",
        spec: b"sleep 1",
        node: None,
        restart: RestartPolicy::Never,
    };
    operator.workloads().put(&workload).expect("said");
    match operator
        .workloads()
        .wait("sleeper", Duration::from_secs(5), |_| true)
    {
        Err(ClientError::Refused {
            error: Error::Unavailable,
            ..
        }) => {}
        other => panic!("node logic away refuses the put, and wait says so: {other:?}"),
    }
}

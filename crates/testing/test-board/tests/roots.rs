//! The image's roots, `roots/<name>/`: fetched, pushed, listed and changed through the real
//! comm core and node logic, and nothing outside a root reached.

use std::io::Cursor;
use std::path::Path;
use std::time::{Duration, Instant};

use orion::actions::{ActionCall, Outcome, Target};
use orion::files::{FileState, Step, Transfer, index::Index};
use orion::{Address, Identity, Orion};
use orion_scratch::Scratch;
use orion_test_board::{Granted, Options, Security, TestBoard};

/// A board named `name` with a root `app` (rw) and a root `logs` (ro), and a directory
/// outside both that holds a secret.
struct Rooted {
    board: TestBoard,
    app: Scratch,
    logs: Scratch,
    outside: Scratch,
}

fn rooted(name: &str, options: Options) -> Rooted {
    let (app, logs, outside) = (
        Scratch::new(&format!("{name}-app")),
        Scratch::new(&format!("{name}-logs")),
        Scratch::new(&format!("{name}-out")),
    );
    std::fs::write(outside.join("secret"), b"not for clients").expect("secret");
    std::fs::write(logs.join("boot.log"), b"booted\n").expect("log");
    std::fs::create_dir(logs.join("old")).expect("old");
    std::fs::write(logs.join("old/1.log"), b"one\n").expect("log");
    let conf = format!(
        "files.root.app = {} rw\nfiles.root.logs = {} ro\n{}",
        app.display(),
        logs.display(),
        options.conf
    );
    let board = TestBoard::start_with(
        name,
        Options {
            name: name.to_owned(),
            conf,
            ..options
        },
    );
    Rooted {
        board,
        app,
        logs,
        outside,
    }
}

fn run(orion: &Orion, node: &str, action: &str, args: &str) -> (Outcome, String, String) {
    let finished = orion
        .run(&ActionCall {
            name: action,
            target: Target::Node(node.parse().expect("a node")),
            args: args.as_bytes(),
            deadline_ms: 0,
            detached: false,
        })
        .expect("run")
        .finish(Duration::from_secs(20))
        .expect("finished");
    (
        finished.outcome,
        String::from_utf8(finished.output).expect("text"),
        finished.detail,
    )
}

fn push(orion: &Orion, destination: &str, data: &[u8]) -> Transfer {
    orion
        .files()
        .push(destination, &mut Cursor::new(data.to_vec()), |_, _| {})
        .unwrap_or_else(|error| panic!("push {destination}: {error}"))
}

fn fetch(orion: &Orion, name: &str) -> (Transfer, Vec<u8>) {
    let mut into = Cursor::new(Vec::new());
    let transfer = orion
        .files()
        .fetch(name, &mut into, 0, |_, _| {})
        .unwrap_or_else(|error| panic!("fetch {name}: {error}"));
    (transfer, into.into_inner())
}

/// How a fetch that is not given ends: its words.
fn not_given(orion: &Orion, name: &str) -> String {
    let mut into = Cursor::new(Vec::new());
    match orion.files().fetch(name, &mut into, 0, |_, _| {}) {
        Ok(transfer) => {
            assert_ne!(transfer.state, FileState::Committed, "{name} was given");
            transfer.detail
        }
        Err(error) => error.to_string(),
    }
}

fn parts(root: &Path) -> usize {
    std::fs::read_dir(root.join(".orion/incoming"))
        .map(|dir| {
            dir.filter(|entry| {
                entry
                    .as_ref()
                    .is_ok_and(|entry| entry.file_name().to_string_lossy().ends_with(".part"))
            })
            .count()
        })
        .unwrap_or(0)
}

#[test]
fn a_root_is_fetched_pushed_listed_and_changed_and_nothing_outside_it_is_reached() {
    let rooted = rooted("rooted", Options::default());
    let orion = rooted.board.connect("tool").expect("connect");
    // Links out of the roots, each a way a client might try.
    std::os::unix::fs::symlink(rooted.outside.join("secret"), rooted.logs.join("abs"))
        .expect("link");
    std::os::unix::fs::symlink("../../", rooted.logs.join("up")).expect("link");
    std::os::unix::fs::symlink(&*rooted.outside, rooted.app.join("out")).expect("link");

    // Fetched, as it is.
    let (given, bytes) = fetch(&orion, "roots/logs/old/1.log");
    assert_eq!(given.state, FileState::Committed, "{}", given.detail);
    assert_eq!(bytes, b"one\n");
    // Nothing outside a root, by any way there.
    for name in [
        "roots/logs/abs",
        "roots/logs/up/secret",
        "roots/app/out/secret",
    ] {
        let words = not_given(&orion, name);
        assert!(words.contains("outside the root"), "{name}: {words}");
    }
    assert!(not_given(&orion, "roots/nope/x").contains("no such root: nope"));
    assert!(not_given(&orion, "roots/logs/old").contains("not a file"));
    assert!(not_given(&orion, "roots/app/.orion/placed").contains(".orion is the root's own"));

    // Pushed into place, in directories made for it, and nothing left behind.
    let put = push(&orion, "roots/app/a/b.txt", b"pushed");
    assert_eq!(put.state, FileState::Committed, "{}", put.detail);
    assert_eq!(
        std::fs::read(rooted.app.join("a/b.txt")).expect("there"),
        b"pushed"
    );
    assert_eq!(parts(&rooted.app), 0);
    assert_eq!(fetch(&orion, "roots/app/a/b.txt").1, b"pushed");
    // Not into a read-only root, not into the roots' own, not out through a link.
    let refused = push(&orion, "roots/logs/new.log", b"x");
    assert_eq!(refused.state, FileState::Rejected);
    assert_eq!(refused.detail, "the root is read-only");
    assert!(!rooted.logs.join("new.log").exists());
    let refused = push(&orion, "roots/app/.orion/placed", b"x");
    assert_eq!(refused.detail, ".orion is the root's own");
    let refused = push(&orion, "roots/app/out/planted", b"x");
    assert_eq!(refused.state, FileState::Rejected);
    assert_eq!(refused.detail, "the destination is outside the store");
    assert!(!rooted.outside.join("planted").exists());

    // Listed: the roots, then a root without its own directory.
    let node = "rooted";
    assert_eq!(
        run(&orion, node, "files.list", ""),
        (
            Outcome::Succeeded,
            "root rw app\nroot ro logs\n".to_owned(),
            String::new()
        )
    );
    let (outcome, listed, _) = run(&orion, node, "files.list", "roots/app");
    assert_eq!(outcome, Outcome::Succeeded);
    let names: Vec<&str> = listed
        .lines()
        .map(|line| line.rsplit(' ').next().unwrap_or(""))
        .collect();
    assert_eq!(names, ["a", "out"], "{listed}");
    assert!(listed.starts_with("dir 0 "), "{listed}");
    assert!(listed.contains("\nlink 0 "), "{listed}");
    let (_, listed, _) = run(&orion, node, "files.list", "roots/logs/old");
    assert!(listed.starts_with("file 4 "), "{listed}");

    // Changed, within a root that takes changes.
    assert_eq!(
        run(&orion, node, "files.mkdir", "roots/app/d").0,
        Outcome::Succeeded
    );
    assert_eq!(
        run(
            &orion,
            node,
            "files.rename",
            "roots/app/a/b.txt roots/app/d/c.txt"
        )
        .0,
        Outcome::Succeeded
    );
    assert_eq!(
        std::fs::read(rooted.app.join("d/c.txt")).expect("moved"),
        b"pushed"
    );
    push(&orion, "roots/app/a/b.txt", b"again");
    let (outcome, _, detail) = run(
        &orion,
        node,
        "files.rename",
        "roots/app/a/b.txt roots/app/d/c.txt",
    );
    assert_eq!(
        (outcome, detail.as_str()),
        (Outcome::Failed, "there already")
    );
    assert_eq!(
        run(
            &orion,
            node,
            "files.rename",
            "roots/app/a/b.txt roots/app/d/c.txt replace"
        )
        .0,
        Outcome::Succeeded
    );
    assert_eq!(
        std::fs::read(rooted.app.join("d/c.txt")).expect("replaced"),
        b"again"
    );
    let (outcome, _, detail) = run(&orion, node, "files.remove", "roots/app/d");
    assert_eq!(
        (outcome, detail.as_str()),
        (Outcome::Failed, "the directory is not empty")
    );
    assert_eq!(
        run(&orion, node, "files.remove", "roots/app/d/c.txt").0,
        Outcome::Succeeded
    );
    assert_eq!(
        run(&orion, node, "files.remove", "roots/app/d").0,
        Outcome::Succeeded
    );
    assert!(!rooted.app.join("d").exists());
    // Removing a link removes the link, not what it leads to.
    assert_eq!(
        run(&orion, node, "files.remove", "roots/app/out").0,
        Outcome::Succeeded
    );
    assert!(rooted.outside.join("secret").exists());
    for (action, args) in [
        ("files.remove", "roots/logs/boot.log"),
        ("files.mkdir", "roots/logs/new"),
        ("files.rename", "roots/logs/boot.log roots/logs/b.log"),
    ] {
        let (outcome, _, detail) = run(&orion, node, action, args);
        assert_eq!(
            (outcome, detail.as_str()),
            (Outcome::Rejected, "the root is read-only")
        );
    }
    assert!(rooted.logs.join("boot.log").exists());
    let (outcome, _, detail) = run(&orion, node, "files.rename", "roots/app/a roots/logs/a");
    assert_eq!(
        (outcome, detail.as_str()),
        (Outcome::Rejected, "the root is read-only")
    );
    let (outcome, _, detail) = run(&orion, node, "files.remove", "roots/app/../logs/boot.log");
    assert_eq!(outcome, Outcome::Rejected, "{detail}");

    // The index of a root: each file, its size as its bound, links and its own left out.
    let (given, bytes) = fetch(&orion, "roots/logs/index");
    assert_eq!(given.state, FileState::Committed, "{}", given.detail);
    let index = Index::parse(std::str::from_utf8(&bytes).expect("text")).expect("an index");
    let entries: Vec<(&str, u64)> = index
        .entries
        .iter()
        .map(|entry| (entry.name.as_str(), entry.max_bytes))
        .collect();
    assert_eq!(
        entries,
        [("roots/logs/boot.log", 7), ("roots/logs/old/1.log", 4)]
    );
    assert_eq!(index.total, 11);
    assert!(index.entries.iter().all(|entry| entry.user_data));
    let (_, bytes) = fetch(&orion, "roots/app/index");
    let index = Index::parse(std::str::from_utf8(&bytes).expect("text")).expect("an index");
    // What is left in it is a directory with nothing in it.
    assert!(index.entries.is_empty(), "{index:?}");
    assert!(rooted.board.node_runs());
}

#[test]
fn a_push_broken_off_never_replaces_the_file_and_goes_on_after_node_logic_restarts() {
    let mut rooted = rooted("broken", Options::default());
    std::fs::write(rooted.app.join("image"), b"old").expect("old");
    let data: Vec<u8> = (0..4_000_000_u32).map(|i| (i % 251) as u8).collect();
    let had = {
        let orion = rooted.board.connect("first").expect("connect");
        let files = orion.files();
        let mut pushing = files
            .start_push("roots/app/image", Cursor::new(data.clone()), None)
            .expect("push");
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Step::Progress { received, .. } =
                pushing.step(Duration::from_millis(20)).expect("step")
                && received >= 1_000_000
            {
                break received;
            }
            assert!(Instant::now() < deadline, "no progress");
        }
        // The session ends here, part way.
    };
    assert_eq!(
        std::fs::read(rooted.app.join("image")).expect("there"),
        b"old"
    );
    assert_eq!(parts(&rooted.app), 1);
    rooted.board.stop_node();
    rooted.board.start_node();
    assert_eq!(
        std::fs::read(rooted.app.join("image")).expect("there"),
        b"old"
    );
    let orion = rooted.board.connect("second").expect("connect");
    let files = orion.files();
    let mut pushing = files
        .start_push("roots/app/image", Cursor::new(data.clone()), None)
        .expect("push");
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut first = None;
    let done = loop {
        match pushing.step(Duration::from_millis(20)).expect("step") {
            Step::Done(transfer) => break transfer,
            Step::Progress { received, .. } => {
                first.get_or_insert(received);
            }
            _ => {}
        }
        assert!(Instant::now() < deadline, "not done");
    };
    assert_eq!(done.state, FileState::Committed, "{}", done.detail);
    let first = first.expect("progress");
    assert!(first >= had, "went on from {first}, after {had} were there");
    assert_eq!(
        std::fs::read(rooted.app.join("image")).expect("there"),
        data
    );
    assert_eq!(parts(&rooted.app), 0);
}

/// `fetchers` fetches of `name` at once, each by `deadline`, each whole.
fn fetches_at_once(board: &TestBoard, name: &str, want: &[u8], fetchers: usize) {
    let deadline = Instant::now() + Duration::from_secs(60);
    std::thread::scope(|scope| {
        let fetches: Vec<_> = (0..fetchers)
            .map(|i| {
                scope.spawn(move || {
                    let orion = board
                        .connect(&format!("fetcher-{i}"))
                        .map_err(|error| error.to_string())?;
                    let files = orion.files();
                    let mut fetching = files
                        .start_fetch(name, Cursor::new(Vec::new()), 0)
                        .map_err(|error| error.to_string())?;
                    loop {
                        match fetching.step(Duration::from_millis(50)) {
                            Ok(Step::Done(transfer)) => return Ok(transfer),
                            Ok(_) => {}
                            Err(error) => return Err(error.to_string()),
                        }
                        if Instant::now() > deadline {
                            return Err("not done in time".to_owned());
                        }
                    }
                })
            })
            .collect();
        for (i, fetch) in fetches.into_iter().enumerate() {
            let transfer: Result<Transfer, String> = fetch.join().expect("a fetch");
            let transfer = transfer.unwrap_or_else(|why| panic!("fetch {i} of {fetchers}: {why}"));
            assert_eq!(transfer.state, FileState::Committed, "{}", transfer.detail);
            assert_eq!(transfer.received, want.len() as u64);
        }
    });
}

#[test]
fn fetches_at_once_from_a_slow_node_logic_past_a_full_outbox_are_paced_not_lost() {
    use std::sync::atomic::Ordering;

    let rooted = rooted(
        "slowgive",
        Options {
            conf: "comm.outbox_bytes = 131072\n".to_owned(),
            ..Options::default()
        },
    );
    let data: Vec<u8> = (0..4_000_000_u32).map(|i| (i % 253) as u8).collect();
    std::fs::write(rooted.app.join("big"), &data).expect("big");
    fetches_at_once(&rooted.board, "roots/app/big", &data, 2);
    // Node logic slow to read every frame: what the clients say waits for it, and the files
    // wait with it.
    let delay = orion_node_app::slow::delay("slowgive");
    delay.store(2, Ordering::Relaxed);
    fetches_at_once(&rooted.board, "roots/app/big", &data, 4);
    delay.store(0, Ordering::Relaxed);
    assert!(rooted.board.node_runs());
}

#[test]
fn with_security_on_the_grants_decide_and_a_read_only_root_refuses_everyone() {
    let reader = Identity::generate();
    let lister = Identity::generate();
    let rooted = rooted(
        "granted",
        Options {
            security: Security::On,
            network: true,
            granted: vec![
                Granted {
                    key: reader.public(),
                    read: true,
                    actions: Vec::new(),
                },
                Granted {
                    key: lister.public(),
                    read: true,
                    actions: vec!["files.*".to_owned()],
                },
            ],
            ..Options::default()
        },
    );
    let network = Address::Network {
        host: rooted.board.network().expect("network way in").to_string(),
        key: None,
    };
    let connect = |identity: Identity, name: &str| {
        Orion::connect_with(
            &network,
            name,
            orion::Options {
                identity: Some(identity),
                ..orion::Options::default()
            },
        )
        .expect("an operator")
    };
    let reader = connect(reader, "reader");
    // Granted reading: a fetch.
    assert_eq!(fetch(&reader, "roots/logs/boot.log").1, b"booted\n");
    // Not granted a push, nor an action.
    let pushed = reader
        .files()
        .push("roots/app/x", &mut Cursor::new(b"x".to_vec()), |_, _| {});
    assert!(pushed.is_err() || pushed.is_ok_and(|done| done.state != FileState::Committed));
    assert!(!rooted.app.join("x").exists());
    let listed = reader.run(&ActionCall {
        name: "files.list",
        target: Target::Node("granted".parse().expect("a node")),
        args: b"",
        deadline_ms: 0,
        detached: false,
    });
    let listed = listed.and_then(|mut run| Ok(run.finish(Duration::from_secs(10))?));
    assert!(
        listed
            .as_ref()
            .map_or(true, |done| done.outcome != Outcome::Succeeded),
        "{listed:?}"
    );
    // Granted the actions: it lists and changes, and still only where a root takes changes.
    let lister = connect(lister, "lister");
    assert_eq!(
        run(&lister, "granted", "files.list", "").0,
        Outcome::Succeeded
    );
    assert_eq!(
        run(&lister, "granted", "files.mkdir", "roots/app/d").0,
        Outcome::Succeeded
    );
    assert!(rooted.app.join("d").is_dir());
    let (outcome, _, detail) = run(&lister, "granted", "files.remove", "roots/logs/boot.log");
    assert_eq!(
        (outcome, detail.as_str()),
        (Outcome::Rejected, "the root is read-only")
    );
    // On the board, with security on, the same.
    let local = rooted.board.connect("local").expect("connect");
    let refused = push(&local, "roots/logs/x", b"x");
    assert_eq!(refused.detail, "the root is read-only");
}

#[test]
fn a_fetch_goes_on_from_what_the_client_has() {
    let rooted = rooted("resumed", Options::default());
    let data: Vec<u8> = (0..300_000_u32).map(|i| (i % 241) as u8).collect();
    std::fs::write(rooted.app.join("half"), &data).expect("file");
    let orion = rooted.board.connect("tool").expect("connect");
    let mut into = Cursor::new(data[..100_000].to_vec());
    let transfer = orion
        .files()
        .fetch("roots/app/half", &mut into, 100_000, |_, _| {})
        .expect("fetch");
    assert_eq!(transfer.state, FileState::Committed, "{}", transfer.detail);
    assert_eq!(into.into_inner(), data);
}

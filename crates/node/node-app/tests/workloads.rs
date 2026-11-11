//! Workloads through node logic with the sample executor, which runs real commands: each
//! side is restarted in turn. Orion restarting leaves runs alone and takes them back; an
//! executor restarting with nothing has its workloads started again.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use orion::records::RecordClass;
use orion::workloads::{RestartPolicy, Workload, WorkloadState};
use orion::{Address, Orion};
use orion_node_app::{Node, NodeSettings};
use orion_sample_executor::{Sample, run};
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
    settings.save_after = Duration::from_millis(10);
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

/// The sample executor in a thread of its own. Dropping it ends it and every command it ran:
/// what a restart of the executor comes to.
struct Executor {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Executor {
    fn start(socket: &Path) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let (socket, stopping) = (socket.to_owned(), stop.clone());
        let thread = std::thread::spawn(move || {
            let mut sample = Sample::new(vec!["process".parse().expect("kind")]);
            run(&socket, &mut sample, || !stopping.load(Ordering::Relaxed)).expect("run");
        });
        Self {
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for Executor {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// A command that adds its process id to `file` each time it starts, and then sleeps.
fn sleeper(file: &Path) -> String {
    format!("echo $$ >> {}; exec sleep 120", file.display())
}

/// The process ids `file` has: one for each time the command was started.
fn starts(file: &Path) -> Vec<u32> {
    std::fs::read_to_string(file)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| line.trim().parse().ok())
        .collect()
}

fn alive(pid: u32) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|stat| {
        stat.rsplit(')')
            .next()
            .is_some_and(|rest| !rest.trim_start().starts_with('Z'))
    })
}

fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !done() {
        assert!(Instant::now() < deadline, "{what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn workload<'a>(name: &'a str, spec: &'a str, restart: RestartPolicy) -> Workload<'a> {
    Workload {
        name,
        kind: "process",
        spec: spec.as_bytes(),
        node: None,
        restart,
    }
}

/// Puts a workload, again and again while node logic is away, and waits until it is where
/// the caller wants it.
fn reaches(orion: &Orion, workload: &Workload<'_>, state: WorkloadState) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        // Putting it again as it is changes nothing and asks where it is.
        orion.workloads().put(workload).expect("put");
        let reached = orion
            .workloads()
            .wait(workload.name, Duration::from_millis(200), |tracked| {
                tracked.state == state
            })
            .expect("connected");
        if reached.is_some() {
            return;
        }
        let now = orion.workloads().status(workload.name).expect("connected");
        assert!(
            Instant::now() < deadline,
            "{} never was {state:?}: {now:?}",
            workload.name
        );
    }
}

#[test]
fn each_side_restarts_in_turn_and_the_workload_is_where_it_should_be() {
    let dir = dir("workloads");
    let board = Board::start_for_node(&dir);
    let address = Address::Local(board.socket().to_owned());
    let logic = Logic::start(settings(&dir));
    let operator = Orion::connect(&address, "operator").expect("connect");
    let file = dir.join("pids");
    let spec = sleeper(&file);
    let sleeper = workload("sleeper", &spec, RestartPolicy::Always);

    // No executor offers the kind: it is pending.
    reaches(&operator, &sleeper, WorkloadState::Pending);
    assert!(starts(&file).is_empty());

    // One comes: it is started, once.
    let executor = Executor::start(board.socket());
    reaches(&operator, &sleeper, WorkloadState::Running);
    // Running is said as soon as the command is spawned, a moment before it writes its id.
    until("the command has started", || !starts(&file).is_empty());
    let first = starts(&file);
    assert_eq!(first.len(), 1);
    assert!(alive(first[0]));
    // What should run is a record.
    let kept = operator
        .records()
        .get(RecordClass::Workload, "")
        .expect("records");
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].key, "sleeper");

    // Orion restarts. The run carries on, and Orion takes it back by name: the same
    // process, not started a second time.
    std::thread::sleep(Duration::from_millis(200));
    drop(logic);
    assert!(alive(first[0]), "the run does not depend on node logic");
    let logic = Logic::start(settings(&dir));
    reaches(&operator, &sleeper, WorkloadState::Running);
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(starts(&file), first, "it was adopted, not started again");
    assert!(alive(first[0]));

    // The executor goes away: nobody knows where the run is.
    drop(executor);
    assert!(
        !alive(first[0]),
        "the sample executor takes its commands with it"
    );
    reaches(&operator, &sleeper, WorkloadState::Lost);

    // It comes back with nothing: the workload is started again.
    let executor = Executor::start(board.socket());
    reaches(&operator, &sleeper, WorkloadState::Running);
    until("the command has started again", || starts(&file).len() >= 2);
    std::thread::sleep(Duration::from_millis(300));
    let second = starts(&file);
    assert_eq!(second.len(), 2, "started again, once");
    assert!(alive(second[1]));

    // Removed: it stops, the record stays until the executor says it is gone, then goes.
    operator
        .workloads()
        .remove("sleeper", false)
        .expect("remove");
    let gone = operator
        .workloads()
        .wait("sleeper", Duration::from_secs(10), |tracked| {
            tracked.state == WorkloadState::Gone
        })
        .expect("connected");
    assert!(gone.is_some());
    assert!(!alive(second[1]));
    until("the record is gone", || {
        operator
            .records()
            .get(RecordClass::Workload, "")
            .expect("records")
            .is_empty()
    });
    // And after another restart of Orion it does not come back.
    std::thread::sleep(Duration::from_millis(200));
    drop(logic);
    let _logic = Logic::start(settings(&dir));
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(starts(&file).len(), 2);
    drop(executor);
}

#[test]
fn a_workload_that_fails_is_started_again_and_one_that_must_not_is_left() {
    let dir = dir("workloads-restart");
    let board = Board::start_for_node(&dir);
    let address = Address::Local(board.socket().to_owned());
    let _logic = Logic::start(settings(&dir));
    let _executor = Executor::start(board.socket());
    let operator = Orion::connect(&address, "operator").expect("connect");

    // It exits with an error every time: started again after a second, and again.
    let file = dir.join("crashes");
    let spec = format!("echo $$ >> {}; exit 3", file.display());
    let crasher = workload("crasher", &spec, RestartPolicy::OnFailure);
    reaches(&operator, &crasher, WorkloadState::Failed);
    until("it is started again", || starts(&file).len() >= 2);

    // The same with "never": it fails once and is left.
    let once = dir.join("once");
    let spec = format!("echo $$ >> {}; exit 3", once.display());
    let single = workload("single", &spec, RestartPolicy::Never);
    reaches(&operator, &single, WorkloadState::Failed);
    std::thread::sleep(Duration::from_millis(1500));
    assert_eq!(starts(&once).len(), 1);

    // One that ends well under "never" is gone, and its record stays until it is removed.
    let quick = workload("quick", "true", RestartPolicy::Never);
    reaches(&operator, &quick, WorkloadState::Gone);
    operator.workloads().remove("quick", false).expect("remove");
    operator
        .workloads()
        .remove("crasher", true)
        .expect("remove");
    operator
        .workloads()
        .remove("single", false)
        .expect("remove");
    until("the records are gone", || {
        operator
            .records()
            .get(RecordClass::Workload, "")
            .expect("records")
            .is_empty()
    });
}

/// An executor whose workload will not die: it is asked to stop, told to end, and goes on.
struct Undying {
    kinds: Vec<orion::executor::WorkloadKind>,
    running: Vec<orion::executor::WorkloadName>,
    stops: Arc<std::sync::atomic::AtomicU32>,
    kills: Arc<std::sync::atomic::AtomicU32>,
}

impl orion::executor::Executor for Undying {
    fn kinds(&self) -> &[orion::executor::WorkloadKind] {
        &self.kinds
    }

    fn start(
        &mut self,
        name: &orion::executor::WorkloadName,
        _spec: &[u8],
    ) -> Result<(), orion::executor::ExecError> {
        self.running.push(name.clone());
        Ok(())
    }

    fn stop(&mut self, _name: &orion::executor::WorkloadName) {
        self.stops.fetch_add(1, Ordering::Relaxed);
    }

    fn kill(&mut self, _name: &orion::executor::WorkloadName) {
        self.kills.fetch_add(1, Ordering::Relaxed);
    }

    fn inventory(&self) -> Vec<(orion::executor::WorkloadName, WorkloadState)> {
        self.running
            .iter()
            .map(|name| (name.clone(), WorkloadState::Running))
            .collect()
    }
}

#[test]
fn a_process_that_will_not_die_stays_stopping_until_it_is_removed_by_force() {
    use std::sync::atomic::AtomicU32;
    let dir = dir("workloads-undying");
    let board = Board::start_for_node(&dir);
    let address = Address::Local(board.socket().to_owned());
    let mut quick = settings(&dir);
    quick.stop_grace = Duration::from_millis(300);
    let _logic = Logic::start(quick);

    let (stops, kills) = (Arc::new(AtomicU32::new(0)), Arc::new(AtomicU32::new(0)));
    let stop = Arc::new(AtomicBool::new(false));
    let (socket, stopping, counted) = (
        board.socket().to_owned(),
        stop.clone(),
        (stops.clone(), kills.clone()),
    );
    let executor = std::thread::spawn(move || {
        let orion = Orion::connect_with(
            &Address::Local(socket),
            "undying",
            orion::Options {
                roles: orion::Roles::CLIENT.and(orion::Roles::EXECUTOR),
                ..orion::Options::default()
            },
        )
        .expect("connect");
        let mut undying = Undying {
            kinds: vec!["process".parse().expect("kind")],
            running: Vec::new(),
            stops: counted.0,
            kills: counted.1,
        };
        let mut serving = orion.executor();
        // Node logic may not be there yet: offer until it is.
        while serving.start(&undying).is_err() {
            std::thread::sleep(Duration::from_millis(10));
        }
        while !stopping.load(Ordering::Relaxed) {
            if serving
                .turn(&mut undying, Duration::from_millis(20))
                .is_err()
            {
                return;
            }
        }
    });

    let operator = Orion::connect(&address, "operator").expect("connect");
    let zombie = workload("zombie", "anything", RestartPolicy::Always);
    reaches(&operator, &zombie, WorkloadState::Running);

    // Asked to stop: stopping. Not obeyed: told to end, once. Still there: still stopping,
    // and its record with it.
    operator
        .workloads()
        .remove("zombie", false)
        .expect("remove");
    let stopping_now = operator
        .workloads()
        .wait("zombie", Duration::from_secs(10), |tracked| {
            tracked.state == WorkloadState::Stopping
        })
        .expect("connected");
    assert!(stopping_now.is_some());
    until("it is told to end", || kills.load(Ordering::Relaxed) == 1);
    assert_eq!(stops.load(Ordering::Relaxed), 1);
    std::thread::sleep(Duration::from_millis(1500));
    assert_eq!(kills.load(Ordering::Relaxed), 1, "told once");
    let still = operator.workloads().status("zombie").expect("connected");
    assert_eq!(
        still.map(|tracked| tracked.state),
        Some(WorkloadState::Stopping)
    );
    let kept = operator
        .records()
        .get(RecordClass::Workload, "")
        .expect("records");
    assert_eq!(kept.len(), 1, "the record stays through stopping");

    // Removed by force: the record goes, and what was left behind is said.
    operator.workloads().remove("zombie", true).expect("force");
    let gone = operator
        .workloads()
        .wait("zombie", Duration::from_secs(10), |tracked| {
            tracked.state == WorkloadState::Gone
        })
        .expect("connected")
        .expect("it is gone");
    assert_eq!(
        gone.detail,
        "removed by force: a run was left Stopping; its executor was told to end it"
    );
    until("the record is gone", || {
        operator
            .records()
            .get(RecordClass::Workload, "")
            .expect("records")
            .is_empty()
    });
    until("it is told to end again", || {
        kills.load(Ordering::Relaxed) == 2
    });

    // And it is in the audit log, with what was left.
    stop.store(true, Ordering::Relaxed);
    executor.join().expect("the executor ends");
    drop(_logic);
    let log = std::fs::read_to_string(dir.join("state/audit.log")).expect("audit log");
    assert!(
        log.contains("\"did\":\"remove\",\"what\":\"zombie by force\",\"result\":\"ok: a run was left Stopping\""),
        "{log}"
    );
    assert!(
        log.contains("\"did\":\"remove\",\"what\":\"zombie\",\"result\":\"ok\""),
        "{log}"
    );
}

#[test]
fn a_missing_file_holds_a_workload_pending_and_says_why() {
    use std::io::Cursor;
    let dir = dir("workloads-files");
    let board = Board::start_for_node(&dir);
    let address = Address::Local(board.socket().to_owned());
    let logic = Logic::start(settings(&dir));
    let _executor = Executor::start(board.socket());
    let operator = Orion::connect(&address, "operator").expect("connect");

    let model = b"the weights the detector was trained to".to_vec();
    let (digest, _) = orion::files::digest(&mut Cursor::new(model.clone())).expect("digest");
    let file = dir.join("pids");
    let spec = sleeper(&file);
    let detector = workload("detector", &spec, RestartPolicy::Always);
    let needs = [("models/detector.bin", digest)];

    // Waits until the workload is in `state` with `detail`, putting it again meanwhile so
    // that node logic not being there yet does not matter.
    let reaches_with = |state: WorkloadState, detail: &str| {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            operator
                .workloads()
                .put_needing(&detector, &needs)
                .expect("put");
            let reached = operator
                .workloads()
                .wait("detector", Duration::from_millis(200), |tracked| {
                    tracked.state == state && tracked.detail == detail
                })
                .expect("connected");
            if reached.is_some() {
                return;
            }
            let now = operator.workloads().status("detector").expect("connected");
            assert!(
                Instant::now() < deadline,
                "never {state:?} {detail:?}: {now:?}"
            );
        }
    };

    // The executor is there, the file is not: pending, and it says for what.
    reaches_with(
        WorkloadState::Pending,
        "waiting for file models/detector.bin",
    );
    std::thread::sleep(Duration::from_millis(300));
    assert!(
        starts(&file).is_empty(),
        "it was not started without its file"
    );

    // Another file under that name is not the one it needs.
    let wrong = operator
        .files()
        .push(
            "models/detector.bin",
            &mut Cursor::new(b"something else".to_vec()),
            |_, _| {},
        )
        .expect("push");
    assert_eq!(wrong.state, orion::files::FileState::Committed);
    std::thread::sleep(Duration::from_millis(1300));
    assert!(starts(&file).is_empty());
    reaches_with(
        WorkloadState::Pending,
        "waiting for file models/detector.bin",
    );

    // The file arrives: the workload starts by itself.
    let right = operator
        .files()
        .push("models/detector.bin", &mut Cursor::new(model), |_, _| {})
        .expect("push");
    assert_eq!(right.state, orion::files::FileState::Committed);
    let running = operator
        .workloads()
        .wait("detector", Duration::from_secs(10), |tracked| {
            tracked.state == WorkloadState::Running
        })
        .expect("connected");
    assert!(running.is_some(), "it starts when its file is in place");
    until("the command has started", || starts(&file).len() == 1);

    // After node logic restarts, the file is still known to be the one: the run is adopted
    // and nothing waits.
    std::thread::sleep(Duration::from_millis(200));
    drop(logic);
    let _logic = Logic::start(settings(&dir));
    reaches_with(WorkloadState::Running, "");
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(starts(&file).len(), 1);
    operator
        .workloads()
        .remove("detector", true)
        .expect("remove");
}

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

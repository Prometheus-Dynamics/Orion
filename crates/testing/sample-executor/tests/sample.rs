//! The sample executor: what it does with real commands, and that it works with Orion, both
//! inside a test and as a program of its own.

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use orion::executor::{Executor, WorkloadKind, WorkloadName, WorkloadState};
use orion::workloads::{RestartPolicy, Workload};
use orion::{Address, Orion};
use orion_sample_executor::{Sample, run};
use orion_stubs::Board;

fn kind() -> WorkloadKind {
    "process".parse().expect("kind")
}

fn name(text: &str) -> WorkloadName {
    text.parse().expect("name")
}

fn dir(test: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("orion-sample-{test}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("directory");
    dir
}

/// A command that writes its process id to `file` and then sleeps.
fn sleeper(file: &Path) -> String {
    format!("echo $$ > {}; exec sleep 60", file.display())
}

fn pid(file: &Path) -> u32 {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(pid) = std::fs::read_to_string(file)
            .ok()
            .and_then(|text| text.trim().parse().ok())
        {
            return pid;
        }
        assert!(Instant::now() < deadline, "the command never started");
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn alive(pid: u32) -> bool {
    // A process that has exited and is not yet reaped still has a directory: its state
    // says so.
    std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|stat| {
        stat.rsplit(')')
            .next()
            .is_some_and(|rest| !rest.trim_start().starts_with('Z'))
    })
}

fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(Instant::now() < deadline, "{what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn state(sample: &Sample, workload: &str) -> Option<WorkloadState> {
    sample
        .inventory()
        .into_iter()
        .find(|(found, _)| *found == name(workload))
        .map(|(_, state)| state)
}

#[test]
fn a_command_runs_is_stopped_and_is_gone() {
    let dir = dir("stop");
    let mut sample = Sample::new(vec![kind()]);
    assert_eq!(sample.kinds(), [kind()]);
    assert!(sample.is_empty());
    let file = dir.join("pid");
    sample
        .start(&name("sleeper"), sleeper(&file).as_bytes())
        .expect("start");
    assert_eq!(state(&sample, "sleeper"), Some(WorkloadState::Running));
    let pid = pid(&file);
    assert!(alive(pid));
    sample.stop(&name("sleeper"));
    until("the command ends", || state(&sample, "sleeper").is_none());
    assert!(!alive(pid));
    assert!(sample.is_empty());
    // Stopping or killing what is not there is nothing.
    sample.stop(&name("sleeper"));
    sample.kill(&name("nobody"));
}

#[test]
fn a_command_that_will_not_stop_is_stopping_until_it_is_killed() {
    let dir = dir("stubborn");
    let mut sample = Sample::new(vec![kind()]);
    let file = dir.join("pid");
    let stubborn = format!(
        "trap '' TERM; echo $$ > {}; while true; do sleep 1; done",
        file.display()
    );
    sample
        .start(&name("stubborn"), stubborn.as_bytes())
        .expect("start");
    let pid = pid(&file);
    sample.stop(&name("stubborn"));
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(state(&sample, "stubborn"), Some(WorkloadState::Stopping));
    assert!(alive(pid));
    sample.kill(&name("stubborn"));
    assert_eq!(state(&sample, "stubborn"), None);
    until("it is dead", || !alive(pid));
}

#[test]
fn how_a_command_ends_is_what_the_inventory_says() {
    let mut sample = Sample::new(vec![kind()]);
    // It ends well: it is gone.
    sample.start(&name("quick"), b"true").expect("start");
    until("it ends", || state(&sample, "quick").is_none());
    // It ends badly: it is failed, and stays listed so.
    sample.start(&name("broken"), b"exit 3").expect("start");
    until("it fails", || {
        state(&sample, "broken") == Some(WorkloadState::Failed)
    });
    assert_eq!(state(&sample, "broken"), Some(WorkloadState::Failed));
    // Started again, it runs again. Stopped while failed, it is forgotten.
    sample.start(&name("broken"), b"sleep 60").expect("start");
    assert_eq!(state(&sample, "broken"), Some(WorkloadState::Running));
    sample.start(&name("broken"), b"exit 3").expect("start");
    until("it fails", || {
        state(&sample, "broken") == Some(WorkloadState::Failed)
    });
    sample.stop(&name("broken"));
    assert_eq!(state(&sample, "broken"), None);
    // A spec that is not a command line cannot be started.
    assert!(sample.start(&name("empty"), b"  ").is_err());
    assert!(sample.start(&name("binary"), &[0xFF, 0xFE]).is_err());
    assert!(sample.is_empty());
}

#[test]
fn an_executor_that_ends_takes_its_commands_with_it() {
    let dir = dir("drop");
    let mut sample = Sample::new(vec![kind()]);
    let file = dir.join("pid");
    sample
        .start(&name("sleeper"), sleeper(&file).as_bytes())
        .expect("start");
    let pid = pid(&file);
    drop(sample);
    assert!(!alive(pid));
}

fn put(orion: &Orion, workload: &str, spec: &str) {
    orion
        .workloads()
        .put(&Workload {
            name: workload,
            kind: "process",
            spec: spec.as_bytes(),
            node: None,
            restart: RestartPolicy::Never,
        })
        .expect("put");
}

fn reaches(orion: &Orion, workload: &str, state: WorkloadState) {
    let reached = orion
        .workloads()
        .wait(workload, Duration::from_secs(10), |tracked| {
            tracked.state == state
        })
        .expect("connected");
    assert!(reached.is_some(), "{workload} never was {state:?}");
}

#[test]
fn it_runs_what_orion_asks_and_says_how_it_goes() {
    let dir = dir("orion");
    let board = Board::start("sample-executor");
    let stop = Arc::new(AtomicBool::new(false));
    let (socket, stopping) = (board.socket().to_owned(), stop.clone());
    let executor = std::thread::spawn(move || {
        let mut sample = Sample::new(vec![kind()]);
        run(&socket, &mut sample, || !stopping.load(Ordering::Relaxed)).expect("run");
    });
    let orion =
        Orion::connect(&Address::Local(board.socket().to_owned()), "operator").expect("connect");
    // The executor has to have offered its kind before a workload of it can be started.
    std::thread::sleep(Duration::from_millis(200));

    let file = dir.join("pid");
    put(&orion, "sleeper", &sleeper(&file));
    reaches(&orion, "sleeper", WorkloadState::Running);
    let pid = pid(&file);
    assert!(alive(pid));
    put(&orion, "broken", "exit 3");
    reaches(&orion, "broken", WorkloadState::Failed);

    orion.workloads().remove("sleeper", false).expect("remove");
    reaches(&orion, "sleeper", WorkloadState::Gone);
    assert!(!alive(pid));

    stop.store(true, Ordering::Relaxed);
    executor.join().expect("the executor ends");
}

struct Program(Child);

impl Drop for Program {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn as_a_program_it_does_the_same() {
    let dir = dir("program");
    let board = Board::start("sample-executor-program");
    let program = Program(
        Command::new(env!("CARGO_BIN_EXE_orion-sample-executor"))
            .arg("--socket")
            .arg(board.socket())
            .args(["--kind", "process"])
            .spawn()
            .expect("the program starts"),
    );
    let orion =
        Orion::connect(&Address::Local(board.socket().to_owned()), "operator").expect("connect");
    std::thread::sleep(Duration::from_millis(300));
    let file = dir.join("pid");
    put(&orion, "sleeper", &sleeper(&file));
    reaches(&orion, "sleeper", WorkloadState::Running);
    let pid = pid(&file);
    assert!(alive(pid));
    orion.workloads().remove("sleeper", true).expect("remove");
    until("the command is killed", || !alive(pid));
    drop(program);

    // Told something it does not understand, it says how it is used and does not start.
    let wrong = Command::new(env!("CARGO_BIN_EXE_orion-sample-executor"))
        .arg("--nonsense")
        .output()
        .expect("runs");
    assert_eq!(wrong.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&wrong.stderr).contains("usage:"));
}

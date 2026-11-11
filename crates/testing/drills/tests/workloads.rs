//! The table's last two rows. An executor dies: everything else goes on, its workloads stay
//! on record as lost, and they are started again when it is back. Orion dies, both parts:
//! what runs keeps running, and Orion takes it back by name without starting it again.

mod support;

use std::path::Path;
use std::time::Duration;

use orion::workloads::{RestartPolicy, Workload, WorkloadState};
use orion::{Address, Orion};
use support::{Board, alive, clean};

const LONG: Duration = Duration::from_secs(10);
const NAME: &str = "drill.sleep";

/// A command that adds its process number to `file` each time it starts, then sleeps.
fn sleeper(file: &Path) -> String {
    format!("echo $$ >> {}; exec sleep 600", file.display())
}

fn starts(file: &Path) -> Vec<u32> {
    std::fs::read_to_string(file)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| line.trim().parse().ok())
        .collect()
}

/// A client, once the comm core is up.
fn client(board: &Board) -> Orion {
    let started = std::time::Instant::now();
    loop {
        match Orion::connect(&Address::Local(board.socket.clone()), "drill") {
            Ok(orion) => return orion,
            Err(error) => assert!(started.elapsed() < LONG, "connect: {error}"),
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn put(orion: &Orion, file: &Path) {
    let spec = sleeper(file);
    orion
        .workloads()
        .put(&Workload {
            name: NAME,
            kind: "process",
            spec: spec.as_bytes(),
            node: None,
            restart: RestartPolicy::Always,
        })
        .expect("put");
}

/// Waits until the workload is in `state`. Putting it again as it is changes nothing and
/// asks where it is, which a client that did not put it needs.
fn reaches(orion: &Orion, file: &Path, state: WorkloadState) {
    let started = std::time::Instant::now();
    loop {
        put(orion, file);
        let reached = orion
            .workloads()
            .wait(NAME, Duration::from_millis(200), |tracked| {
                tracked.state == state
            })
            .expect("wait");
        if reached.is_some() {
            return;
        }
        assert!(
            started.elapsed() < LONG,
            "not {state:?}: {:?}",
            orion.workloads().status(NAME)
        );
    }
}

fn wait_starts(file: &Path, count: usize) -> Vec<u32> {
    let started = std::time::Instant::now();
    loop {
        let pids = starts(file);
        if pids.len() >= count {
            return pids;
        }
        assert!(
            started.elapsed() < LONG,
            "started {pids:?}, not {count} times"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Waits until node logic has saved the workload's record. A put is kept from then on: it
/// is saved `records.save_after_ms` after it (10 ms here) and the time a save takes, and a
/// node logic killed before that starts without it.
fn saved(board: &Board) {
    let snapshot = board.dir.join("node/records.v1.snap");
    let started = std::time::Instant::now();
    while !std::fs::read(&snapshot)
        .is_ok_and(|bytes| bytes.windows(NAME.len()).any(|w| w == NAME.as_bytes()))
    {
        assert!(started.elapsed() < LONG, "the workload was never saved");
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn an_executor_dies_and_its_workloads_are_lost_then_started_again() {
    let board = Board::new("executor", "");
    let _comm = board.comm();
    let _node = board.node();
    let executor = board.role("executor", &[]);
    let publisher = board.role("publisher", &[]);
    let subscriber = board.role("subscriber", &[]);
    subscriber.wait_for("seen", 100, LONG);
    let file = board.dir.join("starts");
    let orion = client(&board);
    reaches(&orion, &file, WorkloadState::Running);
    let first = wait_starts(&file, 1)[0];

    executor.kill();

    // Its command went with it, and the record stays, marked lost.
    reaches(&orion, &file, WorkloadState::Lost);
    let died = std::time::Instant::now();
    while alive(first) {
        assert!(died.elapsed() < LONG, "the command outlived its executor");
        std::thread::sleep(Duration::from_millis(10));
    }
    // Everything else went on.
    let seen = subscriber.last("seen");
    subscriber.wait_for("seen", seen + 200, LONG);
    board.ctl_until(&["resource", "list"], LONG);

    // The executor is back with nothing: Orion starts the workload again, once.
    let _executor = board.role("executor", &[]);
    reaches(&orion, &file, WorkloadState::Running);
    let pids = wait_starts(&file, 2);
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        starts(&file).len(),
        2,
        "started once more, not twice: {pids:?}"
    );
    assert!(alive(pids[1]));
    assert_eq!(publisher.count("reconnected"), 0);
    clean(&subscriber);
    orion.workloads().remove(NAME, true).expect("remove");
}

#[test]
fn orion_dies_whole_and_takes_back_what_kept_running() {
    let board = Board::new("both", "");
    let comm = board.comm();
    let node = board.node();
    let _executor = board.role("executor", &[]);
    let publisher = board.role("publisher", &[]);
    let subscriber = board.role("subscriber", &[]);
    let file = board.dir.join("starts");
    let orion = client(&board);
    reaches(&orion, &file, WorkloadState::Running);
    let first = wait_starts(&file, 1)[0];
    saved(&board);
    drop(orion);

    comm.kill();
    node.kill();

    // The workload and the graphs keep running.
    let seen = subscriber.last("seen");
    subscriber.wait_for("seen", seen + 300, LONG);
    assert!(alive(first), "the workload kept running");

    // Orion comes back and finds it by name: running, and never started a second time.
    let _comm = board.comm();
    let _node = board.node();
    let orion = client(&board);
    reaches(&orion, &file, WorkloadState::Running);
    publisher.wait_line("reconnected", LONG);
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(starts(&file), [first], "adopted, not started again");
    assert!(alive(first));
    clean(&subscriber);
    orion.workloads().remove(NAME, true).expect("remove");
}

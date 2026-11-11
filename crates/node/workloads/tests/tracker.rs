//! Every transition of the tracker: each state, and each thing that can happen in it.

use WorkloadState::{Failed, Gone, Lost, Pending, Running, Starting, Stopping};
use orion_msg_workloads::{RestartPolicy, WorkloadState};
use orion_types::{Error, WorkloadKind, WorkloadName};
use orion_workloads::{Change, Command, Put, Removed, Reported, RestartConfig, Tracker, Workload};

fn name(text: &str) -> WorkloadName {
    text.parse().expect("name")
}

fn graph() -> WorkloadKind {
    "graph".parse().expect("kind")
}

fn workload(text: &str, restart: RestartPolicy) -> Workload {
    Workload {
        restart,
        ..Workload::new(name(text), graph(), b"spec")
    }
}

/// A tracker and a clock.
struct Bench {
    tracker: Tracker,
    now: u64,
}

impl Bench {
    fn new() -> Self {
        Self::with(RestartConfig::default())
    }

    fn with(config: RestartConfig) -> Self {
        Self {
            tracker: Tracker::new(config),
            now: 1_000_000,
        }
    }

    /// With an executor for graphs that has nothing.
    fn with_executor() -> Self {
        let mut bench = Self::new();
        assert!(bench.up(&[]).is_empty());
        bench
    }

    fn up(&mut self, inventory: &[(&str, WorkloadState)]) -> Vec<WorkloadName> {
        let inventory: Vec<_> = inventory
            .iter()
            .map(|(text, state)| (name(text), *state))
            .collect();
        self.tracker.executor_up(&[graph()], &inventory, self.now)
    }

    fn down(&mut self) {
        self.tracker.executor_down(&[graph()], self.now);
    }

    fn put(&mut self, text: &str, restart: RestartPolicy) -> Put {
        self.tracker.put(workload(text, restart), self.now)
    }

    fn remove(&mut self, text: &str) -> Result<Removed, Error> {
        self.tracker.remove(&name(text), false, self.now)
    }

    fn force(&mut self, text: &str) -> Result<Removed, Error> {
        self.tracker.remove(&name(text), true, self.now)
    }

    fn report(&mut self, text: &str, state: WorkloadState) -> Reported {
        self.tracker.report(&name(text), state, "", self.now)
    }

    fn pass(&mut self, ms: u64) {
        self.now += ms;
        self.tracker.tick(self.now);
    }

    fn state(&self, text: &str) -> Option<WorkloadState> {
        self.tracker.get(&name(text)).map(|tracked| tracked.state)
    }

    fn commands(&mut self) -> Vec<Command> {
        self.tracker.take_commands()
    }

    fn states(&mut self) -> Vec<(WorkloadState, bool)> {
        self.tracker
            .take_changes()
            .into_iter()
            .map(|change| (change.state, change.dropped))
            .collect()
    }

    /// A workload that is running, with nothing left to read.
    fn running(text: &str, restart: RestartPolicy) -> Self {
        let mut bench = Self::with_executor();
        bench.put(text, restart);
        bench.report(text, Running);
        bench.commands();
        bench.states();
        bench
    }
}

fn start(text: &str) -> Command {
    Command::Start(name(text))
}

fn stop(text: &str) -> Command {
    Command::Stop(name(text))
}

// --- Nothing there ---

#[test]
fn put_with_no_executor_is_pending() {
    let mut bench = Bench::new();
    assert_eq!(bench.put("a", RestartPolicy::Always), Put::Created);
    assert_eq!(bench.state("a"), Some(Pending));
    assert!(bench.commands().is_empty());
    assert_eq!(bench.states(), [(Pending, false)]);
}

#[test]
fn put_with_an_executor_is_starting() {
    let mut bench = Bench::with_executor();
    assert_eq!(bench.put("a", RestartPolicy::Always), Put::Created);
    assert_eq!(bench.state("a"), Some(Starting));
    assert_eq!(bench.commands(), [start("a")]);
    assert_eq!(bench.states(), [(Starting, false)]);
}

#[test]
fn removing_what_is_not_there_is_not_found() {
    let mut bench = Bench::new();
    assert_eq!(bench.remove("a"), Err(Error::NotFound));
    assert_eq!(bench.force("a"), Err(Error::NotFound));
}

#[test]
fn a_report_of_an_unknown_run_is_an_orphan_and_of_an_unknown_end_is_nothing() {
    let mut bench = Bench::with_executor();
    for state in [Starting, Running, Stopping] {
        assert_eq!(bench.report("stray", state), Reported::Orphan);
    }
    for state in [Gone, Failed, Pending, Lost] {
        assert_eq!(bench.report("stray", state), Reported::Ignored);
    }
    assert!(bench.tracker.is_empty());
    assert!(bench.states().is_empty());
}

// --- Pending ---

#[test]
fn pending_starts_when_its_executor_comes() {
    let mut bench = Bench::new();
    bench.put("a", RestartPolicy::Always);
    assert!(bench.up(&[]).is_empty());
    assert_eq!(bench.state("a"), Some(Starting));
    assert_eq!(bench.commands(), [start("a")]);
}

#[test]
fn pending_is_not_started_by_an_executor_for_another_kind() {
    let mut bench = Bench::new();
    bench.put("a", RestartPolicy::Always);
    let other: WorkloadKind = "container".parse().expect("kind");
    bench
        .tracker
        .executor_up(std::slice::from_ref(&other), &[], bench.now);
    assert_eq!(bench.state("a"), Some(Pending));
    assert!(bench.tracker.has_executor(&other));
    assert!(!bench.tracker.has_executor(&graph()));
}

#[test]
fn pending_removed_is_dropped_at_once() {
    let mut bench = Bench::new();
    bench.put("a", RestartPolicy::Always);
    bench.states();
    assert_eq!(bench.remove("a"), Ok(Removed::Dropped));
    assert_eq!(bench.state("a"), None);
    assert_eq!(bench.states(), [(Gone, true)]);
    assert!(bench.commands().is_empty());
}

#[test]
fn pending_is_adopted_when_the_executor_already_runs_it() {
    // Orion restarted: it knows what should run, and the run never stopped.
    let mut bench = Bench::new();
    bench.put("a", RestartPolicy::Always);
    assert!(bench.up(&[("a", Running)]).is_empty());
    assert_eq!(bench.state("a"), Some(Running));
    assert!(bench.commands().is_empty(), "it is not started twice");
}

#[test]
fn pending_put_again_is_unchanged_and_with_new_content_replaced() {
    let mut bench = Bench::new();
    bench.put("a", RestartPolicy::Always);
    assert_eq!(bench.put("a", RestartPolicy::Always), Put::Unchanged);
    let mut other = workload("a", RestartPolicy::Always);
    other.spec = b"new".to_vec();
    assert_eq!(bench.tracker.put(other, bench.now), Put::Replaced);
    assert_eq!(bench.state("a"), Some(Pending));
    assert_eq!(
        bench.tracker.get(&name("a")).expect("a").workload.spec,
        b"new"
    );
}

// --- Starting ---

#[test]
fn starting_becomes_running_when_reported() {
    let mut bench = Bench::with_executor();
    bench.put("a", RestartPolicy::Always);
    assert_eq!(bench.report("a", Starting), Reported::Applied);
    assert_eq!(bench.state("a"), Some(Starting));
    assert_eq!(bench.report("a", Running), Reported::Applied);
    assert_eq!(bench.state("a"), Some(Running));
    assert_eq!(bench.states(), [(Starting, false), (Running, false)]);
}

#[test]
fn starting_that_fails_waits_and_is_started_again() {
    let mut bench = Bench::with_executor();
    bench.put("a", RestartPolicy::Always);
    bench.commands();
    assert_eq!(bench.report("a", Failed), Reported::Applied);
    assert_eq!(bench.state("a"), Some(Failed));
    assert_eq!(bench.tracker.next_deadline(), Some(bench.now + 1_000));
    bench.pass(999);
    assert!(bench.commands().is_empty());
    bench.pass(1);
    assert_eq!(bench.state("a"), Some(Starting));
    assert_eq!(bench.commands(), [start("a")]);
    assert_eq!(bench.tracker.next_deadline(), None);
}

#[test]
fn starting_removed_is_stopping_until_gone() {
    let mut bench = Bench::with_executor();
    bench.put("a", RestartPolicy::Always);
    bench.commands();
    assert_eq!(bench.remove("a"), Ok(Removed::Stopping));
    assert_eq!(bench.state("a"), Some(Stopping));
    assert_eq!(bench.commands(), [stop("a")]);
    // It comes up anyway before it hears the stop: still Stopping, and not asked twice.
    bench.report("a", Running);
    assert_eq!(bench.state("a"), Some(Stopping));
    assert!(bench.commands().is_empty());
    bench.report("a", Gone);
    assert_eq!(bench.state("a"), None);
}

#[test]
fn starting_is_lost_when_its_executor_goes() {
    let mut bench = Bench::with_executor();
    bench.put("a", RestartPolicy::Always);
    bench.down();
    assert_eq!(bench.state("a"), Some(Lost));
}

// --- Running ---

#[test]
fn running_that_ends_is_started_again_by_policy() {
    for (policy, failed, after) in [
        (RestartPolicy::Always, false, Pending),
        (RestartPolicy::Always, true, Failed),
        (RestartPolicy::OnFailure, false, Gone),
        (RestartPolicy::OnFailure, true, Failed),
        (RestartPolicy::Never, false, Gone),
        (RestartPolicy::Never, true, Failed),
    ] {
        let mut bench = Bench::running("a", policy);
        bench.report("a", if failed { Failed } else { Gone });
        assert_eq!(bench.state("a"), Some(after), "{policy:?} failed={failed}");
        let again =
            policy == RestartPolicy::Always || (policy == RestartPolicy::OnFailure && failed);
        assert_eq!(bench.tracker.next_deadline().is_some(), again);
        bench.pass(1_000);
        if again {
            assert_eq!(
                bench.state("a"),
                Some(Starting),
                "{policy:?} failed={failed}"
            );
            assert_eq!(bench.commands(), [start("a")]);
        } else {
            // The record stays, where it ended, until it is removed.
            assert_eq!(bench.state("a"), Some(after));
            assert!(bench.commands().is_empty());
            bench.pass(10_000_000);
            assert_eq!(bench.state("a"), Some(after));
            assert_eq!(bench.remove("a"), Ok(Removed::Dropped));
        }
    }
}

#[test]
fn running_removed_stays_until_the_executor_says_gone() {
    let mut bench = Bench::running("a", RestartPolicy::Always);
    assert_eq!(bench.remove("a"), Ok(Removed::Stopping));
    assert_eq!(bench.commands(), [stop("a")]);
    assert_eq!(bench.state("a"), Some(Stopping));
    // Time alone never drops it: it is told to end, once, and stays.
    bench.pass(100_000_000);
    assert_eq!(bench.state("a"), Some(Stopping));
    assert_eq!(bench.commands(), [Command::Kill(name("a"))]);
    // Asked again, it is asked nothing new.
    assert_eq!(bench.remove("a"), Ok(Removed::Stopping));
    assert!(bench.commands().is_empty());
    bench.states();
    assert_eq!(bench.report("a", Gone), Reported::Applied);
    assert_eq!(bench.state("a"), None);
    assert_eq!(bench.states(), [(Gone, true)]);
    assert!(bench.commands().is_empty(), "removed is not restarted");
}

#[test]
fn running_removed_that_fails_on_the_way_out_is_gone_too() {
    let mut bench = Bench::running("a", RestartPolicy::Always);
    bench.remove("a").expect("remove");
    bench.report("a", Failed);
    assert_eq!(bench.state("a"), None);
    assert_eq!(bench.tracker.next_deadline(), None);
}

#[test]
fn force_remove_drops_the_record_and_kills_the_run() {
    let mut bench = Bench::running("a", RestartPolicy::Always);
    assert_eq!(bench.force("a"), Ok(Removed::Dropped));
    assert_eq!(bench.state("a"), None);
    assert_eq!(bench.commands(), [Command::Kill(name("a"))]);
    assert_eq!(bench.states(), [(Gone, true)]);
    // What the executor says of it afterwards is about nothing Orion has.
    assert_eq!(bench.report("a", Gone), Reported::Ignored);
    assert_eq!(bench.report("a", Running), Reported::Orphan);
}

#[test]
fn running_put_again_changes_nothing() {
    let mut bench = Bench::running("a", RestartPolicy::Always);
    assert_eq!(bench.put("a", RestartPolicy::Always), Put::Unchanged);
    assert_eq!(bench.state("a"), Some(Running));
    assert!(bench.commands().is_empty());
    assert!(bench.states().is_empty());
}

#[test]
fn running_put_with_new_content_is_stopped_and_started_with_it() {
    let mut bench = Bench::running("a", RestartPolicy::Never);
    let mut newer = workload("a", RestartPolicy::Never);
    newer.spec = b"v2".to_vec();
    assert_eq!(bench.tracker.put(newer, bench.now), Put::Replaced);
    assert_eq!(bench.state("a"), Some(Stopping));
    assert_eq!(bench.commands(), [stop("a")]);
    // Gone, and at once started again, whatever the restart policy: it was asked for.
    bench.report("a", Gone);
    assert_eq!(bench.state("a"), Some(Starting));
    assert_eq!(bench.commands(), [start("a")]);
    assert_eq!(
        bench.tracker.get(&name("a")).expect("a").workload.spec,
        b"v2"
    );
}

#[test]
fn running_is_lost_when_its_executor_goes() {
    let mut bench = Bench::running("a", RestartPolicy::Always);
    bench.down();
    assert_eq!(bench.state("a"), Some(Lost));
    assert_eq!(bench.states(), [(Lost, false)]);
    assert!(bench.commands().is_empty());
}

#[test]
fn a_report_that_it_is_stopping_on_its_own_is_stopping() {
    let mut bench = Bench::running("a", RestartPolicy::Always);
    bench.report("a", Stopping);
    assert_eq!(bench.state("a"), Some(Stopping));
    assert!(bench.commands().is_empty(), "nobody asked");
    // It is wanted, so when it is gone it is started again.
    bench.report("a", Gone);
    assert_eq!(bench.state("a"), Some(Pending));
    bench.pass(1_000);
    assert_eq!(bench.state("a"), Some(Starting));
}

// --- Stopping ---

#[test]
fn stopping_put_again_is_kept_and_started_when_gone() {
    let mut bench = Bench::running("a", RestartPolicy::Always);
    bench.remove("a").expect("remove");
    bench.commands();
    assert_eq!(bench.put("a", RestartPolicy::Always), Put::Kept);
    assert_eq!(bench.state("a"), Some(Stopping));
    assert!(bench.commands().is_empty());
    bench.report("a", Gone);
    assert_eq!(bench.state("a"), Some(Starting));
    assert_eq!(bench.commands(), [start("a")]);
}

#[test]
fn stopping_is_lost_when_its_executor_goes_and_dropped_when_it_is_back_without_it() {
    let mut bench = Bench::running("a", RestartPolicy::Always);
    bench.remove("a").expect("remove");
    bench.down();
    assert_eq!(bench.state("a"), Some(Lost));
    assert_eq!(bench.remove("a"), Ok(Removed::Waiting));
    bench.states();
    assert!(bench.up(&[]).is_empty());
    assert_eq!(bench.state("a"), None);
    assert_eq!(bench.states(), [(Gone, true)]);
}

#[test]
fn stopping_force_removed_is_killed() {
    let mut bench = Bench::running("a", RestartPolicy::Always);
    bench.remove("a").expect("remove");
    bench.commands();
    assert_eq!(bench.force("a"), Ok(Removed::Dropped));
    assert_eq!(bench.commands(), [Command::Kill(name("a"))]);
}

// --- Gone and Failed, at rest ---

#[test]
fn gone_and_failed_at_rest_stay_through_an_executor_coming_and_going() {
    for end in [Gone, Failed] {
        let mut bench = Bench::running("a", RestartPolicy::Never);
        bench.report("a", end);
        bench.down();
        assert_eq!(bench.state("a"), Some(end), "no run to lose");
        bench.up(&[]);
        assert_eq!(bench.state("a"), Some(end), "not started again");
        assert!(bench.commands().is_empty());
        // The same content again changes nothing: to run it again, remove it and put it.
        assert_eq!(bench.put("a", RestartPolicy::Never), Put::Unchanged);
        assert_eq!(bench.state("a"), Some(end));
        // Its end reported twice is nothing.
        assert_eq!(bench.report("a", end), Reported::Ignored);
    }
}

#[test]
fn gone_at_rest_put_with_new_content_runs_again() {
    let mut bench = Bench::running("a", RestartPolicy::Never);
    bench.report("a", Gone);
    assert_eq!(bench.put("a", RestartPolicy::OnFailure), Put::Replaced);
    assert_eq!(bench.state("a"), Some(Starting));
    assert_eq!(bench.commands(), [start("a")]);
}

// --- Failed, waiting to be started again ---

#[test]
fn the_wait_doubles_to_a_cap_and_never_gives_up() {
    let mut bench = Bench::running("a", RestartPolicy::OnFailure);
    let mut waits = Vec::new();
    for _ in 0..12 {
        bench.report("a", Failed);
        let due = bench.tracker.next_deadline().expect("a restart is due");
        waits.push(due - bench.now);
        bench.pass(due - bench.now);
        assert_eq!(bench.state("a"), Some(Starting));
    }
    assert_eq!(
        waits,
        [
            1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 60_000, 60_000, 60_000, 60_000, 60_000,
            60_000
        ]
    );
    assert_eq!(bench.tracker.get(&name("a")).expect("a").attempts, 12);
}

#[test]
fn a_run_that_stayed_up_starts_the_count_again() {
    let mut bench = Bench::running("a", RestartPolicy::Always);
    for _ in 0..4 {
        bench.report("a", Failed);
        let due = bench.tracker.next_deadline().expect("due");
        bench.pass(due - bench.now);
        bench.report("a", Running);
    }
    // Up for ten seconds, then it fails: the wait is the first again.
    bench.pass(10_000);
    bench.report("a", Failed);
    assert_eq!(bench.tracker.next_deadline(), Some(bench.now + 1_000));
    // Up for less: the wait goes on doubling.
    bench.pass(1_000);
    bench.report("a", Running);
    bench.pass(9_999);
    bench.report("a", Failed);
    assert_eq!(bench.tracker.next_deadline(), Some(bench.now + 2_000));
}

#[test]
fn told_a_number_of_tries_it_gives_up_after_them() {
    let mut bench = Bench::with(RestartConfig {
        max_attempts: Some(2),
        ..RestartConfig::default()
    });
    bench.up(&[]);
    bench.put("a", RestartPolicy::Always);
    for _ in 0..2 {
        bench.report("a", Failed);
        let due = bench.tracker.next_deadline().expect("due");
        bench.pass(due - bench.now);
        assert_eq!(bench.state("a"), Some(Starting));
    }
    bench.report("a", Failed);
    assert_eq!(bench.state("a"), Some(Failed));
    assert_eq!(bench.tracker.next_deadline(), None);
    let detail = &bench.tracker.get(&name("a")).expect("a").detail;
    assert!(detail.starts_with("gave up after 2 restarts"), "{detail}");
}

#[test]
fn waiting_to_restart_with_no_executor_becomes_pending_and_starts_when_one_comes() {
    let mut bench = Bench::running("a", RestartPolicy::Always);
    bench.report("a", Failed);
    bench.down();
    assert_eq!(bench.state("a"), Some(Failed), "it has no run to lose");
    bench.pass(1_000);
    assert_eq!(bench.state("a"), Some(Pending));
    assert!(bench.commands().is_empty());
    bench.up(&[]);
    assert_eq!(bench.state("a"), Some(Starting));
    assert_eq!(bench.commands(), [start("a")]);
}

#[test]
fn an_executor_that_comes_does_not_cut_a_wait_short() {
    let mut bench = Bench::running("a", RestartPolicy::Always);
    bench.report("a", Failed);
    bench.down();
    bench.up(&[]);
    assert_eq!(bench.state("a"), Some(Failed));
    assert!(bench.commands().is_empty());
    bench.pass(1_000);
    assert_eq!(bench.commands(), [start("a")]);
}

#[test]
fn waiting_to_restart_removed_is_dropped_and_put_new_starts_now() {
    let mut bench = Bench::running("a", RestartPolicy::Always);
    bench.report("a", Failed);
    assert_eq!(bench.remove("a"), Ok(Removed::Dropped));
    assert_eq!(bench.tracker.next_deadline(), None);

    let mut bench = Bench::running("a", RestartPolicy::Always);
    bench.report("a", Failed);
    let mut newer = workload("a", RestartPolicy::Always);
    newer.spec = b"fixed".to_vec();
    assert_eq!(bench.tracker.put(newer, bench.now), Put::Replaced);
    assert_eq!(bench.state("a"), Some(Starting));
    assert_eq!(bench.tracker.get(&name("a")).expect("a").attempts, 0);
}

// --- Lost ---

#[test]
fn lost_is_adopted_when_the_executor_is_back_with_it() {
    let mut bench = Bench::running("a", RestartPolicy::Always);
    bench.down();
    assert!(bench.up(&[("a", Running)]).is_empty());
    assert_eq!(bench.state("a"), Some(Running));
    assert!(bench.commands().is_empty());
}

#[test]
fn lost_is_started_again_at_once_when_the_executor_is_back_with_nothing() {
    for policy in [
        RestartPolicy::Always,
        RestartPolicy::OnFailure,
        RestartPolicy::Never,
    ] {
        let mut bench = Bench::running("a", policy);
        bench.down();
        assert!(bench.up(&[]).is_empty());
        assert_eq!(bench.state("a"), Some(Starting), "{policy:?}");
        assert_eq!(bench.commands(), [start("a")]);
        // The executor restarting is not the workload failing.
        assert_eq!(bench.tracker.get(&name("a")).expect("a").attempts, 0);
    }
}

#[test]
fn lost_removed_waits_and_is_stopped_when_the_executor_is_back_with_it() {
    let mut bench = Bench::running("a", RestartPolicy::Always);
    bench.down();
    assert_eq!(bench.remove("a"), Ok(Removed::Waiting));
    assert_eq!(bench.state("a"), Some(Lost));
    assert!(bench.commands().is_empty(), "there is nobody to ask");
    bench.up(&[("a", Running)]);
    assert_eq!(bench.state("a"), Some(Stopping));
    assert_eq!(bench.commands(), [stop("a")]);
    bench.report("a", Gone);
    assert_eq!(bench.state("a"), None);
}

#[test]
fn lost_force_removed_is_dropped_with_nobody_to_tell() {
    let mut bench = Bench::running("a", RestartPolicy::Always);
    bench.down();
    assert_eq!(bench.force("a"), Ok(Removed::Dropped));
    assert!(bench.commands().is_empty());
    // If the executor comes back with it, it is a stray and is stopped.
    assert_eq!(bench.up(&[("a", Running)]), [name("a")]);
}

#[test]
fn lost_put_with_new_content_is_replaced_when_the_executor_is_back() {
    let mut bench = Bench::running("a", RestartPolicy::Always);
    bench.down();
    let mut newer = workload("a", RestartPolicy::Always);
    newer.spec = b"v2".to_vec();
    assert_eq!(bench.tracker.put(newer, bench.now), Put::Replaced);
    assert_eq!(bench.state("a"), Some(Lost));
    bench.up(&[("a", Running)]);
    assert_eq!(bench.state("a"), Some(Stopping));
    assert_eq!(bench.commands(), [stop("a")]);
    bench.report("a", Gone);
    assert_eq!(bench.commands(), [start("a")]);
}

#[test]
fn lost_that_ended_while_nobody_looked_is_treated_as_its_end() {
    let mut bench = Bench::running("a", RestartPolicy::OnFailure);
    bench.down();
    bench.up(&[("a", Failed)]);
    assert_eq!(bench.state("a"), Some(Failed));
    assert_eq!(bench.tracker.next_deadline(), Some(bench.now + 1_000));

    let mut bench = Bench::running("a", RestartPolicy::OnFailure);
    bench.down();
    bench.up(&[("a", Gone)]);
    assert_eq!(bench.state("a"), Some(Gone));
    assert_eq!(bench.tracker.next_deadline(), None);
}

// --- An executor's inventory ---

#[test]
fn an_executor_that_says_what_it_has_again_starts_nothing_twice() {
    // Told to start, and the executor's next word is everything it has, taken before it
    // heard: the workload is not started a second time.
    let mut bench = Bench::with_executor();
    bench.put("a", RestartPolicy::Always);
    assert_eq!(bench.commands(), [start("a")]);
    assert!(bench.up(&[]).is_empty());
    assert_eq!(bench.state("a"), Some(Starting));
    assert!(bench.commands().is_empty());
    // And again once it runs.
    bench.report("a", Running);
    assert!(bench.up(&[("a", Running)]).is_empty());
    assert_eq!(bench.state("a"), Some(Running));
    assert!(bench.commands().is_empty());
}

#[test]
fn what_an_executor_has_that_orion_does_not_want_is_returned_to_be_stopped() {
    let mut bench = Bench::new();
    bench.put("wanted", RestartPolicy::Always);
    let strays = bench.up(&[
        ("wanted", Running),
        ("stray", Running),
        ("starting", Starting),
        ("ended", Gone),
        ("broken", Failed),
    ]);
    assert_eq!(strays, [name("stray"), name("starting")]);
    assert_eq!(bench.tracker.len(), 1);
}

#[test]
fn changes_come_in_order_with_their_detail() {
    let mut bench = Bench::with_executor();
    bench.put("a", RestartPolicy::Always);
    bench
        .tracker
        .report(&name("a"), Failed, "exit 3", bench.now);
    let changes = bench.tracker.take_changes();
    assert_eq!(
        changes,
        [
            Change {
                name: name("a"),
                state: Starting,
                detail: String::new(),
                dropped: false
            },
            Change {
                name: name("a"),
                state: Failed,
                detail: "restart 1 in 1000 ms: exit 3".into(),
                dropped: false
            },
        ]
    );
    assert!(bench.tracker.take_changes().is_empty());
}

#[test]
fn the_wait_never_overflows() {
    let config = RestartConfig {
        first_ms: u64::MAX / 2,
        max_ms: u64::MAX,
        ..RestartConfig::default()
    };
    assert_eq!(config.wait_ms(1), u64::MAX / 2);
    assert_eq!(config.wait_ms(3), u64::MAX);
    assert_eq!(config.wait_ms(u32::MAX), u64::MAX);
    assert_eq!(RestartConfig::default().wait_ms(0), 1_000);
    assert_eq!(RestartConfig::default().wait_ms(200), 60_000);
}

#[test]
fn a_workload_is_what_its_put_says() {
    use orion_msg_workloads::{NeededFile, RequiredLabel, WorkloadPut};
    use orion_wire::{Bytes, List};
    let files = [NeededFile {
        name: "graphs/a.bin".parse().expect("file"),
        digest: [7; 32],
    }];
    let labels = [RequiredLabel {
        key: "zone",
        value: "a",
    }];
    let put = WorkloadPut {
        name: name("a"),
        kind: graph(),
        spec: Bytes(b"spec"),
        files: List::new(&files),
        node: Some("left".parse().expect("node")),
        labels: List::new(&labels),
        cpus: Bytes(&[0b1100]),
        restart: RestartPolicy::OnFailure,
    };
    let workload = Workload::from_put(&put).expect("workload");
    assert_eq!(workload.spec, b"spec");
    assert_eq!(workload.files.len(), 1);
    assert_eq!(workload.files[0].1, [7; 32]);
    assert_eq!(workload.labels, [("zone".to_owned(), "a".to_owned())]);
    assert_eq!(workload.cpus, [0b1100]);
    assert_eq!(workload.restart, RestartPolicy::OnFailure);
    assert_eq!(workload.node.expect("node").as_str(), "left");
}

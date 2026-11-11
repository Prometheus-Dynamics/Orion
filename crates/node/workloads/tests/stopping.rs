//! Stopping: a stop that is not obeyed becomes a kill, once, and a forced remove says what
//! it left behind.

use WorkloadState::{Gone, Running, Stopping};
use orion_msg_workloads::{RestartPolicy, WorkloadState};
use orion_types::{Error, WorkloadKind, WorkloadName};
use orion_workloads::{Command, Put, Removed, Reported, RestartConfig, Tracker, Workload};

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

// --- Stop, then kill ---

#[test]
fn a_run_that_does_not_stop_is_told_to_end_once_and_stays_stopping() {
    let mut bench = Bench::running("a", RestartPolicy::Always);
    assert_eq!(bench.remove("a"), Ok(Removed::Stopping));
    assert_eq!(bench.commands(), [stop("a")]);
    assert_eq!(bench.tracker.next_deadline(), Some(bench.now + 10_000));
    bench.pass(9_999);
    assert!(bench.commands().is_empty());
    bench.pass(1);
    assert_eq!(bench.commands(), [Command::Kill(name("a"))]);
    assert_eq!(bench.state("a"), Some(Stopping));
    assert_eq!(
        bench.tracker.get(&name("a")).expect("a").detail,
        "it did not stop when asked and was told to end"
    );
    // Told once. It stays Stopping, with its record, for as long as it is not gone.
    assert_eq!(bench.tracker.next_deadline(), None);
    bench.pass(100_000_000);
    assert!(bench.commands().is_empty());
    assert_eq!(bench.state("a"), Some(Stopping));
    // Whatever it says of itself meanwhile, it is stopping.
    bench.report("a", Running);
    assert_eq!(bench.state("a"), Some(Stopping));
    bench.report("a", Gone);
    assert_eq!(bench.state("a"), None);
}

#[test]
fn a_run_that_stops_in_time_is_not_told_to_end() {
    let mut bench = Bench::running("a", RestartPolicy::Always);
    bench.remove("a").expect("remove");
    bench.commands();
    bench.pass(5_000);
    bench.report("a", Gone);
    bench.pass(100_000);
    assert!(bench.commands().is_empty());
}

#[test]
fn a_replaced_run_that_does_not_stop_is_told_to_end_too() {
    let mut bench = Bench::running("a", RestartPolicy::Always);
    let mut newer = workload("a", RestartPolicy::Always);
    newer.spec = b"v2".to_vec();
    bench.tracker.put(newer, bench.now);
    assert_eq!(bench.commands(), [stop("a")]);
    bench.pass(10_000);
    assert_eq!(bench.commands(), [Command::Kill(name("a"))]);
    // Gone at last: the new content starts, and its own stop starts a new count.
    bench.report("a", Gone);
    assert_eq!(bench.commands(), [start("a")]);
    bench.report("a", Running);
    bench.remove("a").expect("remove");
    assert_eq!(bench.commands(), [stop("a")]);
    assert_eq!(bench.tracker.next_deadline(), Some(bench.now + 10_000));
}

#[test]
fn a_forced_remove_says_what_it_left_behind() {
    let dropped = |bench: &mut Bench| -> String {
        bench
            .tracker
            .take_changes()
            .into_iter()
            .rev()
            .find(|change| change.dropped)
            .expect("a record was dropped")
            .detail
    };
    // A run that would not die.
    let mut bench = Bench::running("a", RestartPolicy::Always);
    bench.remove("a").expect("remove");
    bench.pass(10_000);
    bench.commands();
    assert_eq!(bench.force("a"), Ok(Removed::Dropped));
    assert_eq!(bench.commands(), [Command::Kill(name("a"))]);
    assert_eq!(
        dropped(&mut bench),
        "removed by force: a run was left Stopping; its executor was told to end it"
    );
    // A run whose executor is away.
    let mut bench = Bench::running("a", RestartPolicy::Always);
    bench.down();
    bench.force("a").expect("force");
    assert_eq!(
        dropped(&mut bench),
        "removed by force: a run was left Lost with no executor to end it"
    );
    // Nothing running.
    let mut bench = Bench::new();
    bench.put("a", RestartPolicy::Always);
    bench.force("a").expect("force");
    assert_eq!(dropped(&mut bench), "removed by force: nothing was running");
}

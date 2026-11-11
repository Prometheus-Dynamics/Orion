//! The workload tracker.
//!
//! Orion holds what should be running and tracks every run. This is that, as plain data with
//! no clock, no socket and no executor: it is told what happened and when, and says what to
//! do. Whoever owns it (E5) carries the commands to executors and the changes to whoever
//! watches.
//!
//! A workload is in one of the wire's states:
//!
//! | State | Meaning here |
//! | --- | --- |
//! | Pending | It should run and nothing runs it yet: no executor offers its kind, or it waits to be started again |
//! | Starting | An executor was told to start it |
//! | Running | The executor says it runs |
//! | Stopping | The executor was asked to stop it, or says it is stopping |
//! | Gone | It ended and is not started again. The record stays until it is removed |
//! | Failed | It failed. With a restart due it is started again then; otherwise it stays |
//! | Lost | Its executor went away, so nobody knows |
//!
//! The rules that are not in the table:
//!
//! - A record stays until its run is reported gone. Removing a workload that runs asks for a
//!   stop and keeps the record, Stopping, until the executor says the run ended. Removing
//!   with force drops the record at once and has the run killed. That is the only way a record
//!   leaves early.
//! - A run that ends is started again as its restart policy says, after a wait that doubles
//!   each time up to a cap. It never gives up unless it is told a number of tries. A run that
//!   stayed up long enough starts the count again.
//! - When an executor comes, what it has is adopted by name before anything is started. What
//!   it was running and no longer has is started again at once: the run was lost with the
//!   executor, it did not fail.
//! - Putting a workload that is already here with the same content changes nothing. With
//!   other content, the run is stopped and started again with the new.

mod workload;

use std::collections::{BTreeMap, BTreeSet};

use orion_msg_workloads::{RestartPolicy, WorkloadState};
use orion_types::{Error, WorkloadKind, WorkloadName};

pub use workload::Workload;

/// When a run that ended is started again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestartConfig {
    /// The wait before the first restart.
    pub first_ms: u64,
    /// The longest wait. Each restart waits twice as long as the one before, up to this.
    pub max_ms: u64,
    /// A run that stayed up this long is not counted against the next wait.
    pub reset_after_ms: u64,
    /// How many restarts in a row before giving up. `None` never gives up.
    pub max_attempts: Option<u32>,
}

impl Default for RestartConfig {
    fn default() -> Self {
        Self {
            first_ms: 1_000,
            max_ms: 60_000,
            reset_after_ms: 10_000,
            max_attempts: None,
        }
    }
}

impl RestartConfig {
    /// The wait before restart number `attempt`, counted from one.
    #[must_use]
    pub fn wait_ms(&self, attempt: u32) -> u64 {
        let doublings = attempt.saturating_sub(1).min(63);
        self.first_ms
            .checked_shl(doublings)
            .filter(|wait| wait >> doublings == self.first_ms)
            .map_or(self.max_ms, |wait| wait.min(self.max_ms))
    }
}

/// Something for an executor to do.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    /// Start the workload. Its kind and spec are in [`Tracker::get`].
    Start(WorkloadName),
    /// Ask the run to stop.
    Stop(WorkloadName),
    /// End the run now.
    Kill(WorkloadName),
}

/// A workload changed state, for whoever is told.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Change {
    /// The workload.
    pub name: WorkloadName,
    /// Where it now is.
    pub state: WorkloadState,
    /// A description for people.
    pub detail: String,
    /// Whether its record is gone with this change.
    pub dropped: bool,
}

/// What a put did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Put {
    /// There was no such workload. There is now.
    Created,
    /// The workload was here with other content, and has the new.
    Replaced,
    /// The workload was being removed. It is wanted again.
    Kept,
    /// The workload was here with the same content.
    Unchanged,
}

/// What a remove did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Removed {
    /// The record is gone.
    Dropped,
    /// The run was asked to stop. The record goes when it is reported gone.
    Stopping,
    /// Nobody knows where the run is. The record goes when its executor is back and says.
    Waiting,
}

/// What a report from an executor did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reported {
    /// It was taken.
    Applied,
    /// It said nothing new, or was about a run that is not there.
    Ignored,
    /// The executor has a run of something Orion does not want. It is to be stopped.
    Orphan,
}

/// One workload, as tracked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tracked {
    /// What should run.
    pub workload: Workload,
    /// Where it is.
    pub state: WorkloadState,
    /// A description for people.
    pub detail: String,
    /// Whether it should exist. `false` once it is removed, while its run is not yet gone.
    pub wanted: bool,
    /// Whether the run is to be stopped and started again, with content put since it started.
    pub replace: bool,
    /// Whether an executor has, or may have, a run of it.
    pub run: bool,
    /// How many times in a row it was started again without staying up.
    pub attempts: u32,
    /// When it is started again, on the caller's clock.
    pub retry_at: Option<u64>,
    /// Since when it has been running, on the caller's clock.
    pub running_since: Option<u64>,
}

/// The tracker.
#[derive(Debug, Default)]
pub struct Tracker {
    config: RestartConfig,
    workloads: BTreeMap<WorkloadName, Tracked>,
    /// The kinds an executor is here for.
    executors: BTreeSet<WorkloadKind>,
    commands: Vec<Command>,
    changes: Vec<Change>,
}

const fn alive(state: WorkloadState) -> bool {
    matches!(
        state,
        WorkloadState::Starting | WorkloadState::Running | WorkloadState::Stopping
    )
}

impl Tracker {
    /// An empty tracker.
    #[must_use]
    pub fn new(config: RestartConfig) -> Self {
        Self {
            config,
            ..Self::default()
        }
    }

    /// One workload.
    #[must_use]
    pub fn get(&self, name: &WorkloadName) -> Option<&Tracked> {
        self.workloads.get(name)
    }

    /// Every workload, by name.
    pub fn iter(&self) -> impl Iterator<Item = (&WorkloadName, &Tracked)> {
        self.workloads.iter()
    }

    /// How many workloads there are.
    #[must_use]
    pub fn len(&self) -> usize {
        self.workloads.len()
    }

    /// Whether there are none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.workloads.is_empty()
    }

    /// Whether an executor is here for `kind`.
    #[must_use]
    pub fn has_executor(&self, kind: &WorkloadKind) -> bool {
        self.executors.contains(kind)
    }

    /// Takes what there is for executors to do, in order.
    pub fn take_commands(&mut self) -> Vec<Command> {
        std::mem::take(&mut self.commands)
    }

    /// Takes the changes of state since the last call, in order.
    pub fn take_changes(&mut self) -> Vec<Change> {
        std::mem::take(&mut self.changes)
    }

    /// The earliest time a restart is due: when to call [`Tracker::tick`] next.
    #[must_use]
    pub fn next_deadline(&self) -> Option<u64> {
        self.workloads
            .values()
            .filter_map(|tracked| tracked.retry_at)
            .min()
    }

    fn set(
        &mut self,
        name: &WorkloadName,
        tracked: &mut Tracked,
        state: WorkloadState,
        detail: &str,
    ) {
        if tracked.state == state && tracked.detail == detail {
            return;
        }
        tracked.state = state;
        detail.clone_into(&mut tracked.detail);
        self.changes.push(Change {
            name: name.clone(),
            state,
            detail: detail.to_owned(),
            dropped: false,
        });
    }

    fn dropped(&mut self, name: &WorkloadName, detail: &str) {
        self.changes.push(Change {
            name: name.clone(),
            state: WorkloadState::Gone,
            detail: detail.to_owned(),
            dropped: true,
        });
    }

    /// Starts a workload that has no run, or leaves it Pending for an executor.
    fn settle(&mut self, name: &WorkloadName, tracked: &mut Tracked) {
        tracked.retry_at = None;
        tracked.running_since = None;
        if self.executors.contains(&tracked.workload.kind) {
            tracked.run = true;
            self.set(name, tracked, WorkloadState::Starting, "");
            self.commands.push(Command::Start(name.clone()));
        } else {
            self.set(
                name,
                tracked,
                WorkloadState::Pending,
                "no executor offers its kind",
            );
        }
    }

    /// Asks for the run to stop, once.
    fn stop(&mut self, name: &WorkloadName, tracked: &mut Tracked, detail: &str) {
        if tracked.state != WorkloadState::Stopping {
            self.commands.push(Command::Stop(name.clone()));
        }
        self.set(name, tracked, WorkloadState::Stopping, detail);
    }

    /// The run ended. `false` if the record goes with it.
    fn ended(
        &mut self,
        name: &WorkloadName,
        tracked: &mut Tracked,
        failed: bool,
        detail: &str,
        now: u64,
    ) -> bool {
        tracked.run = false;
        let ran_for = tracked
            .running_since
            .take()
            .map_or(0, |since| now.saturating_sub(since));
        if !tracked.wanted {
            self.dropped(name, detail);
            return false;
        }
        if std::mem::take(&mut tracked.replace) {
            tracked.attempts = 0;
            self.settle(name, tracked);
            return true;
        }
        let again = match tracked.workload.restart {
            RestartPolicy::Always => true,
            RestartPolicy::OnFailure => failed,
            _ => false,
        };
        let ended = if failed {
            WorkloadState::Failed
        } else {
            WorkloadState::Gone
        };
        if !again {
            self.set(name, tracked, ended, detail);
            return true;
        }
        if ran_for >= self.config.reset_after_ms {
            tracked.attempts = 0;
        }
        tracked.attempts += 1;
        if self
            .config
            .max_attempts
            .is_some_and(|most| tracked.attempts > most)
        {
            let gave_up = format!("gave up after {} restarts: {detail}", tracked.attempts - 1);
            self.set(name, tracked, WorkloadState::Failed, &gave_up);
            return true;
        }
        let wait = self.config.wait_ms(tracked.attempts);
        tracked.retry_at = Some(now.saturating_add(wait));
        // A run that ended well and is to run again is waiting to be started: Pending.
        let waiting = if failed {
            WorkloadState::Failed
        } else {
            WorkloadState::Pending
        };
        let detail = format!(
            "restart {} in {wait} ms{}{detail}",
            tracked.attempts,
            if detail.is_empty() { "" } else { ": " }
        );
        self.set(name, tracked, waiting, &detail);
        true
    }

    /// Takes what an executor says of a run it has.
    fn seen(
        &mut self,
        name: &WorkloadName,
        tracked: &mut Tracked,
        state: WorkloadState,
        detail: &str,
        now: u64,
    ) {
        tracked.run = true;
        tracked.retry_at = None;
        if state == WorkloadState::Running {
            tracked.running_since.get_or_insert(now);
        }
        if state == WorkloadState::Stopping || tracked.state == WorkloadState::Stopping {
            // It stops, whoever asked. Once asked to stop it is Stopping until it is gone,
            // whatever it says on the way.
            self.set(name, tracked, WorkloadState::Stopping, detail);
        } else if !tracked.wanted {
            self.stop(name, tracked, "removed");
        } else if tracked.replace {
            self.stop(name, tracked, "stopping to start with new content");
        } else {
            self.set(name, tracked, state, detail);
        }
    }

    /// Says a workload should exist.
    pub fn put(&mut self, workload: Workload, now: u64) -> Put {
        let _ = now;
        let name = workload.name.clone();
        let Some(mut tracked) = self.workloads.remove(&name) else {
            let mut tracked = Tracked {
                workload,
                state: WorkloadState::Pending,
                detail: String::new(),
                wanted: true,
                replace: false,
                run: false,
                attempts: 0,
                retry_at: None,
                running_since: None,
            };
            self.settle(&name, &mut tracked);
            self.workloads.insert(name, tracked);
            return Put::Created;
        };
        let same = tracked.workload == workload;
        let outcome = match (same, tracked.wanted) {
            (true, true) => Put::Unchanged,
            (true, false) => Put::Kept,
            (false, _) => Put::Replaced,
        };
        tracked.wanted = true;
        if !same {
            tracked.workload = workload;
            tracked.attempts = 0;
        }
        if outcome != Put::Unchanged {
            if !tracked.run {
                // Nothing runs: it starts with what it now is.
                tracked.replace = false;
                self.settle(&name, &mut tracked);
            } else if !same || tracked.state == WorkloadState::Stopping {
                // What runs is the old content, or is already on its way out: it is started
                // again when it is gone.
                tracked.replace = true;
                if tracked.state != WorkloadState::Lost {
                    self.stop(&name, &mut tracked, "stopping to start with new content");
                }
            }
        }
        self.workloads.insert(name, tracked);
        outcome
    }

    /// Says a workload should stop existing.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if there is no such workload.
    pub fn remove(&mut self, name: &WorkloadName, force: bool, now: u64) -> Result<Removed, Error> {
        let _ = now;
        let mut tracked = self.workloads.remove(name).ok_or(Error::NotFound)?;
        if force {
            if tracked.run && self.executors.contains(&tracked.workload.kind) {
                self.commands.push(Command::Kill(name.clone()));
            }
            self.dropped(name, "removed by force");
            return Ok(Removed::Dropped);
        }
        if !tracked.run {
            self.dropped(name, "removed");
            return Ok(Removed::Dropped);
        }
        tracked.wanted = false;
        tracked.replace = false;
        let outcome = if tracked.state == WorkloadState::Lost {
            Removed::Waiting
        } else {
            self.stop(name, &mut tracked, "removed");
            Removed::Stopping
        };
        self.workloads.insert(name.clone(), tracked);
        Ok(outcome)
    }

    /// An executor says where one workload is.
    pub fn report(
        &mut self,
        name: &WorkloadName,
        state: WorkloadState,
        detail: &str,
        now: u64,
    ) -> Reported {
        let Some(mut tracked) = self.workloads.remove(name) else {
            return if alive(state) {
                Reported::Orphan
            } else {
                Reported::Ignored
            };
        };
        let mut keep = true;
        let outcome = match state {
            _ if alive(state) => {
                self.seen(name, &mut tracked, state, detail, now);
                Reported::Applied
            }
            WorkloadState::Gone | WorkloadState::Failed if tracked.run => {
                keep = self.ended(
                    name,
                    &mut tracked,
                    state == WorkloadState::Failed,
                    detail,
                    now,
                );
                Reported::Applied
            }
            // The end of a run that is not there, or a state an executor does not decide.
            _ => Reported::Ignored,
        };
        if keep {
            self.workloads.insert(name.clone(), tracked);
        }
        outcome
    }

    /// An executor for `kinds` is here, and has `inventory`. Its runs are adopted by name,
    /// what it should be running and is not is started, and the names it has that Orion does
    /// not want are returned: they are to be stopped.
    pub fn executor_up(
        &mut self,
        kinds: &[WorkloadKind],
        inventory: &[(WorkloadName, WorkloadState)],
        now: u64,
    ) -> Vec<WorkloadName> {
        self.executors.extend(kinds.iter().cloned());
        let names: Vec<WorkloadName> = self
            .workloads
            .iter()
            .filter(|(_, tracked)| kinds.contains(&tracked.workload.kind))
            .map(|(name, _)| name.clone())
            .collect();
        for name in names {
            let Some(mut tracked) = self.workloads.remove(&name) else {
                continue;
            };
            let has = inventory
                .iter()
                .find(|(item, _)| *item == name)
                .map(|(_, state)| *state);
            let mut keep = true;
            match has {
                Some(state) if alive(state) => {
                    self.seen(&name, &mut tracked, state, "", now);
                }
                Some(WorkloadState::Failed) if tracked.run => {
                    keep = self.ended(&name, &mut tracked, true, "", now);
                }
                Some(WorkloadState::Gone) if tracked.run => {
                    keep = self.ended(&name, &mut tracked, false, "", now);
                }
                _ if tracked.run && tracked.state == WorkloadState::Lost => {
                    // Its executor went away with it running, and the executor that is here
                    // now knows nothing of it: the run went with the executor. That is not
                    // the workload failing. (For an executor that never went away, a run
                    // that is not in what it has is one it was just told to start, or one
                    // whose end it reports itself.)
                    tracked.run = false;
                    tracked.replace = false;
                    if tracked.wanted {
                        self.settle(&name, &mut tracked);
                    } else {
                        self.dropped(&name, "its executor no longer has it");
                        keep = false;
                    }
                }
                _ => {
                    if tracked.state == WorkloadState::Pending && tracked.retry_at.is_none() {
                        self.settle(&name, &mut tracked);
                    }
                }
            }
            if keep {
                self.workloads.insert(name, tracked);
            }
        }
        inventory
            .iter()
            .filter(|(name, state)| alive(*state) && !self.workloads.contains_key(name))
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// The executor for `kinds` went away. Nobody knows where its runs are.
    pub fn executor_down(&mut self, kinds: &[WorkloadKind], now: u64) {
        let _ = now;
        for kind in kinds {
            self.executors.remove(kind);
        }
        let names: Vec<WorkloadName> = self
            .workloads
            .iter()
            .filter(|(_, tracked)| tracked.run && kinds.contains(&tracked.workload.kind))
            .map(|(name, _)| name.clone())
            .collect();
        for name in names {
            if let Some(mut tracked) = self.workloads.remove(&name) {
                self.set(
                    &name,
                    &mut tracked,
                    WorkloadState::Lost,
                    "its executor went away",
                );
                self.workloads.insert(name, tracked);
            }
        }
    }

    /// Starts again what is due at `now`.
    pub fn tick(&mut self, now: u64) {
        let due: Vec<WorkloadName> = self
            .workloads
            .iter()
            .filter(|(_, tracked)| tracked.retry_at.is_some_and(|at| at <= now))
            .map(|(name, _)| name.clone())
            .collect();
        for name in due {
            if let Some(mut tracked) = self.workloads.remove(&name) {
                self.settle(&name, &mut tracked);
                self.workloads.insert(name, tracked);
            }
        }
    }
}

//! Workloads on the wire.
//!
//! The tracker (E4) knows what should run and where each run is, and talks to nothing. This
//! is what talks: it takes `WorkloadPut` and `WorkloadRemove` from programs, tells executors
//! to start, stop and kill, takes what they report, and tells whoever asked about a workload
//! where it now is.
//!
//! - **What should run is a record.** A put is kept in the store under the workload's name,
//!   so it is saved, reaches the other nodes, and is still known when node logic starts
//!   again. The record goes when the workload is really gone.
//! - **Pending until an executor offers the kind.** An executor says which kinds it runs and
//!   then everything it has. Only then is anything started on it, and what it already has
//!   is taken back by name instead.
//! - **If Orion restarts, runs carry on.** Node logic starts again knowing what should run
//!   and nothing of what does. Each executor that is still connected is asked to say
//!   everything it has again, and its runs are adopted by name: nothing is started twice.
//! - **If an executor restarts with nothing, its workloads are started again.** While it is
//!   away they are lost: nobody knows.
//! - **A kind has one executor.** A second program that offers a kind another has is refused.
//!
//! Who is told: every session that put or removed a workload hears each change of it until
//! the workload is gone or the session is. Putting a workload again as it is changes nothing
//! and is how to ask where it is.

mod place;

use std::collections::{BTreeMap, BTreeSet};

use orion_msg_records::RecordClass;
use orion_msg_workloads::{
    ExecutorAsk, ExecutorInventory, ExecutorKill, ExecutorOffer, ExecutorReport, ExecutorStart,
    ExecutorStop, WorkloadPut, WorkloadRemove, WorkloadState, WorkloadStatus,
};
use orion_reply::{Asked, Out};
use orion_store::Store;
use orion_types::{Error, FileName, NodeName, WorkloadKind, WorkloadName};
use orion_wire::{Bytes, Decode, kind};
use orion_workloads::{Command, Put, Reported, Tracker, Workload};

pub use orion_workloads::RestartConfig;
pub use place::{LABEL_KIND, LABEL_SCHEDULABLE};
use tracing::{info, warn};

/// The most sessions told about one workload. The oldest goes first.
pub const MAX_ASKERS: usize = 16;

/// What is on the board, as far as starting a workload goes.
pub trait Board {
    /// Whether the file `name` is in place with that digest.
    fn has(&self, name: &FileName, digest: &[u8; 32]) -> bool;
}

/// A board on which every file is there: for a node that keeps no files.
#[derive(Clone, Copy, Debug, Default)]
pub struct Everything;

impl Board for Everything {
    fn has(&self, _name: &FileName, _digest: &[u8; 32]) -> bool {
        true
    }
}

/// An executor: the kinds it offered, and whether it has said what it has.
#[derive(Debug)]
struct Offering {
    kinds: Vec<WorkloadKind>,
    up: bool,
}

/// Which of the files a workload needs is not on the board, as a reason to wait.
fn missing(workload: &Workload, board: &dyn Board) -> Option<String> {
    let absent: Vec<&str> = workload
        .files
        .iter()
        .filter(|(name, digest)| !board.has(name, digest))
        .map(|(name, _)| name.as_str())
        .collect();
    match absent.as_slice() {
        [] => None,
        [one] => Some(format!("waiting for file {one}")),
        [first, rest @ ..] => Some(format!("waiting for file {first} and {} more", rest.len())),
    }
}

/// The workloads of one node.
#[derive(Debug)]
pub struct Workloads {
    me: NodeName,
    tracker: Tracker,
    /// Executors by session.
    executors: BTreeMap<u32, Offering>,
    /// Who is told about each workload.
    askers: BTreeMap<WorkloadName, Vec<u32>>,
    /// Which node runs a workload that names none.
    placement: orion_placement::Coordinator,
    /// Workloads let go because another node runs them: their records stay.
    released: BTreeSet<WorkloadName>,
    /// The kinds this node last said its executors run.
    said_kinds: Option<BTreeSet<WorkloadKind>>,
    /// Something placement depends on changed.
    place_due: bool,
}

impl Workloads {
    /// The kinds answered here.
    pub const KINDS: &'static [u8] = &[
        kind::WORKLOAD_PUT,
        kind::WORKLOAD_REMOVE,
        kind::EXECUTOR_OFFER,
        kind::EXECUTOR_REPORT,
        kind::EXECUTOR_INVENTORY,
    ];

    /// The workloads of the node `me`, with every workload the store has for it waiting to
    /// be adopted or started. Call it once the store is loaded.
    #[must_use]
    pub fn restored(
        me: NodeName,
        config: RestartConfig,
        store: &Store,
        board: &dyn Board,
        now_ms: u64,
    ) -> Self {
        let placement =
            orion_placement::Coordinator::new(me.clone(), orion_placement::DEFAULT_GRACE_MS);
        let mut workloads = Self {
            me,
            tracker: Tracker::new(config),
            executors: BTreeMap::new(),
            askers: BTreeMap::new(),
            placement,
            released: BTreeSet::new(),
            said_kinds: None,
            place_due: true,
        };
        let mut known = 0;
        for record in store.list(RecordClass::Workload, "") {
            let workload =
                WorkloadPut::decode(&record.version.value).and_then(|put| Workload::from_put(&put));
            match workload {
                Ok(workload) if workloads.mine(&workload) => {
                    let why = missing(&workload, board);
                    workloads.tracker.hold(&workload.name, why.as_deref());
                    workloads.tracker.put(workload, now_ms);
                    known += 1;
                }
                Ok(_) => {}
                Err(_) => warn!("the record of workload {} cannot be read", record.key),
            }
        }
        // Nobody asked about these in this life: nobody is told how they came to be here.
        workloads.tracker.take_changes();
        workloads.tracker.take_commands();
        if known != 0 {
            info!("{known} workloads should run here");
        }
        workloads
    }

    /// The tracker, to look at.
    #[must_use]
    pub const fn tracker(&self) -> &Tracker {
        &self.tracker
    }

    /// How many executors are connected.
    #[must_use]
    pub fn executors(&self) -> usize {
        self.executors.len()
    }

    /// When a restart is next due: when to call [`Workloads::tick`].
    #[must_use]
    pub fn next_deadline(&self) -> Option<u64> {
        self.tracker.next_deadline()
    }

    /// Holds back every workload that needs a file the board does not have, saying which,
    /// and lets go those whose files are all there. A workload starts only when every file
    /// it needs is on the board. Call it when a file has been put in place, and now and
    /// then: a file can also go away.
    pub fn review(
        &mut self,
        board: &dyn Board,
        store: &mut Store,
        wall_ms: u64,
        out: &mut dyn Out,
    ) {
        let needing: Vec<(WorkloadName, Option<String>)> = self
            .tracker
            .iter()
            .filter(|(_, tracked)| !tracked.workload.files.is_empty())
            .map(|(name, tracked)| (name.clone(), missing(&tracked.workload, board)))
            .collect();
        for (name, why) in needing {
            self.tracker.hold(&name, why.as_deref());
        }
        self.carry(store, wall_ms, out);
    }

    /// Whether a workload runs on this node: it names this node, or none.
    fn mine(&self, workload: &Workload) -> bool {
        workload.node.as_ref().is_none_or(|node| *node == self.me)
    }

    /// The executor that runs `kind`, if one has said what it has.
    fn executor_for(&self, kind: &WorkloadKind) -> Option<u32> {
        self.executors
            .iter()
            .find(|(_, offering)| offering.up && offering.kinds.contains(kind))
            .map(|(session, _)| *session)
    }

    fn ask(&mut self, name: &WorkloadName, session: u32) {
        let askers = self.askers.entry(name.clone()).or_default();
        askers.retain(|asker| *asker != session);
        if askers.len() >= MAX_ASKERS {
            askers.remove(0);
        }
        askers.push(session);
    }

    fn tell_one(&self, session: u32, name: &WorkloadName, out: &mut dyn Out) {
        let Some(tracked) = self.tracker.get(name) else {
            return;
        };
        let _ = out.send(
            session,
            &WorkloadStatus {
                name: name.clone(),
                state: tracked.state,
                node: Some(self.me.clone()),
                detail: &tracked.detail,
            },
        );
    }

    /// Carries what the tracker decided: commands to executors, changes to whoever asked,
    /// and records that are gone out of the store.
    fn carry(&mut self, store: &mut Store, wall_ms: u64, out: &mut dyn Out) {
        for command in self.tracker.take_commands() {
            let (name, what) = match &command {
                Command::Start(name) => (name, "start"),
                Command::Stop(name) => (name, "stop"),
                Command::Kill(name) => (name, "kill"),
            };
            let Some(tracked) = self.tracker.get(name) else {
                // A kill of what was removed by force: the record is gone, the kind is not
                // known any more. Every executor is told; the one that has it acts.
                if matches!(command, Command::Kill(_)) {
                    for session in self.executors.keys() {
                        let _ = out.send(*session, &ExecutorKill { name: name.clone() });
                    }
                }
                continue;
            };
            let Some(session) = self.executor_for(&tracked.workload.kind) else {
                warn!("no executor to {what} workload {name}");
                continue;
            };
            let _ = match command {
                Command::Start(_) => out.send(
                    session,
                    &ExecutorStart {
                        name: name.clone(),
                        kind: tracked.workload.kind.clone(),
                        spec: Bytes(&tracked.workload.spec),
                        cpus: Bytes(&tracked.workload.cpus),
                    },
                ),
                Command::Stop(_) => out.send(session, &ExecutorStop { name: name.clone() }),
                Command::Kill(_) => out.send(session, &ExecutorKill { name: name.clone() }),
            };
        }
        for change in self.tracker.take_changes() {
            if let Some(askers) = self.askers.get(&change.name) {
                let status = WorkloadStatus {
                    name: change.name.clone(),
                    state: change.state,
                    node: Some(self.me.clone()),
                    detail: &change.detail,
                };
                for asker in askers {
                    let _ = out.send(*asker, &status);
                }
            }
            if change.dropped {
                self.askers.remove(&change.name);
                // Not this node's to say it runs any more.
                store.unobserve(RecordClass::Workload, change.name.as_str(), wall_ms);
                if !self.released.remove(&change.name) {
                    store.delete(RecordClass::Workload, change.name.as_str(), wall_ms);
                }
            }
        }
    }

    /// Answers one request. `now_ms` is on a clock that only goes forward, for restarts;
    /// `wall_ms` is the time of day, for the store.
    ///
    /// # Errors
    ///
    /// [`Error`] for the caller, as a refusal.
    pub fn request(
        &mut self,
        asked: &Asked<'_>,
        store: &mut Store,
        board: &dyn Board,
        now_ms: u64,
        wall_ms: u64,
        out: &mut dyn Out,
    ) -> Result<(), Error> {
        let from = asked.session;
        match asked.kind {
            kind::WORKLOAD_PUT => {
                let put: WorkloadPut<'_> = asked.body()?;
                let workload = Workload::from_put(&put)?;
                let name = workload.name.clone();
                // What should run is a record: kept, saved, and known to the other nodes.
                let same = store
                    .get(RecordClass::Workload, name.as_str())
                    .is_some_and(|version| version.value == asked.payload);
                if !same {
                    store.put(RecordClass::Workload, name.as_str(), asked.payload, wall_ms);
                }
                self.ask(&name, from);
                if workload.node.is_none() {
                    // Which node runs it is placement's to say. A put that changes nothing
                    // here is told nothing by the tracker: the asker is told where it is.
                    let unchanged = self
                        .tracker
                        .get(&name)
                        .is_some_and(|tracked| tracked.workload == workload);
                    self.place(store, board, now_ms, wall_ms, out);
                    if self.tracker.get(&name).is_some() {
                        if unchanged {
                            self.tell_one(from, &name, out);
                        }
                        return Ok(());
                    }
                    let owner = place::assigned(
                        store,
                        &place::view(store),
                        &orion_placement::Needs::of(&workload),
                    );
                    let detail = owner.as_ref().map_or_else(
                        || "another node is to take it".to_owned(),
                        |node| format!("it runs on node {node}"),
                    );
                    return out.send(
                        from,
                        &WorkloadStatus {
                            name,
                            state: WorkloadState::Pending,
                            node: owner,
                            detail: &detail,
                        },
                    );
                }
                if !self.mine(&workload) {
                    // Another node's to run. If it ran here, it no longer should; its
                    // record stays.
                    if self.tracker.get(&name).is_some() {
                        self.released.insert(name.clone());
                        let _ = self.tracker.remove(&name, false, now_ms);
                    }
                    self.carry(store, wall_ms, out);
                    let detail = format!(
                        "it is for node {}",
                        workload.node.as_ref().map_or("", NodeName::as_str)
                    );
                    return out.send(
                        from,
                        &WorkloadStatus {
                            name,
                            state: WorkloadState::Pending,
                            node: workload.node.clone(),
                            detail: &detail,
                        },
                    );
                }
                // Held before it is put, so that it is not started without its files.
                let why = missing(&workload, board);
                self.tracker.hold(&name, why.as_deref());
                if self.tracker.put(workload, now_ms) == Put::Unchanged {
                    // Nothing changed, so nothing is said by the tracker: the asker is told
                    // where it is.
                    self.tell_one(from, &name, out);
                }
                self.carry(store, wall_ms, out);
                Ok(())
            }
            kind::WORKLOAD_REMOVE => {
                let remove: WorkloadRemove = asked.body()?;
                if self.tracker.get(&remove.name).is_none() {
                    // Not run here. If there is a record of it, for another node, it goes.
                    return store
                        .delete(RecordClass::Workload, remove.name.as_str(), wall_ms)
                        .map(|_| ())
                        .ok_or(Error::NotFound);
                }
                self.ask(&remove.name, from);
                self.tracker.remove(&remove.name, remove.force, now_ms)?;
                self.carry(store, wall_ms, out);
                Ok(())
            }
            kind::EXECUTOR_OFFER => {
                let offer: ExecutorOffer<'_> = asked.body()?;
                let mut kinds = Vec::new();
                for offered in offer.kinds.iter() {
                    kinds.push(offered?.kind);
                }
                let taken = self.executors.iter().any(|(session, offering)| {
                    *session != from && offering.kinds.iter().any(|kind| kinds.contains(kind))
                });
                if taken {
                    return Err(Error::AlreadyExists);
                }
                if let Some(before) = self.executors.insert(from, Offering { kinds, up: false })
                    && before.up
                {
                    // It offers anew: what it had is not known until it says.
                    self.tracker.executor_down(&before.kinds, now_ms);
                    self.carry(store, wall_ms, out);
                }
                // Whatever it said before may have been to an Orion that is gone, and what
                // it says next is everything: ask.
                let kinds = self
                    .executors
                    .get(&from)
                    .map_or(0, |offering| offering.kinds.len().min(255) as u8);
                out.send(from, &ExecutorAsk { kinds })
            }
            kind::EXECUTOR_INVENTORY => {
                let inventory: ExecutorInventory<'_> = asked.body()?;
                let mut has = Vec::new();
                for item in inventory.workloads.iter() {
                    let item = item?;
                    has.push((item.name, item.state));
                }
                let offering = self.executors.get_mut(&from).ok_or(Error::Denied)?;
                offering.up = true;
                let kinds = offering.kinds.clone();
                let strays = self.tracker.executor_up(&kinds, &has, now_ms);
                // What this node can run changed.
                self.place_due = true;
                for stray in strays {
                    // It runs something Orion does not want.
                    let _ = out.send(from, &ExecutorStop { name: stray });
                }
                self.carry(store, wall_ms, out);
                Ok(())
            }
            kind::EXECUTOR_REPORT => {
                let report: ExecutorReport<'_> = asked.body()?;
                let offering = self.executors.get(&from).ok_or(Error::Denied)?;
                if let Some(tracked) = self.tracker.get(&report.name)
                    && !offering.kinds.contains(&tracked.workload.kind)
                {
                    // Not the executor of that workload's kind.
                    return Err(Error::Denied);
                }
                if !offering.up {
                    // It has not said what it has yet: that will say this too.
                    return Ok(());
                }
                let reported =
                    self.tracker
                        .report(&report.name, report.state, report.detail, now_ms);
                if reported == Reported::Orphan {
                    let _ = out.send(from, &ExecutorStop { name: report.name });
                }
                self.carry(store, wall_ms, out);
                Ok(())
            }
            _ => Err(Error::Unsupported),
        }
    }

    /// A session is gone. An executor that goes leaves its runs lost. A program that goes is
    /// no longer told anything.
    pub fn gone(
        &mut self,
        session: u32,
        store: &mut Store,
        now_ms: u64,
        wall_ms: u64,
        out: &mut dyn Out,
    ) {
        for askers in self.askers.values_mut() {
            askers.retain(|asker| *asker != session);
        }
        if let Some(offering) = self.executors.remove(&session)
            && offering.up
        {
            self.tracker.executor_down(&offering.kinds, now_ms);
            self.place_due = true;
            self.carry(store, wall_ms, out);
        }
    }

    /// Forgets every session: the comm core they came through is gone. Every run is lost
    /// until its executor is back.
    pub fn forget_sessions(&mut self, now_ms: u64) {
        self.askers.clear();
        self.place_due = true;
        for (_, offering) in std::mem::take(&mut self.executors) {
            if offering.up {
                self.tracker.executor_down(&offering.kinds, now_ms);
            }
        }
        // Nobody is left to tell or to command.
        self.tracker.take_changes();
        self.tracker.take_commands();
    }

    /// Starts again what is due.
    pub fn tick(&mut self, store: &mut Store, now_ms: u64, wall_ms: u64, out: &mut dyn Out) {
        self.tracker.tick(now_ms);
        self.carry(store, wall_ms, out);
    }
}

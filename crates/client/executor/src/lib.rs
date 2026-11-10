//! The client's executor role.
//!
//! An executor is any program that connects and says "I run this kind of workload". Orion
//! decides what should run and tells it; the executor starts and stops what it is told and
//! reports what is true. It keeps no record of its own of what should be running.
//!
//! A program implements [`Executor`] and hands it to [`Serving`]:
//!
//! - [`Serving::start`] says which kinds the program runs and everything it is running now.
//! - [`Serving::turn`] takes what Orion has asked since the last turn (start, stop, kill),
//!   calls the executor for each, then asks the executor what it has and reports every
//!   workload whose state changed. The executor never has to remember to report: what it
//!   says it has is compared with what Orion was last told.
//! - After the connection is made again, [`Serving::start`] once more sends the whole
//!   inventory, which is how Orion finds runs that carried on without it and takes them back
//!   by name.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt;
use std::rc::Rc;
use std::time::Duration;

use orion_connect::{Client, ClientError, Cx, Handler, Incoming};
use orion_msg_workloads::{
    ExecutorInventory, ExecutorKill, ExecutorOffer, ExecutorReport, ExecutorStart, ExecutorStop,
    InventoryItem, OfferedKind,
};
use orion_wire::{List, kind};

pub use orion_msg_workloads::WorkloadState;
pub use orion_types::{WorkloadKind, WorkloadName};

/// How many requests wait for the executor's next turn. More than this and the oldest
/// request for a start is answered as failed, busy.
pub const REQUEST_QUEUE: usize = 256;

/// Why a workload could not be started.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecError(pub String);

impl fmt::Display for ExecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ExecError {}

/// What a program that runs workloads implements.
pub trait Executor {
    /// The kinds of workload this program runs.
    fn kinds(&self) -> &[WorkloadKind];

    /// Starts the workload `name` from `spec`, which only the executor reads. It need not be
    /// running when this returns: say `Starting` in the inventory until it is.
    ///
    /// # Errors
    ///
    /// Why it could not be started. Orion is told it failed, with this.
    fn start(&mut self, name: &WorkloadName, spec: &[u8]) -> Result<(), ExecError>;

    /// As [`Executor::start`], with the kind and the CPUs the workload may use (a bitmask,
    /// bit 0 the first CPU; empty for any). An executor that runs several kinds or pins its
    /// workloads implements this; the others get `start`.
    ///
    /// # Errors
    ///
    /// As [`Executor::start`].
    fn start_on(
        &mut self,
        name: &WorkloadName,
        _kind: &WorkloadKind,
        spec: &[u8],
        _cpus: &[u8],
    ) -> Result<(), ExecError> {
        self.start(name, spec)
    }

    /// Asks the workload to stop. It is gone when it is no longer in the inventory.
    fn stop(&mut self, name: &WorkloadName);

    /// Ends the workload now.
    fn kill(&mut self, name: &WorkloadName);

    /// Everything this program has, and where each is. This is the truth Orion goes by: a
    /// workload that is not here is gone.
    fn inventory(&self) -> Vec<(WorkloadName, WorkloadState)>;
}

enum Asked {
    Start {
        name: WorkloadName,
        kind: WorkloadKind,
        spec: Vec<u8>,
        cpus: Vec<u8>,
    },
    Stop(WorkloadName),
    Kill(WorkloadName),
}

#[derive(Default)]
struct State {
    asked: VecDeque<Asked>,
}

impl Handler for State {
    fn kinds(&self) -> &'static [u8] {
        &[
            kind::EXECUTOR_START,
            kind::EXECUTOR_STOP,
            kind::EXECUTOR_KILL,
        ]
    }

    fn handle(&mut self, frame: &Incoming<'_>, cx: &mut Cx<'_>) -> Result<(), ClientError> {
        let asked = match frame.kind() {
            kind::EXECUTOR_START => {
                let start: ExecutorStart<'_> = frame.body()?;
                if self.asked.len() >= REQUEST_QUEUE {
                    // The program is not taking its turns. Orion is told now, and tries again
                    // by its own rules, instead of the start waiting unseen.
                    cx.send(&ExecutorReport {
                        name: start.name,
                        state: WorkloadState::Failed,
                        detail: "the executor is busy",
                    })?;
                    return Ok(());
                }
                Asked::Start {
                    name: start.name,
                    kind: start.kind,
                    spec: start.spec.0.to_vec(),
                    cpus: start.cpus.0.to_vec(),
                }
            }
            kind::EXECUTOR_STOP => Asked::Stop(frame.body::<ExecutorStop>()?.name),
            _ => Asked::Kill(frame.body::<ExecutorKill>()?.name),
        };
        // A stop or a kill is always taken: they only ever make less run.
        self.asked.push_back(asked);
        Ok(())
    }
}

/// An executor at work on one session.
pub struct Serving {
    client: Client,
    state: Rc<RefCell<State>>,
    /// What Orion was last told about each workload.
    told: Vec<(WorkloadName, WorkloadState)>,
}

impl fmt::Debug for Serving {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Serving({} workloads)", self.told.len())
    }
}

impl Serving {
    /// An executor on `client`'s session. Call [`Serving::start`] next.
    #[must_use]
    pub fn new(client: &Client) -> Self {
        let state = Rc::new(RefCell::new(State::default()));
        client.add(state.clone());
        Self {
            client: client.clone(),
            state,
            told: Vec::new(),
        }
    }

    /// Says which kinds `executor` runs and everything it has now. Call it once the session
    /// is up, and again each time the session has been made anew: the inventory is how Orion
    /// takes back runs that carried on without it.
    ///
    /// # Errors
    ///
    /// If the connection broke.
    pub fn start(&mut self, executor: &impl Executor) -> Result<(), ClientError> {
        let kinds: Vec<OfferedKind> = executor
            .kinds()
            .iter()
            .map(|kind| OfferedKind { kind: kind.clone() })
            .collect();
        self.client.send(&ExecutorOffer {
            kinds: List::new(&kinds),
        })?;
        let has = executor.inventory();
        let items: Vec<InventoryItem<'_>> = has
            .iter()
            .map(|(name, state)| InventoryItem {
                name: name.clone(),
                state: *state,
                detail: "",
            })
            .collect();
        self.client.send(&ExecutorInventory {
            workloads: List::new(&items),
        })?;
        // Requests from before belong to a session that is gone.
        self.state.borrow_mut().asked.clear();
        self.told = has;
        Ok(())
    }

    fn report(
        &self,
        name: &WorkloadName,
        state: WorkloadState,
        detail: &str,
    ) -> Result<(), ClientError> {
        self.client.send(&ExecutorReport {
            name: name.clone(),
            state,
            detail,
        })?;
        Ok(())
    }

    /// One turn: waits up to `wait` for Orion to ask something, does what was asked, and
    /// reports every workload whose state changed since Orion was last told. Call it in the
    /// program's loop; `wait` is how long a workload that changes by itself (one that exits)
    /// may go unreported.
    ///
    /// # Errors
    ///
    /// If the connection broke. Reconnect, call [`Serving::start`], and carry on.
    pub fn turn(
        &mut self,
        executor: &mut impl Executor,
        wait: Duration,
    ) -> Result<(), ClientError> {
        if self.state.borrow().asked.is_empty() {
            self.client.pump(wait)?;
        }
        self.client.drain()?;
        loop {
            let Some(asked) = self.state.borrow_mut().asked.pop_front() else {
                break;
            };
            match asked {
                Asked::Start {
                    name,
                    kind,
                    spec,
                    cpus,
                } => {
                    if let Err(error) = executor.start_on(&name, &kind, &spec, &cpus) {
                        self.report(&name, WorkloadState::Failed, &error.0)?;
                        // What was told is what the inventory is compared with next.
                        self.told.retain(|(told, _)| *told != name);
                        self.told.push((name, WorkloadState::Failed));
                    }
                }
                Asked::Stop(name) => executor.stop(&name),
                Asked::Kill(name) => executor.kill(&name),
            }
        }
        let has = executor.inventory();
        for (name, state) in &has {
            if !self
                .told
                .iter()
                .any(|(told, was)| told == name && was == state)
            {
                self.report(name, *state, "")?;
            }
        }
        for (name, was) in &self.told {
            // No longer in the inventory: gone, unless it was last told as failed to start,
            // which never was in it.
            let still = has.iter().any(|(now, _)| now == name);
            if !still && !matches!(was, WorkloadState::Gone | WorkloadState::Failed) {
                self.report(name, WorkloadState::Gone, "")?;
            }
        }
        self.told = has;
        Ok(())
    }
}

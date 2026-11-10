//! Saying what should run, and hearing where it is.
//!
//! Orion holds what should be running and tracks every run. A program says a workload should
//! exist, or should stop existing, and is told where it is as that changes: pending,
//! starting, running, stopping, gone, or failed or lost.

use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;
use std::time::{Duration, Instant};

use orion_connect::{Client, ClientError, Cx, Handler, Incoming};
use orion_msg_workloads::{WorkloadPut, WorkloadRemove, WorkloadStatus};
use orion_wire::{Bytes, List, kind};

pub use orion_msg_workloads::{RestartPolicy, WorkloadState};

/// A workload that should exist.
#[derive(Clone, Copy, Debug)]
pub struct Workload<'a> {
    /// Its name.
    pub name: &'a str,
    /// Its kind, which picks the executor.
    pub kind: &'a str,
    /// What to run, which only the executor reads.
    pub spec: &'a [u8],
    /// A node it must run on, if it must be that one.
    pub node: Option<&'a str>,
    /// What to do when it ends.
    pub restart: RestartPolicy,
}

/// Where a workload is, as Orion last said.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tracked {
    /// The workload.
    pub name: String,
    /// Where it is.
    pub state: WorkloadState,
    /// The node it is on, if it has one.
    pub node: Option<String>,
    /// A description for people.
    pub detail: String,
}

#[derive(Default)]
struct State {
    tracked: Vec<Tracked>,
}

impl Handler for State {
    fn kinds(&self) -> &'static [u8] {
        &[kind::WORKLOAD_STATUS]
    }

    fn handle(&mut self, frame: &Incoming<'_>, _cx: &mut Cx<'_>) -> Result<(), ClientError> {
        let status: WorkloadStatus<'_> = frame.body()?;
        let now = Tracked {
            name: status.name.to_string(),
            state: status.state,
            node: status.node.map(|node| node.to_string()),
            detail: status.detail.to_owned(),
        };
        match self
            .tracked
            .iter_mut()
            .find(|tracked| tracked.name == now.name)
        {
            Some(tracked) => *tracked = now,
            None => self.tracked.push(now),
        }
        Ok(())
    }
}

/// The workloads this session has asked about.
#[derive(Clone)]
pub struct Workloads {
    client: Client,
    state: Rc<RefCell<State>>,
}

impl fmt::Debug for Workloads {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Workloads")
    }
}

impl Workloads {
    pub(crate) fn new(client: &Client) -> Self {
        let state = Rc::new(RefCell::new(State::default()));
        client.add(state.clone());
        Self {
            client: client.clone(),
            state,
        }
    }

    /// Says `workload` should exist. Where it is follows, and keeps following as it changes.
    ///
    /// # Errors
    ///
    /// If a name is not one, or the connection broke. A refusal is found by
    /// [`Workloads::wait`].
    pub fn put(&self, workload: &Workload<'_>) -> Result<(), ClientError> {
        let name = |_| ClientError::Protocol("a name that is not one");
        self.forget(workload.name);
        self.client.send(&WorkloadPut {
            name: workload.name.parse().map_err(name)?,
            kind: workload.kind.parse().map_err(name)?,
            spec: Bytes(workload.spec),
            files: List::new(&[]),
            node: workload.node.map(str::parse).transpose().map_err(name)?,
            labels: List::new(&[]),
            cpus: Bytes(&[]),
            restart: workload.restart,
        })?;
        Ok(())
    }

    /// Says a workload should stop existing. Its record stays until the run is really gone,
    /// unless `force` is set: that is for a process that will not die.
    ///
    /// # Errors
    ///
    /// If the name is not one, or the connection broke.
    pub fn remove(&self, name: &str, force: bool) -> Result<(), ClientError> {
        self.client.send(&WorkloadRemove {
            name: name
                .parse()
                .map_err(|_| ClientError::Protocol("a name that is not one"))?,
            force,
        })?;
        Ok(())
    }

    fn forget(&self, name: &str) {
        self.state
            .borrow_mut()
            .tracked
            .retain(|tracked| tracked.name != name);
    }

    /// Where the workload `name` is, as Orion last said. It does not wait.
    ///
    /// # Errors
    ///
    /// If the connection broke.
    pub fn status(&self, name: &str) -> Result<Option<Tracked>, ClientError> {
        self.client.drain()?;
        Ok(self
            .state
            .borrow()
            .tracked
            .iter()
            .find(|tracked| tracked.name == name)
            .cloned())
    }

    /// Waits up to `wait` until `reached` says the workload is where the caller wants it,
    /// and returns where it is then. `None` if it never got there in time.
    ///
    /// # Errors
    ///
    /// If the connection broke.
    pub fn wait(
        &self,
        name: &str,
        wait: Duration,
        mut reached: impl FnMut(&Tracked) -> bool,
    ) -> Result<Option<Tracked>, ClientError> {
        let deadline = Instant::now() + wait;
        loop {
            if let Some(tracked) = self.status(name)?
                && reached(&tracked)
            {
                return Ok(Some(tracked));
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Ok(None);
            }
            self.client.pump(left)?;
        }
    }
}

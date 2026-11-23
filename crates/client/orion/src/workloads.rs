//! Saying what should run, and hearing where it is.
//!
//! Orion holds what should be running and tracks every run. A program says a workload should
//! exist, or should stop existing, and is told where it is as that changes: pending,
//! starting, running, stopping, gone, or failed or lost.

use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;
use std::time::{Duration, Instant};

use orion_connect::{Client, ClientError, Cx, Feature, Handler, Incoming};
use orion_msg_workloads::{NeededFile, WorkloadPut, WorkloadRemove, WorkloadStatus};
use orion_types::HlcTimestamp;
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
    /// The stamp of the workload's record as the node holds it: what a later
    /// [`Workloads::put_based_on`] is based on. Zero from a node below level 7, or when the
    /// node holds no record of it.
    pub stamp: HlcTimestamp,
}

#[derive(Default)]
struct State {
    tracked: Vec<Tracked>,
    /// Each workload said, and the request that said it last: its refusal is found there.
    said: Vec<(String, u16)>,
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
            stamp: status.stamp,
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
    /// If a name is not one, or the connection broke. A refusal, `busy` among them, is found
    /// by [`Workloads::wait`].
    pub fn put(&self, workload: &Workload<'_>) -> Result<(), ClientError> {
        self.put_needing(workload, &[])
    }

    /// As [`Workloads::put`], for a workload that needs files: each a destination on the
    /// board and the SHA-256 of what must be there. Orion starts the workload only when
    /// every one of them is in place, and says which it is waiting for until then.
    ///
    /// # Errors
    ///
    /// As [`Workloads::put`].
    pub fn put_needing(
        &self,
        workload: &Workload<'_>,
        files: &[(&str, [u8; 32])],
    ) -> Result<(), ClientError> {
        self.send_put(workload, files, None)
    }

    /// As [`Workloads::put_needing`], but only if the workload's record is still the one
    /// this is based on: `based_on` is the stamp it was read with ([`Tracked::stamp`]), or
    /// zero to make a workload and never replace one. If another wrote it since, nothing is
    /// written and [`Workloads::wait`] gives the refusal ([`ClientError::is_stale`]).
    ///
    /// The node that takes the write checks this. It holds for writers through one board.
    /// Two boards that each take a write based on the same stamp both make it, and the later
    /// stands.
    ///
    /// # Errors
    ///
    /// As [`Workloads::put`]. [`ClientError::Level`], with nothing sent, from a node below
    /// level 7: it would write whatever is there.
    pub fn put_based_on(
        &self,
        workload: &Workload<'_>,
        files: &[(&str, [u8; 32])],
        based_on: HlcTimestamp,
    ) -> Result<(), ClientError> {
        self.client.needs(Feature::BasedOn)?;
        self.send_put(workload, files, Some(based_on))
    }

    fn send_put(
        &self,
        workload: &Workload<'_>,
        files: &[(&str, [u8; 32])],
        based_on: Option<HlcTimestamp>,
    ) -> Result<(), ClientError> {
        let name = |_| ClientError::Protocol("a name that is not one");
        let mut needed = Vec::with_capacity(files.len());
        for (file, digest) in files {
            needed.push(NeededFile {
                name: file.parse().map_err(name)?,
                digest: *digest,
            });
        }
        self.forget(workload.name);
        let seq = self.client.send(&WorkloadPut {
            name: workload.name.parse().map_err(name)?,
            kind: workload.kind.parse().map_err(name)?,
            spec: Bytes(workload.spec),
            files: List::new(&needed),
            node: workload.node.map(str::parse).transpose().map_err(name)?,
            labels: List::new(&[]),
            cpus: Bytes(&[]),
            restart: workload.restart,
            based_on,
        })?;
        self.said(workload.name, seq);
        Ok(())
    }

    /// Keeps the request that last said something of `name`: its refusal is found there.
    fn said(&self, name: &str, seq: u16) {
        let mut state = self.state.borrow_mut();
        state.said.retain(|(said, _)| said != name);
        state.said.push((name.to_owned(), seq));
    }

    /// Says a workload should stop existing. Its record stays until the run is really gone,
    /// unless `force` is set: that is for a process that will not die.
    ///
    /// # Errors
    ///
    /// If the name is not one, or the connection broke.
    pub fn remove(&self, name: &str, force: bool) -> Result<(), ClientError> {
        self.send_remove(name, force, None)
    }

    /// As [`Workloads::remove`], but only if the workload's record is still the one this is
    /// based on, as [`Workloads::put_based_on`]. A refusal is found by [`Workloads::wait`].
    ///
    /// # Errors
    ///
    /// As [`Workloads::remove`]. [`ClientError::Level`], with nothing sent, from a node
    /// below level 7.
    pub fn remove_based_on(
        &self,
        name: &str,
        force: bool,
        based_on: HlcTimestamp,
    ) -> Result<(), ClientError> {
        self.client.needs(Feature::BasedOn)?;
        self.send_remove(name, force, Some(based_on))
    }

    fn send_remove(
        &self,
        name: &str,
        force: bool,
        based_on: Option<HlcTimestamp>,
    ) -> Result<(), ClientError> {
        let seq = self.client.send(&WorkloadRemove {
            name: name
                .parse()
                .map_err(|_| ClientError::Protocol("a name that is not one"))?,
            force,
            based_on,
        })?;
        self.said(name, seq);
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
    /// If the board refused what [`Workloads::put`] said of it (a `busy` refusal,
    /// [`ClientError::is_busy`], is to be said again), or the connection broke.
    pub fn wait(
        &self,
        name: &str,
        wait: Duration,
        mut reached: impl FnMut(&Tracked) -> bool,
    ) -> Result<Option<Tracked>, ClientError> {
        let deadline = Instant::now() + wait;
        loop {
            let said = self
                .state
                .borrow()
                .said
                .iter()
                .find(|(said, _)| said == name)
                .map(|(_, seq)| *seq);
            if let Some(seq) = said {
                self.client.drain()?;
                if let Some(error) = self.client.refusal(seq) {
                    self.state
                        .borrow_mut()
                        .said
                        .retain(|(said, _)| said != name);
                    return Err(error);
                }
            }
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

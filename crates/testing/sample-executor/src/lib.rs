//! The smallest working executor.
//!
//! An executor is any program that connects to Orion and says "I run this kind". Orion tells
//! it what to start, stop and kill; it does that and reports what is true. It keeps no
//! journal: what it has is what its inventory says, and Orion goes by that.
//!
//! This one runs each workload as a shell command. The workload's spec is the command line,
//! given to `sh -c`. It is here for two things: Orion's own tests need an executor that
//! really runs something, and a program that wants to be an executor needs one to read.
//!
//! What it shows, which every executor does:
//!
//! - [`Sample::kinds`] says what it runs.
//! - [`Sample::start`] starts and returns at once. A spec that cannot be started is an
//!   error, which Orion is told as "failed" with the reason.
//! - [`Sample::stop`] asks (here `SIGTERM`), [`Sample::kill`] ends (`SIGKILL`).
//! - [`Sample::inventory`] is the truth. A command that is running is `Running`; one asked
//!   to stop and still there is `Stopping`; one that exited with an error is `Failed` until
//!   it is started again or stopped; one that ended well, or was stopped, is no longer
//!   listed, which is how Orion knows it is gone.
//!
//! [`run`] is the program around it: connect, offer, take turns, and connect again with the
//! same inventory when the connection breaks, so that Orion takes back what kept running.

#![cfg(target_os = "linux")]

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use orion::executor::{ExecError, Executor, WorkloadKind, WorkloadName, WorkloadState};
use orion::{Address, ClientError, Options, Orion, Roles};
use rustix::process::{Pid, Signal, kill_process};

/// One command.
struct Run {
    child: Child,
    /// Whether it was asked to stop: it is then `Stopping` while it is there, and gone
    /// however it ends.
    stopping: bool,
    /// Set once it has exited with an error that nobody asked for.
    failed: bool,
}

/// The executor: the kinds it offers and the commands it has.
pub struct Sample {
    kinds: Vec<WorkloadKind>,
    // The inventory is asked for through a shared reference, and looking at a child to see
    // whether it has exited changes it.
    runs: RefCell<BTreeMap<WorkloadName, Run>>,
}

impl std::fmt::Debug for Sample {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Sample({} commands)", self.runs.borrow().len())
    }
}

fn signal(child: &Child, signal: Signal) {
    if let Some(pid) = i32::try_from(child.id()).ok().and_then(Pid::from_raw) {
        // A child that has already exited has nobody to hear it.
        let _ = kill_process(pid, signal);
    }
}

impl Sample {
    /// An executor for `kinds`, with nothing running.
    #[must_use]
    pub const fn new(kinds: Vec<WorkloadKind>) -> Self {
        Self {
            kinds,
            runs: RefCell::new(BTreeMap::new()),
        }
    }

    /// How many commands it has, running or failed.
    #[must_use]
    pub fn len(&self) -> usize {
        self.runs.borrow().len()
    }

    /// Whether it has none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.runs.borrow().is_empty()
    }
}

impl Executor for Sample {
    fn kinds(&self) -> &[WorkloadKind] {
        &self.kinds
    }

    fn start(&mut self, name: &WorkloadName, spec: &[u8]) -> Result<(), ExecError> {
        let command = std::str::from_utf8(spec)
            .ok()
            .map(str::trim)
            .filter(|command| !command.is_empty())
            .ok_or_else(|| ExecError("the spec is not a command line".to_owned()))?;
        let runs = self.runs.get_mut();
        if let Some(old) = runs.get_mut(name) {
            // Started again: whatever was there under the name makes way.
            signal(&old.child, Signal::KILL);
            let _ = old.child.wait();
        }
        let child = Command::new("sh")
            .arg("-c")
            .arg(command)
            .stdin(Stdio::null())
            .spawn()
            .map_err(|error| ExecError(format!("the command could not be started: {error}")))?;
        runs.insert(
            name.clone(),
            Run {
                child,
                stopping: false,
                failed: false,
            },
        );
        Ok(())
    }

    fn stop(&mut self, name: &WorkloadName) {
        let runs = self.runs.get_mut();
        match runs.get_mut(name) {
            // Nothing runs any more: stopping it is forgetting it.
            Some(run) if run.failed => {
                runs.remove(name);
            }
            Some(run) => {
                run.stopping = true;
                signal(&run.child, Signal::TERM);
            }
            None => {}
        }
    }

    fn kill(&mut self, name: &WorkloadName) {
        if let Some(mut run) = self.runs.get_mut().remove(name) {
            signal(&run.child, Signal::KILL);
            let _ = run.child.wait();
        }
    }

    fn inventory(&self) -> Vec<(WorkloadName, WorkloadState)> {
        let mut runs = self.runs.borrow_mut();
        let mut has = Vec::with_capacity(runs.len());
        runs.retain(|name, run| {
            if run.failed {
                has.push((name.clone(), WorkloadState::Failed));
                return true;
            }
            match run.child.try_wait() {
                Ok(None) => {
                    let state = if run.stopping {
                        WorkloadState::Stopping
                    } else {
                        WorkloadState::Running
                    };
                    has.push((name.clone(), state));
                    true
                }
                // It ended because it was asked to, or by itself and well: it is gone.
                Ok(Some(status)) if run.stopping || status.success() => false,
                // It ended badly, or cannot be looked at: failed, and listed so until it is
                // started again or stopped.
                Ok(Some(_)) | Err(_) => {
                    run.failed = true;
                    has.push((name.clone(), WorkloadState::Failed));
                    true
                }
            }
        });
        has
    }
}

impl Drop for Sample {
    fn drop(&mut self) {
        // An executor that ends takes what it ran with it.
        for run in self.runs.get_mut().values_mut() {
            signal(&run.child, Signal::KILL);
            let _ = run.child.wait();
        }
    }
}

/// Runs `sample` as an executor on the board's socket until `keep_going` says to stop. When
/// the connection breaks it connects again and says what it still has.
///
/// # Errors
///
/// If the comm core says no: connecting again would not change that.
pub fn run(
    socket: &Path,
    sample: &mut Sample,
    mut keep_going: impl FnMut() -> bool,
) -> Result<(), ClientError> {
    let address = Address::Local(socket.to_owned());
    while keep_going() {
        let options = Options {
            roles: Roles::CLIENT.and(Roles::EXECUTOR),
            ..Options::default()
        };
        let orion = match Orion::connect_with(&address, "sample-executor", options) {
            Ok(orion) => orion,
            Err(orion::Error::Client(error)) if error.is_lost() => {
                std::thread::sleep(Duration::from_millis(50));
                continue;
            }
            Err(orion::Error::Client(error)) => return Err(error),
            Err(other) => return Err(ClientError::Io(std::io::Error::other(other.to_string()))),
        };
        let mut serving = orion.executor();
        if serving.start(sample).is_err() {
            continue;
        }
        while keep_going() {
            match serving.turn(sample, Duration::from_millis(20)) {
                Ok(()) => {}
                // The comm core went away. What runs keeps running, and is offered again.
                Err(error) if error.is_lost() => break,
                Err(error) => return Err(error),
            }
        }
    }
    Ok(())
}

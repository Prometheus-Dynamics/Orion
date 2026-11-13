//! The client's actions.
//!
//! An action does one thing once: move an arm, restart a service, start an update. Orion
//! ships almost none. The list is open: any program that connects can add its own with
//! [`Actions::register`], and it must say who may run it. That policy is what the one check
//! decides every run by, so a registered action cannot be reached around it.
//!
//! [`Actions::run`] starts an action and returns at once with a [`Run`]: follow its progress,
//! wait for how it ended, or [`Run::cancel`] it. A run started `detached` carries on if the
//! caller goes away, and [`Actions::attach`] comes back to it.
//!
//! A program that registered an action is handed each run of it as a [`Request::Run`], says
//! how it is doing with [`Registration::progress`] and how it ended with
//! [`Registration::finish`]. It is told when the caller asks to stop.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt;
use std::rc::Rc;
use std::time::{Duration, Instant};

use orion_connect::{Client, ClientError, Cx, Handler, Incoming};
use orion_msg_actions::{
    ActionResult, Attach, CallerGone, Cancel, Claim, Progress, Register, Registered,
    Run as RunMessage,
};
use orion_wire::{Bytes, kind};

pub use orion_check::{Admits, Policy};
pub use orion_msg_actions::{CancelReason, Outcome, Target};

/// How many runs wait for a program to take them before more are turned away as busy.
pub const CLAIM_QUEUE: usize = 64;

/// An action to run.
#[derive(Clone, Debug)]
pub struct ActionCall<'a> {
    /// The action's name.
    pub name: &'a str,
    /// What to run it against.
    pub target: Target,
    /// Its arguments, which only the action reads.
    pub args: &'a [u8],
    /// When to give up, in milliseconds from now. Zero for no deadline.
    pub deadline_ms: u64,
    /// Whether it carries on if this program goes away.
    pub detached: bool,
}

/// An action to add to the list.
#[derive(Clone, Debug)]
pub struct ActionDecl<'a> {
    /// Its name.
    pub name: &'a str,
    /// What it applies to.
    pub target: Target,
    /// A description for people.
    pub description: &'a str,
}

/// How a run is doing.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Doing {
    /// Percent done, or 255 if the action does not say.
    pub percent: u8,
    /// What it is doing now.
    pub message: String,
    /// Whatever else the action reports.
    pub data: Vec<u8>,
}

/// How a run ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Finished {
    /// How it ended.
    pub outcome: Outcome,
    /// What it returned.
    pub output: Vec<u8>,
    /// A description for people if it did not succeed.
    pub detail: String,
}

/// News of a run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Update {
    /// It is under way.
    Doing(Doing),
    /// It ended.
    Finished(Finished),
}

/// What a program that registered an action is asked.
// A run is most of what is asked, and a program matches on its fields: not boxed.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    /// Run the action.
    Run {
        /// The run, to name in [`Registration::progress`] and [`Registration::finish`].
        call: u64,
        /// The action.
        name: String,
        /// What to run it against.
        target: Target,
        /// Its arguments.
        args: Vec<u8>,
        /// When to give up, in milliseconds. Zero for no deadline.
        deadline_ms: u64,
        /// Who asked for the run, as the audit log names them: `local:<uid>`,
        /// `operator:<key>`, `peer:<node>`, `device:<name>` or `anonymous`. Empty from a node
        /// that does not say. A run forwarded from another node was asked for by that node:
        /// `peer:<node>`.
        caller: String,
        /// The token of the session that asked: never the same for two sessions while the
        /// node runs, also when one identity connects twice. [`Request::CallerGone`] names
        /// it when that session ends. Zero from a node that does not say. A run forwarded
        /// from another node was asked for by that node's session.
        session: u64,
    },
    /// Stop this run. Finish it as [`Outcome::Cancelled`], or as whatever it had become.
    Cancel {
        /// The run.
        call: u64,
        /// Why: asked, its caller gone, its deadline passed, node logic stopping. `Unsaid`
        /// from a host that does not say.
        reason: CancelReason,
    },
    /// The session with this token ended, for whatever cause. Said once to each program that
    /// was given a run from it, whether its runs are still going or not: a detached run goes
    /// on until its deadline. Let go of whatever is held for that caller.
    CallerGone {
        /// The session's token, as [`Request::Run`] gave it.
        session: u64,
    },
}

/// An action this program registered.
struct Listed {
    name: String,
    target: Target,
    policy: Policy,
    description: String,
}

/// One run this program started, as far as it has been told.
struct Following {
    call: u64,
    /// The newest progress not yet given to the program.
    doing: Option<Doing>,
    finished: Option<Finished>,
}

#[derive(Default)]
struct State {
    runs: Vec<Following>,
    /// The `seq` of each registration that has been confirmed.
    registered: Vec<u16>,
    /// What this program registered, to register again after a reconnect.
    listed: Vec<Listed>,
    requests: VecDeque<Request>,
    next_call: u64,
}

impl State {
    /// Whether the registration sent as `seq` was confirmed. Its confirmation is used up, so
    /// a later request that comes round to the same number waits for its own.
    fn taken(&mut self, seq: u16) -> bool {
        let at = self
            .registered
            .iter()
            .position(|confirmed| *confirmed == seq);
        at.map(|at| self.registered.swap_remove(at)).is_some()
    }
}

impl Handler for State {
    fn kinds(&self) -> &'static [u8] {
        &[
            kind::ACTION_PROGRESS,
            kind::ACTION_RESULT,
            kind::ACTION_CLAIM,
            kind::ACTION_CANCEL,
            kind::ACTION_REGISTERED,
            kind::ACTION_CALLER_GONE,
        ]
    }

    fn handle(&mut self, frame: &Incoming<'_>, cx: &mut Cx<'_>) -> Result<(), ClientError> {
        match frame.kind() {
            kind::ACTION_PROGRESS => {
                let progress: Progress<'_> = frame.body()?;
                if let Some(run) = self.runs.iter_mut().find(|run| run.call == progress.call) {
                    // Only the newest matters: how it is doing, not how it was.
                    run.doing = Some(Doing {
                        percent: progress.percent,
                        message: progress.message.to_owned(),
                        data: progress.data.0.to_vec(),
                    });
                }
            }
            kind::ACTION_RESULT => {
                let result: ActionResult<'_> = frame.body()?;
                if let Some(run) = self.runs.iter_mut().find(|run| run.call == result.call) {
                    run.finished = Some(Finished {
                        outcome: result.outcome,
                        output: result.output.0.to_vec(),
                        detail: result.detail.to_owned(),
                    });
                }
            }
            kind::ACTION_CLAIM => {
                let claim: Claim<'_> = frame.body()?;
                if self.requests.len() >= CLAIM_QUEUE {
                    // The program is not taking its runs. Say so now, where the caller can
                    // hear it, instead of holding the run until it times out.
                    cx.send(&ActionResult {
                        call: claim.call,
                        outcome: Outcome::Rejected,
                        output: Bytes(&[]),
                        detail: "the action's program is busy",
                    })?;
                    return Ok(());
                }
                self.requests.push_back(Request::Run {
                    call: claim.call,
                    name: claim.name.to_string(),
                    target: claim.target,
                    args: claim.args.0.to_vec(),
                    deadline_ms: claim.deadline_ms,
                    caller: claim.caller.to_owned(),
                    session: claim.session,
                });
            }
            kind::ACTION_CANCEL => {
                let cancel: Cancel = frame.body()?;
                self.requests.push_back(Request::Cancel {
                    call: cancel.call,
                    reason: cancel.reason,
                });
            }
            kind::ACTION_CALLER_GONE => {
                let gone: CallerGone = frame.body()?;
                self.requests.push_back(Request::CallerGone {
                    session: gone.session,
                });
            }
            _ => {
                let registered: Registered = frame.body()?;
                self.registered.push(registered.request_seq);
            }
        }
        Ok(())
    }
}

/// The actions of one session.
#[derive(Clone)]
pub struct Actions {
    client: Client,
    state: Rc<RefCell<State>>,
}

impl fmt::Debug for Actions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Actions")
    }
}

impl Actions {
    /// The actions of `client`'s session.
    #[must_use]
    pub fn new(client: &Client) -> Self {
        let state = Rc::new(RefCell::new(State::default()));
        client.add(state.clone());
        Self {
            client: client.clone(),
            state,
        }
    }

    fn follow(&self, call: u64, seq: u16) -> Run {
        self.state.borrow_mut().runs.push(Following {
            call,
            doing: None,
            finished: None,
        });
        Run {
            client: self.client.clone(),
            state: self.state.clone(),
            call,
            seq,
        }
    }

    /// Starts an action and returns at once. Whether it may run is decided where it runs,
    /// by the policy it was registered with; a refusal is found on the run's first read.
    ///
    /// # Errors
    ///
    /// If the name is not one, or the connection broke.
    pub fn run(&self, action: &ActionCall<'_>) -> Result<Run, ClientError> {
        let call = {
            let mut state = self.state.borrow_mut();
            state.next_call += 1;
            state.next_call
        };
        let seq = self.client.send(&RunMessage {
            call,
            name: action
                .name
                .parse()
                .map_err(|_| ClientError::Protocol("an action name that is not one"))?,
            target: action.target.clone(),
            args: Bytes(action.args),
            deadline_ms: action.deadline_ms,
            detached: action.detached,
        })?;
        Ok(self.follow(call, seq))
    }

    /// Comes back to a run that was started detached. Its latest progress, or how it ended,
    /// follows.
    ///
    /// # Errors
    ///
    /// If the connection broke. A run that is not there is found on the first read.
    pub fn attach(&self, call: u64) -> Result<Run, ClientError> {
        let seq = self.client.send(&Attach { call })?;
        Ok(self.follow(call, seq))
    }

    /// Adds an action to the list. `policy` says who may run it, and is required: there is no
    /// action without one.
    ///
    /// # Errors
    ///
    /// If the comm core refuses: the name is taken, or this program may not add actions.
    pub fn register(
        &self,
        action: &ActionDecl<'_>,
        policy: Policy,
    ) -> Result<Registration, ClientError> {
        let seq = self.client.send(&Register {
            name: action
                .name
                .parse()
                .map_err(|_| ClientError::Protocol("an action name that is not one"))?,
            target: action.target.clone(),
            policy,
            description: action.description,
        })?;
        self.client
            .answered(seq, || self.state.borrow_mut().taken(seq))?;
        self.state.borrow_mut().listed.push(Listed {
            name: action.name.to_owned(),
            target: action.target.clone(),
            policy,
            description: action.description.to_owned(),
        });
        Ok(Registration {
            client: self.client.clone(),
            state: self.state.clone(),
        })
    }
}

impl Actions {
    /// Registers every action again with a comm core that has no memory of this session.
    /// Call it after [`orion_connect::Session::reconnect`]. Runs that were under way are not
    /// brought back: whoever started one is told how it ended by whoever now holds it.
    ///
    /// # Errors
    ///
    /// If the comm core refuses an action it took before, or the connection broke again.
    pub fn restore(&self) -> Result<(), ClientError> {
        // An answer from the session that broke is no answer to this one, which may use the
        // same numbers.
        self.state.borrow_mut().registered.clear();
        let count = self.state.borrow().listed.len();
        for index in 0..count {
            let seq = {
                let state = self.state.borrow();
                let listed = &state.listed[index];
                self.client.send(&Register {
                    name: listed
                        .name
                        .parse()
                        .map_err(|_| ClientError::Protocol("an action name that is not one"))?,
                    target: listed.target.clone(),
                    policy: listed.policy,
                    description: &listed.description,
                })?
            };
            self.client
                .answered(seq, || self.state.borrow_mut().taken(seq))?;
        }
        // What was asked of this program before belongs to a session that is gone.
        self.state.borrow_mut().requests.clear();
        Ok(())
    }
}

/// One run of an action, as its caller follows it.
pub struct Run {
    client: Client,
    state: Rc<RefCell<State>>,
    call: u64,
    seq: u16,
}

impl fmt::Debug for Run {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Run({})", self.call)
    }
}

impl Run {
    /// The run's number: what [`Actions::attach`] takes to come back to it.
    #[must_use]
    pub const fn call(&self) -> u64 {
        self.call
    }

    /// The newest news of the run, if there is any since the last look. It does not wait.
    /// Progress is the newest only; how it ended is given once.
    ///
    /// # Errors
    ///
    /// [`ClientError::Refused`] if the run was refused: no such action, or this caller may
    /// not run it. And if the connection broke.
    pub fn poll(&mut self) -> Result<Option<Update>, ClientError> {
        self.client.drain()?;
        if let Some(error) = self.client.refusal(self.seq) {
            return Err(ClientError::Refused(error));
        }
        let mut state = self.state.borrow_mut();
        let Some(run) = state.runs.iter_mut().find(|run| run.call == self.call) else {
            return Ok(None);
        };
        if let Some(doing) = run.doing.take() {
            return Ok(Some(Update::Doing(doing)));
        }
        Ok(run.finished.take().map(Update::Finished))
    }

    /// Waits up to `wait` for news of the run.
    ///
    /// # Errors
    ///
    /// As [`Run::poll`].
    pub fn wait(&mut self, wait: Duration) -> Result<Option<Update>, ClientError> {
        let deadline = Instant::now() + wait;
        loop {
            if let Some(update) = self.poll()? {
                return Ok(Some(update));
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Ok(None);
            }
            self.client.pump(left)?;
        }
    }

    /// Waits up to `wait` for the run to end, passing over its progress.
    ///
    /// # Errors
    ///
    /// As [`Run::poll`], and a timed-out [`ClientError::Io`] if it has not ended in time. It
    /// is still running then, and can be waited for again.
    pub fn finish(&mut self, wait: Duration) -> Result<Finished, ClientError> {
        let deadline = Instant::now() + wait;
        loop {
            match self.wait(deadline.saturating_duration_since(Instant::now()))? {
                Some(Update::Finished(finished)) => return Ok(finished),
                Some(Update::Doing(_)) => {}
                None => return Err(ClientError::Io(std::io::ErrorKind::TimedOut.into())),
            }
        }
    }

    /// Asks for the run to stop. How it ended still comes as its result.
    ///
    /// # Errors
    ///
    /// If the connection broke.
    pub fn cancel(&mut self) -> Result<(), ClientError> {
        self.client.send(&Cancel {
            call: self.call,
            reason: CancelReason::Asked,
        })?;
        Ok(())
    }
}

impl Drop for Run {
    fn drop(&mut self) {
        self.state
            .borrow_mut()
            .runs
            .retain(|run| run.call != self.call);
    }
}

/// This program's registered actions: where their runs arrive.
pub struct Registration {
    client: Client,
    state: Rc<RefCell<State>>,
}

impl fmt::Debug for Registration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Registration")
    }
}

impl Registration {
    /// The next thing asked of this program, if anything is. It does not wait. Runs of every
    /// action the program registered arrive here, in the order they were asked.
    ///
    /// # Errors
    ///
    /// If the connection broke.
    pub fn poll(&mut self) -> Result<Option<Request>, ClientError> {
        self.client.drain()?;
        Ok(self.state.borrow_mut().requests.pop_front())
    }

    /// Waits up to `wait` for the next thing asked.
    ///
    /// # Errors
    ///
    /// If the connection broke.
    pub fn wait(&mut self, wait: Duration) -> Result<Option<Request>, ClientError> {
        let deadline = Instant::now() + wait;
        loop {
            if let Some(request) = self.poll()? {
                return Ok(Some(request));
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Ok(None);
            }
            self.client.pump(left)?;
        }
    }

    /// Says how a run is doing.
    ///
    /// # Errors
    ///
    /// If the connection broke.
    pub fn progress(
        &mut self,
        call: u64,
        percent: u8,
        message: &str,
        data: &[u8],
    ) -> Result<(), ClientError> {
        self.client.send(&Progress {
            call,
            percent,
            message,
            data: Bytes(data),
        })?;
        Ok(())
    }

    /// Says how a run ended. Every run handed to this program must be finished, whatever
    /// became of it.
    ///
    /// # Errors
    ///
    /// If the connection broke.
    pub fn finish(
        &mut self,
        call: u64,
        outcome: Outcome,
        output: &[u8],
        detail: &str,
    ) -> Result<(), ClientError> {
        self.client.send(&ActionResult {
            call,
            outcome,
            output: Bytes(output),
            detail,
        })?;
        Ok(())
    }
}

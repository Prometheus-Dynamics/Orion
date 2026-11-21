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
//! Node logic names each run with a [`RunHandle`], told as the run starts. A caller's numbers
//! start over with each session, so after a session is lost the number may name another run;
//! the handle never does. [`Run::handle`] reads it, [`Actions::attach_handle`] comes back by it,
//! and [`Run::cancel`] names it once it is known. If the session was lost before the handle
//! came, how the run went is not known here: find out from what the action does, never by
//! coming back to the number alone.
//!
//! A program that registered an action is handed each run of it as a [`Request::Run`], says
//! how it is doing with [`Registration::progress`] and how it ended with
//! [`Registration::finish`]. It is told when the caller asks to stop.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt;
use std::rc::Rc;
use std::time::{Duration, Instant};

use orion_connect::{Client, ClientError, Cx, Feature, Handler, Incoming};
use orion_msg_actions::{
    ActionResult, Attach, CallerGone, Cancel, Claim, Progress, Register, Registered,
    Run as RunMessage, Started,
};
use orion_wire::{Bytes, kind};

pub use orion_check::{Admits, Policy};
pub use orion_msg_actions::{CancelReason, Outcome, RunHandle, Target};

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
    /// The action's own status code, for branching on: what it means is the action's to
    /// say. Zero: none given, or a node too old to pass one on.
    pub code: u32,
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
        /// it when that session ends. Zero from a node that does not say. A run carried
        /// from another node has the token of the session that asked for it there.
        session: u64,
        /// The run goes on without its caller: when [`Request::CallerGone`] names its
        /// session, keep it running until it ends or its deadline passes. False from a node
        /// that does not say.
        detached: bool,
        /// A run this program was doing when its session was lost, kept by node logic and
        /// given back after [`Actions::restore`] (an action registered with
        /// [`Actions::register_kept`]): the same `call`, `args` empty. Go on with the job and
        /// finish it.
        kept: bool,
    },
    /// Stop this run. Finish it as [`Outcome::Cancelled`], or as whatever it had become: a run
    /// that finished first says so. Node logic waits a bound for the answer when it stopped
    /// the run itself (a deadline, or node logic stopping); with none by then the run ends
    /// [`Outcome::Indeterminate`], never as cancelled or timed out by assumption.
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
    keep_ms: u32,
}

/// One run this program started, as far as it has been told.
struct Following {
    call: u64,
    /// The run's handle, once node logic has said it.
    handle: Option<RunHandle>,
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
    /// The `seq` each run's finish went as, for [`Registration::refused`].
    finished: Vec<(u64, u16)>,
}

impl State {
    /// The run a frame is about: by the handle it names, else by its number. A run whose
    /// handle is known is never matched by number, which may be another run's now.
    fn about(&mut self, call: u64, instance: u64, run: u64) -> Option<&mut Following> {
        let handle = RunHandle::said(instance, run);
        let at = handle
            .and_then(|handle| self.runs.iter().position(|run| run.handle == Some(handle)))
            .or_else(|| {
                self.runs
                    .iter()
                    .position(|run| run.call == call && run.handle.is_none())
            })?;
        Some(&mut self.runs[at])
    }

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
            kind::ACTION_STARTED,
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
            kind::ACTION_STARTED => {
                let started: Started<'_> = frame.body()?;
                if let Some(run) = self
                    .runs
                    .iter_mut()
                    .find(|run| run.call == started.call && run.handle.is_none())
                {
                    run.handle = RunHandle::said(started.instance, started.run);
                }
            }
            kind::ACTION_PROGRESS => {
                let progress: Progress<'_> = frame.body()?;
                if let Some(run) = self.about(progress.call, progress.instance, progress.run) {
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
                if let Some(run) = self.about(result.call, result.instance, result.run) {
                    run.finished = Some(Finished {
                        outcome: result.outcome,
                        output: result.output.0.to_vec(),
                        detail: result.detail.to_owned(),
                        code: result.code,
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
                        code: 0,
                        instance: 0,
                        run: 0,
                        action: "",
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
                    detached: claim.detached,
                    kept: claim.kept,
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

    fn follow(&self, call: u64, handle: Option<RunHandle>, seq: u16) -> Run {
        self.state.borrow_mut().runs.push(Following {
            call,
            handle,
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
    /// If the name is not one, or the connection broke. [`ClientError::Level`] for a detached run
    /// on a node below [`Feature::DetachedRun`]'s level: nothing is sent.
    pub fn run(&self, action: &ActionCall<'_>) -> Result<Run, ClientError> {
        // An older node takes a detached run with no deadline and no limit, and never tells
        // its program the caller has gone.
        if action.detached {
            self.client.needs(Feature::DetachedRun)?;
        }
        let call = self.next_call();
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
            // Its own session: only another node's node logic speaks for another.
            on_behalf: 0,
        })?;
        Ok(self.follow(call, None, seq))
    }

    fn next_call(&self) -> u64 {
        let mut state = self.state.borrow_mut();
        state.next_call += 1;
        state.next_call
    }

    /// Comes back to a run that was started detached, by its number. Its latest progress, or
    /// how it ended, follows. The number names the run this caller last started under it: a
    /// run started under it before that is reached by [`Actions::attach_handle`] only.
    ///
    /// # Errors
    ///
    /// If the connection broke. A run that is not there is found on the first read.
    pub fn attach(&self, call: u64) -> Result<Run, ClientError> {
        let seq = self.client.send(&Attach {
            call,
            instance: 0,
            run: 0,
        })?;
        Ok(self.follow(call, None, seq))
    }

    /// Comes back to the detached run `handle` names, whatever its number. Its latest
    /// progress, or how it ended, follows. A handle from before node logic last started is
    /// refused with [`orion_types::Error::RunGone`]: that run is gone.
    ///
    /// # Errors
    ///
    /// [`ClientError::Level`] on a node below [`Feature::RunHandle`]'s level: nothing is sent.
    /// If the connection broke. A run that is not there is found on the first read.
    pub fn attach_handle(&self, handle: RunHandle) -> Result<Run, ClientError> {
        // An older node reads the number alone, which may be another run's.
        self.client.needs(Feature::RunHandle)?;
        let call = self.next_call();
        let seq = self.client.send(&Attach {
            call,
            instance: handle.instance,
            run: handle.run,
        })?;
        Ok(self.follow(call, Some(handle), seq))
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
        self.register_keeping(action, policy, 0)
    }

    /// As [`Actions::register`], and node logic keeps this action's detached runs going for
    /// up to `keep` when this program's session is lost (no more than the node's limit, a
    /// minute). After [`Actions::restore`] as the same identity, each run still kept is here
    /// again as a [`Request::Run`] with `kept`; one not back by `keep` ends
    /// [`Outcome::Indeterminate`]. A lost session is the error a call returns, `is_lost`:
    /// hold the jobs of these runs then, do not cancel them.
    ///
    /// # Errors
    ///
    /// [`ClientError::Level`] from a node below [`Feature::KeptRuns`]'s level, and as
    /// [`Actions::register`].
    pub fn register_kept(
        &self,
        action: &ActionDecl<'_>,
        policy: Policy,
        keep: Duration,
    ) -> Result<Registration, ClientError> {
        self.client.needs(Feature::KeptRuns)?;
        let keep_ms = u32::try_from(keep.as_millis()).unwrap_or(u32::MAX).max(1);
        self.register_keeping(action, policy, keep_ms)
    }

    fn register_keeping(
        &self,
        action: &ActionDecl<'_>,
        policy: Policy,
        keep_ms: u32,
    ) -> Result<Registration, ClientError> {
        let seq = self.client.send(&Register {
            name: action
                .name
                .parse()
                .map_err(|_| ClientError::Protocol("an action name that is not one"))?,
            target: action.target.clone(),
            policy,
            description: action.description,
            keep_ms,
        })?;
        self.client
            .answered(seq, || self.state.borrow_mut().taken(seq))?;
        self.state.borrow_mut().listed.push(Listed {
            name: action.name.to_owned(),
            target: action.target.clone(),
            policy,
            description: action.description.to_owned(),
            keep_ms,
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
    /// brought back, but for an action registered with [`Actions::register_kept`]: when this
    /// returns, every run node logic kept for this program is waiting in the [`Registration`]
    /// as a [`Request::Run`] with `kept`. A run it was doing that is not there was not kept.
    ///
    /// # Errors
    ///
    /// If the comm core refuses an action it took before, or the connection broke again.
    pub fn restore(&self) -> Result<(), ClientError> {
        // An answer from the session that broke is no answer to this one, which may use the
        // same numbers, and what was asked of this program then belongs to that session. Runs
        // kept for it arrive with each registration's answer, and stay.
        self.state.borrow_mut().registered.clear();
        self.state.borrow_mut().requests.clear();
        self.state.borrow_mut().finished.clear();
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
                    keep_ms: listed.keep_ms,
                })?
            };
            self.client
                .answered(seq, || self.state.borrow_mut().taken(seq))?;
        }
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

    /// The run's handle, which names it whatever becomes of this session: what
    /// [`Actions::attach_handle`] takes. Known once node logic has said it, which any
    /// [`Run::poll`] or [`Run::wait`] reads. Never known from a node below
    /// [`Feature::RunHandle`]'s level, or for a run carried to another node.
    #[must_use]
    pub fn handle(&self) -> Option<RunHandle> {
        self.state
            .borrow()
            .runs
            .iter()
            .find(|run| run.call == self.call)
            .and_then(|run| run.handle)
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
            return Err(error);
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

    /// Asks for the run to stop. How it ended still comes as its result. It names the run by
    /// its handle once that is known; by its number alone, a node stops only a run this same
    /// session started under it.
    ///
    /// # Errors
    ///
    /// If the connection broke.
    pub fn cancel(&mut self) -> Result<(), ClientError> {
        let handle = self.handle();
        self.client.send(&Cancel {
            call: self.call,
            reason: CancelReason::Asked,
            instance: handle.map_or(0, |handle| handle.instance),
            run: handle.map_or(0, |handle| handle.run),
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
            instance: 0,
            run: 0,
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
        self.finish_with_code(call, outcome, 0, output, detail)
    }

    /// Says how a run ended, with the action's own status `code` for the caller to branch on
    /// (zero for none). What a code means is the action's to define and document; the words
    /// for people still go in `detail`.
    ///
    /// # Errors
    ///
    /// If the connection broke.
    pub fn finish_with_code(
        &mut self,
        call: u64,
        outcome: Outcome,
        code: u32,
        output: &[u8],
        detail: &str,
    ) -> Result<(), ClientError> {
        let seq = self.client.send(&ActionResult {
            call,
            outcome,
            output: Bytes(output),
            detail,
            code,
            instance: 0,
            run: 0,
            action: "",
        })?;
        let mut state = self.state.borrow_mut();
        state.finished.retain(|(finished, _)| *finished != call);
        state.finished.push((call, seq));
        Ok(())
    }

    /// Why node logic refused how run `call` ended, if it did and it has arrived: a kept run
    /// that had already ended (its program not back in time) is refused, naming how it
    /// ended. Read once.
    ///
    /// # Errors
    ///
    /// If the connection broke.
    pub fn refused(&mut self, call: u64) -> Result<Option<ClientError>, ClientError> {
        self.client.drain()?;
        let seq = self
            .state
            .borrow()
            .finished
            .iter()
            .find(|(finished, _)| *finished == call)
            .map(|(_, seq)| *seq);
        Ok(seq.and_then(|seq| self.client.refusal(seq)))
    }
}

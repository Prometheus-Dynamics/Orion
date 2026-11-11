//! The open list of actions.
//!
//! Orion ships almost no actions of its own. Any program that connects adds its own with
//! `Register`, naming what the action runs against, a node or a resource, and who may run it.
//! A `Run` for that name and target is handed to the program as a `Claim`; what the program
//! says of the run, `Progress` and `ActionResult`, goes back to the caller.
//!
//! This list does not decide who may run what. The policy an action was registered with is
//! what the one check is told at the door, so a `Run` only arrives here if it passed. The
//! list routes, and keeps track of runs:
//!
//! - A run has two numbers. The caller's `call` is the caller's own, unique on its session.
//!   The number in `Claim` is the list's, unique here. Each side only ever sees its own.
//! - A caller that goes away has its run cancelled, unless the run is detached.
//! - A program that goes away takes its actions off the list, and its runs end as failed.
//! - A run with a deadline that passes ends as timed out, and the program is told to stop.
//!
//! Everything that happens to a run is also an [`Event`], for whoever shows runs to people.

use orion_msg_actions::{
    ActionResult, Cancel, Claim, Outcome, Progress, Register, Registered, Run, Target,
};
use orion_reply::{Asked, Out};
use orion_types::{ActionName, Error, NodeName};
use orion_wire::{Bytes, kind};

/// The most runs under way at once. More are refused as busy.
pub const MAX_RUNS: usize = 4096;

/// One thing that happened to a run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    /// The run's number on this list.
    pub run: u64,
    /// The action.
    pub name: ActionName,
    /// What it runs against.
    pub target: Target,
    /// Percent done, or 255 if unknown.
    pub percent: u8,
    /// What it is doing, or why it ended as it did.
    pub message: String,
    /// How it ended, once it has.
    pub outcome: Option<Outcome>,
}

#[derive(Debug)]
struct Listed {
    name: ActionName,
    target: Target,
    owner: u32,
    /// The registration as it came: the same again is the same action, anything else is
    /// another program's attempt at the name.
    registration: Vec<u8>,
}

#[derive(Debug)]
struct Running {
    id: u64,
    name: ActionName,
    target: Target,
    owner: u32,
    /// Who is told, and the run's number there. `None` once the caller has gone.
    caller: Option<(u32, u64)>,
    /// When it times out, on the clock `tick` is given.
    deadline: Option<u64>,
    /// Whether it carries on when its caller goes.
    detached: bool,
}

/// The list.
#[derive(Debug)]
pub struct Actions {
    me: NodeName,
    listed: Vec<Listed>,
    running: Vec<Running>,
    next: u64,
    events: Vec<Event>,
}

impl Actions {
    /// The kinds answered here.
    pub const KINDS: &'static [u8] = &[
        kind::ACTION_REGISTER,
        kind::ACTION_RUN,
        kind::ACTION_PROGRESS,
        kind::ACTION_RESULT,
        kind::ACTION_CANCEL,
    ];

    /// An empty list on the node `me`.
    #[must_use]
    pub const fn new(me: NodeName) -> Self {
        Self {
            me,
            listed: Vec::new(),
            running: Vec::new(),
            next: 0,
            events: Vec::new(),
        }
    }

    /// How many actions are on the list.
    #[must_use]
    pub const fn listed(&self) -> usize {
        self.listed.len()
    }

    /// How many runs are under way.
    #[must_use]
    pub const fn running(&self) -> usize {
        self.running.len()
    }

    /// Forgets every action and run: the comm core they came through is gone, and so are the
    /// sessions that had them.
    pub fn forget_sessions(&mut self) {
        self.listed.clear();
        self.running.clear();
    }

    /// Takes what happened to runs since the last call, in order.
    pub fn take_events(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.events)
    }

    /// When the next deadline passes: when to call [`Actions::tick`].
    #[must_use]
    pub fn next_deadline(&self) -> Option<u64> {
        self.running.iter().filter_map(|run| run.deadline).min()
    }

    fn event(&mut self, at: usize, percent: u8, message: &str, outcome: Option<Outcome>) {
        let run = &self.running[at];
        self.events.push(Event {
            run: run.id,
            name: run.name.clone(),
            target: run.target.clone(),
            percent,
            message: message.to_owned(),
            outcome,
        });
    }

    /// Ends the run at `at`: the caller is told, and it is forgotten.
    fn end(&mut self, at: usize, outcome: Outcome, output: &[u8], detail: &str, out: &mut dyn Out) {
        self.event(at, 255, detail, Some(outcome));
        let run = self.running.remove(at);
        if let Some((caller, call)) = run.caller {
            let _ = out.send(
                caller,
                &ActionResult {
                    call,
                    outcome,
                    output: Bytes(output),
                    detail,
                },
            );
        }
    }

    /// Answers one request, at `now_ms` on whatever clock [`Actions::tick`] is given.
    ///
    /// # Errors
    ///
    /// [`Error`] for the caller, as a refusal.
    pub fn request(
        &mut self,
        asked: &Asked<'_>,
        now_ms: u64,
        out: &mut dyn Out,
    ) -> Result<(), Error> {
        let from = asked.session;
        match asked.kind {
            kind::ACTION_REGISTER => {
                let register: Register<'_> = asked.body()?;
                if matches!(&register.target, Target::Node(node) if *node != self.me) {
                    // An action lives where its program is connected.
                    return Err(Error::Unsupported);
                }
                match self
                    .listed
                    .iter()
                    .find(|listed| listed.name == register.name && listed.target == register.target)
                {
                    Some(same) if same.owner == from && same.registration == asked.payload => {}
                    Some(_) => return Err(Error::AlreadyExists),
                    None => self.listed.push(Listed {
                        name: register.name.clone(),
                        target: register.target.clone(),
                        owner: from,
                        registration: asked.payload.to_vec(),
                    }),
                }
                out.send(
                    from,
                    &Registered {
                        request_seq: asked.seq,
                    },
                )
            }
            kind::ACTION_RUN => {
                let run: Run<'_> = asked.body()?;
                let owner = self
                    .listed
                    .iter()
                    .find(|listed| listed.name == run.name && listed.target == run.target)
                    .map(|listed| listed.owner)
                    .ok_or(Error::NotFound)?;
                if self
                    .running
                    .iter()
                    .any(|other| other.caller == Some((from, run.call)))
                {
                    return Err(Error::AlreadyExists);
                }
                if self.running.len() >= MAX_RUNS {
                    return Err(Error::Busy);
                }
                self.next += 1;
                let id = self.next;
                out.send(
                    owner,
                    &Claim {
                        call: id,
                        name: run.name.clone(),
                        target: run.target.clone(),
                        args: run.args,
                        deadline_ms: run.deadline_ms,
                    },
                )?;
                self.running.push(Running {
                    id,
                    name: run.name.clone(),
                    target: run.target.clone(),
                    owner,
                    caller: Some((from, run.call)),
                    deadline: (run.deadline_ms != 0)
                        .then(|| now_ms.saturating_add(run.deadline_ms)),
                    detached: run.detached,
                });
                self.event(self.running.len() - 1, 255, "claimed", None);
                Ok(())
            }
            kind::ACTION_PROGRESS => {
                let progress: Progress<'_> = asked.body()?;
                let Some(at) = self.owned(progress.call, from)? else {
                    return Ok(());
                };
                self.event(at, progress.percent, progress.message, None);
                if let Some((caller, call)) = self.running[at].caller {
                    out.push(
                        caller,
                        &Progress {
                            call,
                            percent: progress.percent,
                            message: progress.message,
                            data: progress.data,
                        },
                    )?;
                }
                Ok(())
            }
            kind::ACTION_RESULT => {
                let result: ActionResult<'_> = asked.body()?;
                let Some(at) = self.owned(result.call, from)? else {
                    return Ok(());
                };
                self.end(at, result.outcome, result.output.0, result.detail, out);
                Ok(())
            }
            kind::ACTION_CANCEL => {
                let cancel: Cancel = asked.body()?;
                let run = self
                    .running
                    .iter()
                    .find(|run| run.caller == Some((from, cancel.call)))
                    .ok_or(Error::NotFound)?;
                out.send(run.owner, &Cancel { call: run.id })
            }
            _ => Err(Error::Unsupported),
        }
    }

    /// The run `id`, if `from` is the program that runs it. `None` for a run that already
    /// ended: a program may well say something of a run a moment after it timed out.
    fn owned(&self, id: u64, from: u32) -> Result<Option<usize>, Error> {
        match self.running.iter().position(|run| run.id == id) {
            Some(at) if self.running[at].owner == from => Ok(Some(at)),
            Some(_) => Err(Error::Denied),
            None if id != 0 && id <= self.next => Ok(None),
            None => Err(Error::NotFound),
        }
    }

    /// A session is gone: its actions leave the list, the runs it was doing end as failed,
    /// and the runs it asked for are cancelled unless they are detached.
    pub fn gone(&mut self, session: u32, out: &mut dyn Out) {
        self.listed.retain(|listed| listed.owner != session);
        let mut at = 0;
        while at < self.running.len() {
            if self.running[at].owner == session {
                self.end(
                    at,
                    Outcome::Failed,
                    &[],
                    "the program that runs it went away",
                    out,
                );
                continue;
            }
            let run = &mut self.running[at];
            if run.caller.is_some_and(|(caller, _)| caller == session) {
                run.caller = None;
                if !run.detached {
                    let _ = out.send(run.owner, &Cancel { call: run.id });
                }
            }
            at += 1;
        }
    }

    /// Ends the runs whose deadline has passed at `now_ms`.
    pub fn tick(&mut self, now_ms: u64, out: &mut dyn Out) {
        let mut at = 0;
        while at < self.running.len() {
            if self.running[at].deadline.is_some_and(|due| due <= now_ms) {
                let (owner, id) = (self.running[at].owner, self.running[at].id);
                let _ = out.send(owner, &Cancel { call: id });
                self.end(at, Outcome::TimedOut, &[], "it ran out of time", out);
            } else {
                at += 1;
            }
        }
    }
}

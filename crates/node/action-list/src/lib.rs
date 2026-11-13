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
//! - A caller that goes away has its run cancelled, unless the run is detached. A detached
//!   run carries on, and whoever started it can come back to it with `Attach`, from any
//!   session: it is then told the latest progress, or how the run ended.
//! - A program that goes away takes its actions off the list, and its runs end as failed.
//! - A run with a deadline that passes ends as timed out, and the program is told to stop.
//!
//! Everything that happens to a run is also an [`Event`], for whoever shows runs to people.

use std::collections::VecDeque;

use orion_msg_actions::{
    ActionResult, Attach, Cancel, Claim, Outcome, Progress, Register, Registered, Run, Target,
};
use orion_reply::{Asked, Out};
use orion_types::{ActionName, Error, NodeName};
use orion_wire::{Bytes, kind};

/// The most runs under way at once. More are refused as busy.
pub const MAX_RUNS: usize = 4096;

/// How many detached runs that ended with nobody attached are kept for their starter to come
/// back to. The oldest goes first.
pub const MAX_ENDED: usize = 256;

/// How long such a run is kept.
pub const ENDED_FOR_MS: u64 = 10 * 60 * 1000;

/// Who started a run, as the comm core said: what a session must be again to come back to it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Starter {
    class: u8,
    uid: u32,
    name: String,
}

impl Starter {
    fn of(asked: &Asked<'_>) -> Self {
        Self {
            class: asked.class,
            uid: asked.uid,
            name: asked.caller.to_owned(),
        }
    }
}

/// A detached run that ended while nobody was attached.
#[derive(Debug)]
struct Ended {
    call: u64,
    starter: Starter,
    outcome: Outcome,
    output: Vec<u8>,
    detail: String,
    at_ms: u64,
}

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
    /// The session that is told. `None` once the caller has gone.
    caller: Option<u32>,
    /// The run's number for its caller.
    call: u64,
    starter: Starter,
    /// The latest progress of a detached run, for whoever comes back to it.
    last: Option<(u8, String, Vec<u8>)>,
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
    ended: VecDeque<Ended>,
}

impl Actions {
    /// The kinds answered here.
    pub const KINDS: &'static [u8] = &[
        kind::ACTION_REGISTER,
        kind::ACTION_RUN,
        kind::ACTION_PROGRESS,
        kind::ACTION_RESULT,
        kind::ACTION_CANCEL,
        kind::ACTION_ATTACH,
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
            ended: VecDeque::new(),
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
        self.ended.clear();
    }

    /// How many detached runs that ended are kept for their starter.
    #[must_use]
    pub fn ended(&self) -> usize {
        self.ended.len()
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
    fn end(
        &mut self,
        at: usize,
        outcome: Outcome,
        output: &[u8],
        detail: &str,
        now_ms: u64,
        out: &mut dyn Out,
    ) {
        self.event(at, 255, detail, Some(outcome));
        let run = self.running.remove(at);
        if let Some(caller) = run.caller {
            let _ = out.send(
                caller,
                &ActionResult {
                    call: run.call,
                    outcome,
                    output: Bytes(output),
                    detail,
                },
            );
        }
        if run.detached {
            // How a detached run ended is kept for whoever started it, also when a caller
            // seems to be there: its session may have ended a moment ago without this node
            // having heard, and then the result just sent went nowhere.
            self.forget_old(now_ms);
            if self.ended.len() >= MAX_ENDED {
                self.ended.pop_front();
            }
            self.ended.push_back(Ended {
                call: run.call,
                starter: run.starter,
                outcome,
                output: output.to_vec(),
                detail: detail.to_owned(),
                at_ms: now_ms,
            });
        }
    }

    fn forget_old(&mut self, now_ms: u64) {
        while self
            .ended
            .front()
            .is_some_and(|ended| now_ms.saturating_sub(ended.at_ms) > ENDED_FOR_MS)
        {
            self.ended.pop_front();
        }
    }

    /// Comes back to a detached run: the latest progress, or how it ended.
    fn attach(&mut self, asked: &Asked<'_>, now_ms: u64, out: &mut dyn Out) -> Result<(), Error> {
        let attach: Attach = asked.body()?;
        let starter = Starter::of(asked);
        // The newest of the starter's runs with that number.
        if let Some(run) = self
            .running
            .iter_mut()
            .rev()
            .find(|run| run.detached && run.call == attach.call && run.starter == starter)
        {
            run.caller = Some(asked.session);
            if let Some((percent, message, data)) = &run.last {
                out.push(
                    asked.session,
                    &Progress {
                        call: attach.call,
                        percent: *percent,
                        message,
                        data: Bytes(data),
                    },
                )?;
            }
            return Ok(());
        }
        self.forget_old(now_ms);
        let at = self
            .ended
            .iter()
            .rposition(|ended| ended.call == attach.call && ended.starter == starter)
            .ok_or(Error::NotFound)?;
        let ended = self.ended.remove(at).ok_or(Error::NotFound)?;
        out.send(
            asked.session,
            &ActionResult {
                call: ended.call,
                outcome: ended.outcome,
                output: Bytes(&ended.output),
                detail: &ended.detail,
            },
        )
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
                    .any(|other| other.caller == Some(from) && other.call == run.call)
                {
                    return Err(Error::AlreadyExists);
                }
                if self.running.len() >= MAX_RUNS {
                    return Err(Error::Busy);
                }
                // The caller's numbers start over with each of its sessions. What was kept of
                // an earlier run under this number is not this run's.
                let starter = Starter::of(asked);
                self.ended
                    .retain(|ended| !(ended.call == run.call && ended.starter == starter));
                self.next += 1;
                let id = self.next;
                let principal = asked.principal();
                out.send(
                    owner,
                    &Claim {
                        call: id,
                        name: run.name.clone(),
                        target: run.target.clone(),
                        args: run.args,
                        deadline_ms: run.deadline_ms,
                        caller: &principal,
                    },
                )?;
                self.running.push(Running {
                    id,
                    name: run.name.clone(),
                    target: run.target.clone(),
                    owner,
                    caller: Some(from),
                    call: run.call,
                    starter,
                    last: None,
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
                let run = &mut self.running[at];
                if run.detached {
                    run.last = Some((
                        progress.percent,
                        progress.message.to_owned(),
                        progress.data.0.to_vec(),
                    ));
                }
                if let Some(caller) = run.caller {
                    let call = run.call;
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
                self.end(
                    at,
                    result.outcome,
                    result.output.0,
                    result.detail,
                    now_ms,
                    out,
                );
                Ok(())
            }
            kind::ACTION_CANCEL => {
                let cancel: Cancel = asked.body()?;
                let run = self
                    .running
                    .iter()
                    .find(|run| run.caller == Some(from) && run.call == cancel.call)
                    .ok_or(Error::NotFound)?;
                out.send(run.owner, &Cancel { call: run.id })
            }
            kind::ACTION_ATTACH => self.attach(asked, now_ms, out),
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
    pub fn gone(&mut self, session: u32, now_ms: u64, out: &mut dyn Out) {
        self.listed.retain(|listed| listed.owner != session);
        let mut at = 0;
        while at < self.running.len() {
            if self.running[at].owner == session {
                self.end(
                    at,
                    Outcome::Failed,
                    &[],
                    "the program that runs it went away",
                    now_ms,
                    out,
                );
                continue;
            }
            let run = &mut self.running[at];
            if run.caller == Some(session) {
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
                self.end(
                    at,
                    Outcome::TimedOut,
                    &[],
                    "it ran out of time",
                    now_ms,
                    out,
                );
            } else {
                at += 1;
            }
        }
    }
}

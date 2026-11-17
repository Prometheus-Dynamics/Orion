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
//! - A run has two numbers. The caller's `call` is the caller's own, unique on its session,
//!   and starts over with each session. The number in `Claim` is the list's, unique here.
//! - What identifies a run across sessions is the list's: a [`RunHandle`], this list's
//!   instance (drawn at random when node logic starts) and its number here. `Started` gives it
//!   to the caller, and `Attach` and `Cancel` take it. No other run is ever given the same
//!   handle; one from before node logic started names nothing, and is refused as such.
//! - Without a handle, a caller's number reaches only what it can name without doubt: a cancel,
//!   a run this same session started; an attach, the one run of the starter's with that number.
//!   A new run under a number makes the starter's older runs under it reachable by handle only.
//! - A caller that goes away has its run cancelled, unless the run is detached. A detached
//!   run carries on, and whoever started it can come back to it with `Attach`, from any
//!   session: it is then told the latest progress, or how the run ended, the same each time
//!   while that is kept; after, it is refused as expired. A detached run must
//!   have a deadline, no longer than the node takes, so that it ends without its caller.
//! - `Claim` names the caller's session by a token that is never reused. When that session
//!   ends, for any cause, each program that was given a run from it is told once with
//!   `CallerGone`, whether its runs are still going or not.
//! - A program that goes away takes its actions off the list, and its runs end as failed.
//! - A run with a deadline that passes ends as timed out, and the program is told to stop.
//!
//! Everything that happens to a run is also an [`Event`], for whoever shows runs to people.

use std::collections::VecDeque;

use orion_msg_actions::{
    ActionResult, CallerGone, Cancel, CancelReason, Claim, Outcome, Progress, Register, Registered,
    Run, RunHandle, Started, Target,
};
use orion_reply::{Asked, Out};
use orion_types::{ActionName, Error, NodeName};
use orion_wire::{Bytes, kind};

mod attach;

use attach::fresh_instance;

/// The most runs under way at once. More are refused as busy.
pub const MAX_RUNS: usize = 4096;

/// How many detached runs that ended with nobody attached are kept for their starter to come
/// back to. The oldest goes first.
pub const MAX_ENDED: usize = 256;

/// How long such a run is kept.
pub const ENDED_FOR_MS: u64 = 10 * 60 * 1000;

/// How many runs whose end is no longer kept are still known to have been, so that coming
/// back to one is refused as expired, not as a run that never was. The oldest goes first.
pub const MAX_EXPIRED: usize = 1024;

/// The longest deadline a detached run may have unless the node says otherwise: an hour.
pub const MAX_DETACHED_MS: u64 = 60 * 60 * 1000;

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
    id: u64,
    name: ActionName,
    call: u64,
    starter: Starter,
    /// A later run of the starter's took the number: this is reachable by handle only.
    superseded: bool,
    outcome: Outcome,
    code: u32,
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
    /// The session that started it, which alone may cancel it by number; `None` once that
    /// session has ended, since the comm core's session numbers come round again.
    origin: Option<u32>,
    /// The run's number for its caller.
    call: u64,
    starter: Starter,
    /// A later run of the starter's took the number: this is reachable by handle only.
    superseded: bool,
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
    /// This start of node logic, in every handle it gives.
    instance: u64,
    /// Words for the last refusal, when the code alone would not say enough.
    said: Option<String>,
    listed: Vec<Listed>,
    running: Vec<Running>,
    next: u64,
    events: Vec<Event>,
    ended: VecDeque<Ended>,
    /// Runs whose end is no longer kept: number, call and starter.
    expired: VecDeque<(u64, u64, Starter)>,
    /// Each caller that asked for a run, as its session and, for a peer carrying runs for
    /// sessions of its own, that node's token for one of them; and its token here. The comm
    /// core's numbers come round again, tokens never do.
    tokens: Vec<Caller>,
    next_token: u64,
    /// Each program that was given a run, and the token of the session that asked for it:
    /// who is told when that session ends.
    given: Vec<(u32, u64)>,
    max_detached_ms: u64,
    /// Tokens of callers that ended since [`Actions::take_ended`].
    ended_tokens: Vec<u64>,
}

/// A caller: a session, or one session behind a peer's.
#[derive(Debug)]
struct Caller {
    session: u32,
    on_behalf: u64,
    token: u64,
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
        kind::ACTION_CALLER_GONE,
    ];

    /// An empty list on the node `me`, with an instance of its own.
    #[must_use]
    pub fn new(me: NodeName) -> Self {
        Self::with_instance(me, fresh_instance())
    }

    /// As [`Actions::new`], with `instance` for its handles: a test's, to stand in for a
    /// restart that drew the same or another.
    #[must_use]
    pub const fn with_instance(me: NodeName, instance: u64) -> Self {
        Self {
            me,
            instance,
            said: None,
            listed: Vec::new(),
            running: Vec::new(),
            next: 0,
            events: Vec::new(),
            ended: VecDeque::new(),
            expired: VecDeque::new(),
            tokens: Vec::new(),
            next_token: 0,
            given: Vec::new(),
            max_detached_ms: MAX_DETACHED_MS,
            ended_tokens: Vec::new(),
        }
    }

    /// The words for the last refusal, if the code alone does not say enough.
    pub fn take_said(&mut self) -> Option<String> {
        self.said.take()
    }

    /// This list's instance: what every handle it gives starts with.
    #[must_use]
    pub const fn instance(&self) -> u64 {
        self.instance
    }

    /// A refusal of `error`, in these words.
    fn refuse(&mut self, error: Error, words: String) -> Error {
        self.said = Some(words);
        error
    }

    /// Takes detached runs with a deadline up to `max_ms`, instead of [`MAX_DETACHED_MS`].
    pub const fn set_max_detached(&mut self, max_ms: u64) {
        self.max_detached_ms = max_ms;
    }

    /// The token of `session`, or of the session behind it that a peer named `on_behalf`,
    /// made the first time it is asked for.
    fn token(&mut self, session: u32, on_behalf: u64) -> u64 {
        if let Some(caller) = self
            .tokens
            .iter()
            .find(|caller| caller.session == session && caller.on_behalf == on_behalf)
        {
            return caller.token;
        }
        self.next_token += 1;
        self.tokens.push(Caller {
            session,
            on_behalf,
            token: self.next_token,
        });
        self.next_token
    }

    /// The token of whoever asked `asked`, a `Run`: what a run carried to another node names
    /// as its [`Run::on_behalf`], so the session's end can follow it there.
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`] for a body that is not a `Run`.
    pub fn caller_token(&mut self, asked: &Asked<'_>) -> Result<u64, Error> {
        let run: Run<'_> = asked.body()?;
        Ok(self.token(asked.session, on_behalf(asked, &run)))
    }

    /// The tokens of callers that ended since the last call, for runs this node carried
    /// elsewhere on their behalf.
    pub fn take_ended(&mut self) -> Vec<u64> {
        std::mem::take(&mut self.ended_tokens)
    }

    /// The caller at `at` ended: each program given a run from it is told, once.
    fn caller_ended(&mut self, at: usize, out: &mut dyn Out) {
        let token = self.tokens.swap_remove(at).token;
        for (owner, _) in self.given.iter().filter(|(_, given)| *given == token) {
            let _ = out.send(*owner, &CallerGone { session: token });
        }
        self.given.retain(|(_, given)| *given != token);
        self.ended_tokens.push(token);
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

    /// Whether `session` asked for a run of one of `names` that is still under way.
    #[must_use]
    pub fn asked_by(&self, session: u32, names: &[String]) -> bool {
        self.running.iter().any(|run| {
            run.caller == Some(session) && names.iter().any(|name| name == run.name.as_str())
        })
    }

    /// The actions listed now: each one's name and what it is on.
    pub fn actions(&self) -> impl Iterator<Item = (&ActionName, &Target)> {
        self.listed
            .iter()
            .map(|listed| (&listed.name, &listed.target))
    }

    /// Forgets every action and run: the comm core they came through is gone, and so are the
    /// sessions that had them.
    pub fn forget_sessions(&mut self) {
        self.listed.clear();
        self.running.clear();
        self.ended.clear();
        self.expired.clear();
        self.tokens.clear();
        self.given.clear();
        self.ended_tokens.clear();
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
        (outcome, code): (Outcome, u32),
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
                    code,
                    instance: self.instance,
                    run: run.id,
                    action: run.name.as_str(),
                },
            );
        }
        if run.detached {
            // How a detached run ended is kept for whoever started it, also when a caller
            // seems to be there: its session may have ended a moment ago without this node
            // having heard, and then the result just sent went nowhere.
            self.forget_old(now_ms);
            if self.ended.len() >= MAX_ENDED
                && let Some(oldest) = self.ended.pop_front()
            {
                self.expire(&oldest);
            }
            self.ended.push_back(Ended {
                id: run.id,
                name: run.name,
                call: run.call,
                starter: run.starter,
                superseded: run.superseded,
                outcome,
                code,
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
            if let Some(oldest) = self.ended.pop_front() {
                self.expire(&oldest);
            }
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
                if run.detached {
                    // A run that outlives its caller must end by itself some time.
                    if run.deadline_ms == 0 {
                        return Err(Error::NoDeadline);
                    }
                    if run.deadline_ms > self.max_detached_ms {
                        return Err(Error::DeadlineTooLong);
                    }
                }
                let owner = self
                    .listed
                    .iter()
                    .find(|listed| listed.name == run.name && listed.target == run.target)
                    .map(|listed| listed.owner)
                    .ok_or(Error::NotFound)?;
                if self
                    .running
                    .iter()
                    .any(|other| other.origin == Some(from) && other.call == run.call)
                {
                    return Err(Error::AlreadyExists);
                }
                if self.running.len() >= MAX_RUNS {
                    return Err(Error::Busy);
                }
                // The caller's numbers start over with each of its sessions. The starter's
                // earlier runs under this number, going or kept, are not this run, detached or
                // not: from now on they are reached by their handles only, and a number alone
                // never gives back an older run's answer in this one's place.
                let starter = Starter::of(asked);
                for older in self
                    .running
                    .iter_mut()
                    .filter(|older| older.starter == starter)
                {
                    older.superseded |= older.call == run.call;
                }
                for older in self
                    .ended
                    .iter_mut()
                    .filter(|older| older.starter == starter)
                {
                    older.superseded |= older.call == run.call;
                }
                self.next += 1;
                let id = self.next;
                let principal = asked.principal();
                let session = self.token(from, on_behalf(asked, &run));
                out.send(
                    owner,
                    &Claim {
                        call: id,
                        name: run.name.clone(),
                        target: run.target.clone(),
                        args: run.args,
                        deadline_ms: run.deadline_ms,
                        caller: &principal,
                        session,
                        detached: run.detached,
                    },
                )?;
                if !self.given.contains(&(owner, session)) {
                    self.given.push((owner, session));
                }
                out.send(
                    from,
                    &Started {
                        call: run.call,
                        instance: self.instance,
                        run: id,
                        action: run.name.as_str(),
                    },
                )?;
                self.running.push(Running {
                    id,
                    name: run.name.clone(),
                    target: run.target.clone(),
                    owner,
                    caller: Some(from),
                    origin: Some(from),
                    call: run.call,
                    starter,
                    superseded: false,
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
                    let (call, id) = (run.call, run.id);
                    out.push(
                        caller,
                        &Progress {
                            call,
                            percent: progress.percent,
                            message: progress.message,
                            data: progress.data,
                            instance: self.instance,
                            run: id,
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
                    (result.outcome, result.code),
                    result.output.0,
                    result.detail,
                    now_ms,
                    out,
                );
                Ok(())
            }
            kind::ACTION_CANCEL => {
                let cancel: Cancel = asked.body()?;
                let at = match RunHandle::said(cancel.instance, cancel.run) {
                    Some(handle) => self.by_handle(handle, &Starter::of(asked))?,
                    // A number alone: a run this same session started under it, and no other.
                    None => self
                        .running
                        .iter()
                        .position(|run| run.origin == Some(from) && run.call == cancel.call),
                };
                let run = &self.running[at.ok_or(Error::NotFound)?];
                out.send(
                    run.owner,
                    &Cancel {
                        call: run.id,
                        reason: reason_of(asked, cancel.reason),
                        instance: 0,
                        run: 0,
                    },
                )
            }
            kind::ACTION_ATTACH => self.attach(asked, now_ms, out),
            kind::ACTION_CALLER_GONE => {
                // A peer's session that it carried runs here for ended there.
                if asked.class != orion_check::PrincipalClass::Peer as u8 {
                    return Err(Error::Denied);
                }
                let gone: CallerGone = asked.body()?;
                if gone.session != 0
                    && let Some(at) = self.tokens.iter().position(|caller| {
                        caller.session == from && caller.on_behalf == gone.session
                    })
                {
                    self.caller_ended(at, out);
                }
                Ok(())
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
    /// the runs it asked for are cancelled unless they are detached, and every program that
    /// was given a run from it is told.
    pub fn gone(&mut self, session: u32, now_ms: u64, out: &mut dyn Out) {
        self.listed.retain(|listed| listed.owner != session);
        self.given.retain(|(owner, _)| *owner != session);
        let mut at = 0;
        while at < self.running.len() {
            if self.running[at].owner == session {
                self.end(
                    at,
                    (Outcome::Failed, 0),
                    &[],
                    "the program that runs it went away",
                    now_ms,
                    out,
                );
                continue;
            }
            let run = &mut self.running[at];
            if run.origin == Some(session) {
                run.origin = None;
            }
            if run.caller == Some(session) {
                run.caller = None;
                if !run.detached {
                    let _ = out.send(
                        run.owner,
                        &Cancel {
                            call: run.id,
                            reason: CancelReason::CallerGone,
                            instance: 0,
                            run: 0,
                        },
                    );
                }
            }
            at += 1;
        }
        // After its runs are dealt with: then the session itself, and every session a peer
        // carried runs for over it.
        while let Some(at) = self
            .tokens
            .iter()
            .position(|caller| caller.session == session)
        {
            self.caller_ended(at, out);
        }
    }

    /// Node logic is stopping: every run is cancelled where it runs, and ends for its caller
    /// as cancelled. Nothing is kept for a starter to come back to.
    pub fn stopping(&mut self, now_ms: u64, out: &mut dyn Out) {
        while let Some(run) = self.running.first() {
            let _ = out.send(
                run.owner,
                &Cancel {
                    call: run.id,
                    reason: CancelReason::Stopping,
                    instance: 0,
                    run: 0,
                },
            );
            self.end(
                0,
                (Outcome::Cancelled, 0),
                &[],
                "node logic is stopping",
                now_ms,
                out,
            );
        }
        self.ended.clear();
    }

    /// Ends the runs whose deadline has passed at `now_ms`.
    pub fn tick(&mut self, now_ms: u64, out: &mut dyn Out) {
        let mut at = 0;
        while at < self.running.len() {
            if self.running[at].deadline.is_some_and(|due| due <= now_ms) {
                let (owner, id) = (self.running[at].owner, self.running[at].id);
                let _ = out.send(
                    owner,
                    &Cancel {
                        call: id,
                        reason: CancelReason::Deadline,
                        instance: 0,
                        run: 0,
                    },
                );
                self.end(
                    at,
                    (Outcome::TimedOut, orion_msg_actions::DEADLINE_CODE),
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

/// Why a run is stopped at `asked`'s word: [`CancelReason::Asked`], unless `asked` is another
/// node's node logic passing on why it stopped a run it carried here.
fn reason_of(asked: &Asked<'_>, said: CancelReason) -> CancelReason {
    if asked.class == orion_check::PrincipalClass::Peer as u8 && said != CancelReason::Unsaid {
        said
    } else {
        CancelReason::Asked
    }
}

/// Whom `asked`'s `run` is for behind its session: what a peer named, and nothing for anyone
/// else, so no caller can pass for another.
fn on_behalf(asked: &Asked<'_>, run: &Run<'_>) -> u64 {
    if asked.class == orion_check::PrincipalClass::Peer as u8 {
        run.on_behalf
    } else {
        0
    }
}

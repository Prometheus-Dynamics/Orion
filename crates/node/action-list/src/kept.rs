//! Detached runs kept while their program's session is lost.
//!
//! A program that registers an action with `keep_ms` asks node logic to keep that action's
//! detached runs going when its session is lost: a program between sessions may be holding a
//! job on hardware that is still under way. Such a run is held, for the program's `keep_ms`
//! (no more than [`MAX_KEEP_MS`]):
//!
//! - its caller, if attached, is told it is restoring, as progress, not an end;
//! - a cancel asked meanwhile is kept, and given to the program when it is back;
//! - the program comes back when a session of the same identity registers the action again.
//!   Each held run of that action is given to it again, a `Claim` with `kept` and the same
//!   number, before the registration is answered, so a program has every kept run by the time
//!   its registration is confirmed. It goes on and says how the run ends;
//! - with the program not back in time, the run ends [`Outcome::Indeterminate`]: the job may
//!   well have finished, so it is never said to have failed or been cancelled. A program that
//!   reports on it after that is refused, naming how it ended.
//!
//! A run whose deadline passes while it is held is told to stop as soon as its program is
//! back. Node logic stopping ends every held run as indeterminate at once: there is nobody to
//! tell.

use orion_msg_actions::{CancelReason, Claim, Outcome};
use orion_reply::Out;
use orion_types::{ActionName, Error};
use orion_wire::Bytes;

use crate::{Actions, MAX_EXPIRED, Starter, Target};

/// The longest node logic keeps a run for a program between sessions.
pub const MAX_KEEP_MS: u64 = 60_000;

/// A run whose program's session was lost, kept for that program.
#[derive(Debug)]
pub(crate) struct Held {
    /// The identity that must register the action again to be given it back.
    pub(crate) by: Starter,
    /// When it ends as indeterminate if nobody has come back.
    pub(crate) until_ms: u64,
    /// How long it was kept for, for the words if nobody comes back.
    pub(crate) keep_ms: u64,
    /// A cancel asked while it was held, for the program when it is back.
    pub(crate) cancel: Option<CancelReason>,
}

const RESTORING: &str =
    "restoring: the program that runs it lost its session, and the run is kept for it";
const RESTORED: &str = "restored: the program that runs it is back";

impl Actions {
    /// The run at `at`, whose program's session is gone, is kept for `keep_ms` for `by`.
    pub(crate) fn hold(
        &mut self,
        at: usize,
        by: Starter,
        keep_ms: u64,
        now_ms: u64,
        out: &mut dyn Out,
    ) {
        let keep_ms = keep_ms.min(MAX_KEEP_MS);
        let run = &mut self.running[at];
        run.held = Some(Held {
            by,
            until_ms: now_ms.saturating_add(keep_ms),
            keep_ms,
            cancel: run.stop.map(|stop| stop.reason),
        });
        self.say(at, RESTORING, out);
    }

    /// The run at `at`'s caller, if attached, and its events are told `message`, at the run's
    /// last percent.
    fn say(&mut self, at: usize, message: &str, out: &mut dyn Out) {
        let percent = self.running[at]
            .last
            .as_ref()
            .map_or(255, |(percent, _, _)| *percent);
        let _ = self.doing(at, percent, message, &[], out);
    }

    /// `session`, as `by`, registered `name` on `target` again: each run of it held for `by`
    /// is given to it, and goes on.
    pub(crate) fn give_back(
        &mut self,
        session: u32,
        by: &Starter,
        (name, target): (&ActionName, &Target),
        now_ms: u64,
        out: &mut dyn Out,
    ) {
        for at in 0..self.running.len() {
            let run = &self.running[at];
            let Some(held) = &run.held else { continue };
            if held.by != *by || run.name != *name || run.target != *target {
                continue;
            }
            let cancel = held.cancel;
            let left = run
                .deadline
                .map_or(0, |due| due.saturating_sub(now_ms).max(1));
            let sent = out.send(
                session,
                &Claim {
                    call: run.id,
                    name: run.name.clone(),
                    target: run.target.clone(),
                    args: Bytes(&[]),
                    deadline_ms: left,
                    caller: &run.principal,
                    session: run.token,
                    detached: true,
                    kept: true,
                },
            );
            if sent.is_err() {
                continue;
            }
            let run = &mut self.running[at];
            run.owner = session;
            run.held = None;
            if !self.given.contains(&(session, run.token)) {
                self.given.push((session, run.token));
            }
            self.say(at, RESTORED, out);
            let overdue = self.running[at].deadline.is_some_and(|due| due <= now_ms);
            match cancel {
                _ if overdue => self.tell_to_stop(at, CancelReason::Deadline, now_ms, out),
                Some(reason) if self.running[at].stop.is_some() => {
                    // It was being stopped when its session was lost: stopped again, the same
                    // bound from now.
                    self.running[at].stop = None;
                    self.tell_to_stop(at, reason, now_ms, out);
                }
                Some(reason) => {
                    let id = self.running[at].id;
                    let _ = out.send(
                        session,
                        &orion_msg_actions::Cancel {
                            call: id,
                            reason,
                            instance: 0,
                            run: 0,
                        },
                    );
                }
                None => {}
            }
        }
    }

    /// `session` is gone, and its actions are still listed: the run at `at` is held if it is a
    /// detached run `session` was doing of an action it registered to be kept. True if it is.
    pub(crate) fn keep(&mut self, at: usize, session: u32, now_ms: u64, out: &mut dyn Out) -> bool {
        let run = &self.running[at];
        if run.owner != session || run.held.is_some() || !run.detached {
            return false;
        }
        let Some(listed) = self.listed.iter().find(|listed| {
            listed.owner == session
                && listed.keep_ms > 0
                && listed.name == run.name
                && listed.target == run.target
        }) else {
            return false;
        };
        let (keep_ms, by) = (listed.keep_ms, listed.by.clone());
        self.hold(at, by, keep_ms, now_ms, out);
        true
    }

    /// The run at `at` is held: a cancel is kept for its program. False if it is not held.
    pub(crate) fn cancel_held(&mut self, at: usize, reason: CancelReason) -> bool {
        match &mut self.running[at].held {
            Some(held) => {
                held.cancel = Some(reason);
                true
            }
            None => false,
        }
    }

    /// Ends each held run whose program did not come back in time; with `all`, every held run
    /// (node logic is stopping). True if the run at `at` was ended.
    pub(crate) fn end_held(
        &mut self,
        at: usize,
        all: bool,
        now_ms: u64,
        out: &mut dyn Out,
    ) -> bool {
        let Some(held) = &self.running[at].held else {
            return false;
        };
        let detail = if all {
            "node logic is stopping while the program that runs it is between sessions, so how it ended is not known".to_owned()
        } else if held.until_ms <= now_ms {
            format!(
                "the program that runs it lost its session and did not come back within {} ms, so how it ended is not known",
                held.keep_ms
            )
        } else {
            return false;
        };
        let id = self.running[at].id;
        self.end(at, (Outcome::Indeterminate, 0), &[], &detail, now_ms, out);
        if self.lost.len() >= MAX_EXPIRED {
            self.lost.pop_front();
        }
        self.lost.push_back(id);
        true
    }

    /// A report on run `id`, which is no longer under way: refused by name if it ended because
    /// its program did not come back, else let pass (a program may well say something of a
    /// run a moment after it ended).
    pub(crate) fn late(&mut self, id: u64) -> Result<Option<usize>, Error> {
        if self.lost.contains(&id) {
            return Err(self.refuse(
                Error::RunGone,
                format!(
                    "run {id} already ended as indeterminate: the program that runs it lost its session and did not come back in time"
                ),
            ));
        }
        Ok(None)
    }
}

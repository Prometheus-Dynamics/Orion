//! Runs node logic stops itself: at a run's deadline, and every run when node logic stops.
//!
//! The program is told to stop, as a caller's cancel tells it, and the run stays open for its
//! word: a program may have finished the run a moment before, and then the run ended as it
//! says, not as node logic asked. Node logic waits up to a bound ([`CONFIRM_MS`] unless the
//! node says otherwise). What the program says by then ends the run; a run cancelled at its
//! deadline ends as timed out, with [`DEADLINE_CODE`], as it always has. With no word by then
//! the run ends [`Outcome::Indeterminate`]: how it ended is not known, and node logic does
//! not guess.
//!
//! The bound also caps how long node logic's stop waits: each run ends by then, one way or
//! the other.

use orion_msg_actions::{Cancel, CancelReason, DEADLINE_CODE, Outcome};
use orion_reply::Out;

use crate::Actions;

/// How long a program has to say how a run ended once node logic told it to stop: 10 s.
/// Longer than a program's own wait on what it drives (a device daemon's 5 s), so its answer,
/// even one that says it does not know, arrives first; well inside a service's 30 s to stop.
pub const CONFIRM_MS: u64 = 10_000;

/// Node logic told a run's program to stop: why, and until when it waits for its word.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Stop {
    pub(crate) by_ms: u64,
    pub(crate) reason: CancelReason,
}

impl Actions {
    /// Gives a program `confirm_ms` instead of [`CONFIRM_MS`] to say how a run ended once
    /// node logic told it to stop.
    pub const fn set_confirm(&mut self, confirm_ms: u64) {
        self.confirm_ms = confirm_ms;
    }

    /// Node logic is stopping: every run's program is told to stop, saying why, and no new run
    /// is taken. Each run ends when its program says how, or as
    /// [`Outcome::Indeterminate`] at the bound, through [`Actions::tick`]; node logic stops once
    /// [`Actions::running`] is 0, by [`Actions::next_deadline`] at the latest. Nothing is kept
    /// for a starter to come back to.
    pub fn stopping(&mut self, now_ms: u64, out: &mut dyn Out) {
        self.closing = true;
        self.ended.clear();
        let mut at = 0;
        while at < self.running.len() {
            if self.end_held(at, true, now_ms, out) {
                continue;
            }
            if self.running[at].stop.is_none() {
                self.tell_to_stop(at, CancelReason::Stopping, now_ms, out);
            }
            at += 1;
        }
    }

    /// Whether node logic is stopping: see [`Actions::stopping`].
    #[must_use]
    pub const fn closing(&self) -> bool {
        self.closing
    }

    /// At `now_ms`: tells the program of each run whose deadline has passed to stop, and ends
    /// as [`Outcome::Indeterminate`] each run whose program said nothing by its bound.
    pub fn tick(&mut self, now_ms: u64, out: &mut dyn Out) {
        let mut at = 0;
        while at < self.running.len() {
            if self.end_held(at, false, now_ms, out) {
                continue;
            }
            let run = &self.running[at];
            if run.held.is_some() {
                // Its program is between sessions: nothing to tell until it is back.
                at += 1;
                continue;
            }
            if let Some(stop) = run.stop {
                if stop.by_ms <= now_ms {
                    let why = if stop.reason == CancelReason::Deadline {
                        "it ran out of time and was told to stop"
                    } else {
                        "node logic is stopping and told it to stop"
                    };
                    let detail = format!(
                        "{why}; its program did not say how it ended within {} ms, so it is not known",
                        self.confirm_ms
                    );
                    self.end(at, (Outcome::Indeterminate, 0), &[], &detail, now_ms, out);
                    continue;
                }
            } else if run.deadline.is_some_and(|due| due <= now_ms) {
                self.tell_to_stop(at, CancelReason::Deadline, now_ms, out);
            }
            at += 1;
        }
    }

    /// Tells the program of the run at `at` to stop, for `reason`, and waits for its word.
    pub(crate) fn tell_to_stop(
        &mut self,
        at: usize,
        reason: CancelReason,
        now_ms: u64,
        out: &mut dyn Out,
    ) {
        let run = &mut self.running[at];
        let _ = out.send(
            run.owner,
            &Cancel {
                call: run.id,
                reason,
                instance: 0,
                run: 0,
            },
        );
        run.deadline = None;
        run.stop = Some(Stop {
            by_ms: now_ms.saturating_add(self.confirm_ms),
            reason,
        });
        let doing = if reason == CancelReason::Deadline {
            "out of time: told to stop"
        } else {
            "node logic is stopping: told to stop"
        };
        self.event(at, 255, doing, None);
    }

    /// How the run at `at` ends, from what its program said: as it says, but a run its program
    /// stopped because its deadline passed ran out of time.
    pub(crate) fn confirmed(&self, at: usize, (outcome, code): (Outcome, u32)) -> (Outcome, u32) {
        match self.running[at].stop {
            Some(stop)
                if stop.reason == CancelReason::Deadline && outcome == Outcome::Cancelled =>
            {
                (Outcome::TimedOut, DEADLINE_CODE)
            }
            _ => (outcome, code),
        }
    }
}

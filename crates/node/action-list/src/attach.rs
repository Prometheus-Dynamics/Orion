//! Coming back to a run: by the handle node logic gave it, or by its number alone, and what
//! names this start of node logic.

use orion_msg_actions::{ActionResult, Attach, Progress, RunHandle, Started};
use orion_reply::{Asked, Out};
use orion_types::Error;
use orion_wire::Bytes;

use crate::{Actions, Starter};

impl Actions {
    /// The running run `handle` names, if `starter` started it; `None` if it is not running.
    ///
    /// # Errors
    ///
    /// [`Error::RunGone`] for a handle from another start of node logic.
    pub(crate) fn by_handle(
        &mut self,
        handle: RunHandle,
        starter: &Starter,
    ) -> Result<Option<usize>, Error> {
        if handle.instance != self.instance {
            return Err(self.refuse(
                Error::RunGone,
                format!(
                    "run {handle} is from before node logic last started: it is gone, and how \
                     it ended is not known here"
                ),
            ));
        }
        Ok(self
            .running
            .iter()
            .position(|run| run.id == handle.run && run.starter == *starter))
    }

    /// Comes back to a detached run: its handle, then the latest progress, or how it ended.
    pub(crate) fn attach(
        &mut self,
        asked: &Asked<'_>,
        now_ms: u64,
        out: &mut dyn Out,
    ) -> Result<(), Error> {
        let attach: Attach = asked.body()?;
        let starter = Starter::of(asked);
        self.forget_old(now_ms);
        let (running, ended) = match RunHandle::said(attach.instance, attach.run) {
            Some(handle) => {
                let running = self
                    .by_handle(handle, &starter)?
                    .filter(|at| self.running[*at].detached);
                let ended = running.is_none().then(|| {
                    self.ended
                        .iter()
                        .position(|ended| ended.id == handle.run && ended.starter == starter)
                });
                (running, ended.flatten())
            }
            // A number alone names the starter's one run under it that no later run took.
            None => {
                let named = |call: u64, superseded: bool, of: &Starter| {
                    call == attach.call && *of == starter && !superseded
                };
                let running = self
                    .running
                    .iter()
                    .position(|run| run.detached && named(run.call, run.superseded, &run.starter));
                let ended = self
                    .ended
                    .iter()
                    .position(|ended| named(ended.call, ended.superseded, &ended.starter));
                if running.is_none() && ended.is_none() {
                    let taken = self.running.iter().any(|run| {
                        run.detached
                            && run.superseded
                            && run.call == attach.call
                            && run.starter == starter
                    }) || self.ended.iter().any(|ended| {
                        ended.superseded && ended.call == attach.call && ended.starter == starter
                    });
                    if taken {
                        return Err(self.refuse(
                            Error::NotFound,
                            format!(
                                "a later run took number {}: come back to the earlier one by \
                                 its handle",
                                attach.call
                            ),
                        ));
                    }
                }
                (running, ended)
            }
        };
        if let Some(at) = running {
            let run = &mut self.running[at];
            run.caller = Some(asked.session);
            out.send(
                asked.session,
                &Started {
                    call: attach.call,
                    instance: self.instance,
                    run: run.id,
                    action: run.name.as_str(),
                },
            )?;
            if let Some((percent, message, data)) = &run.last {
                out.push(
                    asked.session,
                    &Progress {
                        call: run.call,
                        percent: *percent,
                        message,
                        data: Bytes(data),
                        instance: self.instance,
                        run: run.id,
                    },
                )?;
            }
            return Ok(());
        }
        let ended = self
            .ended
            .remove(ended.ok_or(Error::NotFound)?)
            .ok_or(Error::NotFound)?;
        out.send(
            asked.session,
            &Started {
                call: attach.call,
                instance: self.instance,
                run: ended.id,
                action: ended.name.as_str(),
            },
        )?;
        out.send(
            asked.session,
            &ActionResult {
                call: ended.call,
                outcome: ended.outcome,
                output: Bytes(&ended.output),
                detail: &ended.detail,
                code: ended.code,
                instance: self.instance,
                run: ended.id,
                action: ended.name.as_str(),
            },
        )
    }
}

/// An instance no earlier start of node logic is likely to have drawn: the process's random
/// hash keys, over the time and the process. Never zero, which says "no handle".
pub(crate) fn fresh_instance() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    if let Ok(now) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        hasher.write_u128(now.as_nanos());
    }
    hasher.write_u32(std::process::id());
    hasher.finish().max(1)
}

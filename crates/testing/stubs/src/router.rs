//! An action router: the open list of actions, and runs handed to whoever registered them,
//! each decided by the policy it was registered with.

use orion_msg_actions::{
    ActionResult, Attach, Cancel, Claim, Outcome, Progress, Register, Registered, Run,
};
use orion_server::{
    Admits, Checked, Cx, Delivery, Error, Incoming, Policy, Service, SessionId, Sessions, What,
};
use orion_wire::{Bytes, FrameView, body, kind};

#[derive(Debug)]
struct Listed {
    name: String,
    owner: SessionId,
    policy: Policy,
}

#[derive(Debug)]
struct Running {
    id: u64,
    caller: Option<(SessionId, u64)>,
    owner: SessionId,
    detached: bool,
    doing: Option<(u8, String, Vec<u8>)>,
    finished: Option<(Outcome, Vec<u8>, String)>,
}

/// The router.
#[derive(Debug, Default)]
pub struct Router {
    listed: Vec<Listed>,
    running: Vec<Running>,
    next: u64,
}

fn tell(sessions: &mut Sessions, run: &Running) {
    let Some((caller, call)) = run.caller else {
        return;
    };
    if let Some((percent, message, data)) = &run.doing {
        let progress = Progress {
            call,
            percent: *percent,
            message,
            data: Bytes(data),
        };
        let _ = sessions.send(caller, &progress, Delivery::MayDrop);
    }
    if let Some((outcome, output, detail)) = &run.finished {
        let result = ActionResult {
            call,
            outcome: *outcome,
            output: Bytes(output),
            detail,
        };
        let _ = sessions.send(caller, &result, Delivery::Must);
    }
}

impl Service for Router {
    fn kinds(&self) -> &'static [u8] {
        &[
            kind::ACTION_REGISTER,
            kind::ACTION_RUN,
            kind::ACTION_PROGRESS,
            kind::ACTION_RESULT,
            kind::ACTION_CANCEL,
            kind::ACTION_ATTACH,
        ]
    }

    fn asks<'a>(&self, frame: &FrameView<'a>) -> Result<(What<'a>, Policy), Error> {
        let local = Policy::new(Admits::LOCAL);
        match frame.kind() {
            kind::ACTION_REGISTER => Ok((What::Write("actions"), local)),
            kind::ACTION_RUN => {
                let (_call, rest) = body::take::<u64>(frame.payload())?;
                let (name, _) = body::take::<&str>(rest)?;
                let listed = self
                    .listed
                    .iter()
                    .find(|listed| listed.name == name)
                    .ok_or(Error::NotFound)?;
                Ok((What::Run(name), listed.policy))
            }
            _ => Ok((
                What::Read("actions"),
                Policy::new(Admits::ANYONE).without_grant(),
            )),
        }
    }

    fn handle(&mut self, request: Checked<Incoming<'_>>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let incoming = request.get();
        let from = cx.from();
        match incoming.kind() {
            kind::ACTION_REGISTER => {
                let register: Register<'_> = incoming.body()?;
                if self
                    .listed
                    .iter()
                    .any(|listed| listed.name == register.name.as_str())
                {
                    return Err(Error::AlreadyExists);
                }
                self.listed.push(Listed {
                    name: register.name.to_string(),
                    owner: from,
                    policy: register.policy,
                });
                cx.reply(&Registered {
                    request_seq: cx.request_seq(),
                })
            }
            kind::ACTION_RUN => {
                let run: Run<'_> = incoming.body()?;
                let owner = self
                    .listed
                    .iter()
                    .find(|listed| listed.name == run.name.as_str())
                    .map(|listed| listed.owner)
                    .ok_or(Error::NotFound)?;
                self.next += 1;
                self.running.push(Running {
                    id: self.next,
                    caller: Some((from, run.call)),
                    owner,
                    detached: run.detached,
                    doing: None,
                    finished: None,
                });
                let caller = cx.caller().principal.to_string();
                cx.sessions().send(
                    owner,
                    &Claim {
                        call: self.next,
                        name: run.name.clone(),
                        target: run.target.clone(),
                        args: run.args,
                        deadline_ms: run.deadline_ms,
                        caller: &caller,
                    },
                    Delivery::Must,
                )
            }
            kind::ACTION_PROGRESS => {
                let progress: Progress<'_> = incoming.body()?;
                let run = self
                    .running
                    .iter_mut()
                    .find(|run| run.id == progress.call && run.owner == from)
                    .ok_or(Error::NotFound)?;
                run.doing = Some((
                    progress.percent,
                    progress.message.to_owned(),
                    progress.data.0.to_vec(),
                ));
                tell(cx.sessions(), run);
                Ok(())
            }
            kind::ACTION_RESULT => {
                let result: ActionResult<'_> = incoming.body()?;
                let run = self
                    .running
                    .iter_mut()
                    .find(|run| run.id == result.call && run.owner == from)
                    .ok_or(Error::NotFound)?;
                run.doing = None;
                run.finished = Some((
                    result.outcome,
                    result.output.0.to_vec(),
                    result.detail.to_owned(),
                ));
                tell(cx.sessions(), run);
                Ok(())
            }
            kind::ACTION_CANCEL => {
                let cancel: Cancel = incoming.body()?;
                let run = self
                    .running
                    .iter()
                    .find(|run| run.caller == Some((from, cancel.call)))
                    .ok_or(Error::NotFound)?;
                cx.sessions()
                    .send(run.owner, &Cancel { call: run.id }, Delivery::Must)
            }
            _ => {
                let attach: Attach = incoming.body()?;
                let run = self
                    .running
                    .iter_mut()
                    .find(|run| {
                        run.detached && run.caller.is_none_or(|(_, call)| call == attach.call)
                    })
                    .ok_or(Error::NotFound)?;
                run.caller = Some((from, attach.call));
                tell(cx.sessions(), run);
                Ok(())
            }
        }
    }

    fn closed(&mut self, session: SessionId, sessions: &mut Sessions) {
        for run in &mut self.running {
            if run.caller.is_some_and(|(caller, _)| caller == session) {
                if run.detached {
                    run.caller = run.caller.map(|(_, call)| (SessionId(u32::MAX), call));
                } else if run.finished.is_none() {
                    let _ = sessions.send(run.owner, &Cancel { call: run.id }, Delivery::Must);
                }
            }
        }
        self.listed.retain(|listed| listed.owner != session);
    }
}

//! The node's own action, `files.send`: a file the node keeps is sent on to a receiver.
//!
//! A receiver is a program that asked for a prefix with `FileSink`, such as a device whose
//! firmware layer takes images into its spare slot. `files.send { file, to }` sends the
//! stored `file` to the destination `to` under that prefix, and the run ends when the
//! receiver says committed or rejected. So an image put on the node once, with any client,
//! reaches a device when someone allowed to runs the action, and as many devices as it is
//! run for.
//!
//! Node logic does this as the comm core's client, like any other program: it registers the
//! action, so the relay checks each run against the action's policy at the door; it is
//! handed the claim; and it sends the file's `FileBegin`, `FileChunk` and `FileCommit`,
//! which pass to the receiver through the sinks as anyone's would, with the receiver's
//! answers coming back to it. The sinks cut chunks to what the receiver takes, and pace a
//! receiver that takes one frame at a time; this side sends one chunk and waits for its
//! answer, so nothing waits in the sinks.
//!
//! - **Resumes.** The transfer's number comes from the file's digest and the destination,
//!   so a run that is tried again resumes where the receiver is.
//! - **Retries.** With no answer for [`QUIET_MS`], the transfer is begun again, which the
//!   receiver answers with where to go on from. After [`TRIES`] such tries the run fails.
//! - **One file at a time.** The file is read a chunk at a time, at the offset the receiver
//!   asked for, into one buffer.

use std::fs::File;
use std::os::unix::fs::FileExt;

use orion_check::{Admits, Policy};
use orion_connect::{Client, ClientError, Cx};
use orion_file_store::Files;
use orion_files_sink::Sinks;
use orion_msg_actions::{ActionResult, Claim, Outcome, Progress, Register, Target};
use orion_msg_files::{FileBegin, FileChunk, FileCommit, FileSend, FileState, FileStatus};
use orion_types::{FileName, NodeName};
use orion_wire::{Body, Bytes, Decode};
use sha2::{Digest, Sha256};

/// The action's name.
pub(crate) const ACTION: &str = "files.send";

/// The largest chunk sent to a receiver that takes any.
const CHUNK: usize = 16 * 1024;

/// How long without an answer before the transfer is begun again.
pub(crate) const QUIET_MS: u64 = 2_000;

/// How many times it is begun again before the run fails.
const TRIES: u32 = 5;

/// The most runs at once. More are rejected.
const MAX_RUNS: usize = 64;

/// The way to the comm core: inside a frame's handling, or between frames.
pub(crate) enum Via<'a, 'b> {
    Cx(&'a mut Cx<'b>),
    Client(&'a Client),
}

impl Via<'_, '_> {
    fn send<B: Body>(&mut self, body: &B) -> Result<u16, ClientError> {
        match self {
            Self::Cx(cx) => cx.send(body),
            Self::Client(client) => client.send(body),
        }
    }

    fn refused(&self, seq: u16) -> Option<orion_types::Error> {
        match self {
            Self::Cx(_) => None,
            Self::Client(client) => client.refusal(seq),
        }
    }
}

/// The registration of the action on node `me`: programs on the board and operators with a
/// grant may run it.
pub(crate) fn register(me: &NodeName) -> Option<Register<'static>> {
    Some(Register {
        name: ACTION.parse().ok()?,
        target: Target::Node(me.clone()),
        policy: Policy::new(Admits::LOCAL.and(Admits::OPERATOR)),
        description: "sends a file the node keeps to the program that receives its destination",
    })
}

#[derive(Debug)]
struct Run {
    /// The run's number on the action list.
    call: u64,
    transfer: u64,
    file: File,
    to: FileName,
    size: u64,
    digest: [u8; 32],
    chunk: usize,
    committing: bool,
    /// When the receiver was last heard, or the transfer last begun.
    heard_ms: u64,
    tries: u32,
    percent: u8,
    /// The `seq` of the last frame sent for it.
    seq: u16,
}

/// The runs under way.
#[derive(Debug, Default)]
pub(crate) struct Sends {
    runs: Vec<Run>,
    buffer: Vec<u8>,
}

/// The transfer's number: the same file to the same destination is the same transfer,
/// numbered as a client that pushes it numbers it, so the receiver resumes either.
fn transfer_of(digest: &[u8; 32], to: &FileName) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(digest);
    hasher.update(to.as_str().as_bytes());
    let number: [u8; 32] = hasher.finalize().into();
    u64::from_le_bytes([
        number[0], number[1], number[2], number[3], number[4], number[5], number[6], number[7],
    ])
}

fn end(
    via: &mut Via<'_, '_>,
    call: u64,
    outcome: Outcome,
    detail: &str,
) -> Result<(), ClientError> {
    via.send(&ActionResult {
        call,
        outcome,
        output: Bytes(&[]),
        detail,
    })
    .map(|_| ())
}

impl Sends {
    /// Whether a run is under way: [`Sends::tick`] wants calling.
    pub(crate) fn active(&self) -> bool {
        !self.runs.is_empty()
    }

    /// Forgets every run: the comm core they went through is gone, and the action list with it.
    pub(crate) fn forget(&mut self) {
        self.runs.clear();
    }

    /// Starts a run the action list handed over.
    pub(crate) fn claim(
        &mut self,
        claim: &Claim<'_>,
        files: &Files,
        sinks: &Sinks,
        now_ms: u64,
        via: &mut Via<'_, '_>,
    ) -> Result<(), ClientError> {
        let call = claim.call;
        if claim.name.as_str() != ACTION {
            return end(
                via,
                call,
                Outcome::Rejected,
                "not an action of node logic's",
            );
        }
        let Ok(args) = FileSend::decode(claim.args.0) else {
            return end(
                via,
                call,
                Outcome::Rejected,
                "the arguments are not a file and a destination",
            );
        };
        let Some((digest, size)) = files.placed(&args.file) else {
            return end(via, call, Outcome::Rejected, "no such file is kept here");
        };
        let Some(max_chunk) = sinks.max_chunk(args.to.as_str()) else {
            return end(
                via,
                call,
                Outcome::Rejected,
                "nothing receives files at that destination",
            );
        };
        let transfer = transfer_of(&digest, &args.to);
        if self.runs.iter().any(|run| run.transfer == transfer) {
            return end(
                via,
                call,
                Outcome::Rejected,
                "that file is already being sent there",
            );
        }
        if self.runs.len() >= MAX_RUNS {
            return end(
                via,
                call,
                Outcome::Rejected,
                "too many files are being sent",
            );
        }
        let file = match File::open(files.path(&args.file)) {
            Ok(file) => file,
            Err(error) => {
                return end(
                    via,
                    call,
                    Outcome::Failed,
                    &format!("cannot read it: {error}"),
                );
            }
        };
        let chunk = match max_chunk {
            0 => CHUNK,
            limit => (limit as usize).min(CHUNK),
        };
        let mut run = Run {
            call,
            transfer,
            file,
            to: args.to,
            size,
            digest,
            chunk,
            committing: false,
            heard_ms: now_ms,
            tries: 0,
            percent: 0,
            seq: 0,
        };
        begin(&mut run, via)?;
        self.runs.push(run);
        Ok(())
    }

    /// The action list asks for run `call` to stop. The receiver keeps what it has.
    pub(crate) fn cancel(&mut self, call: u64, via: &mut Via<'_, '_>) -> Result<(), ClientError> {
        let Some(at) = self.runs.iter().position(|run| run.call == call) else {
            return Ok(());
        };
        self.runs.remove(at);
        end(via, call, Outcome::Cancelled, "cancelled")
    }

    /// The receiver's answer about a transfer. Not one of these runs' is not for here.
    pub(crate) fn status(
        &mut self,
        status: &FileStatus<'_>,
        now_ms: u64,
        via: &mut Via<'_, '_>,
    ) -> Result<(), ClientError> {
        let Some(at) = self
            .runs
            .iter()
            .position(|run| run.transfer == status.transfer)
        else {
            return Ok(());
        };
        let run = &mut self.runs[at];
        run.heard_ms = now_ms;
        run.tries = 0;
        match status.state {
            FileState::Receiving => {
                let received = status.received.min(run.size);
                if received >= run.size {
                    if !run.committing {
                        run.committing = true;
                        run.seq = via.send(&FileCommit {
                            transfer: run.transfer,
                        })?;
                    }
                    return Ok(());
                }
                let want = ((run.size - received) as usize).min(run.chunk);
                self.buffer.resize(want, 0);
                if let Err(error) = run.file.read_exact_at(&mut self.buffer, received) {
                    let call = run.call;
                    self.runs.remove(at);
                    return end(
                        via,
                        call,
                        Outcome::Failed,
                        &format!("cannot read it: {error}"),
                    );
                }
                run.seq = via.send(&FileChunk {
                    transfer: run.transfer,
                    offset: received,
                    data: Bytes(&self.buffer),
                })?;
                let percent = (received * 100 / run.size.max(1)) as u8;
                if percent != run.percent {
                    run.percent = percent;
                    via.send(&Progress {
                        call: run.call,
                        percent,
                        message: "sending",
                        data: Bytes(&[]),
                    })?;
                }
                Ok(())
            }
            FileState::Committed => {
                let call = run.call;
                self.runs.remove(at);
                end(via, call, Outcome::Succeeded, "")
            }
            FileState::Rejected => {
                let call = run.call;
                self.runs.remove(at);
                let detail = if status.detail.is_empty() {
                    "the receiver rejected it"
                } else {
                    status.detail
                };
                end(via, call, Outcome::Failed, detail)
            }
            // Checking the digest, or a state this build does not know: wait.
            _ => Ok(()),
        }
    }

    /// Begins again the transfers the receiver has said nothing of for a while, and ends the
    /// runs that were refused or have tried too often.
    pub(crate) fn tick(&mut self, now_ms: u64, via: &mut Via<'_, '_>) -> Result<(), ClientError> {
        let mut at = 0;
        while at < self.runs.len() {
            let run = &mut self.runs[at];
            if let Some(error) = via.refused(run.seq) {
                let call = run.call;
                self.runs.remove(at);
                end(via, call, Outcome::Failed, &format!("refused: {error}"))?;
                continue;
            }
            if now_ms.saturating_sub(run.heard_ms) >= QUIET_MS {
                run.tries += 1;
                if run.tries > TRIES {
                    let call = run.call;
                    self.runs.remove(at);
                    end(via, call, Outcome::Failed, "the receiver stopped answering")?;
                    continue;
                }
                run.heard_ms = now_ms;
                begin(run, via)?;
            }
            at += 1;
        }
        Ok(())
    }
}

/// Begins the transfer, or begins it again: the receiver answers with where to go on from.
fn begin(run: &mut Run, via: &mut Via<'_, '_>) -> Result<(), ClientError> {
    run.committing = false;
    run.seq = via.send(&FileBegin {
        transfer: run.transfer,
        destination: run.to.clone(),
        size: run.size,
        digest: run.digest,
    })?;
    Ok(())
}

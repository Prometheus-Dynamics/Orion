//! Actions on other nodes.
//!
//! An action lives where the program that registered it is connected. A run for another node,
//! or for a resource another node has, is carried there: this node's logic connects to the
//! other node's comm core over the network, as this node, and runs the action the way any
//! caller would. So the check runs where the action runs. The other node's door sees a peer
//! and decides by the policy the action was registered with there; this node knows nothing of
//! that policy and does not need to.
//!
//! To the caller it is one run: progress and the result come back under its own number,
//! cancel reaches the program, and if the caller goes the run is cancelled unless it was
//! detached. If the other node goes away mid-run, the run ends as failed and says why.
//!
//! Connecting and waiting on a network session would hold node logic up, so each other node
//! has a helper thread, started when the first run for it comes and kept while runs go there.

mod link;

use std::collections::BTreeMap;
use std::sync::mpsc::{self, Receiver, Sender};

use orion_dial::Dialer;
use orion_msg_actions::{ActionResult, Outcome, Progress, Run};
use orion_reply::{Asked, Out};
use orion_types::{Error, NodeName};
use orion_wire::Bytes;

use link::{Command, Event, Link};

/// The most forwarded runs under way at once. One more is refused as busy.
pub const MAX_FORWARDED: usize = 1024;

#[derive(Debug)]
struct Forwarded {
    node: NodeName,
    /// The session that is told. `None` once the caller has gone.
    caller: Option<u32>,
    /// The run's number for its caller, and the `seq` of its request.
    call: u64,
    seq: u16,
    detached: bool,
}

/// Runs carried to other nodes.
#[derive(Debug)]
pub struct Forwards {
    me: NodeName,
    /// How the other nodes are reached, as this one.
    dialer: Dialer,
    links: BTreeMap<NodeName, Link>,
    events: Receiver<Event>,
    reporter: Sender<Event>,
    forwarded: BTreeMap<u64, Forwarded>,
    next: u64,
}

impl Forwards {
    /// Forwarding for the node `dialer` is for: a run goes only to a node it can reach as
    /// this one.
    #[must_use]
    pub fn new(dialer: Dialer) -> Self {
        let (reporter, events) = mpsc::channel();
        Self {
            me: dialer.me.clone(),
            dialer,
            links: BTreeMap::new(),
            events,
            reporter,
            forwarded: BTreeMap::new(),
            next: 0,
        }
    }

    /// Whether runs are under way on other nodes: [`Forwards::poll`] will have something.
    #[must_use]
    pub fn active(&self) -> bool {
        !self.forwarded.is_empty()
    }

    /// How many runs are under way on other nodes.
    #[must_use]
    pub fn forwarded(&self) -> usize {
        self.forwarded.len()
    }

    /// Carries a run to `node`, which is reached at `address` (`host:port`).
    ///
    /// # Errors
    ///
    /// [`Error::Busy`] with too many under way, [`Error::Unsupported`] for a run to this node
    /// itself, [`Error::AlreadyExists`] if the caller already has a run with that number.
    pub fn run(&mut self, asked: &Asked<'_>, node: &NodeName, address: &str) -> Result<(), Error> {
        let run: Run<'_> = asked.body()?;
        if *node == self.me {
            return Err(Error::Unsupported);
        }
        if self
            .forwarded
            .values()
            .any(|other| other.caller == Some(asked.session) && other.call == run.call)
        {
            return Err(Error::AlreadyExists);
        }
        if self.forwarded.len() >= MAX_FORWARDED {
            return Err(Error::Busy);
        }
        self.next += 1;
        let id = self.next;
        let link = self.links.entry(node.clone()).or_insert_with(|| {
            Link::start(node.clone(), self.dialer.clone(), self.reporter.clone())
        });
        link.send(Command::Run {
            id,
            address: address.to_owned(),
            name: run.name.clone(),
            target: run.target.clone(),
            args: run.args.0.to_vec(),
            deadline_ms: run.deadline_ms,
        });
        self.forwarded.insert(
            id,
            Forwarded {
                node: node.clone(),
                caller: Some(asked.session),
                call: run.call,
                seq: asked.seq,
                detached: run.detached,
            },
        );
        Ok(())
    }

    /// Cancels the run `call` of `session`, if it is one that was carried. Whether it was.
    pub fn cancel(&mut self, session: u32, call: u64) -> bool {
        let Some((id, forwarded)) = self
            .forwarded
            .iter()
            .find(|(_, forwarded)| forwarded.caller == Some(session) && forwarded.call == call)
        else {
            return false;
        };
        if let Some(link) = self.links.get(&forwarded.node) {
            link.send(Command::Cancel { id: *id });
        }
        true
    }

    /// A session is gone: the runs it asked for are cancelled where they run, unless they
    /// are detached.
    pub fn gone(&mut self, session: u32) {
        for (id, forwarded) in &mut self.forwarded {
            if forwarded.caller == Some(session) {
                forwarded.caller = None;
                if !forwarded.detached
                    && let Some(link) = self.links.get(&forwarded.node)
                {
                    link.send(Command::Cancel { id: *id });
                }
            }
        }
    }

    /// Forgets every caller: the comm core they came through is gone. Their runs are
    /// cancelled where they run.
    pub fn forget_sessions(&mut self) {
        for (id, forwarded) in std::mem::take(&mut self.forwarded) {
            if let Some(link) = self.links.get(&forwarded.node) {
                link.send(Command::Cancel { id });
            }
        }
    }

    fn finish(
        &mut self,
        id: u64,
        outcome: Outcome,
        output: &[u8],
        detail: &str,
        out: &mut dyn Out,
    ) {
        if let Some(forwarded) = self.forwarded.remove(&id)
            && let Some(caller) = forwarded.caller
        {
            let _ = out.send(
                caller,
                &ActionResult {
                    call: forwarded.call,
                    outcome,
                    output: Bytes(output),
                    detail,
                },
            );
        }
    }

    /// Passes on what the other nodes said since the last call.
    pub fn poll(&mut self, out: &mut dyn Out) {
        while let Ok(event) = self.events.try_recv() {
            match event {
                Event::Progress {
                    id,
                    percent,
                    message,
                    data,
                } => {
                    if let Some(forwarded) = self.forwarded.get(&id)
                        && let Some(caller) = forwarded.caller
                    {
                        let _ = out.push(
                            caller,
                            &Progress {
                                call: forwarded.call,
                                percent,
                                message: &message,
                                data: Bytes(&data),
                            },
                        );
                    }
                }
                Event::Result {
                    id,
                    outcome,
                    output,
                    detail,
                } => self.finish(id, outcome, &output, &detail, out),
                Event::Refused { id, error } => {
                    // The other node said no, or could not be reached: the caller hears it
                    // as the answer to its own request.
                    if let Some(forwarded) = self.forwarded.remove(&id)
                        && let Some(caller) = forwarded.caller
                    {
                        let asked = Asked {
                            session: caller,
                            seq: forwarded.seq,
                            kind: 0,
                            class: 0,
                            uid: 0,
                            caller: "",
                            payload: &[],
                        };
                        out.refuse(&asked, error);
                    }
                }
                Event::Lost { node } => {
                    let lost: Vec<u64> = self
                        .forwarded
                        .iter()
                        .filter(|(_, forwarded)| forwarded.node == node)
                        .map(|(id, _)| *id)
                        .collect();
                    for id in lost {
                        self.finish(
                            id,
                            Outcome::Failed,
                            &[],
                            "the node that runs it went away",
                            out,
                        );
                    }
                }
            }
        }
    }
}

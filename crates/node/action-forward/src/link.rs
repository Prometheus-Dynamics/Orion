//! The helper for one other node: a thread that holds a session to its comm core.

use std::collections::BTreeMap;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::time::{Duration, Instant};

use orion_connect::{ClientError, Roles, Session};
use orion_dial::Dialer;
use orion_msg_actions::{ActionResult, Cancel, Outcome, Progress, Run, Target};
use orion_msg_session::Refusal;
use orion_types::{ActionName, Error, NodeName};
use orion_wire::{Bytes, kind};
use tracing::{info, warn};

/// How long a session with nothing running on it is kept.
const IDLE: Duration = Duration::from_secs(30);

/// How long the helper waits on the session before it looks for something to send.
const TURN: Duration = Duration::from_millis(10);

/// What node logic tells the helper.
#[derive(Debug)]
// A run is sent once and a cancel is rare: boxing the run would cost more than it saves.
#[allow(clippy::large_enum_variant)]
pub(crate) enum Command {
    Run {
        id: u64,
        address: String,
        name: ActionName,
        target: Target,
        args: Vec<u8>,
        deadline_ms: u64,
    },
    Cancel {
        id: u64,
    },
}

/// What the helper tells node logic.
#[derive(Debug)]
pub(crate) enum Event {
    Progress {
        id: u64,
        percent: u8,
        message: String,
        data: Vec<u8>,
    },
    Result {
        id: u64,
        outcome: Outcome,
        output: Vec<u8>,
        detail: String,
    },
    Refused {
        id: u64,
        error: Error,
    },
    /// The session to the node broke: whatever ran there is not heard of again.
    Lost {
        node: NodeName,
    },
}

/// Node logic's end of a helper.
#[derive(Debug)]
pub(crate) struct Link {
    commands: Sender<Command>,
}

impl Link {
    /// Starts the helper for `node`.
    pub(crate) fn start(node: NodeName, dialer: Dialer, events: Sender<Event>) -> Self {
        let (commands, waiting) = mpsc::channel();
        let helper = Helper {
            node,
            dialer,
            events,
            session: None,
            running: BTreeMap::new(),
            idle_since: Instant::now(),
        };
        let spawned = std::thread::Builder::new()
            .name("orion-action-forward".into())
            .spawn(move || helper.run(&waiting));
        if let Err(error) = spawned {
            // With no helper, a run finds the channel closed and is refused.
            warn!("no helper for forwarded actions could be started: {error}");
        }
        Self { commands }
    }

    pub(crate) fn send(&self, command: Command) {
        // A helper that is gone has already said that everything it had is lost.
        let _ = self.commands.send(command);
    }
}

struct Helper {
    node: NodeName,
    dialer: Dialer,
    events: Sender<Event>,
    session: Option<Session>,
    /// The `seq` each run was asked with, to match a refusal.
    running: BTreeMap<u64, u16>,
    idle_since: Instant,
}

impl Helper {
    fn tell(&self, event: Event) {
        let _ = self.events.send(event);
    }

    fn lost(&mut self, why: &ClientError) {
        warn!("the session to {} is gone: {why}", self.node);
        self.session = None;
        self.running.clear();
        self.tell(Event::Lost {
            node: self.node.clone(),
        });
    }

    fn connect(&mut self, address: &str) -> Result<(), ClientError> {
        if self.session.is_some() {
            return Ok(());
        }
        let session = self
            .dialer
            .dial(&self.node, address, "node", Roles::CLIENT)?;
        info!("connected to {} to run actions there", self.node);
        self.session = Some(session);
        Ok(())
    }

    fn command(&mut self, command: Command) {
        match command {
            Command::Run {
                id,
                address,
                name,
                target,
                args,
                deadline_ms,
            } => {
                if let Err(error) = self.connect(&address) {
                    warn!("{} cannot be reached at {address}: {error}", self.node);
                    // A node that is not the one pinned is refused as such, not as away.
                    let error = match error {
                        ClientError::Denied(_) => Error::Denied,
                        _ => Error::Unavailable,
                    };
                    self.tell(Event::Refused { id, error });
                    return;
                }
                let run = Run {
                    call: id,
                    name,
                    target,
                    args: Bytes(&args),
                    deadline_ms,
                    // Whether the caller here may leave is this node's to remember. To the
                    // other node this session is the caller, and it stays.
                    detached: false,
                };
                let sent = self.session.as_mut().map(|session| session.send(&run));
                match sent {
                    Some(Ok(seq)) => {
                        self.running.insert(id, seq);
                    }
                    Some(Err(error)) => {
                        self.tell(Event::Refused {
                            id,
                            error: Error::Unavailable,
                        });
                        self.lost(&error);
                    }
                    None => {}
                }
            }
            Command::Cancel { id } => {
                if self.running.contains_key(&id)
                    && let Some(session) = &mut self.session
                    && let Err(error) = session.send(&Cancel { call: id })
                {
                    self.lost(&error);
                }
            }
        }
    }

    /// Takes what the other node sent, for up to a turn.
    fn listen(&mut self) {
        let Some(session) = &mut self.session else {
            return;
        };
        let event = match session.next(TURN) {
            Ok(None) => return,
            Err(error) => {
                self.lost(&error);
                return;
            }
            Ok(Some(frame)) => match frame.kind() {
                kind::ACTION_PROGRESS => {
                    frame
                        .body::<Progress<'_>>()
                        .ok()
                        .map(|progress| Event::Progress {
                            id: progress.call,
                            percent: progress.percent,
                            message: progress.message.to_owned(),
                            data: progress.data.0.to_vec(),
                        })
                }
                kind::ACTION_RESULT => {
                    frame
                        .body::<ActionResult<'_>>()
                        .ok()
                        .map(|result| Event::Result {
                            id: result.call,
                            outcome: result.outcome,
                            output: result.output.0.to_vec(),
                            detail: result.detail.to_owned(),
                        })
                }
                kind::REFUSAL => frame.body::<Refusal<'_>>().ok().and_then(|refusal| {
                    let id = self
                        .running
                        .iter()
                        .find(|(_, seq)| **seq == refusal.request_seq)
                        .map(|(id, _)| *id)?;
                    Some(Event::Refused {
                        id,
                        error: refusal.error().unwrap_or(Error::Internal),
                    })
                }),
                _ => None,
            },
        };
        if let Some(Event::Result { id, .. } | Event::Refused { id, .. }) = &event {
            self.running.remove(id);
        }
        if let Some(event) = event {
            self.tell(event);
        }
    }

    fn run(mut self, commands: &Receiver<Command>) {
        loop {
            // With no session there is nothing to listen to: wait for something to do.
            let wait = if self.session.is_some() {
                Duration::ZERO
            } else {
                Duration::from_secs(3600)
            };
            match commands.recv_timeout(wait) {
                Ok(command) => {
                    self.command(command);
                    continue;
                }
                Err(RecvTimeoutError::Timeout) => {}
                // Node logic is gone.
                Err(RecvTimeoutError::Disconnected) => return,
            }
            self.listen();
            if self.running.is_empty() {
                if self.idle_since.elapsed() > IDLE {
                    self.session = None;
                }
            } else {
                self.idle_since = Instant::now();
            }
        }
    }
}

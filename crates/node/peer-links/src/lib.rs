//! Node logic's sessions to the other nodes that sync with it.
//!
//! - **One helper a node.** Connecting and waiting on a network session would hold node logic
//!   up, so each other node has a thread that holds a session to that node's comm core,
//!   made as this node (`orion-dial`). It connects again, a second and then up to thirty
//!   apart, while the node is not there.
//! - **Nothing is polled.** What node logic sends goes to the helper over a channel, and the
//!   helper is rung; what the helper heard comes back over a channel, and node logic is rung:
//!   [`PeerLinks::bell`] is readable. A helper with nothing to do wakes once a second, for the
//!   session's heartbeat.
//! - **What is heard**: the link is up, a frame of a kind node logic asked for, the other
//!   node's node logic is back after it was away, the link is down.
//!
//! What is sent on the session is node logic's business; this crate carries frames.

use std::collections::BTreeMap;
use std::io;
use std::os::unix::net::UnixDatagram;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::Duration;

use orion_connect::{ClientError, Roles, Session};
use orion_dial::Dialer;
use orion_types::NodeName;
use orion_wire::kind;
use rustix::event::{PollFd, PollFlags, poll};
use tracing::{info, warn};

/// How long a helper with nothing to do sleeps: the session's heartbeat is looked after then.
const IDLE: Duration = Duration::from_secs(1);
/// The first wait before connecting again.
const FIRST_RETRY: Duration = Duration::from_secs(1);
/// The longest.
const LAST_RETRY: Duration = Duration::from_secs(30);

/// What a helper heard.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Heard {
    /// The session to `node` is up.
    Up(NodeName),
    /// A frame came from `node`, of a kind node logic asked for.
    Frame {
        /// From whom.
        node: NodeName,
        /// Its kind.
        kind: u8,
        /// Its payload.
        payload: Vec<u8>,
    },
    /// The other node's node logic was away and is back: what it was sent then was refused.
    Back(NodeName),
    /// The session to `node` is gone.
    Down(NodeName),
}

enum Command {
    Frame(u8, Vec<u8>),
    Stop,
}

struct Link {
    address: String,
    commands: Sender<Command>,
    bell: UnixDatagram,
    up: Arc<AtomicBool>,
}

impl Link {
    fn tell(&self, command: Command) {
        // A helper that is gone has already said it is down.
        let _ = self.commands.send(command);
        ring(&self.bell);
    }
}

impl Drop for Link {
    fn drop(&mut self) {
        // Not waited for: it may be in the middle of connecting.
        self.tell(Command::Stop);
    }
}

/// Rings `bell`. A bell already ringing is enough.
fn ring(bell: &UnixDatagram) {
    let _ = bell.send(&[1]);
}

/// Forgets every ring so far.
fn hush(bell: &UnixDatagram) {
    let mut byte = [0_u8; 1];
    while bell.recv(&mut byte).is_ok() {}
}

/// The helpers, one a node.
pub struct PeerLinks {
    dialer: Dialer,
    kinds: &'static [u8],
    links: BTreeMap<NodeName, Link>,
    heard: Receiver<Heard>,
    tell: Sender<Heard>,
    /// Node logic's end of its bell, and the end the helpers ring.
    bell: UnixDatagram,
    ringer: UnixDatagram,
}

impl std::fmt::Debug for PeerLinks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerLinks")
            .field("links", &self.links.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl PeerLinks {
    /// Links made by `dialer`, which pass on frames of `kinds`.
    ///
    /// # Errors
    ///
    /// The bell cannot be made.
    pub fn new(dialer: Dialer, kinds: &'static [u8]) -> io::Result<Self> {
        let (bell, ringer) = UnixDatagram::pair()?;
        bell.set_nonblocking(true)?;
        ringer.set_nonblocking(true)?;
        let (tell, heard) = mpsc::channel();
        Ok(Self {
            dialer,
            kinds,
            links: BTreeMap::new(),
            heard,
            tell,
            bell,
            ringer,
        })
    }

    /// What is readable when a helper heard something. Node logic waits on it.
    #[must_use]
    pub const fn bell(&self) -> &UnixDatagram {
        &self.bell
    }

    /// The nodes to keep a session to, with their addresses. A node that is no longer here is
    /// let go; one whose address changed is connected to again.
    pub fn want(&mut self, nodes: &BTreeMap<NodeName, String>) {
        self.links
            .retain(|node, link| nodes.get(node) == Some(&link.address));
        for (node, address) in nodes {
            if *node == self.dialer.me || self.links.contains_key(node) {
                continue;
            }
            match self.start(node, address) {
                Ok(link) => {
                    self.links.insert(node.clone(), link);
                }
                Err(error) => warn!("no helper for {node} could be started: {error}"),
            }
        }
    }

    fn start(&self, node: &NodeName, address: &str) -> io::Result<Link> {
        let (bell, helper_bell) = UnixDatagram::pair()?;
        bell.set_nonblocking(true)?;
        helper_bell.set_nonblocking(true)?;
        let (commands, waiting) = mpsc::channel();
        let up = Arc::new(AtomicBool::new(false));
        let helper = Helper {
            node: node.clone(),
            address: address.to_owned(),
            dialer: self.dialer.clone(),
            kinds: self.kinds,
            commands: waiting,
            bell: helper_bell,
            tell: self.tell.clone(),
            ringer: self.ringer.try_clone()?,
            up: up.clone(),
        };
        std::thread::Builder::new()
            .name("orion-peer-link".into())
            .spawn(move || helper.run())?;
        Ok(Link {
            address: address.to_owned(),
            commands,
            bell,
            up,
        })
    }

    /// Whether the session to `node` is up.
    #[must_use]
    pub fn up(&self, node: &NodeName) -> bool {
        self.links
            .get(node)
            .is_some_and(|link| link.up.load(Ordering::Acquire))
    }

    /// Sends a frame to `node`. False if there is no session up to it: the frame went nowhere.
    pub fn send(&self, node: &NodeName, kind: u8, payload: &[u8]) -> bool {
        match self.links.get(node) {
            Some(link) if link.up.load(Ordering::Acquire) => {
                link.tell(Command::Frame(kind, payload.to_vec()));
                true
            }
            _ => false,
        }
    }

    /// What the helpers heard since the last call, in order. The bell is quiet after.
    pub fn take(&mut self) -> Vec<Heard> {
        hush(&self.bell);
        self.heard.try_iter().collect()
    }
}

struct Helper {
    node: NodeName,
    address: String,
    dialer: Dialer,
    kinds: &'static [u8],
    commands: Receiver<Command>,
    bell: UnixDatagram,
    tell: Sender<Heard>,
    ringer: UnixDatagram,
    up: Arc<AtomicBool>,
}

/// Why a session ended.
enum Ended {
    Stopped,
    Lost(ClientError),
}

impl Helper {
    fn heard(&self, heard: Heard) {
        let _ = self.tell.send(heard);
        ring(&self.ringer);
    }

    /// Waits up to `wait` for the bell. True if the helper is to stop. Frames asked for
    /// meanwhile went nowhere: the other side is told where this node is when it is up.
    fn idle(&self, wait: Duration) -> bool {
        let mut fds = [PollFd::new(&self.bell, PollFlags::IN)];
        let _ = poll(&mut fds, Some(&timespec(wait)));
        hush(&self.bell);
        self.commands
            .try_iter()
            .any(|command| matches!(command, Command::Stop))
    }

    fn run(self) {
        let mut retry = FIRST_RETRY;
        let mut said = false;
        loop {
            match self
                .dialer
                .dial(&self.node, &self.address, "sync", Roles::CLIENT)
            {
                Ok(session) => {
                    info!("connected to {} to sync with it", self.node);
                    retry = FIRST_RETRY;
                    said = false;
                    self.up.store(true, Ordering::Release);
                    self.heard(Heard::Up(self.node.clone()));
                    let ended = self.serve(session);
                    self.up.store(false, Ordering::Release);
                    self.heard(Heard::Down(self.node.clone()));
                    match ended {
                        Ended::Stopped => return,
                        Ended::Lost(why) => warn!("the session to {} is gone: {why}", self.node),
                    }
                }
                Err(error) => {
                    if !said {
                        warn!(
                            "{} cannot be reached at {} to sync: {error}",
                            self.node, self.address
                        );
                        said = true;
                    }
                }
            }
            if self.idle(retry) {
                return;
            }
            retry = (retry * 2).min(LAST_RETRY);
        }
    }

    fn serve(&self, mut session: Session) -> Ended {
        loop {
            // Everything that came is handled before waiting again.
            loop {
                match session.next(Duration::ZERO) {
                    Ok(Some(incoming)) => {
                        let kind = incoming.kind();
                        if kind == kind::NODE_BACK {
                            self.heard(Heard::Back(self.node.clone()));
                        } else if self.kinds.contains(&kind) {
                            let payload = incoming.payload().to_vec();
                            self.heard(Heard::Frame {
                                node: self.node.clone(),
                                kind,
                                payload,
                            });
                        }
                        // A refusal: the other node's node logic is away, and it says when
                        // it is back.
                    }
                    Ok(None) => break,
                    Err(error) => return Ended::Lost(error),
                }
            }
            hush(&self.bell);
            for command in self.commands.try_iter() {
                match command {
                    Command::Stop => return Ended::Stopped,
                    Command::Frame(kind, payload) => {
                        if let Err(error) = session.send_frame(kind, &payload, &[]) {
                            return Ended::Lost(error);
                        }
                    }
                }
            }
            let mut fds = [
                PollFd::new(&self.bell, PollFlags::IN),
                PollFd::from_borrowed_fd(session.as_fd(), PollFlags::IN),
            ];
            let _ = poll(&mut fds, Some(&timespec(IDLE)));
        }
    }
}

fn timespec(wait: Duration) -> rustix::time::Timespec {
    rustix::time::Timespec {
        tv_sec: wait.as_secs() as i64,
        tv_nsec: i64::from(wait.subsec_nanos()),
    }
}

//! Topics across boards.
//!
//! A program subscribes to `<node>/<topic>` exactly as it subscribes to a topic of its own
//! board: it is handed a ring and reads it. The ring is a local copy. This is what fills it.
//!
//! - **Only while someone asked.** When the first program here subscribes to a topic of
//!   another node, this comm core connects to that node's comm core and subscribes there,
//!   as any subscriber without a ring does. When the last one leaves, it unsubscribes. When
//!   nothing of a node is asked for any more, the connection to it is closed.
//! - **A local copy of the ring.** The other node sends the topic's declaration and then
//!   each sample. The declaration makes a ring of the same shape here, which the waiting
//!   subscribers are handed; each sample is written into it and the subscribers that are
//!   asleep are rung. From there on they are same-board subscribers.
//! - **Sample times in this board's time.** A sample's time is on its publisher's board's
//!   monotonic clock. The link compares the two boards' monotonic clocks, with the request
//!   the time service answers,
//!   and moves each sample's time by the difference, so that "the value at or before this
//!   frame" means the same on both boards. Boards whose clocks agree (time sync is what
//!   makes them) have a difference smaller than the comparison can measure, and their
//!   times are left as they are.
//! - **A link that breaks** tells the subscribers here that the publisher is gone, keeps
//!   their rings, and connects again. When it is back and the declaration is the same, they
//!   are told the topic is live again and go on in the ring they have.
//!
//! How the other node limits what it sends, and what it drops for a slow link, is its own
//! outbound part's business: to it this comm core is one more subscriber.

#![cfg(target_os = "linux")]

mod link;

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::rc::Rc;

use orion_broker::{Topic, TopicId, TopicWatch, Topics};
use orion_server::{Comm, SessionId, Sessions};
use orion_tcp::TcpOptions;
use orion_types::{NodeName, TopicName};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Where a node is, and the key it must prove it holds, if one is known.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Reach {
    /// Its network address.
    pub addr: SocketAddr,
    /// The key it must hold. `None` to take whoever answers there.
    pub key: Option<[u8; 32]>,
}

/// How a node's name becomes somewhere to connect to: a table from the settings, what
/// discovery found, or both.
pub trait Resolve {
    /// Where `node` is, if that is known.
    fn reach(&self, node: &NodeName) -> Option<Reach>;
}

impl Resolve for BTreeMap<NodeName, SocketAddr> {
    fn reach(&self, node: &NodeName) -> Option<Reach> {
        self.get(node).map(|addr| Reach {
            addr: *addr,
            key: None,
        })
    }
}

/// One node and where it is, as the settings give it: `name=address:port`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerNode {
    /// The node.
    pub name: NodeName,
    /// Its network address.
    pub addr: SocketAddr,
}

impl std::str::FromStr for PeerNode {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, String> {
        let (name, addr) = text.split_once('=').ok_or("expected name=address:port")?;
        Ok(Self {
            name: name
                .trim()
                .parse()
                .map_err(|error| format!("{name:?} is not a node's name: {error}"))?,
            addr: addr
                .trim()
                .parse()
                .map_err(|error| format!("{addr:?} is not an address: {error}"))?,
        })
    }
}

/// Where nodes are, from the settings, with the key each must hold from whoever knows it:
/// the node's trust store.
pub struct Table {
    nodes: Vec<PeerNode>,
    key: Box<KeyOf>,
}

/// Gives the key a node is pinned to, if it is.
type KeyOf = dyn Fn(&NodeName) -> Option<[u8; 32]>;

impl std::fmt::Debug for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Table({} nodes)", self.nodes.len())
    }
}

impl Table {
    /// A table of `nodes`. `key` gives the key a node is pinned to, if it is.
    #[must_use]
    pub fn new(
        nodes: Vec<PeerNode>,
        key: impl Fn(&NodeName) -> Option<[u8; 32]> + 'static,
    ) -> Self {
        Self {
            nodes,
            key: Box::new(key),
        }
    }
}

impl Resolve for Table {
    fn reach(&self, node: &NodeName) -> Option<Reach> {
        let known = self.nodes.iter().find(|known| known.name == *node)?;
        Some(Reach {
            addr: known.addr,
            key: (self.key)(node),
        })
    }
}

/// How far a link is.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LinkStats {
    /// Whether it is connected now.
    pub connected: bool,
    /// How many topics are carried over it.
    pub topics: usize,
    /// Samples written into local rings.
    pub samples: u64,
    /// How many nanoseconds are added to a sample's time: this board's clock less the other
    /// board's, or zero when that is smaller than the comparison can measure.
    pub offset_ns: i64,
}

/// What a link's task is told.
#[derive(Debug)]
pub(crate) enum Command {
    /// Someone here asked for this topic.
    Subscribe(TopicName, TopicId),
    /// Nobody here asks for it any more.
    Unsubscribe(TopicId),
}

struct Carried {
    commands: mpsc::UnboundedSender<Command>,
    /// The topics asked for, by their number here.
    wanted: Vec<TopicId>,
    stats: Rc<RefCell<LinkStats>>,
    task: JoinHandle<()>,
}

pub(crate) struct Shared {
    pub(crate) me: NodeName,
    pub(crate) comm: Comm,
    pub(crate) topics: Rc<RefCell<Topics>>,
    pub(crate) options: TcpOptions,
    pub(crate) resolve: Box<dyn Resolve>,
    /// This board's clock, in nanoseconds: what sample times here are on.
    pub(crate) clock: Box<dyn Fn() -> u64>,
}

struct State {
    shared: Rc<Shared>,
    links: BTreeMap<NodeName, Carried>,
}

/// The part of a comm core that carries other nodes' topics. Dropping it closes every link.
pub struct TopicPeers {
    state: Rc<RefCell<State>>,
}

impl std::fmt::Debug for TopicPeers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TopicPeers({} links)", self.state.borrow().links.len())
    }
}

impl Drop for TopicPeers {
    fn drop(&mut self) {
        for carried in self.state.borrow().links.values() {
            carried.task.abort();
        }
    }
}

/// Told by the broker when a topic of another node is asked for, or no longer is.
struct Watching(Rc<RefCell<State>>);

impl Watching {
    /// Someone here asked for `topic`. If it is another node's, it is asked of that node.
    fn asked(&mut self, topic: &Topic) {
        let Some(node) = &topic.node else {
            return;
        };
        let mut state = self.0.borrow_mut();
        let shared = state.shared.clone();
        let carried = state.links.entry(node.clone()).or_insert_with(|| {
            let (commands, waiting) = mpsc::unbounded_channel();
            let stats = Rc::new(RefCell::new(LinkStats::default()));
            let task =
                tokio::task::spawn_local(link::run(shared, node.clone(), waiting, stats.clone()));
            Carried {
                commands,
                wanted: Vec::new(),
                stats,
                task,
            }
        });
        if !carried.wanted.contains(&topic.id) {
            carried.wanted.push(topic.id);
            let _ = carried
                .commands
                .send(Command::Subscribe(topic.name.clone(), topic.id));
        }
    }
}

impl TopicWatch for Watching {
    fn declared(&mut self, _topic: &Topic, _sessions: &mut Sessions) {}

    fn subscribed(&mut self, topic: &Topic, _session: SessionId, _sessions: &mut Sessions) {
        self.asked(topic);
    }

    fn waiting(&mut self, topic: &Topic, _session: SessionId, _sessions: &mut Sessions) {
        // The first to ask for another node's topic always waits: there is no local copy
        // until the link has made one.
        self.asked(topic);
    }

    fn left(&mut self, topic: &Topic, _session: SessionId, _sessions: &mut Sessions) {
        let Some(node) = &topic.node else {
            return;
        };
        if !topic.subscribers.is_empty() {
            return;
        }
        // The last one left: it is carried no longer.
        let mut state = self.0.borrow_mut();
        let Some(carried) = state.links.get_mut(node) else {
            return;
        };
        carried.wanted.retain(|id| *id != topic.id);
        let _ = carried.commands.send(Command::Unsubscribe(topic.id));
        if carried.wanted.is_empty()
            && let Some(carried) = state.links.remove(node)
        {
            // Nothing of that node is asked for: the connection to it goes.
            carried.task.abort();
        }
    }
}

impl TopicPeers {
    /// Starts carrying other nodes' topics for the broker that has `topics`. `options` is
    /// how this node connects to others, `resolve` where they are. Call it inside the comm
    /// core's task set.
    #[must_use]
    pub fn start(
        comm: &Comm,
        topics: &Rc<RefCell<Topics>>,
        options: TcpOptions,
        resolve: impl Resolve + 'static,
    ) -> Self {
        Self::with_clock(comm, topics, options, resolve, link::monotonic_ns)
    }

    /// As [`TopicPeers::start`], with the clock sample times here are on.
    #[must_use]
    pub fn with_clock(
        comm: &Comm,
        topics: &Rc<RefCell<Topics>>,
        options: TcpOptions,
        resolve: impl Resolve + 'static,
        clock: impl Fn() -> u64 + 'static,
    ) -> Self {
        let me = topics.borrow().node().clone();
        let state = Rc::new(RefCell::new(State {
            shared: Rc::new(Shared {
                me,
                comm: comm.clone(),
                topics: topics.clone(),
                options,
                resolve: Box::new(resolve),
                clock: Box::new(clock),
            }),
            links: BTreeMap::new(),
        }));
        let mut table = topics.borrow_mut();
        table.carry_remote();
        table.watch(Watching(state.clone()));
        drop(table);
        Self { state }
    }

    /// How far the link to `node` is, if there is one.
    #[must_use]
    pub fn link(&self, node: &NodeName) -> Option<LinkStats> {
        self.state
            .borrow()
            .links
            .get(node)
            .map(|carried| *carried.stats.borrow())
    }

    /// How many nodes there is a link to.
    #[must_use]
    pub fn links(&self) -> usize {
        self.state.borrow().links.len()
    }
}

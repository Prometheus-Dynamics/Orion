//! Peer sync on connections: `orion-sync` told when a peer is there and handed its frames.
//!
//! - **Two ways to a peer.** Node logic keeps a session to every other node its node records
//!   give an address for (`orion-peer-links`), made as this node. A peer that connected here
//!   is heard through the comm core, which passes its sync frames on. Frames for a peer go on
//!   this node's own session to it when that is up, and otherwise on the session the peer
//!   made: one of the two being able to reach the other is enough.
//! - **Up and down.** A peer is up while either way is. Its observed slice is forgotten when
//!   neither is.
//! - **Asked.** Where sync is with each peer can be asked for, and a stale peer forgiven:
//!   `PeerStatus` and `PeerForgive`, both answered with `PeerStates`.
//! - **Agreed.** When each peer was last agreed with is written to `sync-agreed` in the state
//!   directory when it changes, and read back at start, so a node that was away too long is
//!   still stale after a restart.
//!
//! Sync's times are the wall clock's, in milliseconds: they outlive a restart.

use std::collections::{BTreeMap, BTreeSet};
use std::os::unix::net::UnixDatagram;
use std::path::PathBuf;

use orion_msg_peers::{PeerForgive, PeerState, PeerStates, PeerStatus, SyncState};
use orion_msg_records::{NodeRecord, RecordClass};
use orion_peer_links::{Heard, PeerLinks};
use orion_reply::{Asked, Out};
use orion_store::Store;
use orion_sync::{Sync, SyncConfig};
use orion_types::{Error, HlcTimestamp, NodeName};
use orion_wire::{Decode, List, kind};
use tracing::{info, warn};

use crate::records::Records;

/// The file the agreed times are kept in.
const AGREED_FILE: &str = "sync-agreed";

/// The class bit of a peer, as `orion_check::PrincipalClass` has it.
const PEER_CLASS: u8 = 16;

/// Peer sync and the ways to the peers.
pub(crate) struct Peers {
    sync: Sync,
    links: PeerLinks,
    /// The sessions each peer made to this node, as the comm core numbers them.
    incoming: BTreeMap<NodeName, BTreeSet<u32>>,
    /// Where `sync-agreed` is.
    agreed_file: PathBuf,
    /// What it says.
    agreed: Vec<(NodeName, u64)>,
    /// The node records were looked at for addresses: they are again when one changes.
    looked: bool,
}

/// Where sync's frames for a peer go.
struct To<'a> {
    links: &'a PeerLinks,
    incoming: &'a BTreeMap<NodeName, BTreeSet<u32>>,
    out: &'a mut dyn Out,
}

impl orion_sync::Out for To<'_> {
    fn frame(&mut self, peer: &NodeName, kind: u8, payload: &[u8]) {
        if self.links.send(peer, kind, payload) {
            return;
        }
        if let Some(&session) = self
            .incoming
            .get(peer)
            .and_then(|sessions| sessions.first())
        {
            self.out.frame(session, kind, payload, false);
        }
        // Neither way is up: the peer is told where this node is when one is.
    }
}

impl Peers {
    /// The kinds the comm core passes on from peers.
    pub(crate) const KINDS: &'static [u8] = &[kind::PEER_SYNC, kind::PEER_OBSERVED];

    /// The kinds an operator or a program on the board asks about sync with.
    pub(crate) const ASKED: &'static [u8] = &[kind::PEER_STATUS, kind::PEER_FORGIVE];

    pub(crate) fn new(me: NodeName, links: PeerLinks, state_dir: PathBuf) -> Self {
        let mut sync = Sync::new(me, SyncConfig::default());
        let agreed_file = state_dir.join(AGREED_FILE);
        let agreed = read_agreed(&agreed_file);
        for (peer, at_ms) in &agreed {
            sync.restore_agreed(peer, *at_ms);
        }
        Self {
            sync,
            links,
            incoming: BTreeMap::new(),
            agreed_file,
            agreed,
            looked: false,
        }
    }

    /// What is readable when a link heard something.
    pub(crate) fn bell(&self) -> &UnixDatagram {
        self.links.bell()
    }

    /// When sync next has something to do, on the wall clock.
    pub(crate) fn next_deadline(&self) -> Option<u64> {
        self.sync.next_deadline()
    }

    fn up(&self, peer: &NodeName) -> bool {
        self.links.up(peer)
            || self
                .incoming
                .get(peer)
                .is_some_and(|sessions| !sessions.is_empty())
    }

    /// A frame a peer sent on a session it made here.
    pub(crate) fn request(
        &mut self,
        asked: &Asked<'_>,
        records: &mut Records,
        wall_ms: u64,
        out: &mut dyn Out,
    ) -> Result<(), Error> {
        if asked.class != PEER_CLASS {
            return Err(Error::Denied);
        }
        let from: NodeName = asked.caller.parse().map_err(|_| Error::Denied)?;
        self.incoming
            .entry(from.clone())
            .or_default()
            .insert(asked.session);
        let Self {
            sync,
            links,
            incoming,
            ..
        } = self;
        let mut to = To {
            links,
            incoming,
            out,
        };
        sync.receive(
            records.store_mut(),
            &from,
            asked.kind,
            asked.payload,
            wall_ms,
            &mut to,
        )?;
        self.settle(records, wall_ms, out);
        Ok(())
    }

    /// Where sync is with each peer, after forgiving the one a `PeerForgive` names. Whoever
    /// forgives accepts that what was deleted while the peer was away may come back.
    pub(crate) fn ask(
        &mut self,
        asked: &Asked<'_>,
        records: &Records,
        wall_ms: u64,
        out: &mut dyn Out,
    ) -> Result<(), Error> {
        if asked.kind == kind::PEER_FORGIVE {
            let forgive: PeerForgive = asked.body()?;
            if !self.sync.peers().any(|(peer, ..)| *peer == forgive.node) {
                return Err(Error::NotFound);
            }
            let Self {
                sync,
                links,
                incoming,
                ..
            } = self;
            let mut to = To {
                links,
                incoming,
                out,
            };
            sync.forgive(&forgive.node, records.store(), wall_ms, &mut to);
            warn!(
                "{} was forgiven: what was deleted while it was away may come back",
                forgive.node
            );
            self.save_agreed();
        } else {
            let _: PeerStatus = asked.body()?;
        }
        let states: Vec<PeerState> = self
            .sync
            .peers()
            .map(|(node, status, agreed)| PeerState {
                node: node.clone(),
                state: match status {
                    orion_sync::PeerStatus::Down => SyncState::Down,
                    orion_sync::PeerStatus::Syncing => SyncState::Syncing,
                    orion_sync::PeerStatus::InSync => SyncState::InSync,
                    orion_sync::PeerStatus::Stale => SyncState::Stale,
                },
                agreed_ms: agreed.unwrap_or(0),
            })
            .collect();
        out.send(
            asked.session,
            &PeerStates {
                request_seq: asked.seq,
                peers: List::new(&states),
            },
        )
    }

    /// A session ended. A peer it was the last way to is down.
    pub(crate) fn gone(&mut self, session: u32, store: &mut Store, wall_ms: u64) {
        let mut left = Vec::new();
        for (peer, sessions) in &mut self.incoming {
            if sessions.remove(&session) && sessions.is_empty() {
                left.push(peer.clone());
            }
        }
        for peer in left {
            self.incoming.remove(&peer);
            if !self.up(&peer) {
                self.sync.peer_down(&peer, store, wall_ms);
            }
        }
    }

    /// The comm core is gone, and with it every session a peer made here.
    pub(crate) fn forget_sessions(&mut self, store: &mut Store, wall_ms: u64) {
        let peers: Vec<NodeName> = std::mem::take(&mut self.incoming).into_keys().collect();
        for peer in peers {
            if !self.up(&peer) {
                self.sync.peer_down(&peer, store, wall_ms);
            }
        }
    }

    /// Takes what the links heard, sends what changed here, and keeps the links to the nodes
    /// the records give an address for.
    pub(crate) fn turn(&mut self, records: &mut Records, wall_ms: u64, out: &mut dyn Out) {
        for heard in self.links.take() {
            let Self {
                sync,
                links,
                incoming,
                ..
            } = self;
            let mut to = To {
                links,
                incoming,
                out,
            };
            match heard {
                // The other node's node logic is back: it is told where this node is again.
                Heard::Up(peer) | Heard::Back(peer) => {
                    sync.peer_up(&peer, records.store(), wall_ms, &mut to);
                }
                Heard::Frame {
                    node,
                    kind,
                    payload,
                } => {
                    if let Err(error) =
                        sync.receive(records.store_mut(), &node, kind, &payload, wall_ms, &mut to)
                    {
                        warn!("a sync message from {node} was not taken: {error}");
                    }
                }
                Heard::Down(peer) => {
                    if !incoming
                        .get(&peer)
                        .is_some_and(|sessions| !sessions.is_empty())
                    {
                        sync.peer_down(&peer, records.store_mut(), wall_ms);
                    }
                }
            }
        }
        self.settle(records, wall_ms, out);
        let Self {
            sync,
            links,
            incoming,
            ..
        } = self;
        if sync.next_deadline().is_some_and(|due| due <= wall_ms) {
            sync.tick(
                records.store(),
                wall_ms,
                &mut To {
                    links,
                    incoming,
                    out,
                },
            );
        }
        self.save_agreed();
    }

    /// Tells watchers and peers what changed in the store, and looks at the node records
    /// again if one of them did.
    fn settle(&mut self, records: &mut Records, wall_ms: u64, out: &mut dyn Out) {
        records.publish(out);
        let changes = records.take_for_sync();
        let nodes_changed = changes.as_ref().is_none_or(|changes| {
            changes
                .iter()
                .any(|change| change.record.class == RecordClass::Node)
        });
        let Self {
            sync,
            links,
            incoming,
            ..
        } = self;
        let mut to = To {
            links,
            incoming,
            out,
        };
        match &changes {
            Some(changes) if changes.is_empty() => {}
            Some(changes) => sync.changed(records.store(), changes, wall_ms, &mut to),
            None => sync.resync(records.store(), &mut to),
        }
        if nodes_changed || !self.looked {
            self.looked = true;
            self.links.want(&addresses(records.store()));
        }
    }

    fn save_agreed(&mut self) {
        let agreed: Vec<(NodeName, u64)> = self
            .sync
            .agreed_at()
            .map(|(peer, at)| (peer.clone(), at))
            .collect();
        if agreed == self.agreed {
            return;
        }
        let text: String = agreed
            .iter()
            .map(|(peer, at)| format!("{peer} {at}\n"))
            .collect();
        // Written beside and renamed over: a power cut leaves the old file or the new one.
        let temporary = self.agreed_file.with_extension("new");
        let written = std::fs::write(&temporary, text)
            .and_then(|()| std::fs::rename(&temporary, &self.agreed_file));
        match written {
            Ok(()) => self.agreed = agreed,
            Err(error) => warn!(
                "{} could not be written: {error}",
                self.agreed_file.display()
            ),
        }
    }
}

/// Every other node the records give an address for.
fn addresses(store: &Store) -> BTreeMap<NodeName, String> {
    let mut nodes = BTreeMap::new();
    for record in store.since(RecordClass::Node, "", HlcTimestamp::ZERO) {
        if record.version.deleted {
            continue;
        }
        let Ok(node) = NodeRecord::decode(&record.version.value) else {
            continue;
        };
        if node.name != *store.node() && !node.address.is_empty() {
            nodes.insert(node.name.clone(), node.address.to_owned());
        }
    }
    nodes
}

/// What `sync-agreed` says: lines of a peer and a wall time in milliseconds.
fn read_agreed(path: &std::path::Path) -> Vec<(NodeName, u64)> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let agreed: Vec<(NodeName, u64)> = text
        .lines()
        .filter_map(|line| {
            let (peer, at) = line.split_once(' ')?;
            Some((peer.parse().ok()?, at.trim().parse().ok()?))
        })
        .collect();
    if !agreed.is_empty() {
        info!("{} peers were agreed with before this start", agreed.len());
    }
    agreed
}

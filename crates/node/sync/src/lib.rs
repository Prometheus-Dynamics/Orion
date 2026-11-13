//! Peer sync.
//!
//! Every node keeps the whole of the cluster's desired records and merges them one record at
//! a time, last writer wins (E1). Sync is what carries the versions between nodes. There is
//! no leader and no order of nodes: any two that can talk exchange what the other lacks, and
//! the merge gives the same result whatever arrives, in whatever order, however often.
//!
//! Two messages, both already on the wire:
//!
//! - `PeerSync` with records is **data**: versions to merge. It asks for nothing.
//! - `PeerSync` with no records is a **summary**: the sender's newest stamp and a digest of
//!   everything it has. Data is always followed by one.
//! - `PeerObserved` is the sender's whole observed slice, which replaces the one before.
//!
//! What a node does with a summary from a peer:
//!
//! 1. The digests are equal: they agree. It says so once, with its own summary, if the peer
//!    has not heard this digest from it.
//! 2. It has versions newer than the peer's newest: the peer cannot have those. It sends them.
//! 3. The peer's newest is newer than its own: the peer is ahead. It sends its summary, once
//!    for this pair of digests, and the peer answers by rule 2.
//! 4. Otherwise they differ somewhere in the past, as after a partition. It sends everything,
//!    once for this pair of digests and no more often than `full_interval_ms`.
//!
//! Nothing is sent on a timer. A change is sent to every peer as it is made, and a version
//! taken from one peer is passed on to the others, so a node that hears only some of its
//! peers still gets everything.
//!
//! This is plain data and a function of what it is told. Who a peer is, and the connection to
//! it, are the comm core's.

mod peer;

use std::collections::BTreeMap;

use orion_msg_peers::{PeerObserved, PeerSync};
use orion_msg_records::RecordEntry;
use orion_store::{Change, Merged, Record, Scope, Store};
use orion_types::{Error, HlcTimestamp, NodeName};
use orion_wire::body::encode_fields;
use orion_wire::{Body, Decode, List, kind};
use tracing::warn;

use peer::Peer;
pub use peer::PeerStatus;

/// About how many bytes of records go in one message.
const PAGE_BYTES: usize = 24 * 1024;

/// What sync is told.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SyncConfig {
    /// The least time between two sends of everything to the same peer.
    pub full_interval_ms: u64,
    /// A peer last agreed with longer ago than this is stale: it is not synced with until
    /// someone says it may be, because it would bring back what was deleted while it was
    /// away. This is the store's tombstone retention.
    pub stale_after_ms: u64,
    /// The largest message, payload only.
    pub max_message: usize,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            full_interval_ms: 10_000,
            stale_after_ms: 7 * 24 * 60 * 60 * 1000,
            max_message: 60 * 1024,
        }
    }
}

/// Where messages for peers go.
pub trait Out {
    /// Sends a frame of `kind` with `payload` to `peer`. A failure is the connection's: sync
    /// starts again when the peer is back.
    fn frame(&mut self, peer: &NodeName, kind: u8, payload: &[u8]);
}

/// What has been counted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counters {
    /// Summaries sent.
    pub summaries_sent: u64,
    /// Messages with records sent.
    pub data_sent: u64,
    /// Versions sent.
    pub versions_sent: u64,
    /// Times everything was sent to a peer.
    pub fulls_sent: u64,
    /// Versions from peers that were taken.
    pub applied: u64,
    /// Versions from peers refused for a stamp too far ahead.
    pub skewed: u64,
    /// Messages refused from a stale peer.
    pub refused_stale: u64,
    /// Records that did not fit a message and were not sent.
    pub too_large: u64,
}

/// What a message from a peer did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Received {
    /// Records were merged: so many replaced what was here.
    Data(usize),
    /// A summary, and whether the two now agree.
    Summary(bool),
    /// The peer's observed slice was replaced.
    Observed,
}

/// Peer sync for one node.
#[derive(Debug)]
pub struct Sync {
    me: NodeName,
    config: SyncConfig,
    peers: BTreeMap<NodeName, Peer>,
    counters: Counters,
    scratch: Vec<u8>,
}

fn size(record: &Record) -> usize {
    record.key.len() + record.version.value.len() + 32
}

impl Sync {
    /// Sync for the node `me`.
    #[must_use]
    pub fn new(me: NodeName, config: SyncConfig) -> Self {
        Self {
            me,
            config,
            peers: BTreeMap::new(),
            counters: Counters::default(),
            scratch: vec![0; config.max_message],
        }
    }

    /// What has been counted.
    #[must_use]
    pub const fn counters(&self) -> Counters {
        self.counters
    }

    /// Where sync with `peer` is.
    #[must_use]
    pub fn status(&self, peer: &NodeName) -> PeerStatus {
        self.peers.get(peer).map_or(PeerStatus::Down, Peer::status)
    }

    /// Every peer sync knows of, by name: where sync with it is, and when the two last
    /// agreed, wall clock milliseconds.
    pub fn peers(&self) -> impl Iterator<Item = (&NodeName, PeerStatus, Option<u64>)> {
        self.peers
            .iter()
            .map(|(name, peer)| (name, peer.status(), peer.agreed_ms))
    }

    /// When each peer was last agreed with, to keep across a restart.
    pub fn agreed_at(&self) -> impl Iterator<Item = (&NodeName, u64)> {
        self.peers
            .iter()
            .filter_map(|(name, peer)| peer.agreed_ms.map(|at| (name, at)))
    }

    /// Says when `peer` was last agreed with, as kept from before a restart.
    pub fn restore_agreed(&mut self, peer: &NodeName, at_ms: u64) {
        self.peers.entry(peer.clone()).or_default().agreed_ms = Some(at_ms);
    }

    /// Lets a stale peer sync again. Whoever says this accepts that what was deleted while it
    /// was away may come back.
    pub fn forgive(&mut self, peer: &NodeName, store: &Store, now_ms: u64, out: &mut dyn Out) {
        if let Some(state) = self.peers.get_mut(peer) {
            state.stale = false;
            state.agreed_ms = None;
            if state.up {
                self.summary(peer, store, out);
                let _ = now_ms;
            }
        }
    }

    fn send<B: Body>(&mut self, peer: &NodeName, body: &B, out: &mut dyn Out) -> bool {
        match encode_fields(body, &mut self.scratch) {
            Ok(len) => {
                out.frame(peer, B::KIND, &self.scratch[..len]);
                true
            }
            Err(_) => false,
        }
    }

    fn summary(&mut self, peer: &NodeName, store: &Store, out: &mut dyn Out) {
        let fingerprint = store.fingerprint();
        let message = PeerSync {
            node: self.me.clone(),
            newest: store.newest(),
            fingerprint,
            records: List::new(&[]),
        };
        if self.send(peer, &message, out) {
            self.counters.summaries_sent += 1;
            if let Some(state) = self.peers.get_mut(peer) {
                state.told = Some(fingerprint);
            }
        }
    }

    /// Sends `records` to `peer` in as many messages as it takes, then a summary.
    fn data(&mut self, peer: &NodeName, records: &[Record], store: &Store, out: &mut dyn Out) {
        let (newest, fingerprint) = (store.newest(), store.fingerprint());
        let mut page: Vec<RecordEntry<'_>> = Vec::new();
        let mut bytes = 0;
        let mut flush = |this: &mut Self, page: &mut Vec<RecordEntry<'_>>| {
            if page.is_empty() {
                return;
            }
            let message = PeerSync {
                node: this.me.clone(),
                newest,
                fingerprint,
                records: List::new(page),
            };
            if this.send(peer, &message, out) {
                this.counters.data_sent += 1;
                this.counters.versions_sent += page.len() as u64;
            } else {
                this.counters.too_large += page.len() as u64;
            }
            page.clear();
        };
        for record in records {
            if bytes + size(record) > PAGE_BYTES {
                flush(self, &mut page);
                bytes = 0;
            }
            bytes += size(record);
            page.push(record.entry());
        }
        flush(self, &mut page);
        self.summary(peer, store, out);
    }

    fn observed(&mut self, peer: &NodeName, store: &Store, out: &mut dyn Out) {
        let slice: Vec<Record> = store.observed_slice(&self.me).collect();
        let entries: Vec<RecordEntry<'_>> = slice.iter().map(Record::entry).collect();
        let message = PeerObserved {
            node: self.me.clone(),
            records: List::new(&entries),
        };
        if !self.send(peer, &message, out) {
            self.counters.too_large += entries.len() as u64;
            warn!(
                "what this node observes does not fit one message ({} records): {peer} was not told",
                entries.len()
            );
        }
    }

    /// A connection to `peer` is up, whoever made it. It is told where this node is and what
    /// it sees.
    pub fn peer_up(&mut self, peer: &NodeName, store: &Store, now_ms: u64, out: &mut dyn Out) {
        if *peer == self.me {
            return;
        }
        let stale_after = self.config.stale_after_ms;
        let state = self.peers.entry(peer.clone()).or_default();
        state.connected(now_ms, stale_after);
        let stale = state.stale;
        if stale {
            warn!(
                "{peer} was last agreed with more than {stale_after} ms ago: it is not synced with until it is forgiven"
            );
        } else {
            self.summary(peer, store, out);
        }
        self.observed(peer, store, out);
    }

    /// The connection to `peer` is gone. What it observed is forgotten: nobody knows now.
    pub fn peer_down(&mut self, peer: &NodeName, store: &mut Store, now_ms: u64) {
        if let Some(state) = self.peers.get_mut(peer) {
            state.disconnected(now_ms);
        }
        store.drop_observed(peer);
    }

    /// Takes a message from `from`, the peer the connection is known to be.
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`] if it does not decode, [`Error::Denied`] if it claims to be from
    /// another node or the peer is stale, [`Error::Unsupported`] for another kind.
    pub fn receive(
        &mut self,
        store: &mut Store,
        from: &NodeName,
        kind: u8,
        payload: &[u8],
        now_ms: u64,
        out: &mut dyn Out,
    ) -> Result<Received, Error> {
        if *from == self.me {
            return Err(Error::Denied);
        }
        if !self.peers.get(from).is_some_and(|peer| peer.up) {
            // A message is as good a sign as any that the peer is there.
            self.peer_up(from, store, now_ms, out);
        }
        match kind {
            kind::PEER_OBSERVED => {
                let observed = PeerObserved::decode(payload)?;
                if observed.node != *from {
                    return Err(Error::Denied);
                }
                let mut entries = Vec::new();
                for entry in observed.records.iter() {
                    entries.push(entry?);
                }
                store.observed_from(from, &entries);
                Ok(Received::Observed)
            }
            kind::PEER_SYNC => {
                let sync = PeerSync::decode(payload)?;
                if sync.node != *from {
                    return Err(Error::Denied);
                }
                if self.peers.get(from).is_some_and(|peer| peer.stale) {
                    self.counters.refused_stale += 1;
                    return Err(Error::Denied);
                }
                if sync.records.is_empty() {
                    Ok(Received::Summary(self.compare(
                        store,
                        from,
                        sync.newest,
                        sync.fingerprint,
                        now_ms,
                        out,
                    )))
                } else {
                    Ok(Received::Data(self.merge(store, from, &sync, now_ms)?))
                }
            }
            _ => Err(Error::Unsupported),
        }
    }

    fn merge(
        &mut self,
        store: &mut Store,
        from: &NodeName,
        sync: &PeerSync<'_>,
        now_ms: u64,
    ) -> Result<usize, Error> {
        let mut applied = 0;
        for entry in sync.records.iter() {
            let entry = entry?;
            match store.merge(&entry, now_ms) {
                Merged::Applied => {
                    applied += 1;
                    if let Some(peer) = self.peers.get_mut(from) {
                        peer.heard(entry.class.code(), entry.key, entry.stamp);
                    }
                }
                Merged::Skewed(skew) => {
                    self.counters.skewed += 1;
                    warn!(
                        "a record from {from} is stamped {} ms ahead of this node's clock and was refused",
                        skew.ahead_by_ms()
                    );
                }
                Merged::Stale | Merged::Expired => {}
            }
        }
        self.counters.applied += applied as u64;
        Ok(applied)
    }

    /// Compares with a peer's summary and sends what the rules say. Whether the two agree.
    fn compare(
        &mut self,
        store: &Store,
        from: &NodeName,
        newest: HlcTimestamp,
        theirs: u64,
        now_ms: u64,
        out: &mut dyn Out,
    ) -> bool {
        let mine = store.fingerprint();
        let Some(peer) = self.peers.get_mut(from) else {
            return false;
        };
        peer.newest = newest;
        peer.fingerprint = theirs;
        peer.heard_summary = true;
        if mine == theirs {
            peer.agreed(now_ms);
            if peer.told != Some(mine) {
                self.summary(from, store, out);
            }
            return true;
        }
        peer.differs();
        let pair = (mine, theirs);
        let newer: Vec<Record> = store
            .all()
            .filter(|record| record.version.stamp > newest)
            .collect();
        if !newer.is_empty() {
            self.data(from, &newer, store, out);
            return false;
        }
        if newest > store.newest() && peer.asked != Some(pair) {
            // The peer is ahead. Told where this node is, it sends what is newer. If they
            // still differ after that, the difference is in the past.
            peer.asked = Some(pair);
            self.summary(from, store, out);
            return false;
        }
        if peer.full_for != Some(pair) {
            let due = peer
                .last_full_ms
                .map_or(0, |at| at.saturating_add(self.config.full_interval_ms));
            if now_ms >= due {
                self.full(from, store, pair, now_ms, out);
                return false;
            }
            peer.full_due = Some(due);
        }
        // Nothing to send but where this node is, if the peer has not heard it: should it
        // have caught up meanwhile, it says so.
        if self
            .peers
            .get(from)
            .is_some_and(|peer| peer.told != Some(mine))
        {
            self.summary(from, store, out);
        }
        false
    }

    fn full(
        &mut self,
        peer: &NodeName,
        store: &Store,
        pair: (u64, u64),
        now_ms: u64,
        out: &mut dyn Out,
    ) {
        if let Some(state) = self.peers.get_mut(peer) {
            state.full_for = Some(pair);
            state.last_full_ms = Some(now_ms);
            state.full_due = None;
        }
        self.counters.fulls_sent += 1;
        let all: Vec<Record> = store.all().collect();
        self.data(peer, &all, store, out);
    }

    /// Sends what changed in the store to the peers: desired versions to everyone but the
    /// peer they came from, and this node's observed slice if it changed. Give it what
    /// `Store::take_changes` returned.
    pub fn changed(&mut self, store: &Store, changes: &[Change], now_ms: u64, out: &mut dyn Out) {
        let mine = store.fingerprint();
        let observed = changes
            .iter()
            .any(|change| matches!(&change.scope, Scope::Observed(node) if *node == self.me));
        let desired: Vec<&Record> = changes
            .iter()
            .filter(|change| change.scope == Scope::Desired)
            .map(|change| &change.record)
            .collect();
        let peers: Vec<NodeName> = self
            .peers
            .iter()
            .filter(|(_, peer)| peer.up)
            .map(|(name, _)| name.clone())
            .collect();
        for name in peers {
            let Some(peer) = self.peers.get_mut(&name) else {
                continue;
            };
            let stale = peer.stale;
            if !stale && peer.heard_summary && peer.fingerprint == mine {
                // What changed here brought this node to where the peer said it is.
                peer.agreed(now_ms);
            }
            let theirs: Vec<Record> = desired
                .iter()
                .filter(|record| {
                    !peer.has_heard(record.class.code(), &record.key, record.version.stamp)
                })
                .map(|record| (*record).clone())
                .collect();
            peer.forget_heard();
            if !theirs.is_empty() && !stale {
                self.data(&name, &theirs, store, out);
            }
            if observed {
                self.observed(&name, store, out);
            }
        }
    }

    /// More changed than the store kept track of: every peer is told where this node is, and
    /// the rules find what each lacks.
    pub fn resync(&mut self, store: &Store, out: &mut dyn Out) {
        let peers: Vec<NodeName> = self
            .peers
            .iter()
            .filter(|(_, peer)| peer.up && !peer.stale)
            .map(|(name, _)| name.clone())
            .collect();
        for name in peers {
            if let Some(peer) = self.peers.get_mut(&name) {
                peer.forget_heard();
                peer.asked = None;
            }
            self.summary(&name, store, out);
            self.observed(&name, store, out);
        }
    }

    /// When [`Sync::tick`] next has something to do.
    #[must_use]
    pub fn next_deadline(&self) -> Option<u64> {
        self.peers
            .values()
            .filter(|peer| peer.up)
            .filter_map(|peer| peer.full_due)
            .min()
    }

    /// Sends what was held back by `full_interval_ms` and is now due.
    pub fn tick(&mut self, store: &Store, now_ms: u64, out: &mut dyn Out) {
        let due: Vec<NodeName> = self
            .peers
            .iter()
            .filter(|(_, peer)| peer.up && peer.full_due.is_some_and(|at| at <= now_ms))
            .map(|(name, _)| name.clone())
            .collect();
        for name in due {
            let Some(peer) = self.peers.get(&name) else {
                continue;
            };
            let pair = (store.fingerprint(), peer.fingerprint);
            if pair.0 == pair.1 || peer.full_for == Some(pair) {
                if let Some(peer) = self.peers.get_mut(&name) {
                    peer.full_due = None;
                }
                continue;
            }
            self.full(&name, store, pair, now_ms, out);
        }
    }
}

//! The record store.
//!
//! A record is a named piece of state. Every node keeps a full copy of the cluster's
//! **desired** records and they converge without a leader: each version of a record carries
//! a stamp from the writing node's hybrid logical clock, and for one record the version with
//! the greater stamp wins. That is a join over a total order, so the result is the same
//! whatever order versions arrive in and however often they are sent again.
//!
//! - **A local write** is stamped by this node's clock, which is always ahead of every stamp
//!   the node has seen.
//! - **A version from a peer** replaces the one here if its stamp is greater. On equal
//!   stamps, which only a restored backup or two node names with the same tag can produce, a
//!   delete beats a write and then the larger value wins: the rule only has to be the same
//!   everywhere.
//! - **A delete is a version too**: a tombstone, kept so that a node which missed it cannot
//!   bring the record back. Tombstones are collected after a retention time, and a delete
//!   older than that which has nothing to delete here is ignored.
//! - **A stamp too far ahead of this node's clock** is refused. One node with a wrong clock
//!   must not be able to write versions nobody can ever override.
//!
//! **Observed** records are different: what a node sees right now. Each node owns its own
//! slice and sends the whole of it; nothing is merged across nodes, and a slice goes when its
//! node does.
//!
//! The store is plain data with no lock and no I/O. Node logic owns it from one task, so
//! nothing is held across an `await`.

use std::collections::{BTreeMap, VecDeque};

use orion_msg_records::{RecordClass, RecordEntry};
use orion_types::{HlcClockSkew, HlcTimestamp, HybridLogicalClock, NodeName, hlc_node_tag};
use orion_wire::Bytes;

/// How many changes wait to be taken before the store stops keeping them and says so.
pub const CHANGE_QUEUE: usize = 4096;

/// What the store is told.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreConfig {
    /// A stamp further ahead of this node's clock than this is refused.
    pub max_drift_ms: u64,
    /// How long a delete is remembered.
    pub tombstone_retention_ms: u64,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            max_drift_ms: 300_000,
            tombstone_retention_ms: 7 * 24 * 60 * 60 * 1000,
        }
    }
}

/// One version of a record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Version {
    /// When it was written.
    pub stamp: HlcTimestamp,
    /// Whether it says the record is gone.
    pub deleted: bool,
    /// The typed record, encoded. The store keeps and merges it without reading it.
    pub value: Vec<u8>,
}

/// A record: which one, and its version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    /// The sort of record.
    pub class: RecordClass,
    /// Its name, unique within its class.
    pub key: String,
    /// Its version.
    pub version: Version,
}

impl Record {
    /// The record as it goes on the wire.
    #[must_use]
    pub fn entry(&self) -> RecordEntry<'_> {
        RecordEntry {
            class: self.class,
            key: &self.key,
            stamp: self.version.stamp,
            deleted: self.version.deleted,
            value: Bytes(&self.version.value),
        }
    }
}

/// What merging one version from a peer did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Merged {
    /// It replaced what was here.
    Applied,
    /// What is here is as new or newer. The two nodes agree on this record.
    Stale,
    /// Its stamp is too far ahead of this node's clock. It was refused.
    Skewed(HlcClockSkew),
    /// A delete older than the retention time, with nothing here to delete.
    Expired,
}

/// Whose record changed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Scope {
    /// A desired record.
    Desired,
    /// A record in this node's observed slice, or a peer's.
    Observed(NodeName),
}

/// One change, for whoever pushes changes to watchers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Change {
    /// Whose record.
    pub scope: Scope,
    /// The record as it now is. For an observed record that went, `deleted` is set.
    pub record: Record,
}

/// What the store has counted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counters {
    /// Versions written by this node.
    pub local_writes: u64,
    /// Versions from peers that replaced what was here.
    pub remote_applied: u64,
    /// Versions from peers that lost to what was here.
    pub remote_stale: u64,
    /// Versions refused for a stamp too far ahead.
    pub skewed: u64,
    /// Deletes ignored for being past the retention time.
    pub expired: u64,
    /// Tombstones collected.
    pub collected: u64,
}

type Key = (u8, String);
type Table = BTreeMap<Key, Version>;

fn record(key: &Key, version: &Version) -> Record {
    Record {
        class: RecordClass::from_code(key.0),
        key: key.1.clone(),
        version: version.clone(),
    }
}

/// Whether `candidate` replaces `current`: the greater stamp; on equal stamps a delete beats
/// a write, then the larger value.
fn wins(candidate: &Version, current: &Version) -> bool {
    (candidate.stamp, candidate.deleted, &candidate.value)
        > (current.stamp, current.deleted, &current.value)
}

/// The record store of one node.
#[derive(Debug)]
pub struct Store {
    node: NodeName,
    config: StoreConfig,
    clock: HybridLogicalClock,
    desired: Table,
    observed: BTreeMap<NodeName, Table>,
    changes: VecDeque<Change>,
    /// Changes were dropped because nobody took them. Whoever follows changes starts over.
    overflowed: bool,
    counters: Counters,
}

impl Store {
    /// An empty store for the node `node`.
    #[must_use]
    pub fn new(node: NodeName, config: StoreConfig) -> Self {
        Self {
            clock: HybridLogicalClock::new(hlc_node_tag(node.as_str()), config.max_drift_ms),
            node,
            config,
            desired: Table::new(),
            observed: BTreeMap::new(),
            changes: VecDeque::new(),
            overflowed: false,
            counters: Counters::default(),
        }
    }

    /// A store holding `records`, as kept from before a restart. The clock starts past every
    /// stamp among them, so writes made now order after everything already written, even if
    /// the board's clock was reset.
    #[must_use]
    pub fn restored(
        node: NodeName,
        config: StoreConfig,
        records: impl IntoIterator<Item = Record>,
    ) -> Self {
        let mut store = Self::new(node, config);
        for kept in records {
            store.clock.advance_to(kept.version.stamp);
            let key = (kept.class.code(), kept.key);
            match store.desired.get(&key) {
                Some(current) if !wins(&kept.version, current) => {}
                _ => {
                    store.desired.insert(key, kept.version);
                }
            }
        }
        store
    }

    /// This node's name.
    #[must_use]
    pub const fn node(&self) -> &NodeName {
        &self.node
    }

    /// What has been counted.
    #[must_use]
    pub const fn counters(&self) -> Counters {
        self.counters
    }

    /// The newest stamp this node has issued or accepted.
    #[must_use]
    pub const fn now(&self) -> HlcTimestamp {
        self.clock.last()
    }

    fn changed(&mut self, scope: Scope, key: &Key, version: &Version) {
        if self.changes.len() >= CHANGE_QUEUE {
            self.changes.clear();
            self.overflowed = true;
            return;
        }
        if !self.overflowed {
            self.changes.push_back(Change {
                scope,
                record: record(key, version),
            });
        }
    }

    /// Takes the changes made since the last call, oldest first. `None` if so many were made
    /// without being taken that they were dropped: whoever follows changes then reads the
    /// records again, with [`Store::since`].
    pub fn take_changes(&mut self) -> Option<Vec<Change>> {
        if std::mem::take(&mut self.overflowed) {
            self.changes.clear();
            return None;
        }
        Some(self.changes.drain(..).collect())
    }

    /// Writes a desired record, stamped by this node's clock at `wall_ms`. Returns its stamp.
    pub fn put(
        &mut self,
        class: RecordClass,
        key: &str,
        value: &[u8],
        wall_ms: u64,
    ) -> HlcTimestamp {
        self.write(class, key, false, value, wall_ms)
    }

    /// Deletes a desired record: writes a tombstone. `None` if there is no such record.
    pub fn delete(&mut self, class: RecordClass, key: &str, wall_ms: u64) -> Option<HlcTimestamp> {
        self.get(class, key)?;
        Some(self.write(class, key, true, &[], wall_ms))
    }

    fn write(
        &mut self,
        class: RecordClass,
        key: &str,
        deleted: bool,
        value: &[u8],
        wall_ms: u64,
    ) -> HlcTimestamp {
        let stamp = self.clock.now(wall_ms);
        let key = (class.code(), key.to_owned());
        let version = Version {
            stamp,
            deleted,
            value: value.to_vec(),
        };
        self.changed(Scope::Desired, &key, &version);
        self.desired.insert(key, version);
        self.counters.local_writes += 1;
        stamp
    }

    /// Merges one version of a desired record from a peer, received at `wall_ms`.
    pub fn merge(&mut self, entry: &RecordEntry<'_>, wall_ms: u64) -> Merged {
        if let Err(skew) = self.clock.check(entry.stamp, wall_ms) {
            self.counters.skewed += 1;
            return Merged::Skewed(skew);
        }
        let key = (entry.class.code(), entry.key.to_owned());
        let candidate = Version {
            stamp: entry.stamp,
            deleted: entry.deleted,
            value: if entry.deleted {
                Vec::new()
            } else {
                entry.value.0.to_vec()
            },
        };
        match self.desired.get(&key) {
            Some(current) if !wins(&candidate, current) => {
                self.counters.remote_stale += 1;
                return Merged::Stale;
            }
            // A delete of something this node never had or has already forgotten, from so long
            // ago that its tombstone would be collected at once: nothing to do, and keeping it
            // would let old deletes circulate for ever.
            None if candidate.deleted && self.expired(candidate.stamp, wall_ms) => {
                self.counters.expired += 1;
                return Merged::Expired;
            }
            _ => {}
        }
        // Whatever this node writes next orders after everything it has accepted.
        self.clock.advance_to(candidate.stamp);
        self.changed(Scope::Desired, &key, &candidate);
        self.desired.insert(key, candidate);
        self.counters.remote_applied += 1;
        Merged::Applied
    }

    const fn expired(&self, stamp: HlcTimestamp, wall_ms: u64) -> bool {
        stamp.physical_ms < wall_ms.saturating_sub(self.config.tombstone_retention_ms)
    }

    /// Forgets deletes older than the retention time. Returns how many. This is not a write:
    /// nothing is stamped and nobody is told.
    pub fn collect_tombstones(&mut self, wall_ms: u64) -> usize {
        let before = self.desired.len();
        let cutoff = wall_ms.saturating_sub(self.config.tombstone_retention_ms);
        self.desired
            .retain(|_, version| !version.deleted || version.stamp.physical_ms >= cutoff);
        let collected = before - self.desired.len();
        self.counters.collected += collected as u64;
        collected
    }

    /// A desired record, if it exists and is not deleted.
    #[must_use]
    pub fn get(&self, class: RecordClass, key: &str) -> Option<&Version> {
        self.desired
            .get(&(class.code(), key.to_owned()))
            .filter(|version| !version.deleted)
    }

    /// The desired records of `class` whose keys start with `prefix`, by key. Deleted ones
    /// are left out.
    pub fn list<'a>(
        &'a self,
        class: RecordClass,
        prefix: &'a str,
    ) -> impl Iterator<Item = Record> + 'a {
        self.desired
            .range((class.code(), prefix.to_owned())..)
            .take_while(move |(key, _)| key.0 == class.code() && key.1.starts_with(prefix))
            .filter(|(_, version)| !version.deleted)
            .map(|(key, version)| record(key, version))
    }

    /// Every version of a desired record of `class` under `prefix` that is newer than
    /// `after`, deletes included, oldest first. This is what a watcher that fell behind, or
    /// a peer that is behind, is sent.
    #[must_use]
    pub fn since(&self, class: RecordClass, prefix: &str, after: HlcTimestamp) -> Vec<Record> {
        let mut newer: Vec<Record> = self
            .desired
            .range((class.code(), prefix.to_owned())..)
            .take_while(|(key, _)| key.0 == class.code() && key.1.starts_with(prefix))
            .filter(|(_, version)| version.stamp > after)
            .map(|(key, version)| record(key, version))
            .collect();
        newer.sort_by_key(|record| record.version.stamp);
        newer
    }

    /// Every desired version there is, deletes included, in key order: the whole state, for
    /// keeping across a restart or sending to a peer that has nothing.
    pub fn all(&self) -> impl Iterator<Item = Record> + '_ {
        self.desired
            .iter()
            .map(|(key, version)| record(key, version))
    }

    /// The newest stamp among the desired records.
    #[must_use]
    pub fn newest(&self) -> HlcTimestamp {
        self.desired
            .values()
            .map(|version| version.stamp)
            .max()
            .unwrap_or(HlcTimestamp::ZERO)
    }

    /// A digest of the whole desired state: which records, at which stamps, deleted or not.
    /// Two nodes with the same digest have nothing to send each other.
    #[must_use]
    pub fn fingerprint(&self) -> u64 {
        // FNV-1a over every record's identity and stamp, in key order.
        let mut hash = 0xcbf2_9ce4_8422_2325_u64;
        let mut feed = |bytes: &[u8]| {
            for byte in bytes {
                hash ^= u64::from(*byte);
                hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
            }
        };
        for (key, version) in &self.desired {
            feed(&[key.0]);
            feed(&(key.1.len() as u32).to_le_bytes());
            feed(key.1.as_bytes());
            feed(&version.stamp.physical_ms.to_le_bytes());
            feed(&version.stamp.logical.to_le_bytes());
            feed(&version.stamp.node.to_le_bytes());
            feed(&[u8::from(version.deleted)]);
        }
        hash
    }

    /// How many desired versions are kept, deletes included.
    #[must_use]
    pub fn len(&self) -> usize {
        self.desired.len()
    }

    /// Whether nothing is kept.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.desired.is_empty()
    }

    /// Says this node sees `key` now, with `value`. Returns its stamp.
    pub fn observe(
        &mut self,
        class: RecordClass,
        key: &str,
        value: &[u8],
        wall_ms: u64,
    ) -> HlcTimestamp {
        let stamp = self.clock.now(wall_ms);
        let key = (class.code(), key.to_owned());
        let version = Version {
            stamp,
            deleted: false,
            value: value.to_vec(),
        };
        self.changed(Scope::Observed(self.node.clone()), &key, &version);
        self.observed
            .entry(self.node.clone())
            .or_default()
            .insert(key, version);
        stamp
    }

    /// Says this node no longer sees `key`. Whether it did.
    pub fn unobserve(&mut self, class: RecordClass, key: &str, wall_ms: u64) -> bool {
        let key = (class.code(), key.to_owned());
        let Some(slice) = self.observed.get_mut(&self.node) else {
            return false;
        };
        if slice.remove(&key).is_none() {
            return false;
        }
        let gone = Version {
            stamp: self.clock.now(wall_ms),
            deleted: true,
            value: Vec::new(),
        };
        self.changed(Scope::Observed(self.node.clone()), &key, &gone);
        true
    }

    /// Replaces a peer's whole observed slice with what it just sent. A slice is its node's
    /// alone: nothing is merged, and what the peer no longer sees is gone.
    pub fn observed_from(&mut self, node: &NodeName, entries: &[RecordEntry<'_>]) {
        if *node == self.node {
            // This node's own slice is this node's to say.
            return;
        }
        let mut now = Table::new();
        for entry in entries.iter().filter(|entry| !entry.deleted) {
            now.insert(
                (entry.class.code(), entry.key.to_owned()),
                Version {
                    stamp: entry.stamp,
                    deleted: false,
                    value: entry.value.0.to_vec(),
                },
            );
        }
        let before = self.observed.remove(node).unwrap_or_default();
        for (key, version) in &before {
            if !now.contains_key(key) {
                let gone = Version {
                    stamp: version.stamp,
                    deleted: true,
                    value: Vec::new(),
                };
                self.changed(Scope::Observed(node.clone()), key, &gone);
            }
        }
        for (key, version) in &now {
            if before.get(key) != Some(version) {
                self.changed(Scope::Observed(node.clone()), key, version);
            }
        }
        if !now.is_empty() {
            self.observed.insert(node.clone(), now);
        }
    }

    /// Forgets everything a node observed: it is gone.
    pub fn drop_observed(&mut self, node: &NodeName) {
        self.observed_from(node, &[]);
    }

    /// What `node` observes of `class` under `prefix`, by key.
    pub fn observed<'a>(
        &'a self,
        node: &NodeName,
        class: RecordClass,
        prefix: &'a str,
    ) -> impl Iterator<Item = Record> + 'a {
        self.observed
            .get(node)
            .into_iter()
            .flat_map(move |slice| slice.range((class.code(), prefix.to_owned())..))
            .take_while(move |(key, _)| key.0 == class.code() && key.1.starts_with(prefix))
            .map(|(key, version)| record(key, version))
    }

    /// What every node observes of `class` under `prefix`: each record with the node that
    /// sees it.
    pub fn observed_everywhere<'a>(
        &'a self,
        class: RecordClass,
        prefix: &'a str,
    ) -> impl Iterator<Item = (&'a NodeName, Record)> + 'a {
        self.observed.keys().flat_map(move |node| {
            self.observed(node, class, prefix)
                .map(move |record| (node, record))
        })
    }
}

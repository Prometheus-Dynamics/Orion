//! Records, answered: writes, reads and watches against the store.

use orion_msg_records::{Changed, Get, Put, RecordClass, RecordEntry, Watch};
use orion_store::{Change, Record, Scope, Store};
use orion_types::{Error, HlcTimestamp};
use orion_wire::{List, kind};

use orion_reply::{Asked, Out};
use orion_resources::Resources;

/// About how many bytes of records go in one message. A record larger than this goes alone.
const PAGE_BYTES: usize = 24 * 1024;

/// How long a provider has to write its resources again after node logic connects, before
/// the ones nobody wrote are marked lost.
pub const RECLAIM_MS: u64 = 2_000;

#[derive(Debug)]
struct Watcher {
    session: u32,
    seq: u16,
    class: RecordClass,
    prefix: String,
}

/// The record service: the store, and who is watching it.
#[derive(Debug)]
pub struct Records {
    store: Store,
    resources: Resources,
    watchers: Vec<Watcher>,
    dirty: bool,
    /// When the resources nobody has written again since node logic connected are lost.
    reclaim_at: Option<u64>,
    /// What changed since peer sync last looked. `None`: more than was kept track of.
    for_sync: Option<Vec<Change>>,
    /// A record placement reads changed: a node, a workload, a resource, anyone's.
    placement_due: bool,
}

/// Sends `records` to `session` as answers to `seq`, in as many messages as it takes. With
/// none, one empty message when `always` is set: a read is always answered.
///
/// A record that does not fit a message is an error, and nothing after it is sent.
fn send(
    out: &mut dyn Out,
    session: u32,
    seq: u16,
    records: &[Record],
    always: bool,
) -> Result<(), Error> {
    if records.is_empty() {
        if always {
            out.send(
                session,
                &Changed {
                    request_seq: seq,
                    records: List::new(&[]),
                    more: false,
                },
            )?;
        }
        return Ok(());
    }
    let mut entries: Vec<RecordEntry<'_>> = Vec::new();
    let mut bytes = 0;
    for (index, record) in records.iter().enumerate() {
        entries.push(record.entry());
        bytes += record.key.len() + record.version.value.len() + 24;
        let last = index + 1 == records.len();
        if bytes >= PAGE_BYTES || last {
            out.send(
                session,
                &Changed {
                    request_seq: seq,
                    records: List::new(&entries),
                    more: !last,
                },
            )?;
            entries.clear();
            bytes = 0;
        }
    }
    Ok(())
}

impl Records {
    /// The service over `store`.
    #[must_use]
    pub fn new(store: Store) -> Self {
        Self {
            resources: Resources::new(store.node().clone()),
            store,
            watchers: Vec::new(),
            dirty: false,
            reclaim_at: None,
            for_sync: Some(Vec::new()),
            placement_due: false,
        }
    }

    /// Whether a record placement reads changed since the last call.
    pub fn take_placement_due(&mut self) -> bool {
        std::mem::take(&mut self.placement_due)
    }

    /// What changed in the store since the last call, for peer sync. `None`: more than was
    /// kept track of, and every peer is to be told where this node is.
    pub fn take_for_sync(&mut self) -> Option<Vec<Change>> {
        self.for_sync.replace(Vec::new())
    }

    /// The kinds answered here.
    pub const KINDS: &'static [u8] = &[kind::PUT, kind::GET, kind::WATCH];

    /// The store.
    #[must_use]
    pub const fn store(&self) -> &Store {
        &self.store
    }

    /// The store, to write to. Call [`Records::publish`] afterwards.
    pub const fn store_mut(&mut self) -> &mut Store {
        &mut self.store
    }

    /// Whether the store changed since the last call: it is to be saved.
    pub const fn take_dirty(&mut self) -> bool {
        std::mem::replace(&mut self.dirty, false)
    }

    /// How many watches there are.
    #[must_use]
    pub const fn watching(&self) -> usize {
        self.watchers.len()
    }

    /// Forgets every watch and who provides what: the comm core they came through is gone,
    /// or node logic was away. A provider that is still there writes its resources again
    /// (the comm core tells it to); the ones nobody has written by `now_ms` and
    /// [`RECLAIM_MS`] are lost.
    pub fn forget_sessions(&mut self, now_ms: u64) {
        self.watchers.clear();
        self.resources.forget_sessions();
        self.reclaim_at = Some(now_ms + RECLAIM_MS);
    }

    /// When the resources nobody wrote again are to be marked lost, if that is still to come.
    #[must_use]
    pub const fn reclaim_at(&self) -> Option<u64> {
        self.reclaim_at
    }

    /// Marks lost, once the time has come, the resources of this node nobody wrote again.
    pub fn reclaim(&mut self, now_ms: u64, wall_ms: u64, out: &mut dyn Out) {
        if self.reclaim_at.is_none_or(|at| now_ms < at) {
            return;
        }
        self.reclaim_at = None;
        if self.resources.lose_unprovided(&mut self.store, wall_ms) != 0 {
            self.publish(out);
        }
    }

    /// A session is gone: its watches end, and what it provided is lost.
    pub fn gone(&mut self, session: u32, wall_ms: u64, out: &mut dyn Out) {
        self.watchers.retain(|watcher| watcher.session != session);
        if self.resources.gone(&mut self.store, session, wall_ms) != 0 {
            self.publish(out);
        }
    }

    /// Answers one request.
    ///
    /// # Errors
    ///
    /// [`Error`] for the caller, as a refusal.
    pub fn request(
        &mut self,
        asked: &Asked<'_>,
        wall_ms: u64,
        out: &mut dyn Out,
    ) -> Result<(), Error> {
        let (session, seq) = (asked.session, asked.seq);
        match asked.kind {
            kind::PUT => {
                let put: Put<'_> = asked.body()?;
                // All of a write or none of it: every entry is looked at before any is made.
                for entry in put.records.iter() {
                    let entry = entry?;
                    if entry.class == RecordClass::Workload {
                        // What should run is said with `WorkloadPut`, which also starts it.
                        return Err(Error::Denied);
                    }
                    self.resources
                        .check(&self.store, session, &entry, wall_ms)?;
                }
                let mut stored = Vec::new();
                for entry in put.records.iter() {
                    let entry = entry?;
                    if entry.deleted {
                        if self.store.delete(entry.class, entry.key, wall_ms).is_none() {
                            continue;
                        }
                    } else {
                        self.store
                            .put(entry.class, entry.key, entry.value.0, wall_ms);
                    }
                    self.resources
                        .wrote(&mut self.store, session, &entry, wall_ms);
                    // As stored: with the stamp the store gave it.
                    if let Some(version) = self.store.version(entry.class, entry.key) {
                        stored.push(Record {
                            class: entry.class,
                            key: entry.key.to_owned(),
                            version: version.clone(),
                        });
                    }
                }
                let answered = send(out, session, seq, &stored, true);
                self.publish(out);
                answered
            }
            kind::GET => {
                let get: Get<'_> = asked.body()?;
                let found: Vec<Record> = self.store.list(get.class, get.prefix).collect();
                send(out, session, seq, &found, true)
            }
            kind::WATCH => {
                let watch: Watch<'_> = asked.body()?;
                self.watchers.retain(|watcher| {
                    !(watcher.session == session
                        && watcher.class == watch.class
                        && watcher.prefix == watch.prefix)
                });
                self.watchers.push(Watcher {
                    session,
                    seq,
                    class: watch.class,
                    prefix: watch.prefix.to_owned(),
                });
                let newer = self.store.since(watch.class, watch.prefix, watch.after);
                send(out, session, seq, &newer, false)
            }
            _ => Err(Error::Unsupported),
        }
    }

    /// Tells the watchers what changed in the store since the last call, and marks the store
    /// to be saved.
    pub fn publish(&mut self, out: &mut dyn Out) {
        let taken = self.store.take_changes();
        self.placement_due |= taken.as_ref().is_none_or(|changes| {
            changes.iter().any(|change| {
                matches!(
                    change.record.class,
                    RecordClass::Node | RecordClass::Workload | RecordClass::Resource
                )
            })
        });
        match (&mut self.for_sync, &taken) {
            (Some(kept), Some(changes)) => kept.extend(changes.iter().cloned()),
            (kept, _) => *kept = None,
        }
        let Some(changes) = taken else {
            // More changed than was kept track of: every watcher is sent what it watches,
            // whole. A version it already has is the same record again.
            self.dirty = true;
            for watcher in &self.watchers {
                let all = self
                    .store
                    .since(watcher.class, &watcher.prefix, HlcTimestamp::ZERO);
                let _ = send(out, watcher.session, watcher.seq, &all, false);
            }
            return;
        };
        if changes.is_empty() {
            return;
        }
        self.dirty = true;
        let desired: Vec<Record> = changes
            .into_iter()
            .filter(|change| change.scope == Scope::Desired)
            .map(|change| change.record)
            .collect();
        for watcher in &self.watchers {
            let theirs: Vec<Record> = desired
                .iter()
                .filter(|record| {
                    record.class == watcher.class && record.key.starts_with(&watcher.prefix)
                })
                .cloned()
                .collect();
            let _ = send(out, watcher.session, watcher.seq, &theirs, false);
        }
    }
}

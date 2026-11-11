//! Records, answered: writes, reads and watches against the store.

use orion_connect::ClientError;
use orion_msg_records::{Changed, Get, Put, RecordClass, RecordEntry, Watch};
use orion_store::{Record, Scope, Store};
use orion_types::{Error, HlcTimestamp};
use orion_wire::{List, kind};

use crate::{Asked, Out};

/// About how many bytes of records go in one message. A record larger than this goes alone.
const PAGE_BYTES: usize = 24 * 1024;

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
    watchers: Vec<Watcher>,
    dirty: bool,
}

/// Sends `records` to `session` as answers to `seq`, in as many messages as it takes. With
/// none, one empty message when `always` is set: a read is always answered.
fn send(
    out: &mut dyn Out,
    session: u32,
    seq: u16,
    records: &[Record],
    always: bool,
) -> Result<(), ClientError> {
    if records.is_empty() {
        if always {
            out.answer(
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
            out.answer(
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
    pub const fn new(store: Store) -> Self {
        Self {
            store,
            watchers: Vec::new(),
            dirty: false,
        }
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

    /// Forgets every watch: the comm core they came through is gone, and so are they.
    pub fn forget_sessions(&mut self) {
        self.watchers.clear();
    }

    /// A session is gone.
    pub fn gone(&mut self, session: u32) {
        self.watchers.retain(|watcher| watcher.session != session);
    }

    /// Answers one request.
    ///
    /// # Errors
    ///
    /// [`Error`] for the caller, as a refusal. A failure to send is in `sent`.
    pub(crate) fn request(
        &mut self,
        asked: &Asked<'_>,
        wall_ms: u64,
        out: &mut dyn Out,
        sent: &mut Result<(), ClientError>,
    ) -> Result<(), Error> {
        let (session, seq) = (asked.session, asked.seq);
        match asked.kind {
            kind::PUT => {
                let put: Put<'_> = asked.body()?;
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
                    let class = entry.class;
                    let key = entry.key;
                    // As stored: with the stamp the store gave it.
                    if let Some(version) = self.store.version(class, key) {
                        stored.push(Record {
                            class,
                            key: key.to_owned(),
                            version: version.clone(),
                        });
                    }
                }
                *sent = send(out, session, seq, &stored, true);
                if sent.is_ok() {
                    *sent = self.publish(out);
                }
                Ok(())
            }
            kind::GET => {
                let get: Get<'_> = asked.body()?;
                let found: Vec<Record> = self.store.list(get.class, get.prefix).collect();
                *sent = send(out, session, seq, &found, true);
                Ok(())
            }
            _ => {
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
                *sent = send(out, session, seq, &newer, false);
                Ok(())
            }
        }
    }

    /// Tells the watchers what changed in the store since the last call, and marks the store
    /// to be saved.
    ///
    /// # Errors
    ///
    /// If the comm core could not be sent to.
    pub fn publish(&mut self, out: &mut dyn Out) -> Result<(), ClientError> {
        let Some(changes) = self.store.take_changes() else {
            // More changed than was kept track of: every watcher is sent what it watches,
            // whole. A version it already has is the same record again.
            self.dirty = true;
            for watcher in &self.watchers {
                let all = self
                    .store
                    .since(watcher.class, &watcher.prefix, HlcTimestamp::ZERO);
                send(out, watcher.session, watcher.seq, &all, false)?;
            }
            return Ok(());
        };
        if changes.is_empty() {
            return Ok(());
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
            send(out, watcher.session, watcher.seq, &theirs, false)?;
        }
        Ok(())
    }
}

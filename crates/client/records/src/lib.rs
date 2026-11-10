//! The client's records.
//!
//! A record is a named piece of state with a stamp from the cluster's clock, so any two nodes
//! that have seen the same writes agree on it. [`Records::put`] writes, [`Records::get`] reads
//! once, and [`Records::watch`] gives every change from then on.
//!
//! **Watches are pushed.** A watcher is sent each change as it is made, in the order it was
//! made, and asks for nothing again. A watch starts with what is there already, or with
//! everything newer than a stamp the program kept from last time.
//!
//! **A watcher that falls behind does not grow without bound.** Changes wait for the program
//! in a queue of fixed size. If the program reads too slowly for too long, the queue stops
//! taking changes, and once the program has caught up with what it holds the watch asks the
//! store for everything newer than the last change it was given. Nothing is skipped; a record
//! that changed several times in between is seen at its latest.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt;
use std::rc::Rc;
use std::time::{Duration, Instant};

use orion_connect::{Client, ClientError, Cx, Handler, Incoming};
use orion_msg_records::{Changed, Get, Put, Watch as WatchRequest};
use orion_types::HlcTimestamp;
use orion_wire::{List, kind};

pub use orion_msg_records::{RecordClass, RecordEntry};
pub use orion_wire::Bytes;

/// How many changes wait for a slow watcher before the watch stops taking them and catches
/// up afterwards.
pub const WATCH_QUEUE: usize = 1024;

/// One record, or the news that it is gone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    /// The sort of record.
    pub class: RecordClass,
    /// Its name, unique within its class.
    pub key: String,
    /// When it was written, on the cluster's clock.
    pub stamp: HlcTimestamp,
    /// Whether the record is gone.
    pub deleted: bool,
    /// The typed record, encoded. `orion_msg_records` has the types Orion itself keeps.
    pub value: Vec<u8>,
}

impl Record {
    fn from_entry(entry: &RecordEntry<'_>) -> Self {
        Self {
            class: entry.class,
            key: entry.key.to_owned(),
            stamp: entry.stamp,
            deleted: entry.deleted,
            value: entry.value.0.to_vec(),
        }
    }
}

/// What one request has been answered with so far.
struct Answer {
    seq: u16,
    records: Vec<Record>,
    done: bool,
}

struct Watching {
    /// The watch, whatever request it is now on.
    id: u64,
    seq: u16,
    class: RecordClass,
    prefix: String,
    queue: VecDeque<Record>,
    /// The stamp of the newest change the program has been given or is holding.
    newest: HlcTimestamp,
    /// Changes were turned away because the queue was full.
    behind: bool,
    ended: bool,
}

#[derive(Default)]
struct State {
    answers: Vec<Answer>,
    watches: Vec<Watching>,
    next_watch: u64,
}

impl Handler for State {
    fn kinds(&self) -> &'static [u8] {
        &[kind::CHANGED]
    }

    fn handle(&mut self, frame: &Incoming<'_>, _cx: &mut Cx<'_>) -> Result<(), ClientError> {
        let changed: Changed<'_> = frame.body()?;
        if let Some(answer) = self
            .answers
            .iter_mut()
            .find(|answer| answer.seq == changed.request_seq)
        {
            answer.records.extend(
                changed
                    .records
                    .iter()
                    .flatten()
                    .map(|entry| Record::from_entry(&entry)),
            );
            answer.done = !changed.more;
            return Ok(());
        }
        let Some(watch) = self
            .watches
            .iter_mut()
            .find(|watch| watch.seq == changed.request_seq && !watch.ended)
        else {
            // A watch that was dropped, or an answer nobody is waiting for.
            return Ok(());
        };
        for entry in changed.records.iter().flatten() {
            if watch.behind || watch.queue.len() >= WATCH_QUEUE {
                // Not taken: the watch asks for these again once the program has caught up.
                watch.behind = true;
                break;
            }
            watch.newest = watch.newest.max(entry.stamp);
            watch.queue.push_back(Record::from_entry(&entry));
        }
        Ok(())
    }
}

/// The records of one session.
#[derive(Clone)]
pub struct Records {
    client: Client,
    state: Rc<RefCell<State>>,
}

impl fmt::Debug for Records {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Records")
    }
}

impl Records {
    /// The records of `client`'s session.
    #[must_use]
    pub fn new(client: &Client) -> Self {
        let state = Rc::new(RefCell::new(State::default()));
        client.add(state.clone());
        Self {
            client: client.clone(),
            state,
        }
    }

    /// Waits for the whole answer to the request sent as `seq`.
    fn answer(&self, seq: u16) -> Result<Vec<Record>, ClientError> {
        self.state.borrow_mut().answers.push(Answer {
            seq,
            records: Vec::new(),
            done: false,
        });
        let answered = self.client.answered(seq, || {
            self.state
                .borrow()
                .answers
                .iter()
                .any(|answer| answer.seq == seq && answer.done)
        });
        let mut state = self.state.borrow_mut();
        let at = state.answers.iter().position(|answer| answer.seq == seq);
        let answer = at.map(|at| state.answers.swap_remove(at));
        answered?;
        Ok(answer.map(|answer| answer.records).unwrap_or_default())
    }

    /// Writes records, and returns them as the store kept them: each with the stamp it was
    /// given. A record with `deleted` set says the record is gone.
    ///
    /// # Errors
    ///
    /// If the store refuses the write, or the connection broke.
    pub fn put(&self, records: &[RecordEntry<'_>]) -> Result<Vec<Record>, ClientError> {
        let seq = self.client.send(&Put {
            records: List::new(records),
        })?;
        self.answer(seq)
    }

    /// Reads the records of `class` whose keys start with `prefix`. An empty prefix is all of
    /// them.
    ///
    /// # Errors
    ///
    /// If the store refuses, or the connection broke.
    pub fn get(&self, class: RecordClass, prefix: &str) -> Result<Vec<Record>, ClientError> {
        let seq = self.client.send(&Get { class, prefix })?;
        self.answer(seq)
    }

    /// Watches the records of `class` whose keys start with `prefix`: first everything newer
    /// than `after` ([`HlcTimestamp::ZERO`] for everything there is), then each change as it
    /// is made.
    ///
    /// # Errors
    ///
    /// If the connection broke. A refusal is found on the watch's first read.
    pub fn watch(
        &self,
        class: RecordClass,
        prefix: &str,
        after: HlcTimestamp,
    ) -> Result<Watch, ClientError> {
        let seq = self.client.send(&WatchRequest {
            class,
            prefix,
            after,
        })?;
        let mut state = self.state.borrow_mut();
        state.next_watch += 1;
        let id = state.next_watch;
        state.watches.push(Watching {
            id,
            seq,
            class,
            prefix: prefix.to_owned(),
            queue: VecDeque::with_capacity(WATCH_QUEUE),
            newest: after,
            behind: false,
            ended: false,
        });
        Ok(Watch {
            client: self.client.clone(),
            state: self.state.clone(),
            id,
        })
    }

    /// Asks again for every watch, each from the newest change it was given, of a store that
    /// has no memory of this session. Call it after
    /// [`orion_connect::Session::reconnect`]: nothing is missed, and nothing is seen twice.
    ///
    /// # Errors
    ///
    /// If the connection broke again.
    pub fn restore(&self) -> Result<(), ClientError> {
        let watches: Vec<(u64, RecordClass, String, HlcTimestamp)> = {
            let state = self.state.borrow();
            state
                .watches
                .iter()
                .map(|watch| (watch.id, watch.class, watch.prefix.clone(), watch.newest))
                .collect()
        };
        for (id, class, prefix, after) in watches {
            let seq = self.client.send(&WatchRequest {
                class,
                prefix: &prefix,
                after,
            })?;
            let mut state = self.state.borrow_mut();
            if let Some(watch) = state.watches.iter_mut().find(|watch| watch.id == id) {
                watch.seq = seq;
                watch.behind = false;
            }
        }
        Ok(())
    }
}

/// A watch on some records. Dropping it ends it.
pub struct Watch {
    client: Client,
    state: Rc<RefCell<State>>,
    id: u64,
}

impl fmt::Debug for Watch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Watch({})", self.id)
    }
}

impl Watch {
    /// The stamp of the newest change this watch has been given. Kept by a program, it is
    /// where to watch from next time.
    #[must_use]
    pub fn newest(&self) -> HlcTimestamp {
        let state = self.state.borrow();
        state
            .watches
            .iter()
            .find(|watch| watch.id == self.id)
            .map_or(HlcTimestamp::ZERO, |watch| watch.newest)
    }

    /// The next change, if one is here. It does not wait.
    ///
    /// # Errors
    ///
    /// If the store refused the watch, or the connection broke.
    pub fn poll(&mut self) -> Result<Option<Record>, ClientError> {
        self.client.drain()?;
        let (record, seq, catch_up) = {
            let mut state = self.state.borrow_mut();
            let Some(watch) = state.watches.iter_mut().find(|watch| watch.id == self.id) else {
                return Ok(None);
            };
            let record = watch.queue.pop_front();
            // Caught up with what was held, having turned changes away: ask for the rest.
            let catch_up = (watch.behind && watch.queue.is_empty())
                .then(|| (watch.class, watch.prefix.clone(), watch.newest));
            (record, watch.seq, catch_up)
        };
        if let Some(error) = self.client.refusal(seq) {
            return Err(ClientError::Refused(error));
        }
        if let Some((class, prefix, after)) = catch_up {
            let seq = self.client.send(&WatchRequest {
                class,
                prefix: &prefix,
                after,
            })?;
            let mut state = self.state.borrow_mut();
            if let Some(watch) = state.watches.iter_mut().find(|watch| watch.id == self.id) {
                // The store's pushes now name the new request; the old one's are no longer
                // this watch's.
                watch.seq = seq;
                watch.behind = false;
            }
        }
        Ok(record)
    }

    /// Waits up to `wait` for the next change.
    ///
    /// # Errors
    ///
    /// As [`Watch::poll`].
    pub fn wait(&mut self, wait: Duration) -> Result<Option<Record>, ClientError> {
        let deadline = Instant::now() + wait;
        loop {
            if let Some(record) = self.poll()? {
                return Ok(Some(record));
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Ok(None);
            }
            self.client.pump(left)?;
        }
    }
}

impl Drop for Watch {
    fn drop(&mut self) {
        // The store stops pushing when the session ends; until then what it sends for this
        // watch finds nobody and is dropped.
        self.state
            .borrow_mut()
            .watches
            .retain(|watch| watch.id != self.id);
    }
}

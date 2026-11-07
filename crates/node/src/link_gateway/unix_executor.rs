//! The executor role and record watches of a Unix link session (interface revision 3).
//!
//! An executor session's state goes to the node as a local executor's update; the workloads
//! assigned to its executor come back as `Workloads`, whole at first, then the changed ones and
//! the ids of those gone. A record watch recomputes its records only when the node's state
//! generation moved, and sends those whose digest changed. Both keep a 64-bit digest per record
//! they sent, not the record.

use crate::NodeApp;
use orion::control_plane::{ClientEventKind, WorkloadRecord};
use orion::transport::ipc::{LocalAddress, UnixPeerIdentity};
use orion_core::WorkloadId;
use orion_link::message::{ExecutorState, Record, RecordFilter, RecordKey};
use std::collections::BTreeMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use tokio::sync::Notify;
use tracing::debug;

/// Executor events taken from the node at a time.
const EVENTS_PER_DRAIN: usize = 8;

fn digest(value: &impl std::fmt::Debug) -> u64 {
    // The records do not implement `Hash`; their `Debug` text covers every field.
    struct Sink<'a>(&'a mut DefaultHasher);
    impl std::fmt::Write for Sink<'_> {
        fn write_str(&mut self, s: &str) -> std::fmt::Result {
            s.hash(self.0);
            Ok(())
        }
    }
    let mut hasher = DefaultHasher::new();
    let _ = std::fmt::write(&mut Sink(&mut hasher), format_args!("{value:?}"));
    hasher.finish()
}

/// The changes from `sent` to `now` (by key and digest), with `sent` brought up to date.
fn diff<K: Ord + Clone, R>(
    sent: &mut BTreeMap<K, u64>,
    now: Vec<R>,
    key: impl Fn(&R) -> K,
    digest_of: impl Fn(&R) -> u64,
) -> (Vec<R>, Vec<K>) {
    let mut next = BTreeMap::new();
    let mut updated = Vec::new();
    for record in now {
        let (k, d) = (key(&record), digest_of(&record));
        if sent.get(&k) != Some(&d) {
            updated.push(record);
        }
        next.insert(k, d);
    }
    let removed = sent
        .keys()
        .filter(|k| !next.contains_key(*k))
        .cloned()
        .collect();
    *sent = next;
    (updated, removed)
}

/// What an executor session sends the client: one `Workloads` answer.
#[derive(Debug)]
pub(super) struct WorkloadChange {
    pub(super) bootstrap: bool,
    pub(super) workloads: Vec<WorkloadRecord>,
    pub(super) removed: Vec<WorkloadId>,
}

/// The executor side of a session.
pub(super) struct ExecutorLink {
    source: LocalAddress,
    /// Registered with the node (refused callers stay unregistered: their state is dropped).
    registered: bool,
    /// The executor's state was applied once, so its workloads are watched.
    watching: bool,
    sent: BTreeMap<WorkloadId, u64>,
    bootstrapped: bool,
}

impl ExecutorLink {
    pub(super) fn open(
        app: &NodeApp,
        link: &str,
        device: &str,
        identity: &UnixPeerIdentity,
        wake: Arc<Notify>,
    ) -> Self {
        let name = format!("link:{link}/{device}#executor");
        let source = LocalAddress::new(name.clone());
        let registered = match app.register_link_executor(&source, &name, identity, wake) {
            Ok(()) => true,
            Err(error) => {
                debug!(link, device, %error, "link executor not registered; its state is dropped");
                false
            }
        };
        Self {
            source,
            registered,
            watching: false,
            sent: BTreeMap::new(),
            bootstrapped: false,
        }
    }

    pub(super) fn close(self, app: &NodeApp) {
        if self.registered {
            app.remove_link_client(&self.source);
        }
    }

    pub(super) fn state(
        &mut self,
        app: &NodeApp,
        identity: &UnixPeerIdentity,
        state: ExecutorState,
    ) {
        if !self.registered {
            return;
        }
        match app.apply_link_executor_state(&self.source, identity, state, !self.watching) {
            Ok(()) => self.watching = true,
            Err(error) => {
                debug!(source = %self.source.as_str(), %error, "link executor state refused")
            }
        }
    }

    /// The next change of the executor's workloads, if the node queued one.
    pub(super) fn next_change(&mut self, app: &NodeApp) -> Option<WorkloadChange> {
        if !self.watching {
            return None;
        }
        // Only the newest list matters: each event carries the whole assigned set.
        let latest = app
            .take_link_client_events(&self.source, EVENTS_PER_DRAIN)
            .into_iter()
            .filter_map(|event| match event.event {
                ClientEventKind::ExecutorWorkloads { workloads, .. } => Some(workloads),
                _ => None,
            })
            .next_back()?;
        let bootstrap = !std::mem::replace(&mut self.bootstrapped, true);
        let (workloads, removed) = diff(
            &mut self.sent,
            latest,
            |w: &WorkloadRecord| w.workload_id.clone(),
            digest,
        );
        if !bootstrap && workloads.is_empty() && removed.is_empty() {
            return None;
        }
        Some(WorkloadChange {
            bootstrap,
            workloads,
            removed: if bootstrap { Vec::new() } else { removed },
        })
    }
}

/// A session's record watch.
pub(super) struct RecordWatch {
    pub(super) req: u32,
    filter: RecordFilter,
    sent: BTreeMap<RecordKey, u64>,
    /// The node's state generation the last answer reflects; `None` before the bootstrap.
    generation: Option<u64>,
}

/// What a record watch sends: one `Records` answer.
#[derive(Debug)]
pub(super) struct RecordChange {
    pub(super) bootstrap: bool,
    pub(super) updated: Vec<Record>,
    pub(super) removed: Vec<RecordKey>,
}

impl RecordWatch {
    pub(super) fn new(req: u32, filter: RecordFilter) -> Self {
        Self {
            req,
            filter,
            sent: BTreeMap::new(),
            generation: None,
        }
    }

    /// The change since the last answer, if the node's state moved and the records differ.
    pub(super) fn next_change(&mut self, app: &NodeApp) -> Option<RecordChange> {
        let generation = app.link_state_generation();
        if self.generation == Some(generation) {
            return None;
        }
        let bootstrap = self.generation.is_none();
        self.generation = Some(generation);
        let (updated, removed) = diff(
            &mut self.sent,
            app.link_records(&self.filter),
            Record::key,
            digest,
        );
        if !bootstrap && updated.is_empty() && removed.is_empty() {
            return None;
        }
        Some(RecordChange {
            bootstrap,
            updated,
            removed,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_diff_sends_changed_and_new_records_and_the_keys_of_those_gone() {
        let mut sent = BTreeMap::new();
        let key = |r: &(u8, &str)| r.0;
        let d = |r: &(u8, &str)| digest(r);
        let (up, gone) = diff(&mut sent, vec![(1, "a"), (2, "b")], key, d);
        assert_eq!((up.len(), gone.len()), (2, 0));
        let (up, gone) = diff(&mut sent, vec![(1, "a"), (3, "c")], key, d);
        assert_eq!((up, gone), (vec![(3, "c")], vec![2]));
        let (up, gone) = diff(&mut sent, vec![(1, "z"), (3, "c")], key, d);
        assert_eq!((up, gone), (vec![(1, "z")], vec![]));
    }
}

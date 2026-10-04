//! Volatile status lane: latest value per `(subject, key)` in node memory, with a TTL.
//!
//! Never persisted and never replicated to peers. Local providers and executors publish entries
//! for subjects they own; any local client can query or watch. Watch events are coalesced per
//! client (newest value per key), and memory is bounded by node-wide and per-publisher caps.
//!
//! Expired entries are dropped lazily on every publish, query, and watch subscription, and
//! proactively by a sweeper that rides along with the reconcile loop, so watchers learn about
//! expirations without polling.

pub(crate) mod store;

use super::{
    NodeApp, NodeError,
    local_clients::{
        PendingClientStreamFlush, enqueue_status_event, execute_client_stream_flush,
        finalize_client_stream_flush, prepare_client_stream_flush,
    },
};
use orion::{
    ExecutorId, ProviderId,
    control_plane::{
        StatusChange, StatusEntry, StatusKey, StatusLaneUsageSnapshot, StatusQuery, StatusSubject,
    },
    transport::ipc::LocalAddress,
};
use std::collections::BTreeMap;
use std::sync::{
    Mutex, MutexGuard,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;
use store::{StatusLimits, StatusStore};
use tokio::sync::watch;

/// Who last published the state of a provider or executor over local IPC.
#[derive(Debug, Default)]
struct LocalOwners {
    providers: BTreeMap<ProviderId, LocalAddress>,
    executors: BTreeMap<ExecutorId, LocalAddress>,
}

#[derive(Debug, Default)]
pub(super) struct StatusLaneState {
    store: Mutex<StatusStore>,
    owners: Mutex<LocalOwners>,
    wake: tokio::sync::Notify,
    published_total: AtomicU64,
    expired_total: AtomicU64,
    dropped_total: AtomicU64,
    unauthorized_total: AtomicU64,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A local subject owned by an IPC client.
pub(super) enum OwnedSubject<'a> {
    Provider(&'a ProviderId),
    Executor(&'a ExecutorId),
}

impl NodeApp {
    fn status_limits(&self) -> StatusLimits {
        let tuning = &self.config.runtime_tuning;
        StatusLimits {
            max_entries: tuning.status_max_entries,
            max_entries_per_publisher: tuning.status_max_entries_per_publisher,
            max_ttl_ms: u64::try_from(tuning.status_max_ttl.as_millis()).unwrap_or(u64::MAX),
        }
    }

    /// Records that `source` publishes the state of `subject`, which makes it the subject's
    /// status publisher. The latest state publisher wins, like the state itself.
    pub(super) fn record_status_owner(&self, source: &LocalAddress, subject: OwnedSubject<'_>) {
        let mut owners = lock(&self.state.status.owners);
        match subject {
            OwnedSubject::Provider(id) => {
                if owners.providers.get(id) != Some(source) {
                    owners.providers.insert(id.clone(), source.clone());
                }
            }
            OwnedSubject::Executor(id) => {
                if owners.executors.get(id) != Some(source) {
                    owners.executors.insert(id.clone(), source.clone());
                }
            }
        }
    }

    /// Whether the local client `source` owns `subject`: its provider or executor, a resource of
    /// its provider (or realized by its executor), or a workload its executor runs.
    fn local_client_owns(&self, source: &LocalAddress, subject: &StatusSubject) -> bool {
        let (providers, executors): (Vec<ProviderId>, Vec<ExecutorId>) = {
            let owners = lock(&self.state.status.owners);
            (
                owners
                    .providers
                    .iter()
                    .filter(|(_, owner)| *owner == source)
                    .map(|(id, _)| id.clone())
                    .collect(),
                owners
                    .executors
                    .iter()
                    .filter(|(_, owner)| *owner == source)
                    .map(|(id, _)| id.clone())
                    .collect(),
            )
        };
        match subject {
            StatusSubject::Provider(id) => providers.contains(id),
            StatusSubject::Executor(id) => executors.contains(id),
            StatusSubject::Resource(id) => {
                let store = self.store_read();
                [
                    store.observed.resources.get(id),
                    store.desired.resources.get(id),
                ]
                .into_iter()
                .flatten()
                .any(|resource| {
                    providers.contains(&resource.provider_id)
                        || resource
                            .realized_by_executor_id
                            .as_ref()
                            .is_some_and(|executor| executors.contains(executor))
                })
            }
            StatusSubject::Workload(id) => executors.iter().any(|executor| {
                self.current_executor_workloads(executor)
                    .is_ok_and(|workloads| workloads.iter().any(|w| &w.workload_id == id))
            }),
        }
    }

    /// `PublishStatus` from a local client: every subject must be owned by the client.
    pub(super) fn publish_local_status(
        &self,
        source: &LocalAddress,
        entries: Vec<StatusEntry>,
    ) -> Result<(), NodeError> {
        let mut checked: Vec<&StatusSubject> = Vec::new();
        for entry in &entries {
            if checked.contains(&&entry.subject) {
                continue;
            }
            if !self.local_client_owns(source, &entry.subject) {
                self.state
                    .status
                    .unauthorized_total
                    .fetch_add(1, Ordering::Relaxed);
                return Err(NodeError::Authorization(format!(
                    "client {} does not own status subject {}; publish the provider or executor \
                     state first",
                    source.as_str(),
                    entry.subject
                )));
            }
            checked.push(&entry.subject);
        }
        self.publish_status_as(&format!("ipc:{}", source.as_str()), entries)
    }

    /// Stores a batch for `publisher` (ownership already checked) and notifies watchers.
    pub(super) fn publish_status_as(
        &self,
        publisher: &str,
        entries: Vec<StatusEntry>,
    ) -> Result<(), NodeError> {
        let now_ms = Self::current_time_ms();
        let count = entries.len() as u64;
        let limits = self.status_limits();
        let (expired, result) = {
            let mut store = lock(&self.state.status.store);
            let expired = store.expire(now_ms);
            let result = store.publish(publisher, entries, now_ms, limits);
            (expired, result)
        };
        let status = &self.state.status;
        status
            .expired_total
            .fetch_add(expired.len() as u64, Ordering::Relaxed);
        let result = match result {
            Ok(outcome) => {
                status
                    .published_total
                    .fetch_add(outcome.accepted as u64, Ordering::Relaxed);
                status.wake.notify_one();
                self.notify_status_watchers(outcome.changed, expired);
                return Ok(());
            }
            Err(error) => error,
        };
        status.dropped_total.fetch_add(count, Ordering::Relaxed);
        self.notify_status_watchers(Vec::new(), expired);
        Err(NodeError::Status(result.to_string()))
    }

    /// Live entries matching `query`.
    pub fn query_status(&self, query: &StatusQuery) -> Vec<StatusEntry> {
        self.sweep_expired_status();
        lock(&self.state.status.store).query(query, Self::current_time_ms())
    }

    /// `WatchStatus`: queues a bootstrap event with every matching entry, then coalesced changes.
    pub(super) fn subscribe_status_watch(
        &self,
        source: &LocalAddress,
        query: StatusQuery,
    ) -> Result<(), NodeError> {
        let current = self.query_status(&query);
        self.with_client_mut(source, |client| {
            client.status_watch = Some(query);
            enqueue_status_event(
                client,
                StatusChange {
                    bootstrap: true,
                    updated: current,
                    expired: Vec::new(),
                },
            );
        })
        // The stream server flushes queued events right after it sends the `Accepted` response.
    }

    /// Drops expired entries (notifying watchers) and returns the next expiry time.
    pub(crate) fn sweep_expired_status(&self) -> Option<u64> {
        let (expired, next) = {
            let mut store = lock(&self.state.status.store);
            let expired = store.expire(Self::current_time_ms());
            (expired, store.next_expiry_ms())
        };
        if !expired.is_empty() {
            self.state
                .status
                .expired_total
                .fetch_add(expired.len() as u64, Ordering::Relaxed);
            self.notify_status_watchers(Vec::new(), expired);
        }
        next
    }

    fn notify_status_watchers(&self, updated: Vec<StatusEntry>, expired: Vec<StatusKey>) {
        if updated.is_empty() && expired.is_empty() {
            return;
        }
        let mut flushes = Vec::<PendingClientStreamFlush>::new();
        self.with_client_registry_txn(|txn| {
            for (source, client) in txn.clients_mut() {
                let Some(query) = client.status_watch.as_ref() else {
                    continue;
                };
                let change = StatusChange {
                    bootstrap: false,
                    updated: updated
                        .iter()
                        .filter(|entry| query.matches(&entry.subject, &entry.key))
                        .cloned()
                        .collect(),
                    expired: expired
                        .iter()
                        .filter(|key| query.matches(&key.subject, &key.key))
                        .cloned()
                        .collect(),
                };
                if change.is_empty() {
                    continue;
                }
                enqueue_status_event(client, change);
                if let Some(flush) = prepare_client_stream_flush(source, client) {
                    flushes.push(flush);
                }
            }
        });
        for flush in flushes {
            let delivered = execute_client_stream_flush(&flush);
            let _ = self.with_client_mut_if_present(&flush.source, |client| {
                finalize_client_stream_flush(client, &flush, delivered);
            });
        }
    }

    /// Sweeper loop: sleeps until the earliest expiry (or a publish), never polls.
    pub(super) async fn run_status_expiry(&self, mut shutdown: watch::Receiver<bool>) {
        loop {
            if *shutdown.borrow() {
                return;
            }
            let next = self.sweep_expired_status();
            let sleep = next
                .map(|at| Duration::from_millis(at.saturating_sub(Self::current_time_ms()).max(1)));
            tokio::select! {
                _ = self.state.status.wake.notified() => {}
                _ = async {
                    match sleep {
                        Some(delay) => tokio::time::sleep(delay).await,
                        None => std::future::pending::<()>().await,
                    }
                } => {}
                changed = shutdown.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
            }
        }
    }

    pub(super) fn status_lane_usage(&self) -> StatusLaneUsageSnapshot {
        let limits = self.status_limits();
        let (entries, publishers) = {
            let store = lock(&self.state.status.store);
            (store.len() as u64, store.publishers() as u64)
        };
        let watchers = self
            .clients_read()
            .values()
            .filter(|client| client.status_watch.is_some())
            .count() as u64;
        let status = &self.state.status;
        StatusLaneUsageSnapshot {
            entries,
            max_entries: limits.max_entries as u64,
            max_entries_per_publisher: limits.max_entries_per_publisher as u64,
            max_ttl_ms: limits.max_ttl_ms,
            publishers,
            watchers,
            published_total: status.published_total.load(Ordering::Relaxed),
            expired_total: status.expired_total.load(Ordering::Relaxed),
            dropped_total: status.dropped_total.load(Ordering::Relaxed),
            unauthorized_total: status.unauthorized_total.load(Ordering::Relaxed),
        }
    }
}

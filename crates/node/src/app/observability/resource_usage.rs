//! Memory, state-size, and backlog diagnostics for the observability snapshot.
//!
//! Everything here is either a counter read under a short read lock or a cached value, so the
//! snapshot stays cheap enough for periodic polling on small appliances.

use super::COMMUNICATION_ENDPOINT_RUNTIME_LIMIT;
use crate::app::NodeApp;
use orion::{
    control_plane::{
        DesiredClusterState, LocalStreamUsageSnapshot, MutationBatch, MutationHistoryUsageSnapshot,
        NodeResourceUsageSnapshot, ObservedClusterState, ProcessMemorySnapshot,
        RegistryUsageSnapshot, StateSectionCounts, StateSizeSnapshot, WorkerQueueUsageSnapshot,
    },
    encode_to_vec,
};
use std::{
    fs,
    path::Path,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

/// Caches the encoded mutation-history size so it is recomputed only after the history changes.
///
/// Writers bump `generation` while holding the history write lock, so a reader holding the read
/// lock always observes the generation that matches the history contents it sees.
#[derive(Debug, Default)]
pub(crate) struct MutationHistorySizeCache {
    generation: AtomicU64,
    cached: Mutex<Option<(u64, u64)>>,
}

impl MutationHistorySizeCache {
    pub(crate) fn mark_changed(&self) {
        self.generation.fetch_add(1, Ordering::Relaxed);
    }

    #[allow(clippy::ptr_arg)] // rkyv encodes the owned `Vec` layout the history cap measures.
    pub(crate) fn encoded_bytes(&self, history: &Vec<MutationBatch>) -> Option<u64> {
        let generation = self.generation.load(Ordering::Relaxed);
        let mut cached = self.cached.lock().ok()?;
        if let Some((cached_generation, bytes)) = *cached
            && cached_generation == generation
        {
            return Some(bytes);
        }
        let bytes = encode_to_vec(history).ok()?.len().min(u64::MAX as usize) as u64;
        *cached = Some((generation, bytes));
        Some(bytes)
    }
}

impl NodeApp {
    /// Builds the resource-usage section. Acquires one lock at a time and never nests them.
    pub(in crate::app) fn resource_usage_snapshot(
        &self,
        process: ProcessMemorySnapshot,
    ) -> NodeResourceUsageSnapshot {
        let tuning = &self.config.runtime_tuning;
        let (desired, observed) = {
            let store = self.store_read();
            (
                desired_counts(&store.desired),
                observed_counts(&store.observed),
            )
        };
        let mutation_history = {
            let history = self.mutation_history_read();
            MutationHistoryUsageSnapshot {
                batches: history.len() as u64,
                max_batches: usize_to_u64(tuning.max_mutation_history_batches),
                mutations: usize_to_u64(history.iter().map(|batch| batch.mutations.len()).sum()),
                encoded_bytes: self
                    .state
                    .persisted
                    .mutation_history_size
                    .encoded_bytes(&history),
                max_bytes: usize_to_u64(tuning.max_mutation_history_bytes),
            }
        };
        let (local_streams, local_clients) = self.local_stream_usage();
        let (auth_nonce_peers, auth_seen_nonces) = self.security.seen_nonce_usage();
        let (communication_endpoints, recent_events, recent_event_limit) = {
            let observability = self.observability_read();
            (
                observability.communication_endpoints.len(),
                observability.recent_events.len(),
                observability.event_limit,
            )
        };
        let registries = RegistryUsageSnapshot {
            peers: usize_to_u64(self.peers_read().len()),
            local_clients,
            local_providers: usize_to_u64(self.providers_read().len()),
            local_executors: usize_to_u64(self.executors_read().len()),
            communication_endpoints: usize_to_u64(communication_endpoints),
            communication_endpoint_limit: usize_to_u64(COMMUNICATION_ENDPOINT_RUNTIME_LIMIT),
            recent_events: usize_to_u64(recent_events),
            recent_event_limit: usize_to_u64(recent_event_limit),
            auth_nonce_peers,
            auth_seen_nonces,
        };
        let state = StateSizeSnapshot {
            desired,
            observed,
            persisted_snapshot_bytes: self.storage.as_ref().and_then(|storage| {
                sum_file_sizes(&[
                    storage.snapshot_manifest_path(),
                    storage.snapshot_desired_path(),
                    storage.snapshot_observed_path(),
                    storage.snapshot_applied_path(),
                ])
            }),
            persisted_mutation_history_bytes: self
                .storage
                .as_ref()
                .and_then(|storage| file_size(&storage.mutation_history_path())),
        };

        NodeResourceUsageSnapshot {
            process,
            state,
            mutation_history,
            local_streams,
            worker_queues: self.worker_queue_usage(),
            registries,
        }
    }

    fn local_stream_usage(&self) -> (LocalStreamUsageSnapshot, u64) {
        let tuning = &self.config.runtime_tuning;
        let clients = self.clients_read();
        let mut usage = LocalStreamUsageSnapshot {
            registered_clients: usize_to_u64(clients.len()),
            send_queue_capacity: usize_to_u64(tuning.local_stream_send_queue_capacity),
            client_event_queue_limit: usize_to_u64(tuning.local_client_event_queue_limit),
            ..LocalStreamUsageSnapshot::default()
        };
        for client in clients.values() {
            if let Some(sender) = &client.stream_sender {
                let depth = usize_to_u64(sender.max_capacity().saturating_sub(sender.capacity()));
                usage.attached_streams += 1;
                usage.send_queue_depth_total = usage.send_queue_depth_total.saturating_add(depth);
                usage.send_queue_depth_max = usage.send_queue_depth_max.max(depth);
            }
            usage.state_watchers += u64::from(client.state_watch.is_some());
            usage.executor_watchers += u64::from(client.executor_watch.is_some());
            usage.provider_watchers += u64::from(client.provider_watch.is_some());
            let queued = usize_to_u64(client.queued_events.len());
            usage.queued_client_events_total =
                usage.queued_client_events_total.saturating_add(queued);
            usage.queued_client_events_max = usage.queued_client_events_max.max(queued);
            usage.dropped_client_events_total = usage
                .dropped_client_events_total
                .saturating_add(client.dropped_events);
        }
        let registered = usage.registered_clients;
        (usage, registered)
    }

    fn worker_queue_usage(&self) -> Vec<WorkerQueueUsageSnapshot> {
        let tuning = &self.config.runtime_tuning;
        let mut queues = Vec::with_capacity(3);
        if let Some(worker) = &self.persistence_worker {
            queues.push(WorkerQueueUsageSnapshot {
                name: "persistence".to_owned(),
                capacity: usize_to_u64(tuning.persistence_worker_queue_capacity),
                depth: worker.queue_depth(),
                dropped_total: None,
            });
        }
        if let Some(depth) = self.security.auth_state_worker_queue_depth() {
            queues.push(WorkerQueueUsageSnapshot {
                name: "auth_state".to_owned(),
                capacity: usize_to_u64(tuning.auth_state_worker_queue_capacity),
                depth,
                dropped_total: None,
            });
        }
        if let Some(audit_log) = &self.audit_log {
            queues.push(WorkerQueueUsageSnapshot {
                name: "audit_log".to_owned(),
                capacity: usize_to_u64(tuning.audit_log_queue_capacity),
                depth: audit_log.queued_records(),
                dropped_total: Some(audit_log.dropped_records()),
            });
        }
        queues
    }
}

fn desired_counts(desired: &DesiredClusterState) -> StateSectionCounts {
    StateSectionCounts {
        nodes: usize_to_u64(desired.nodes.len()),
        artifacts: usize_to_u64(desired.artifacts.len()),
        workloads: usize_to_u64(desired.workloads.len()),
        tombstones: usize_to_u64(desired.tombstones.len()),
        resources: usize_to_u64(desired.resources.len()),
        providers: usize_to_u64(desired.providers.len()),
        executors: usize_to_u64(desired.executors.len()),
        leases: usize_to_u64(desired.leases.len()),
    }
}

fn observed_counts(observed: &ObservedClusterState) -> StateSectionCounts {
    StateSectionCounts {
        nodes: usize_to_u64(observed.nodes.len()),
        workloads: usize_to_u64(observed.workloads.len()),
        resources: usize_to_u64(observed.resources.len()),
        leases: usize_to_u64(observed.leases.len()),
        ..StateSectionCounts::default()
    }
}

fn file_size(path: &Path) -> Option<u64> {
    fs::metadata(path).ok().map(|metadata| metadata.len())
}

fn sum_file_sizes(paths: &[std::path::PathBuf]) -> Option<u64> {
    let mut total = None;
    for path in paths {
        if let Some(size) = file_size(path) {
            total = Some(total.unwrap_or(0_u64).saturating_add(size));
        }
    }
    total
}

fn usize_to_u64(value: usize) -> u64 {
    value.min(u64::MAX as usize) as u64
}

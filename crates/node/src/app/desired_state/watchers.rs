use super::NodeApp;
use crate::app::local_clients::{
    PendingClientStreamFlush, archive_fingerprint, enqueue_executor_workloads_event,
    enqueue_provider_leases_event, enqueue_state_snapshot_event, execute_client_stream_flush,
    finalize_client_stream_flush, prepare_client_stream_flush, reserve_state_snapshot_event,
};
use std::collections::{BTreeMap, BTreeSet};

impl NodeApp {
    pub(super) fn notify_desired_state_watchers(&self, previous_revision: orion::Revision) {
        // Check the revision before building a snapshot: a deep clone of desired, observed and
        // applied state is only needed when the desired state actually moved.
        let desired_revision = self.current_desired_revision();
        if desired_revision <= previous_revision {
            return;
        }
        #[cfg(feature = "link-gateway")]
        self.notify_link_desired_change();
        self.notify_client_watchers_with(desired_revision);
    }

    /// Re-evaluates executor and provider watches without a desired-state change (cross-node
    /// binding availability depends on peer liveness).
    pub(in crate::app) fn notify_client_watchers(&self) {
        self.notify_client_watchers_with(self.current_desired_revision());
    }

    /// Queues the state updates and executor/provider watch updates for `desired_revision`. A state
    /// snapshot is copied only for streaming watchers; the others get a reservation and build the
    /// snapshot when they poll.
    fn notify_client_watchers_with(&self, desired_revision: orion::Revision) {
        let streaming_behind = self.clients_read().values().any(|client| {
            client.stream_sender.is_some()
                && client
                    .state_watch
                    .as_ref()
                    .is_some_and(|watch| watch.desired_revision < desired_revision)
        });
        let snapshot = streaming_behind.then(|| self.state_snapshot());
        let client_views: Vec<_> = self
            .clients_read()
            .iter()
            .map(|(source, client)| {
                (
                    source.clone(),
                    client
                        .state_watch
                        .as_ref()
                        .map(|watch| watch.desired_revision),
                    client
                        .executor_watch
                        .as_ref()
                        .map(|watch| (watch.executor_id.clone(), watch.last_fingerprint)),
                    client
                        .provider_watch
                        .as_ref()
                        .map(|watch| (watch.provider_id.clone(), watch.last_fingerprint)),
                )
            })
            .collect();
        let watched_executor_ids: BTreeSet<_> = client_views
            .iter()
            .filter_map(|(_, _, executor_watch, _)| {
                executor_watch
                    .as_ref()
                    .map(|(executor_id, _)| executor_id.clone())
            })
            .collect();
        let watched_provider_ids: BTreeSet<_> = client_views
            .iter()
            .filter_map(|(_, _, _, provider_watch)| {
                provider_watch
                    .as_ref()
                    .map(|(provider_id, _)| provider_id.clone())
            })
            .collect();
        let executor_workloads_by_id: BTreeMap<_, _> = watched_executor_ids
            .into_iter()
            .map(|executor_id| {
                let workloads = self.current_executor_workloads(&executor_id).ok();
                (executor_id, workloads)
            })
            .collect();
        let provider_leases_by_id: BTreeMap<_, _> = watched_provider_ids
            .into_iter()
            .map(|provider_id| {
                let leases = self.current_provider_leases(&provider_id);
                (provider_id, leases)
            })
            .collect();

        let mut updates = Vec::new();
        for (source, previous_watched_revision, executor_watch, provider_watch) in client_views {
            let state_behind =
                previous_watched_revision.filter(|revision| desired_revision > *revision);

            // Compare by reference and clone only the lists that actually changed.
            let executor_update = executor_watch.and_then(|(executor_id, last)| {
                let workloads = executor_workloads_by_id.get(&executor_id)?.as_ref()?;
                let fingerprint = archive_fingerprint(workloads);
                (fingerprint.is_none() || fingerprint != last)
                    .then(|| (executor_id, last, fingerprint, workloads.clone()))
            });

            let provider_update = provider_watch.and_then(|(provider_id, last)| {
                let leases = provider_leases_by_id.get(&provider_id)?;
                let fingerprint = archive_fingerprint(leases);
                (fingerprint.is_none() || fingerprint != last)
                    .then(|| (provider_id, last, fingerprint, leases.clone()))
            });

            if state_behind.is_some() || executor_update.is_some() || provider_update.is_some() {
                updates.push((source, state_behind, executor_update, provider_update));
            }
        }

        let mut pending_flushes = Vec::<PendingClientStreamFlush>::new();
        self.with_client_registry_txn(|txn| {
            for (source, state_behind, executor_update, provider_update) in updates {
                let Some(client) = txn.client_mut_if_present(&source) else {
                    continue;
                };

                if let Some(previous_watched_revision) = state_behind
                    && client
                        .state_watch
                        .as_ref()
                        .map(|watch| watch.desired_revision)
                        == Some(previous_watched_revision)
                {
                    let revision = match (client.stream_sender.is_some(), snapshot.as_ref()) {
                        (true, Some(snapshot)) => {
                            enqueue_state_snapshot_event(client, snapshot.clone());
                            snapshot.state.desired.revision
                        }
                        _ => {
                            reserve_state_snapshot_event(client);
                            desired_revision
                        }
                    };
                    if let Some(watch) = client.state_watch.as_mut() {
                        watch.desired_revision = revision;
                    }
                }

                if let Some((executor_id, last, fingerprint, workloads)) = executor_update
                    && client.executor_watch.as_ref().is_some_and(|watch| {
                        watch.executor_id == executor_id && watch.last_fingerprint == last
                    })
                {
                    if let Some(watch) = client.executor_watch.as_mut() {
                        watch.last_fingerprint = fingerprint;
                    }
                    enqueue_executor_workloads_event(client, executor_id, workloads);
                }

                if let Some((provider_id, last, fingerprint, leases)) = provider_update
                    && client.provider_watch.as_ref().is_some_and(|watch| {
                        watch.provider_id == provider_id && watch.last_fingerprint == last
                    })
                {
                    if let Some(watch) = client.provider_watch.as_mut() {
                        watch.last_fingerprint = fingerprint;
                    }
                    enqueue_provider_leases_event(client, provider_id, leases);
                }

                if let Some(flush) = prepare_client_stream_flush(&source, client) {
                    pending_flushes.push(flush);
                }
            }
        });

        for mut flush in pending_flushes {
            let delivered = execute_client_stream_flush(&mut flush);
            let _ = self.with_client_mut_if_present(&flush.source, |client| {
                finalize_client_stream_flush(client, &flush, delivered);
            });
        }
    }

    /// The current observed-watch generation (the observed state is recorded first).
    pub(in crate::app) fn observed_watch_generation(&self) -> u64 {
        let fingerprint =
            crate::app::local_clients::archive_fingerprint(&self.store_read().observed);
        self.state
            .observed_watch
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .observe(fingerprint)
    }

    /// Sends a state snapshot to every `include_observed` watcher when the observed state changed
    /// since its last snapshot. Called after observed-state writes and at the end of every
    /// reconcile pass; cheap when nobody watches.
    pub(in crate::app) fn notify_observed_watchers(&self) {
        if !self
            .clients_read()
            .values()
            .any(|client| client.wants_observed())
        {
            return;
        }
        let generation = self.observed_watch_generation();
        let behind = self
            .clients_read()
            .values()
            .any(|client| client.wants_observed() && client.observed_generation < generation);
        if !behind {
            return;
        }
        // Only a streaming watcher needs the snapshot now; the others build it when they poll.
        let streaming_behind = self.clients_read().values().any(|client| {
            client.wants_observed()
                && client.observed_generation < generation
                && client.stream_sender.is_some()
        });
        let snapshot = streaming_behind.then(|| self.state_snapshot());
        let desired_revision = self.current_desired_revision();
        let mut pending_flushes = Vec::<PendingClientStreamFlush>::new();
        self.with_client_registry_txn(|txn| {
            for (source, client) in txn.clients_mut() {
                if !client.wants_observed() || client.observed_generation >= generation {
                    continue;
                }
                match (client.stream_sender.is_some(), snapshot.as_ref()) {
                    (true, Some(snapshot)) => {
                        enqueue_state_snapshot_event(client, snapshot.clone());
                    }
                    _ => reserve_state_snapshot_event(client),
                }
                client.observed_generation = generation;
                if let Some(watch) = client.state_watch.as_mut() {
                    watch.desired_revision = watch.desired_revision.max(desired_revision);
                }
                if let Some(flush) = prepare_client_stream_flush(source, client) {
                    pending_flushes.push(flush);
                }
            }
        });
        for mut flush in pending_flushes {
            let delivered = execute_client_stream_flush(&mut flush);
            let _ = self.with_client_mut_if_present(&flush.source, |client| {
                finalize_client_stream_flush(client, &flush, delivered);
            });
        }
    }

    pub(crate) fn flush_client_stream_for_source(
        &self,
        source: &orion::transport::ipc::LocalAddress,
    ) {
        let pending = self.with_client_mut_if_present(source, |client| {
            prepare_client_stream_flush(source, client)
        });
        let Some(Some(mut flush)) = pending else {
            return;
        };
        let delivered = execute_client_stream_flush(&mut flush);
        let _ = self.with_client_mut_if_present(source, |client| {
            finalize_client_stream_flush(client, &flush, delivered);
        });
    }
}

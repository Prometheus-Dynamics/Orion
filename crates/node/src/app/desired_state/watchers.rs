use super::NodeApp;
use crate::app::local_clients::{
    PendingClientStreamFlush, enqueue_executor_workloads_event, enqueue_provider_leases_event,
    enqueue_state_snapshot_event, execute_client_stream_flush, finalize_client_stream_flush,
    prepare_client_stream_flush,
};
use std::collections::{BTreeMap, BTreeSet};

impl NodeApp {
    pub(super) fn notify_desired_state_watchers(&self, previous_revision: orion::Revision) {
        let snapshot = self.state_snapshot();
        if snapshot.state.desired.revision <= previous_revision {
            return;
        }

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
                        .map(|watch| (watch.executor_id.clone(), watch.last_workloads.clone())),
                    client
                        .provider_watch
                        .as_ref()
                        .map(|watch| (watch.provider_id.clone(), watch.last_leases.clone())),
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
            let state_snapshot = previous_watched_revision
                .filter(|revision| snapshot.state.desired.revision > *revision)
                .map(|revision| (revision, snapshot.clone()));

            let executor_update = if let Some((executor_id, last_workloads)) = executor_watch {
                executor_workloads_by_id
                    .get(&executor_id)
                    .and_then(|workloads| workloads.clone())
                    .filter(|workloads| workloads != &last_workloads)
                    .map(|workloads| (executor_id, last_workloads, workloads))
            } else {
                None
            };

            let provider_update = if let Some((provider_id, last_leases)) = provider_watch {
                provider_leases_by_id
                    .get(&provider_id)
                    .cloned()
                    .filter(|leases| leases != &last_leases)
                    .map(|leases| (provider_id, last_leases, leases))
            } else {
                None
            };

            if state_snapshot.is_some() || executor_update.is_some() || provider_update.is_some() {
                updates.push((source, state_snapshot, executor_update, provider_update));
            }
        }

        let mut pending_flushes = Vec::<PendingClientStreamFlush>::new();
        self.with_client_registry_txn(|txn| {
            for (source, state_snapshot, executor_update, provider_update) in updates {
                let Some(client) = txn.client_mut_if_present(&source) else {
                    continue;
                };

                if let Some((previous_watched_revision, snapshot)) = state_snapshot
                    && client
                        .state_watch
                        .as_ref()
                        .map(|watch| watch.desired_revision)
                        == Some(previous_watched_revision)
                {
                    let desired_revision = snapshot.state.desired.revision;
                    enqueue_state_snapshot_event(client, snapshot);
                    if let Some(watch) = client.state_watch.as_mut() {
                        watch.desired_revision = desired_revision;
                    }
                }

                if let Some((executor_id, last_workloads, workloads)) = executor_update
                    && client
                        .executor_watch
                        .as_ref()
                        .map(|watch| (watch.executor_id.clone(), watch.last_workloads.clone()))
                        == Some((executor_id.clone(), last_workloads))
                {
                    if let Some(watch) = client.executor_watch.as_mut() {
                        watch.last_workloads = workloads.clone();
                    }
                    enqueue_executor_workloads_event(client, executor_id, workloads);
                }

                if let Some((provider_id, last_leases, leases)) = provider_update
                    && client
                        .provider_watch
                        .as_ref()
                        .map(|watch| (watch.provider_id.clone(), watch.last_leases.clone()))
                        == Some((provider_id.clone(), last_leases))
                {
                    if let Some(watch) = client.provider_watch.as_mut() {
                        watch.last_leases = leases.clone();
                    }
                    enqueue_provider_leases_event(client, provider_id, leases);
                }

                if let Some(flush) = prepare_client_stream_flush(&source, client) {
                    pending_flushes.push(flush);
                }
            }
        });

        for flush in pending_flushes {
            let delivered = execute_client_stream_flush(&flush);
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
        let Some(Some(flush)) = pending else {
            return;
        };
        let delivered = execute_client_stream_flush(&flush);
        let _ = self.with_client_mut_if_present(source, |client| {
            finalize_client_stream_flush(client, &flush, delivered);
        });
    }
}

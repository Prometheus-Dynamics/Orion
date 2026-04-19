use super::observability::push_observability_event_with_context;
use super::{NodeApp, NodeError, PeerSyncExecution, ReconcileLoopHandle};
use orion::control_plane::{NodeObservabilitySnapshot, ObservabilityEventKind};
use std::{future::Future, sync::Arc, time::Duration};
use tracing::{error, info};

impl NodeApp {
    fn spawn_background_loop<F, Fut>(&self, interval: Duration, mut tick: F) -> ReconcileLoopHandle
    where
        F: FnMut(NodeApp) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let app = self.clone();
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

        let task = tokio::spawn(async move {
            loop {
                if *shutdown_rx.borrow() {
                    break;
                }
                tick(app.clone()).await;
                tokio::select! {
                    _ = shutdown_rx.changed() => {}
                    _ = tokio::time::sleep(interval) => {}
                }
            }
        });

        ReconcileLoopHandle {
            shutdown_tx,
            task: Arc::new(std::sync::Mutex::new(Some(task))),
        }
    }

    fn spawn_fallible_background_loop<F, Fut>(
        &self,
        interval: Duration,
        task_name: &'static str,
        mut tick: F,
    ) -> ReconcileLoopHandle
    where
        F: FnMut(NodeApp) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), NodeError>> + Send + 'static,
    {
        let app = self.clone();
        let node_id = self.config.node_id.clone();
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

        let task = tokio::spawn(async move {
            loop {
                if *shutdown_rx.borrow() {
                    break;
                }
                if let Err(err) = tick(app.clone()).await {
                    error!(node = %node_id, task = task_name, error = %err, "background loop iteration failed");
                }
                tokio::select! {
                    _ = shutdown_rx.changed() => {}
                    _ = tokio::time::sleep(interval) => {}
                }
            }
        });

        ReconcileLoopHandle {
            shutdown_tx,
            task: Arc::new(std::sync::Mutex::new(Some(task))),
        }
    }

    pub fn spawn_reconcile_loop(&self, interval: Duration) -> ReconcileLoopHandle {
        self.spawn_fallible_background_loop(interval, "reconcile", |app| async move {
            app.tick_async().await.map(|_| ())
        })
    }

    pub fn spawn_peer_sync_loop(&self, interval: Duration) -> ReconcileLoopHandle {
        self.spawn_peer_sync_loop_with_execution(interval, self.config.peer_sync_execution)
    }

    pub fn spawn_peer_sync_loop_with_execution(
        &self,
        interval: Duration,
        execution: PeerSyncExecution,
    ) -> ReconcileLoopHandle {
        self.spawn_fallible_background_loop(interval, "peer_sync", move |app| async move {
            let failures = app
                .sync_all_peers_with_execution(execution)
                .await
                .into_iter()
                .filter_map(|(node_id, result)| result.err().map(|err| format!("{node_id}: {err}")))
                .collect::<Vec<_>>();
            if failures.is_empty() {
                Ok(())
            } else {
                Err(NodeError::Storage(format!(
                    "peer sync iteration reported failures: {}",
                    failures.join("; ")
                )))
            }
        })
    }

    pub fn spawn_observability_loop(&self, interval: Duration) -> ReconcileLoopHandle {
        self.spawn_background_loop(interval, |app| async move {
            let snapshot = app.observability_snapshot();
            log_observability_summary(&snapshot);
        })
    }
}

fn log_observability_summary(snapshot: &NodeObservabilitySnapshot) {
    info!(
        node = %snapshot.node_id,
        desired_revision = %snapshot.desired_revision,
        observed_revision = %snapshot.observed_revision,
        applied_revision = %snapshot.applied_revision,
        replay_failures = snapshot.replay.failure_count,
        replay_last_error_category = ?snapshot.replay.last_error_category,
        peer_sync_failures = snapshot.peer_sync.failure_count,
        peer_sync_last_error_category = ?snapshot.peer_sync.last_error_category,
        reconcile_failures = snapshot.reconcile.failure_count,
        reconcile_last_error_category = ?snapshot.reconcile.last_error_category,
        mutation_failures = snapshot.mutation_apply.failure_count,
        mutation_last_error_category = ?snapshot.mutation_apply.last_error_category,
        persistence_failures = snapshot.persistence.state_persist.failure_count,
        persistence_last_error_category = ?snapshot.persistence.state_persist.last_error_category,
        persistence_last_duration_ms = snapshot.persistence.state_persist.last_duration_ms,
        artifact_write_failures = snapshot.persistence.artifact_write.failure_count,
        artifact_write_last_error_category = ?snapshot.persistence.artifact_write.last_error_category,
        artifact_write_last_duration_ms = snapshot.persistence.artifact_write.last_duration_ms,
        persistence_worker_enqueue_waits = snapshot.persistence.worker_enqueue_wait_count,
        persistence_worker_enqueue_wait_ms_total = snapshot.persistence.worker_enqueue_wait_ms_total,
        persistence_worker_enqueue_wait_ms_max = snapshot.persistence.worker_enqueue_wait_ms_max,
        http_request_failures = snapshot.transport.http_request_failures,
        http_malformed_input_count = snapshot.transport.http_malformed_input_count,
        ipc_frame_failures = snapshot.transport.ipc_frame_failures,
        ipc_malformed_input_count = snapshot.transport.ipc_malformed_input_count,
        reconnect_count = snapshot.transport.reconnect_count,
        audit_log_dropped_records = snapshot.audit_log_dropped_records,
        configured_peer_count = snapshot.configured_peer_count,
        degraded_peer_count = snapshot.degraded_peer_count,
        active_clients = snapshot.client_sessions.registered_clients,
        live_streams = snapshot.client_sessions.live_stream_clients,
        queued_client_events = snapshot.client_sessions.queued_client_events,
        "observability summary"
    );
}

pub(super) fn record_stale_stream_eviction(
    app: &NodeApp,
    source: &orion::transport::ipc::LocalAddress,
    duration: Duration,
) {
    let mut observability = app.observability_lock();
    observability.stale_client_evictions_total =
        observability.stale_client_evictions_total.saturating_add(1);
    push_observability_event_with_context(
        &mut observability,
        ObservabilityEventKind::ClientStaleEviction,
        false,
        Some(duration),
        Some(source.as_str().to_owned()),
        Some(source.as_str().to_owned()),
        format!("source={}", source.as_str()),
    );
}

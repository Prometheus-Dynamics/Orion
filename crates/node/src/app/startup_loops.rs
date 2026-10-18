use super::observability::push_observability_event_with_context;
use super::{NodeApp, NodeError, PeerSyncExecution, ReconcileLoopHandle};
use orion::control_plane::ObservabilityEventKind;
use std::{future::Future, sync::Arc, time::Duration};
use tracing::{debug, error};

impl NodeApp {
    pub(super) fn spawn_background_loop<F, Fut>(
        &self,
        interval: Duration,
        mut tick: F,
    ) -> ReconcileLoopHandle
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
                    // A dropped handle closes the channel; stop instead of spinning on the
                    // immediately-ready `changed()` error.
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                    }
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
                    // A dropped handle closes the channel; stop instead of spinning on the
                    // immediately-ready `changed()` error.
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                    }
                    _ = tokio::time::sleep(interval) => {}
                }
            }
        });

        ReconcileLoopHandle {
            shutdown_tx,
            task: Arc::new(std::sync::Mutex::new(Some(task))),
        }
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

    /// Re-reads the kernel clock state every `runtime_tuning.clock_refresh_interval` and
    /// republishes the node's clock facts when they change meaningfully.
    pub fn spawn_clock_facts_loop(&self) -> ReconcileLoopHandle {
        let interval = self.config.runtime_tuning.clock_refresh_interval;
        self.spawn_background_loop(interval, |app| async move {
            if app.refresh_clock_facts_from(&crate::KernelClockStatusSource) {
                debug!(node = %app.config.node_id, clock = ?app.published_clock_facts(), "published clock facts");
            }
        })
    }
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

use super::{NodeApp, NodeError, PeerSyncExecution};
use orion::NodeId;
use std::time::Duration;

impl NodeApp {
    pub async fn sync_all_peers(&self) -> Vec<(NodeId, Result<(), NodeError>)> {
        self.sync_all_peers_with_execution(self.config.peer_sync_execution)
            .await
    }

    pub async fn sync_all_peers_serial(&self) -> Vec<(NodeId, Result<(), NodeError>)> {
        let peer_ids: Vec<_> = self.peers_read().keys().cloned().collect();
        let mut results = Vec::with_capacity(peer_ids.len());
        for peer_id in peer_ids {
            let result = self.sync_peer(&peer_id).await;
            results.push((peer_id, result));
        }
        results
    }

    pub async fn sync_all_peers_parallel(
        &self,
        max_in_flight: usize,
    ) -> Vec<(NodeId, Result<(), NodeError>)> {
        let peer_ids: Vec<_> = self.peers_read().keys().cloned().collect();
        if peer_ids.is_empty() {
            return Vec::new();
        }

        let peer_count = peer_ids.len();
        let max_in_flight = self.effective_parallel_in_flight(max_in_flight, peer_ids.len());
        let mut pending = peer_ids.into_iter();
        let mut join_set = tokio::task::JoinSet::new();

        for slot in 0..max_in_flight {
            if let Some(peer_id) = pending.next() {
                Self::spawn_sync_peer_task(
                    &mut join_set,
                    self.clone(),
                    peer_id,
                    self.parallel_spawn_stagger_ms(slot, peer_count),
                );
            }
        }

        let mut results = Vec::new();
        while let Some(joined) = join_set.join_next().await {
            match joined {
                Ok(result) => results.push(result),
                Err(err) => results.push((
                    NodeId::new("peer-sync.task"),
                    Err(NodeError::Storage(format!(
                        "parallel peer sync task failed: {err}"
                    ))),
                )),
            }

            if let Some(peer_id) = pending.next() {
                Self::spawn_sync_peer_task(
                    &mut join_set,
                    self.clone(),
                    peer_id,
                    self.parallel_followup_stagger_ms(peer_count),
                );
            }
        }

        results.sort_by(|left, right| left.0.cmp(&right.0));
        results
    }

    fn spawn_sync_peer_task(
        join_set: &mut tokio::task::JoinSet<(NodeId, Result<(), NodeError>)>,
        app: NodeApp,
        peer_id: NodeId,
        stagger_ms: u64,
    ) {
        join_set.spawn(async move {
            if stagger_ms > 0 {
                tokio::time::sleep(Duration::from_millis(stagger_ms)).await;
            }
            let result = app.sync_peer(&peer_id).await;
            (peer_id, result)
        });
    }

    pub(super) fn parallel_spawn_stagger_ms(&self, slot: usize, peer_count: usize) -> u64 {
        let tuning = &self.config.runtime_tuning;
        if peer_count <= tuning.peer_sync_parallel_no_stagger_peer_count_threshold {
            return 0;
        }
        (slot as u64)
            .saturating_mul(tuning.peer_sync_parallel_spawn_stagger_step_ms)
            .min(tuning.peer_sync_parallel_spawn_stagger_max_ms)
    }

    fn parallel_followup_stagger_ms(&self, peer_count: usize) -> u64 {
        let tuning = &self.config.runtime_tuning;
        if peer_count <= tuning.peer_sync_parallel_no_stagger_peer_count_threshold {
            0
        } else {
            tuning.peer_sync_parallel_followup_stagger_ms
        }
    }

    pub(super) fn effective_parallel_in_flight(
        &self,
        requested: usize,
        peer_count: usize,
    ) -> usize {
        if peer_count == 0 {
            return 0;
        }

        let requested = requested.max(1).min(peer_count);
        let tuning = &self.config.runtime_tuning;
        let adaptive_cap = if peer_count <= 2 {
            requested
        } else if peer_count <= tuning.peer_sync_parallel_small_cluster_peer_count_threshold {
            tuning.peer_sync_parallel_small_cluster_cap
        } else {
            tuning.peer_sync_parallel_large_cluster_cap
        };
        requested.min(adaptive_cap).max(1)
    }

    pub async fn sync_all_peers_with_execution(
        &self,
        execution: PeerSyncExecution,
    ) -> Vec<(NodeId, Result<(), NodeError>)> {
        match execution {
            PeerSyncExecution::Serial => self.sync_all_peers_serial().await,
            PeerSyncExecution::Parallel { max_in_flight } => {
                self.sync_all_peers_parallel(max_in_flight).await
            }
        }
    }
}

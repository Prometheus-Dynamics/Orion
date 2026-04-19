use super::{
    AuditEventKind, NodeApp, NodeError, NodeSnapshot, peer_sync_troubleshooting_hint,
    stable_fingerprint, write_audit_record,
};
use crate::PeerSyncExecution;
use crate::config::AuditLogOverloadPolicy;
use crate::peer::{PeerConfig, PeerState, PeerSyncStatus, PeerTrustStatus};
use orion::{
    NodeId,
    control_plane::{
        AuditLogBackpressureMode, ClientSessionMetricsSnapshot, NodeHealthSnapshot,
        NodeHealthStatus, NodeObservabilitySnapshot, NodeReadinessSnapshot, NodeReadinessStatus,
        PersistenceMetricsSnapshot, TransportMetricsSnapshot,
    },
};
use orion_core::PublicKeyHex;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;

impl NodeApp {
    pub fn snapshot(&self) -> NodeSnapshot {
        let store = self.store_read();
        let peers = self.peers_read();
        let providers = self.providers_read();
        let executors = self.executors_read();
        let snapshot = store.snapshot();

        NodeSnapshot {
            node_id: snapshot.local_node_id,
            desired_revision: snapshot.desired_revision,
            observed_revision: snapshot.observed_revision,
            applied_revision: snapshot.applied_revision,
            registered_peers: peers.len(),
            registered_providers: providers.len(),
            registered_executors: executors.len(),
            pending_commands: 0,
        }
    }

    pub fn observability_snapshot(&self) -> NodeObservabilitySnapshot {
        let revisions = self.current_revisions();
        let clients = self.clients_read();
        let peers = self.peers_read();
        let observability = self.observability_read();
        let ready_peer_count = peers
            .values()
            .filter(|peer| {
                matches!(
                    peer.sync_status,
                    PeerSyncStatus::Ready | PeerSyncStatus::Synced
                )
            })
            .count() as u64;
        let pending_peer_count = peers
            .values()
            .filter(|peer| {
                matches!(
                    peer.sync_status,
                    PeerSyncStatus::Configured
                        | PeerSyncStatus::Negotiating
                        | PeerSyncStatus::Syncing
                )
            })
            .count() as u64;
        let degraded_peer_count = peers
            .values()
            .filter(|peer| {
                matches!(
                    peer.sync_status,
                    PeerSyncStatus::Error | PeerSyncStatus::BackingOff
                )
            })
            .count() as u64;
        let configured_peer_count = peers.len() as u64;
        let peer_sync_parallel_in_flight_cap = match self.config.peer_sync_execution {
            PeerSyncExecution::Serial => usize::from(!peers.is_empty()),
            PeerSyncExecution::Parallel { max_in_flight } => {
                self.effective_parallel_in_flight(max_in_flight, peers.len())
            }
        };
        let persistence_worker_metrics = self
            .persistence_worker
            .as_ref()
            .map(|worker| worker.metrics_snapshot())
            .unwrap_or_default();

        NodeObservabilitySnapshot {
            node_id: self.config.node_id.clone(),
            desired_revision: revisions.desired,
            observed_revision: revisions.observed,
            applied_revision: revisions.applied,
            configured_peer_count,
            ready_peer_count,
            pending_peer_count,
            degraded_peer_count,
            peer_sync_parallel_in_flight_cap: peer_sync_parallel_in_flight_cap
                .min(u64::MAX as usize) as u64,
            replay: observability.replay.snapshot(),
            peer_sync: observability.peer_sync.snapshot(),
            reconcile: observability.reconcile.snapshot(),
            mutation_apply: observability.mutation_apply.snapshot(),
            persistence: PersistenceMetricsSnapshot {
                state_persist: observability.persistence.snapshot(),
                artifact_write: observability.artifact_write.snapshot(),
                worker_queue_capacity: self
                    .config
                    .runtime_tuning
                    .persistence_worker_queue_capacity
                    .min(u64::MAX as usize) as u64,
                worker_enqueue_count: persistence_worker_metrics.enqueue_count,
                worker_enqueue_wait_count: persistence_worker_metrics.enqueue_wait_count,
                worker_enqueue_wait_ms_total: persistence_worker_metrics.enqueue_wait_ms_total,
                worker_enqueue_wait_ms_max: persistence_worker_metrics.enqueue_wait_ms_max,
            },
            audit_log_queue_capacity: self
                .config
                .runtime_tuning
                .audit_log_queue_capacity
                .min(u64::MAX as usize) as u64,
            audit_log_backpressure_mode: match self.config.runtime_tuning.audit_log_overload_policy
            {
                AuditLogOverloadPolicy::Block => AuditLogBackpressureMode::Block,
                AuditLogOverloadPolicy::DropNewest => AuditLogBackpressureMode::DropNewest,
            },
            audit_log_dropped_records: self
                .audit_log
                .as_ref()
                .map(|audit_log| audit_log.dropped_records())
                .unwrap_or(0),
            client_sessions: ClientSessionMetricsSnapshot {
                registered_clients: clients.len() as u64,
                live_stream_clients: clients
                    .values()
                    .filter(|client| client.stream_sender.is_some())
                    .count() as u64,
                state_watch_clients: clients
                    .values()
                    .filter(|client| client.state_watch.is_some())
                    .count() as u64,
                executor_watch_clients: clients
                    .values()
                    .filter(|client| client.executor_watch.is_some())
                    .count() as u64,
                provider_watch_clients: clients
                    .values()
                    .filter(|client| client.provider_watch.is_some())
                    .count() as u64,
                queued_client_events: clients
                    .values()
                    .map(|client| client.queued_events.len() as u64)
                    .sum(),
                registrations_total: observability.client_registrations_total,
                stream_attaches_total: observability.client_stream_attaches_total,
                stream_detaches_total: observability.client_stream_detaches_total,
                stale_evictions_total: observability.stale_client_evictions_total,
                rate_limited_total: observability.client_rate_limited_total,
            },
            transport: TransportMetricsSnapshot {
                http_request_failures: observability.http_request_failures,
                http_malformed_input_count: observability.http_malformed_input_count,
                http_tls_failures: observability.http_tls_failures,
                http_tls_client_auth_failures: observability.http_tls_client_auth_failures,
                ipc_frame_failures: observability.ipc_frame_failures,
                ipc_malformed_input_count: observability.ipc_malformed_input_count,
                reconnect_count: observability.reconnect_count,
            },
            recent_events: observability.recent_events.iter().cloned().collect(),
        }
    }

    pub fn health_snapshot(&self) -> NodeHealthSnapshot {
        let lifecycle = self.lifecycle_snapshot();
        let peers = self.peers_read();
        let degraded_peer_count = peers
            .values()
            .filter(|peer| {
                matches!(
                    peer.sync_status,
                    PeerSyncStatus::Error | PeerSyncStatus::BackingOff
                )
            })
            .count() as u64;
        let mut reasons = Vec::new();
        if !lifecycle.replay_completed {
            reasons.push("startup replay not completed".to_owned());
        } else if !lifecycle.replay_successful {
            reasons.push("startup replay failed".to_owned());
        }
        if !lifecycle.http_bound {
            reasons.push("http transport not bound".to_owned());
        }
        if !lifecycle.ipc_bound {
            reasons.push("ipc control transport not bound".to_owned());
        }
        if !lifecycle.ipc_stream_bound {
            reasons.push("ipc stream transport not bound".to_owned());
        }
        if degraded_peer_count > 0 {
            reasons.push(format!("{} peers degraded", degraded_peer_count));
        }
        let status = if reasons.is_empty() {
            NodeHealthStatus::Healthy
        } else {
            NodeHealthStatus::Degraded
        };

        NodeHealthSnapshot {
            node_id: self.config.node_id.clone(),
            alive: true,
            replay_completed: lifecycle.replay_completed,
            replay_successful: lifecycle.replay_successful,
            http_bound: lifecycle.http_bound,
            ipc_bound: lifecycle.ipc_bound,
            ipc_stream_bound: lifecycle.ipc_stream_bound,
            degraded_peer_count,
            status,
            reasons,
        }
    }

    pub fn readiness_snapshot(&self) -> NodeReadinessSnapshot {
        let lifecycle = self.lifecycle_snapshot();
        let peers = self.peers_read();
        let ready_peer_count = peers
            .values()
            .filter(|peer| {
                matches!(
                    peer.sync_status,
                    PeerSyncStatus::Ready | PeerSyncStatus::Synced
                )
            })
            .count() as u64;
        let pending_peer_count = peers
            .values()
            .filter(|peer| {
                matches!(
                    peer.sync_status,
                    PeerSyncStatus::Configured
                        | PeerSyncStatus::Negotiating
                        | PeerSyncStatus::Syncing
                )
            })
            .count() as u64;
        let degraded_peer_count = peers
            .values()
            .filter(|peer| {
                matches!(
                    peer.sync_status,
                    PeerSyncStatus::Error | PeerSyncStatus::BackingOff
                )
            })
            .count() as u64;
        let initial_sync_complete = peers.is_empty()
            || peers.values().all(|peer| {
                !matches!(
                    peer.sync_status,
                    PeerSyncStatus::Configured
                        | PeerSyncStatus::Negotiating
                        | PeerSyncStatus::Syncing
                )
            });
        let mut reasons = Vec::new();
        if !lifecycle.replay_completed {
            reasons.push("startup replay not completed".to_owned());
        } else if !lifecycle.replay_successful {
            reasons.push("startup replay failed".to_owned());
        }
        if !lifecycle.http_bound {
            reasons.push("http transport not bound".to_owned());
        }
        if !lifecycle.ipc_bound {
            reasons.push("ipc control transport not bound".to_owned());
        }
        if !lifecycle.ipc_stream_bound {
            reasons.push("ipc stream transport not bound".to_owned());
        }
        if !initial_sync_complete {
            reasons.push("initial peer sync still pending".to_owned());
        }
        let ready = reasons.is_empty();

        NodeReadinessSnapshot {
            node_id: self.config.node_id.clone(),
            ready,
            replay_completed: lifecycle.replay_completed,
            replay_successful: lifecycle.replay_successful,
            http_bound: lifecycle.http_bound,
            ipc_bound: lifecycle.ipc_bound,
            ipc_stream_bound: lifecycle.ipc_stream_bound,
            initial_sync_complete,
            ready_peer_count,
            pending_peer_count,
            degraded_peer_count,
            status: if ready {
                NodeReadinessStatus::Ready
            } else {
                NodeReadinessStatus::NotReady
            },
            reasons,
        }
    }

    pub fn peer_states(&self) -> Vec<PeerState> {
        self.peers_read().values().cloned().collect()
    }

    pub fn peer_trust_statuses(&self) -> Result<Vec<PeerTrustStatus>, NodeError> {
        let peers = self.peers_read();
        let mut node_ids = self.security.configured_peer_node_ids()?;
        node_ids.extend(self.security.trust_store_node_ids()?);
        let mut statuses = node_ids
            .into_iter()
            .map(|node_id| {
                let learned_tls_root_cert_fingerprint = self
                    .security
                    .trusted_peer_tls_root_cert_pem(&node_id)?
                    .map(|pem| format!("{:016x}", stable_fingerprint(&pem)));
                Ok(PeerTrustStatus {
                    last_error_kind: peers
                        .get(&node_id)
                        .and_then(|peer| peer.last_error_kind.clone()),
                    troubleshooting_hint: peers
                        .get(&node_id)
                        .and_then(|peer| peer.last_error_kind.clone())
                        .map(|kind| peer_sync_troubleshooting_hint(&kind)),
                    base_url: peers.get(&node_id).map(|peer| peer.base_url.clone()),
                    configured_public_key_hex: self
                        .security
                        .configured_peer_public_key_hex(&node_id)?,
                    trusted_public_key_hex: self.security.trusted_peer_public_key_hex(&node_id)?,
                    configured_tls_root_cert_path: peers.get(&node_id).and_then(|peer| {
                        peer.tls_root_cert_path
                            .as_ref()
                            .map(|path| path.display().to_string())
                    }),
                    learned_tls_root_cert_fingerprint,
                    revoked: self.security.is_peer_revoked(&node_id)?,
                    sync_status: peers
                        .get(&node_id)
                        .map(|peer| peer.sync_status.clone().into()),
                    last_error: peers.get(&node_id).and_then(|peer| peer.last_error.clone()),
                    node_id,
                })
            })
            .collect::<Result<Vec<_>, NodeError>>()?;
        statuses.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        Ok(statuses)
    }

    pub fn peer_trust_status(
        &self,
        node_id: &NodeId,
    ) -> Result<Option<PeerTrustStatus>, NodeError> {
        Ok(self
            .peer_trust_statuses()?
            .into_iter()
            .find(|status| &status.node_id == node_id))
    }

    pub fn local_public_key_hex(&self) -> PublicKeyHex {
        self.security.public_key_hex()
    }

    pub fn trusted_peer_public_key_hex(
        &self,
        node_id: &NodeId,
    ) -> Result<Option<PublicKeyHex>, NodeError> {
        self.security.trusted_peer_public_key_hex(node_id)
    }

    pub fn is_peer_revoked(&self, node_id: &NodeId) -> Result<bool, NodeError> {
        self.security.is_peer_revoked(node_id)
    }

    pub fn revoke_peer_identity(&self, node_id: &NodeId) -> Result<bool, NodeError> {
        let revoked = self.security.revoke_peer(node_id)?;
        if revoked {
            self.record_audit_event(
                AuditEventKind::PeerRevoked,
                Some(node_id.as_str().to_owned()),
                format!("revoked trusted peer `{node_id}`"),
            );
        }
        Ok(revoked)
    }

    pub fn replace_peer_identity_hex(
        &self,
        node_id: &NodeId,
        public_key_hex: &str,
    ) -> Result<(), NodeError> {
        self.security
            .replace_trusted_peer_key_hex(node_id, public_key_hex)?;
        self.record_audit_event(
            AuditEventKind::PeerIdentityReplaced,
            Some(node_id.as_str().to_owned()),
            format!("replaced trusted peer identity for `{node_id}`"),
        );
        Ok(())
    }

    pub(super) fn current_time_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64
    }

    pub(super) fn record_audit_event(
        &self,
        kind: AuditEventKind,
        subject: Option<String>,
        message: String,
    ) {
        let Some(audit_log) = &self.audit_log else {
            return;
        };
        if let Err(err) = write_audit_record(
            audit_log,
            self.config.node_id.as_str(),
            kind,
            subject,
            message,
        ) {
            warn!(node = %self.config.node_id, error = %err, "failed to append audit log event");
        }
    }

    pub fn register_peer(&self, peer: PeerConfig) -> Result<NodeId, NodeError> {
        let node_id = peer.node_id.clone();
        self.security.configure_peer(&peer)?;
        self.with_peers_mut(|peers| {
            if peers.contains_key(&node_id) {
                return Err(NodeError::DuplicatePeer(node_id.clone()));
            }
            peers.insert(node_id.clone(), PeerState::from_config(peer));
            Ok(())
        })?;
        Ok(node_id)
    }

    pub fn enroll_peer(&self, peer: PeerConfig) -> Result<NodeId, NodeError> {
        self.security.configure_peer(&peer)?;
        let node_id = peer.node_id.clone();
        let base_url = peer.base_url.clone();
        self.with_peers_mut(|peers| match peers.get_mut(&node_id) {
            Some(existing) => {
                existing.base_url = peer.base_url;
                existing.tls_root_cert_path = peer.tls_root_cert_path;
                existing.last_error = None;
            }
            None => {
                peers.insert(node_id.clone(), PeerState::from_config(peer));
            }
        });
        self.record_audit_event(
            AuditEventKind::PeerEnrolled,
            Some(node_id.as_str().to_owned()),
            format!("enrolled peer `{node_id}` at `{base_url}`"),
        );
        Ok(node_id)
    }

    pub fn trust_peer_tls_root_cert_pem(
        &self,
        node_id: &NodeId,
        tls_root_cert_pem: Vec<u8>,
    ) -> Result<(), NodeError> {
        self.security
            .trust_peer_tls_root_cert_pem(node_id, tls_root_cert_pem)?;
        self.evict_peer_client(node_id);
        self.record_audit_event(
            AuditEventKind::PeerTlsPretrusted,
            Some(node_id.as_str().to_owned()),
            format!("pretrusted TLS root certificate for peer `{node_id}`"),
        );
        Ok(())
    }
}

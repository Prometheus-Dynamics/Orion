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
        AuditLogBackpressureMode, ClientSessionMetricsSnapshot, CommunicationEndpointScope,
        CommunicationEndpointSnapshot, CommunicationMetricsSnapshot, CommunicationTransportKind,
        HostMetricsSnapshot, NodeHealthSnapshot, NodeHealthStatus, NodeObservabilitySnapshot,
        NodeReadinessSnapshot, NodeReadinessStatus, PersistenceMetricsSnapshot,
        TransportMetricsSnapshot,
    },
};
use orion_core::PublicKeyHex;
use std::{
    collections::BTreeMap,
    fs,
    time::{SystemTime, UNIX_EPOCH},
};
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
        let mut communication = Vec::new();
        for (source, client) in clients.iter() {
            let mut labels = BTreeMap::new();
            labels.insert(
                "client_name".to_owned(),
                client.session.client_name.to_string(),
            );
            labels.insert(
                "client_role".to_owned(),
                format!("{:?}", client.session.role),
            );
            labels.insert("local_address".to_owned(), source.as_str().to_owned());
            communication.push(CommunicationEndpointSnapshot {
                id: format!("ipc/local-unary/{}", source.as_str()),
                transport: CommunicationTransportKind::Ipc,
                scope: CommunicationEndpointScope::LocalUnary,
                local: Some(source.as_str().to_owned()),
                remote: None,
                labels: labels.clone(),
                connected: true,
                queued: None,
                metrics: client.unary_metrics.snapshot(),
            });

            let mut stream_labels = labels;
            if let Some(pid) = client.stream_peer_pid {
                stream_labels.insert("pid".to_owned(), pid.to_string());
            }
            if let Some(uid) = client.stream_peer_uid {
                stream_labels.insert("uid".to_owned(), uid.to_string());
            }
            if let Some(gid) = client.stream_peer_gid {
                stream_labels.insert("gid".to_owned(), gid.to_string());
            }
            communication.push(CommunicationEndpointSnapshot {
                id: format!("ipc/local-stream/{}", source.as_str()),
                transport: CommunicationTransportKind::Ipc,
                scope: CommunicationEndpointScope::LocalStream,
                local: Some(source.as_str().to_owned()),
                remote: None,
                labels: stream_labels,
                connected: client.stream_sender.is_some(),
                queued: Some(client.queued_events.len().min(u64::MAX as usize) as u64),
                metrics: client.stream_metrics.snapshot(),
            });
        }
        for (node_id, peer) in peers.iter() {
            let mut labels = BTreeMap::new();
            labels.insert("peer_node_id".to_owned(), node_id.to_string());
            labels.insert("base_url".to_owned(), peer.base_url.to_string());
            labels.insert("sync_status".to_owned(), format!("{:?}", peer.sync_status));
            if let Some(kind) = &peer.last_error_kind {
                labels.insert("last_error_kind".to_owned(), kind.to_string());
            }
            let metrics = observability
                .peer_http_communication
                .get(node_id)
                .map(|metrics| metrics.snapshot())
                .unwrap_or_else(empty_communication_metrics);
            communication.push(CommunicationEndpointSnapshot {
                id: format!("http/peer-sync/{node_id}"),
                transport: CommunicationTransportKind::Http,
                scope: CommunicationEndpointScope::PeerSync,
                local: Some(self.config.node_id.to_string()),
                remote: Some(peer.base_url.to_string()),
                labels,
                connected: !matches!(
                    peer.sync_status,
                    PeerSyncStatus::BackingOff | PeerSyncStatus::Error
                ),
                queued: None,
                metrics,
            });
        }
        communication.extend(
            observability
                .communication_endpoints
                .values()
                .map(|endpoint| endpoint.snapshot()),
        );

        NodeObservabilitySnapshot {
            node_id: self.config.node_id.clone(),
            desired_revision: revisions.desired,
            observed_revision: revisions.observed,
            applied_revision: revisions.applied,
            maintenance: self.maintenance_state_read().clone(),
            peer_sync_paused: self.peer_sync_paused(),
            remote_desired_state_blocked: self.remote_desired_state_blocked(),
            host: host_metrics_snapshot(),
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
                worker_operation_count: persistence_worker_metrics.operation_count,
                worker_queue_wait_count: persistence_worker_metrics.queue_wait_count,
                worker_queue_wait_ms_total: persistence_worker_metrics.queue_wait_ms_total,
                worker_queue_wait_ms_max: persistence_worker_metrics.queue_wait_ms_max,
                worker_reply_wait_count: persistence_worker_metrics.reply_wait_count,
                worker_reply_wait_ms_total: persistence_worker_metrics.reply_wait_ms_total,
                worker_reply_wait_ms_max: persistence_worker_metrics.reply_wait_ms_max,
                worker_operation_ms_total: persistence_worker_metrics.operation_ms_total,
                worker_operation_ms_max: persistence_worker_metrics.operation_ms_max,
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
            communication,
            recent_events: observability.recent_events.iter().cloned().collect(),
        }
    }

    pub fn health_snapshot(&self) -> NodeHealthSnapshot {
        let lifecycle = self.lifecycle_snapshot();
        let peers = self.peers_read();
        let communication_degraded_count = self.communication_degraded_count();
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
        if communication_degraded_count > 0 {
            reasons.push(format!(
                "{} communication endpoints degraded",
                communication_degraded_count
            ));
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

impl NodeApp {
    fn communication_degraded_count(&self) -> u64 {
        let observability = self.observability_read();
        observability
            .communication_endpoints
            .values()
            .filter(|endpoint| communication_endpoint_degraded(&endpoint.snapshot()))
            .count()
            .saturating_add(
                observability
                    .peer_http_communication
                    .values()
                    .filter(|metrics| communication_metrics_degraded(&metrics.snapshot()))
                    .count(),
            )
            .min(u64::MAX as usize) as u64
    }
}

fn communication_endpoint_degraded(endpoint: &CommunicationEndpointSnapshot) -> bool {
    let critical_disconnected = !endpoint.connected && !endpoint.scope.is_local_stream();
    critical_disconnected || communication_metrics_degraded(&endpoint.metrics)
}

fn communication_metrics_degraded(metrics: &CommunicationMetricsSnapshot) -> bool {
    metrics.recent.failures > 0
        && (metrics.recent.successes == 0
            || metrics.last_failure_at_ms >= metrics.last_success_at_ms)
}

fn empty_communication_metrics() -> CommunicationMetricsSnapshot {
    super::CommunicationMetrics::default().snapshot()
}

fn host_metrics_snapshot() -> HostMetricsSnapshot {
    let process_status = process_status_snapshot();
    let process_smaps = process_smaps_rollup_snapshot();
    HostMetricsSnapshot {
        hostname: read_trimmed("/proc/sys/kernel/hostname"),
        os_name: std::env::consts::OS.to_owned(),
        os_version: os_release_value("VERSION_ID"),
        kernel_version: read_trimmed("/proc/sys/kernel/osrelease"),
        architecture: std::env::consts::ARCH.to_owned(),
        uptime_seconds: proc_uptime_seconds(),
        load_1_milli: proc_loadavg_milli(0),
        load_5_milli: proc_loadavg_milli(1),
        load_15_milli: proc_loadavg_milli(2),
        memory_total_bytes: proc_meminfo_kib("MemTotal").map(kib_to_bytes),
        memory_available_bytes: proc_meminfo_kib("MemAvailable").map(kib_to_bytes),
        swap_total_bytes: proc_meminfo_kib("SwapTotal").map(kib_to_bytes),
        swap_free_bytes: proc_meminfo_kib("SwapFree").map(kib_to_bytes),
        process_id: std::process::id(),
        process_rss_bytes: process_rss_pages().map(|pages| {
            pages
                .saturating_mul(page_size_bytes())
                .min(u64::MAX as usize) as u64
        }),
        process_pss_bytes: process_smaps.pss_bytes,
        process_private_dirty_bytes: process_smaps.private_dirty_bytes,
        process_anonymous_bytes: process_smaps.anonymous_bytes,
        process_vm_size_bytes: process_status.vm_size_bytes,
        process_vm_data_bytes: process_status.vm_data_bytes,
        process_vm_hwm_bytes: process_status.vm_hwm_bytes,
        process_threads: process_status.threads,
        process_fd_count: process_fd_count(),
    }
}

#[derive(Default)]
struct ProcessStatusSnapshot {
    vm_size_bytes: Option<u64>,
    vm_data_bytes: Option<u64>,
    vm_hwm_bytes: Option<u64>,
    threads: Option<u64>,
}

#[derive(Default)]
struct ProcessSmapsRollupSnapshot {
    pss_bytes: Option<u64>,
    private_dirty_bytes: Option<u64>,
    anonymous_bytes: Option<u64>,
}

fn read_trimmed(path: &str) -> Option<String> {
    fs::read_to_string(path)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn os_release_value(key: &str) -> Option<String> {
    let contents = fs::read_to_string("/etc/os-release").ok()?;
    contents.lines().find_map(|line| {
        let (line_key, value) = line.split_once('=')?;
        (line_key == key).then(|| value.trim_matches('"').to_owned())
    })
}

fn proc_uptime_seconds() -> Option<u64> {
    let contents = fs::read_to_string("/proc/uptime").ok()?;
    let first = contents.split_whitespace().next()?;
    let whole_seconds = first.split('.').next()?;
    whole_seconds.parse().ok()
}

fn proc_loadavg_milli(index: usize) -> Option<u64> {
    let contents = fs::read_to_string("/proc/loadavg").ok()?;
    let value = contents.split_whitespace().nth(index)?;
    decimal_to_milli(value)
}

fn proc_meminfo_kib(key: &str) -> Option<u64> {
    let contents = fs::read_to_string("/proc/meminfo").ok()?;
    contents.lines().find_map(|line| {
        let (line_key, rest) = line.split_once(':')?;
        if line_key != key {
            return None;
        }
        rest.split_whitespace().next()?.parse().ok()
    })
}

fn process_rss_pages() -> Option<usize> {
    let contents = fs::read_to_string("/proc/self/statm").ok()?;
    contents.split_whitespace().nth(1)?.parse().ok()
}

fn process_status_snapshot() -> ProcessStatusSnapshot {
    let mut snapshot = ProcessStatusSnapshot::default();
    let Ok(contents) = fs::read_to_string("/proc/self/status") else {
        return snapshot;
    };
    for line in contents.lines() {
        if let Some(value) = proc_status_kib_line(line, "VmSize") {
            snapshot.vm_size_bytes = Some(kib_to_bytes(value));
        } else if let Some(value) = proc_status_kib_line(line, "VmData") {
            snapshot.vm_data_bytes = Some(kib_to_bytes(value));
        } else if let Some(value) = proc_status_kib_line(line, "VmHWM") {
            snapshot.vm_hwm_bytes = Some(kib_to_bytes(value));
        } else if let Some(value) = proc_status_plain_line(line, "Threads") {
            snapshot.threads = Some(value);
        }
    }
    snapshot
}

fn process_smaps_rollup_snapshot() -> ProcessSmapsRollupSnapshot {
    let mut snapshot = ProcessSmapsRollupSnapshot::default();
    let Ok(contents) = fs::read_to_string("/proc/self/smaps_rollup") else {
        return snapshot;
    };
    for line in contents.lines() {
        if let Some(value) = proc_status_kib_line(line, "Pss") {
            snapshot.pss_bytes = Some(kib_to_bytes(value));
        } else if let Some(value) = proc_status_kib_line(line, "Private_Dirty") {
            snapshot.private_dirty_bytes = Some(kib_to_bytes(value));
        } else if let Some(value) = proc_status_kib_line(line, "Anonymous") {
            snapshot.anonymous_bytes = Some(kib_to_bytes(value));
        }
    }
    snapshot
}

fn proc_status_kib_line(line: &str, key: &str) -> Option<u64> {
    let value = proc_status_plain_line(line, key)?;
    line.split_whitespace()
        .nth(2)
        .filter(|unit| *unit == "kB")?;
    Some(value)
}

fn proc_status_plain_line(line: &str, key: &str) -> Option<u64> {
    let (line_key, rest) = line.split_once(':')?;
    if line_key != key {
        return None;
    }
    rest.split_whitespace().next()?.parse().ok()
}

fn process_fd_count() -> Option<u64> {
    let entries = fs::read_dir("/proc/self/fd").ok()?;
    Some(entries.filter_map(Result::ok).count() as u64)
}

fn page_size_bytes() -> usize {
    let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    usize::try_from(size).unwrap_or(0)
}

fn decimal_to_milli(value: &str) -> Option<u64> {
    let (whole, fractional) = value.split_once('.').unwrap_or((value, ""));
    let whole = whole.parse::<u64>().ok()?;
    let mut digits = fractional.chars().take(3).collect::<String>();
    while digits.len() < 3 {
        digits.push('0');
    }
    let fractional = digits.parse::<u64>().ok()?;
    Some(whole.saturating_mul(1000).saturating_add(fractional))
}

fn kib_to_bytes(kib: u64) -> u64 {
    kib.saturating_mul(1024)
}

use super::{
    NodeApp, NodeError,
    local_clients::{
        ExecutorWatchState, ProviderWatchState, enqueue_executor_workloads_event,
        enqueue_provider_leases_event, enqueue_state_snapshot_event,
    },
    observability::public_http_mutual_tls_mode,
};
use crate::peer::PeerConfig;
use orion::{
    ExecutorId, ProviderId,
    control_plane::{
        ClientEvent, ClientEventPoll, ClientHello, ClientSession, ControlMessage,
        ExecutorStateUpdate, ObservabilityEventKind, PeerTrustRecord, PeerTrustSnapshot,
        ProviderRecord, ProviderStateUpdate, StateWatch, WorkloadRecord,
    },
    runtime::{ExecutorSnapshot, ProviderSnapshot},
    transport::ipc::{ControlEnvelope, LocalAddress},
};
use orion_core::SessionId;
use std::time::Duration;

impl NodeApp {
    pub(crate) fn apply_local_control_message(
        &self,
        source: &LocalAddress,
        message: ControlMessage,
    ) -> Result<ControlMessage, NodeError> {
        let now_ms = Self::current_time_ms();
        self.prune_expired_clients(now_ms, Some(source));
        if !matches!(
            message,
            ControlMessage::ClientHello(_) | ControlMessage::Ping | ControlMessage::Pong
        ) {
            self.enforce_local_rate_limit(source)?;
        }

        let response = match message {
            ControlMessage::ClientHello(hello) => self.register_local_client(source, hello),
            ControlMessage::ProviderState(update) => {
                self.apply_provider_state_update(source, update)?;
                Ok(ControlMessage::Accepted)
            }
            ControlMessage::ExecutorState(update) => {
                self.apply_executor_state_update(source, update)?;
                Ok(ControlMessage::Accepted)
            }
            ControlMessage::QueryExecutorWorkloads(query) => Ok(ControlMessage::ExecutorWorkloads(
                self.executor_workloads(source, &query)?,
            )),
            ControlMessage::WatchExecutorWorkloads(query) => {
                self.subscribe_executor_watch(source, query)?;
                Ok(ControlMessage::Accepted)
            }
            ControlMessage::QueryProviderLeases(query) => Ok(ControlMessage::ProviderLeases(
                self.provider_leases(source, &query)?,
            )),
            ControlMessage::EnrollPeer(enrollment) => {
                let orion::control_plane::PeerEnrollment {
                    node_id,
                    base_url,
                    trusted_public_key_hex,
                    trusted_tls_root_cert_pem,
                } = enrollment;
                self.enroll_peer(PeerConfig {
                    node_id: node_id.clone(),
                    base_url,
                    trusted_public_key_hex,
                    tls_root_cert_path: None,
                })?;
                if let Some(tls_root_cert_pem) = trusted_tls_root_cert_pem {
                    self.trust_peer_tls_root_cert_pem(&node_id, tls_root_cert_pem)?;
                }
                Ok(ControlMessage::Accepted)
            }
            ControlMessage::QueryPeerTrust => Ok(ControlMessage::PeerTrust(PeerTrustSnapshot {
                http_mutual_tls_mode: public_http_mutual_tls_mode(self.http_mutual_tls_mode),
                peers: self
                    .peer_trust_statuses()?
                    .into_iter()
                    .map(|status| PeerTrustRecord {
                        node_id: status.node_id,
                        base_url: status.base_url,
                        configured_public_key_hex: status.configured_public_key_hex,
                        trusted_public_key_hex: status.trusted_public_key_hex,
                        configured_tls_root_cert_path: status.configured_tls_root_cert_path,
                        learned_tls_root_cert_fingerprint: status.learned_tls_root_cert_fingerprint,
                        revoked: status.revoked,
                        sync_status: status.sync_status,
                        last_error: status.last_error,
                        last_error_kind: status.last_error_kind,
                        troubleshooting_hint: status.troubleshooting_hint,
                    })
                    .collect(),
            })),
            ControlMessage::QueryStateSnapshot => {
                Ok(ControlMessage::Snapshot(self.state_snapshot()))
            }
            ControlMessage::QueryObservability => Ok(ControlMessage::Observability(Box::new(
                self.observability_snapshot(),
            ))),
            ControlMessage::RevokePeer(node_id) => {
                self.revoke_peer_identity(&node_id)?;
                Ok(ControlMessage::Accepted)
            }
            ControlMessage::ReplacePeerIdentity(update) => {
                self.replace_peer_identity_hex(&update.node_id, &update.public_key_hex)?;
                Ok(ControlMessage::Accepted)
            }
            ControlMessage::RotateHttpTlsIdentity => {
                self.rotate_http_tls_identity()?;
                Ok(ControlMessage::Accepted)
            }
            ControlMessage::QueryMaintenance => {
                Ok(ControlMessage::MaintenanceStatus(self.maintenance_status()))
            }
            ControlMessage::UpdateMaintenance(command) => Ok(ControlMessage::MaintenanceStatus(
                self.update_maintenance(command)?,
            )),
            ControlMessage::WatchProviderLeases(query) => {
                self.subscribe_provider_watch(source, query)?;
                Ok(ControlMessage::Accepted)
            }
            ControlMessage::WatchState(watch) => {
                self.subscribe_state_watch(source, &watch)?;
                Ok(ControlMessage::Accepted)
            }
            ControlMessage::PollClientEvents(poll) => Ok(ControlMessage::ClientEvents(
                self.poll_client_events(source, &poll)?,
            )),
            ControlMessage::Ping => Ok(ControlMessage::Pong),
            ControlMessage::Pong => Ok(ControlMessage::Accepted),
            ControlMessage::Hello(_) => Ok(ControlMessage::Hello(self.peer_hello()?)),
            ControlMessage::SyncRequest(request) => {
                Ok(match self.build_sync_response(request)? {
                    orion::transport::http::HttpResponsePayload::Accepted => {
                        ControlMessage::Accepted
                    }
                    orion::transport::http::HttpResponsePayload::Hello(hello) => {
                        ControlMessage::Hello(hello)
                    }
                    orion::transport::http::HttpResponsePayload::Summary(_) => {
                        ControlMessage::Rejected(
                            "summary response is not valid for sync request".into(),
                        )
                    }
                    orion::transport::http::HttpResponsePayload::Snapshot(snapshot) => {
                        ControlMessage::Snapshot(snapshot)
                    }
                    orion::transport::http::HttpResponsePayload::Mutations(batch) => {
                        ControlMessage::Mutations(batch)
                    }
                    orion::transport::http::HttpResponsePayload::Health(_) => {
                        ControlMessage::Rejected(
                            "health response is not valid for sync request".into(),
                        )
                    }
                    orion::transport::http::HttpResponsePayload::Readiness(_) => {
                        ControlMessage::Rejected(
                            "readiness response is not valid for sync request".into(),
                        )
                    }
                    orion::transport::http::HttpResponsePayload::Observability(_) => {
                        ControlMessage::Rejected(
                            "observability response is not valid for sync request".into(),
                        )
                    }
                })
            }
            ControlMessage::SyncSummaryRequest(_) => Ok(ControlMessage::Rejected(
                "summary requests are only supported over peer HTTP".into(),
            )),
            ControlMessage::SyncDiffRequest(_) => Ok(ControlMessage::Rejected(
                "diff requests are only supported over peer HTTP".into(),
            )),
            ControlMessage::Snapshot(snapshot) => {
                self.adopt_remote_snapshot(snapshot)?;
                Ok(ControlMessage::Accepted)
            }
            ControlMessage::Mutations(batch) => {
                self.apply_remote_mutations(&batch)?;
                Ok(ControlMessage::Accepted)
            }
            ControlMessage::ClientWelcome(_)
            | ControlMessage::ExecutorWorkloads(_)
            | ControlMessage::ProviderLeases(_)
            | ControlMessage::PeerTrust(_)
            | ControlMessage::MaintenanceStatus(_)
            | ControlMessage::Observability(_)
            | ControlMessage::ClientEvents(_)
            | ControlMessage::Accepted
            | ControlMessage::Rejected(_) => Ok(ControlMessage::Rejected(
                "response-only control message received as a request".into(),
            )),
        }?;

        if !matches!(response, ControlMessage::Rejected(_)) {
            self.touch_local_client_activity(source, now_ms);
        }

        Ok(response)
    }

    fn enforce_local_rate_limit(&self, source: &LocalAddress) -> Result<(), NodeError> {
        let now_ms = Self::current_time_ms();
        let window_ms = self
            .config
            .runtime_tuning
            .local_rate_limit_window
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        let max_messages = self
            .config
            .runtime_tuning
            .local_rate_limit_max_messages
            .max(1);

        let rate_limited = self.with_client_mut(source, |client| {
            if now_ms.saturating_sub(client.rate_window_started_ms) >= window_ms {
                client.rate_window_started_ms = now_ms;
                client.rate_window_count = 0;
            }
            client.rate_window_count = client.rate_window_count.saturating_add(1);
            client.rate_window_count > max_messages
        })?;

        if rate_limited {
            self.with_observability_txn(|txn| {
                let observability = txn.state_mut();
                observability.client_rate_limited_total =
                    observability.client_rate_limited_total.saturating_add(1);
                txn.push_event_with_context(
                    ObservabilityEventKind::ClientRateLimited,
                    false,
                    None,
                    Some(source.as_str().to_owned()),
                    Some(source.as_str().to_owned()),
                    format!("source={} max_messages={max_messages}", source.as_str()),
                );
            });
            return Err(NodeError::RateLimitedClient {
                client: source.clone(),
            });
        }

        Ok(())
    }

    pub(super) fn touch_local_client_activity(&self, source: &LocalAddress, now_ms: u64) {
        let _ = self.with_client_mut_if_present(source, |client| {
            client.last_activity_ms = now_ms;
        });
    }

    pub(crate) fn record_local_unary_communication(
        &self,
        source: &LocalAddress,
        bytes_in: u64,
        bytes_out: u64,
        duration: Duration,
        stages: crate::app::CommunicationStageDurations,
    ) {
        let now_ms = Self::current_time_ms();
        let _ = self.with_client_mut_if_present(source, |client| {
            client.unary_metrics.record_received(bytes_in);
            client.unary_metrics.record_sent(bytes_out);
            client.unary_metrics.record_success_exchange_with_stages(
                now_ms, duration, bytes_out, bytes_in, &stages,
            );
        });
    }

    pub(super) fn record_local_stream_received(&self, source: &LocalAddress, bytes: u64) {
        let _ = self.with_client_mut_if_present(source, |client| {
            client.stream_metrics.record_received(bytes);
        });
    }

    pub(super) fn record_local_stream_sent(&self, source: &LocalAddress, bytes: u64) {
        let _ = self.with_client_mut_if_present(source, |client| {
            client.stream_metrics.record_sent(bytes);
        });
    }

    pub(super) fn record_local_stream_success(&self, source: &LocalAddress, duration: Duration) {
        let now_ms = Self::current_time_ms();
        let _ = self.with_client_mut_if_present(source, |client| {
            client
                .stream_metrics
                .record_success_exchange(now_ms, duration, 0, 0);
        });
    }

    pub(super) fn record_local_stream_failure(
        &self,
        source: &LocalAddress,
        duration: Option<Duration>,
        error: impl Into<String>,
    ) {
        let now_ms = Self::current_time_ms();
        let _ = self.with_client_mut_if_present(source, |client| {
            client
                .stream_metrics
                .record_failure(now_ms, duration, error);
            client.stream_sender = None;
        });
    }

    pub(super) fn record_local_stream_reconnect(&self, source: &LocalAddress) {
        let _ = self.with_client_mut_if_present(source, |client| {
            client.stream_metrics.record_reconnect();
        });
    }

    pub(super) fn record_local_stream_peer_identity(
        &self,
        source: &LocalAddress,
        identity: Option<orion::transport::ipc::UnixPeerIdentity>,
    ) {
        let _ = self.with_client_mut_if_present(source, |client| {
            if let Some(identity) = identity {
                client.stream_peer_pid = identity.pid;
                client.stream_peer_uid = Some(identity.uid);
                client.stream_peer_gid = Some(identity.gid);
            }
        });
    }

    fn prune_expired_clients(&self, now_ms: u64, preserve_source: Option<&LocalAddress>) {
        let ttl_ms = self
            .config
            .runtime_tuning
            .local_session_ttl
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        let mut expired = Vec::new();
        {
            let clients = self.clients_read();
            for (source, client) in clients.iter() {
                if preserve_source == Some(source) {
                    continue;
                }
                if client.stream_sender.is_some() {
                    continue;
                }
                if now_ms.saturating_sub(client.last_activity_ms) >= ttl_ms {
                    expired.push((source.clone(), client.session.clone()));
                }
            }
        }
        if expired.is_empty() {
            return;
        }

        self.with_client_registry_txn(|txn| {
            for (source, _) in &expired {
                txn.remove(source);
            }
        });

        self.with_observability_txn(|txn| {
            for (source, session) in expired {
                let observability = txn.state_mut();
                observability.stale_client_evictions_total =
                    observability.stale_client_evictions_total.saturating_add(1);
                txn.push_event_with_context(
                    ObservabilityEventKind::ClientStaleEviction,
                    false,
                    None,
                    Some(session.session_id.to_string()),
                    Some(session.client_name.to_string()),
                    format!("expired inactive client source={}", source.as_str()),
                );
            }
        });
    }

    fn client_expired(&self, source: &LocalAddress, now_ms: u64) -> bool {
        let ttl_ms = self
            .config
            .runtime_tuning
            .local_session_ttl
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        self.clients_read()
            .get(source)
            .map(|client| {
                client.stream_sender.is_none()
                    && now_ms.saturating_sub(client.last_activity_ms) >= ttl_ms
            })
            .unwrap_or(false)
    }

    fn register_local_client(
        &self,
        source: &LocalAddress,
        hello: ClientHello,
    ) -> Result<ControlMessage, NodeError> {
        let session = ClientSession {
            session_id: SessionId::new(format!("{}:{}", self.config.node_id, source.as_str())),
            client_name: hello.client_name,
            role: hello.role,
            node_id: self.config.node_id.clone(),
            source: source.as_str().to_owned(),
        };
        let now_ms = Self::current_time_ms();
        self.prune_expired_clients(now_ms, Some(source));
        if self.client_expired(source, now_ms) {
            self.with_client_registry_txn(|txn| {
                txn.remove(source);
            });
        }
        self.with_client_registry_txn(|txn| {
            txn.upsert_session(
                source,
                session.clone(),
                now_ms,
                self.config.runtime_tuning.local_client_event_queue_limit,
            );
        });
        self.with_observability_txn(|txn| {
            let observability = txn.state_mut();
            observability.client_registrations_total =
                observability.client_registrations_total.saturating_add(1);
            txn.push_event_with_context(
                ObservabilityEventKind::ClientRegistration,
                true,
                None,
                Some(session.session_id.to_string()),
                Some(session.client_name.to_string()),
                format!("client={} role={:?}", session.client_name, session.role),
            );
        });
        Ok(ControlMessage::ClientWelcome(session))
    }

    pub(super) fn attach_local_client_stream(
        &self,
        source: &LocalAddress,
        sender: tokio::sync::mpsc::Sender<ControlEnvelope>,
    ) -> Result<(), NodeError> {
        self.with_client_mut(source, |client| {
            client.stream_sender = Some(sender);
            client.last_activity_ms = Self::current_time_ms();
        })?;
        let reconnected = self.with_observability_txn(|txn| {
            let observability = txn.state_mut();
            let first_stream_for_source = observability
                .seen_stream_sources
                .insert(source.as_str().to_owned());
            let reconnected = !first_stream_for_source;
            if !first_stream_for_source {
                observability.reconnect_count = observability.reconnect_count.saturating_add(1);
            }
            observability.client_stream_attaches_total =
                observability.client_stream_attaches_total.saturating_add(1);
            txn.push_event_with_context(
                ObservabilityEventKind::ClientStreamAttach,
                true,
                None,
                Some(source.as_str().to_owned()),
                Some(source.as_str().to_owned()),
                format!("source={}", source.as_str()),
            );
            reconnected
        });
        if reconnected {
            self.record_local_stream_reconnect(source);
        }
        Ok(())
    }

    pub(super) fn detach_local_client_stream(&self, source: &LocalAddress) {
        let detached = self
            .with_client_mut_if_present(source, |client| {
                if client.stream_sender.take().is_some() {
                    client.last_activity_ms = Self::current_time_ms();
                    true
                } else {
                    false
                }
            })
            .unwrap_or(false);
        if detached {
            self.with_observability_txn(|txn| {
                let observability = txn.state_mut();
                observability.client_stream_detaches_total =
                    observability.client_stream_detaches_total.saturating_add(1);
                txn.push_event_with_context(
                    ObservabilityEventKind::ClientStreamDetach,
                    true,
                    None,
                    Some(source.as_str().to_owned()),
                    Some(source.as_str().to_owned()),
                    format!("source={}", source.as_str()),
                );
            });
        }
    }

    fn subscribe_state_watch(
        &self,
        source: &LocalAddress,
        watch: &StateWatch,
    ) -> Result<(), NodeError> {
        let current_revision = self.current_desired_revision();
        let snapshot = (current_revision > watch.desired_revision).then(|| self.state_snapshot());
        self.with_client_mut(source, |client| {
            client.state_watch = Some(watch.clone());
            if let Some(snapshot) = snapshot {
                enqueue_state_snapshot_event(client, snapshot);
                if let Some(state_watch) = client.state_watch.as_mut() {
                    state_watch.desired_revision = current_revision;
                }
            }
        })
    }

    fn subscribe_executor_watch(
        &self,
        source: &LocalAddress,
        query: orion::control_plane::ExecutorWorkloadQuery,
    ) -> Result<(), NodeError> {
        let workloads = self.executor_workloads(source, &query)?;
        self.with_client_mut(source, |client| {
            client.executor_watch = Some(ExecutorWatchState {
                executor_id: query.executor_id.clone(),
                last_workloads: workloads.clone(),
            });
            enqueue_executor_workloads_event(client, query.executor_id, workloads);
        })
    }

    fn subscribe_provider_watch(
        &self,
        source: &LocalAddress,
        query: orion::control_plane::ProviderLeaseQuery,
    ) -> Result<(), NodeError> {
        let leases = self.provider_leases(source, &query)?;
        self.with_client_mut(source, |client| {
            client.provider_watch = Some(ProviderWatchState {
                provider_id: query.provider_id.clone(),
                last_leases: leases.clone(),
            });
            enqueue_provider_leases_event(client, query.provider_id, leases);
        })
    }

    fn poll_client_events(
        &self,
        source: &LocalAddress,
        poll: &ClientEventPoll,
    ) -> Result<Vec<ClientEvent>, NodeError> {
        self.with_client_mut(source, |client| {
            let max_events = usize::try_from(poll.max_events.max(1)).unwrap_or(usize::MAX);
            let mut events = Vec::new();

            while let Some(event) = client.queued_events.front() {
                if event.sequence <= poll.after_sequence {
                    client.queued_events.pop_front();
                    continue;
                }
                if events.len() >= max_events {
                    break;
                }
                events.push(event.clone());
                client.queued_events.pop_front();
            }

            events
        })
    }

    fn apply_provider_state_update(
        &self,
        _source: &LocalAddress,
        update: ProviderStateUpdate,
    ) -> Result<(), NodeError> {
        self.ensure_provider_record(update.provider.clone())?;
        self.with_store_mut(|store| {
            store.apply_provider_snapshot(ProviderSnapshot {
                provider: update.provider,
                resources: update.resources,
            })
        })?;
        self.persist_state()?;
        let _ = self.tick()?;
        Ok(())
    }

    fn apply_executor_state_update(
        &self,
        _source: &LocalAddress,
        update: ExecutorStateUpdate,
    ) -> Result<(), NodeError> {
        self.ensure_executor_record(update.executor.clone())?;
        self.with_store_mut(|store| {
            store.apply_executor_snapshot(ExecutorSnapshot {
                executor: update.executor,
                workloads: update.workloads,
                resources: update.resources,
            })
        })?;
        self.persist_state()?;
        let _ = self.tick()?;
        Ok(())
    }

    fn ensure_provider_record(&self, record: ProviderRecord) -> Result<(), NodeError> {
        let current = self
            .store_read()
            .desired
            .providers
            .get(&record.provider_id)
            .cloned();
        if current.as_ref() == Some(&record) {
            return Ok(());
        }
        self.put_provider_record_tracked(record)
    }

    fn ensure_executor_record(
        &self,
        record: orion::control_plane::ExecutorRecord,
    ) -> Result<(), NodeError> {
        let current = self
            .store_read()
            .desired
            .executors
            .get(&record.executor_id)
            .cloned();
        if current.as_ref() == Some(&record) {
            return Ok(());
        }
        self.put_executor_record_tracked(record)
    }

    fn executor_workloads(
        &self,
        _source: &LocalAddress,
        query: &orion::control_plane::ExecutorWorkloadQuery,
    ) -> Result<Vec<WorkloadRecord>, NodeError> {
        self.current_executor_workloads(&query.executor_id)
    }

    fn provider_leases(
        &self,
        _source: &LocalAddress,
        query: &orion::control_plane::ProviderLeaseQuery,
    ) -> Result<Vec<orion::control_plane::LeaseRecord>, NodeError> {
        Ok(self.current_provider_leases(&query.provider_id))
    }

    pub(super) fn current_provider_leases(
        &self,
        provider_id: &ProviderId,
    ) -> Vec<orion::control_plane::LeaseRecord> {
        let store = self.store_read();
        let resource_ids: std::collections::BTreeSet<_> = store
            .desired
            .resources
            .values()
            .filter(|resource| &resource.provider_id == provider_id)
            .map(|resource| resource.resource_id.clone())
            .collect();
        store
            .desired
            .leases
            .values()
            .filter(|lease| resource_ids.contains(&lease.resource_id))
            .cloned()
            .collect()
    }

    pub(super) fn current_executor_workloads(
        &self,
        executor_id: &ExecutorId,
    ) -> Result<Vec<WorkloadRecord>, NodeError> {
        let store = self.store_read();
        let executor = store
            .desired
            .executors
            .get(executor_id)
            .cloned()
            .ok_or_else(|| NodeError::Storage(format!("unknown executor {}", executor_id)))?;
        let runtime_types = executor.runtime_types;
        Ok(store
            .desired
            .workloads
            .values()
            .filter(|workload| {
                workload.assigned_node_id.as_ref() == Some(&self.config.node_id)
                    && runtime_types.contains(&workload.runtime_type)
            })
            .cloned()
            .collect())
    }
}

#[cfg(feature = "transport-http")]
use super::desired_sync::merge_desired_cluster_state;
use super::{
    NodeApp, NodeError,
    desired_state::{diff_desired_cluster_state, merge_observed_state, merge_peer_observed_state},
};
use orion::{
    NodeId, ResourceId, Revision,
    control_plane::{
        ControlMessage, DesiredClusterState, MutationBatch, ObservedStateUpdate, StateSnapshot,
    },
    runtime::RuntimeError,
    transport::http::HttpResponsePayload,
};
use std::{collections::BTreeMap, sync::Arc};
use tracing::info_span;

impl NodeApp {
    #[cfg(any(test, feature = "transport-http"))]
    pub(super) async fn adopt_remote_snapshot_async(
        &self,
        snapshot: StateSnapshot,
    ) -> Result<(), NodeError> {
        if self.remote_desired_state_blocked() {
            return Err(NodeError::Authorization(
                "remote desired state adoption is blocked while maintenance isolation is active"
                    .into(),
            ));
        }
        let previous_revision = self.current_desired_revision();
        let remote_desired = snapshot.state.desired.clone();
        self.validate_desired_state(&remote_desired)?;
        let provider_records: Vec<_> = self
            .providers_read()
            .values()
            .map(|provider| provider.provider_record())
            .collect();
        let executor_records: Vec<_> = self
            .executors_read()
            .values()
            .map(|executor| executor.executor_record())
            .collect();

        self.commit_desired_state_update_async(previous_revision, |txn| {
            txn.replace_bundle(
                snapshot.state.desired,
                snapshot.state.observed,
                snapshot.state.applied,
                Vec::new(),
                remote_desired.clone(),
            );

            for provider in provider_records {
                txn.store().desired.put_provider(provider);
            }
            for executor in executor_records {
                txn.store().desired.put_executor(executor);
            }

            let local_delta = diff_desired_cluster_state(&remote_desired, &txn.store().desired);
            txn.replace_history(remote_desired.clone(), Some(local_delta))?;
            Ok(())
        })
        .await?;
        Ok(())
    }

    pub(super) fn adopt_remote_snapshot(&self, snapshot: StateSnapshot) -> Result<(), NodeError> {
        if self.remote_desired_state_blocked() {
            return Err(NodeError::Authorization(
                "remote desired state adoption is blocked while maintenance isolation is active"
                    .into(),
            ));
        }
        let previous_revision = self.current_desired_revision();
        let remote_desired = snapshot.state.desired.clone();
        self.validate_desired_state(&remote_desired)?;
        let provider_records: Vec<_> = self
            .providers_read()
            .values()
            .map(|provider| provider.provider_record())
            .collect();
        let executor_records: Vec<_> = self
            .executors_read()
            .values()
            .map(|executor| executor.executor_record())
            .collect();

        self.commit_desired_state_update(previous_revision, |txn| {
            txn.replace_bundle(
                snapshot.state.desired,
                snapshot.state.observed,
                snapshot.state.applied,
                Vec::new(),
                remote_desired.clone(),
            );

            for provider in provider_records {
                txn.store().desired.put_provider(provider);
            }
            for executor in executor_records {
                txn.store().desired.put_executor(executor);
            }

            let local_delta = diff_desired_cluster_state(&remote_desired, &txn.store().desired);
            txn.replace_history(remote_desired.clone(), Some(local_delta))?;
            Ok(())
        })?;
        Ok(())
    }

    #[cfg(feature = "transport-http")]
    pub(super) async fn apply_remote_mutations_async(
        &self,
        batch: &MutationBatch,
    ) -> Result<(), NodeError> {
        if self.remote_desired_state_blocked() {
            return Err(NodeError::Authorization(
                "remote desired state mutations are blocked while maintenance isolation is active"
                    .into(),
            ));
        }
        let started = std::time::Instant::now();
        let commit_result = {
            let _span = info_span!("mutation_apply", node = %self.config.node_id).entered();
            let previous_revision = self.current_desired_revision();
            let mut candidate = self.current_desired_state();
            if let Err(err) = batch.clone().apply_to_checked(&mut candidate) {
                let error = NodeError::from(err);
                self.record_mutation_apply_failure(started.elapsed(), &error);
                return Err(error);
            }
            if let Err(err) = self.validate_desired_state(&candidate) {
                self.record_mutation_apply_failure(started.elapsed(), &err);
                return Err(err);
            }
            let commit = self.commit_desired_state_update_async(previous_revision, |txn| {
                batch.clone().apply_to_checked(&mut txn.store().desired)?;
                txn.append_batch(batch.clone())?;
                Ok(())
            });
            drop(_span);
            commit.await
        };
        commit_result?;
        self.record_mutation_apply_success(started.elapsed());
        self.reconcile_after_change_async().await?;
        Ok(())
    }

    pub(super) fn apply_remote_mutations(&self, batch: &MutationBatch) -> Result<(), NodeError> {
        if self.remote_desired_state_blocked() {
            return Err(NodeError::Authorization(
                "remote desired state mutations are blocked while maintenance isolation is active"
                    .into(),
            ));
        }
        let _span = info_span!("mutation_apply", node = %self.config.node_id).entered();
        let started = std::time::Instant::now();
        let previous_revision = self.current_desired_revision();
        let mut candidate = self.current_desired_state();
        if let Err(err) = batch.clone().apply_to_checked(&mut candidate) {
            let error = NodeError::from(err);
            self.record_mutation_apply_failure(started.elapsed(), &error);
            return Err(error);
        }
        if let Err(err) = self.validate_desired_state(&candidate) {
            self.record_mutation_apply_failure(started.elapsed(), &err);
            return Err(err);
        }
        self.commit_desired_state_update(previous_revision, |txn| {
            batch.clone().apply_to_checked(&mut txn.store().desired)?;
            txn.append_batch(batch.clone())?;
            Ok(())
        })?;
        self.record_mutation_apply_success(started.elapsed());
        self.reconcile_after_change()?;
        Ok(())
    }

    pub(crate) fn apply_control_message(
        &self,
        message: ControlMessage,
    ) -> Result<HttpResponsePayload, NodeError> {
        match message {
            ControlMessage::Hello(_) => Ok(HttpResponsePayload::Hello(self.peer_hello()?)),
            ControlMessage::SyncRequest(request) => self.build_sync_response(request),
            ControlMessage::SyncSummaryRequest(request) => Ok(HttpResponsePayload::Summary(
                self.desired_state_summary_for_sections(&request.sections)?,
            )),
            ControlMessage::SyncDiffRequest(request) => self.build_sync_diff_response(&request),
            ControlMessage::QueryStateSnapshot => {
                Ok(HttpResponsePayload::Snapshot(self.state_snapshot()))
            }
            ControlMessage::Snapshot(snapshot) => {
                self.adopt_remote_snapshot(snapshot)?;
                Ok(HttpResponsePayload::Accepted)
            }
            ControlMessage::Mutations(batch) => {
                self.apply_remote_mutations(&batch)?;
                Ok(HttpResponsePayload::Accepted)
            }
            ControlMessage::QueryObservability => Ok(HttpResponsePayload::Observability(Box::new(
                self.observability_snapshot(),
            ))),
            ControlMessage::ClientHello(_)
            | ControlMessage::ClientWelcome(_)
            | ControlMessage::ProviderState(_)
            | ControlMessage::ExecutorState(_)
            | ControlMessage::QueryExecutorWorkloads(_)
            | ControlMessage::WatchExecutorWorkloads(_)
            | ControlMessage::ExecutorWorkloads(_)
            | ControlMessage::QueryProviderLeases(_)
            | ControlMessage::WatchProviderLeases(_)
            | ControlMessage::ProviderLeases(_)
            | ControlMessage::EnrollPeer(_)
            | ControlMessage::QueryPeerTrust
            | ControlMessage::PeerTrust(_)
            | ControlMessage::RevokePeer(_)
            | ControlMessage::ReplacePeerIdentity(_)
            | ControlMessage::RotateHttpTlsIdentity
            | ControlMessage::QueryMaintenance
            | ControlMessage::UpdateMaintenance(_)
            | ControlMessage::MaintenanceStatus(_)
            | ControlMessage::Observability(_)
            | ControlMessage::WatchState(_)
            | ControlMessage::PollClientEvents(_)
            | ControlMessage::ClientEvents(_)
            | ControlMessage::Ping
            | ControlMessage::Pong
            | ControlMessage::Accepted
            | ControlMessage::Rejected(_) => Err(NodeError::Storage(
                "local-only control message received on HTTP transport".into(),
            )),
        }
    }

    #[cfg(feature = "transport-http")]
    pub(super) async fn reconcile_conflicting_remote_snapshot(
        &self,
        node_id: &orion::NodeId,
        remote_snapshot: StateSnapshot,
        client: &orion::transport::http::HttpClient,
    ) -> Result<(), NodeError> {
        let local_snapshot = self.state_snapshot();
        let merged_desired = merge_desired_cluster_state(
            &local_snapshot.state.desired,
            &remote_snapshot.state.desired,
        )?;

        let merged_local = merged_desired != local_snapshot.state.desired;
        let merged_remote = merged_desired != remote_snapshot.state.desired;

        if merged_local {
            let mut merged_snapshot = local_snapshot.clone();
            merged_snapshot.state.desired = merged_desired.clone();
            self.adopt_remote_snapshot_async(merged_snapshot).await?;
        }

        if merged_remote {
            let mut merged_snapshot = remote_snapshot;
            merged_snapshot.state.desired = merged_desired;
            self.send_peer_http_request(
                node_id,
                client,
                orion::transport::http::HttpRequestPayload::Control(Box::new(
                    ControlMessage::Snapshot(merged_snapshot),
                )),
            )
            .await?;
        }

        Ok(())
    }

    pub(crate) fn replace_desired_tracked(
        &self,
        desired: DesiredClusterState,
    ) -> Result<(), NodeError> {
        let current = self.current_desired_state();
        let previous_revision = current.revision;
        let batch = diff_desired_cluster_state(&current, &desired);
        if batch.mutations.is_empty() {
            return Ok(());
        }
        self.validate_desired_state(&desired)?;
        self.commit_desired_state_update(previous_revision, |txn| {
            txn.replace_desired(desired);
            txn.append_batch(batch)?;
            Ok(())
        })?;
        Ok(())
    }

    fn validate_desired_state(&self, desired: &DesiredClusterState) -> Result<(), NodeError> {
        for workload in desired.workloads.values().filter(|workload| {
            workload.desired_state == orion::control_plane::DesiredState::Running
        }) {
            let Some(node_id) = workload.assigned_node_id.as_ref() else {
                continue;
            };

            if let Some(node) = desired.nodes.get(node_id)
                && !node.schedulable
            {
                return Err(NodeError::Config(format!(
                    "workload {} is assigned to unschedulable node {}",
                    workload.workload_id, node_id
                )));
            }
        }

        let executors = self.executors_read();
        let local_workloads = desired
            .workloads
            .values()
            .filter(|workload| workload.assigned_node_id.as_ref() == Some(&self.config.node_id))
            .filter(|workload| {
                let store = self.store_read();
                store.allows_local_workload(workload)
            });

        for workload in local_workloads {
            for executor in executors.values() {
                let executor_record = executor.executor_record();
                if executor_record.node_id == self.config.node_id
                    && executor_record
                        .runtime_types
                        .contains(&workload.runtime_type)
                {
                    executor.validate_workload(workload)?;
                }
            }
        }
        drop(executors);

        let providers = self.providers_read();
        let provider_snapshots = providers
            .values()
            .map(|provider| {
                let snapshot = provider.snapshot();
                (
                    snapshot.provider.provider_id.clone(),
                    (
                        Arc::clone(provider),
                        snapshot
                            .resources
                            .into_iter()
                            .filter(|resource| {
                                resource.availability
                                    == orion::control_plane::AvailabilityState::Available
                            })
                            .collect::<Vec<_>>(),
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();
        drop(providers);
        let executors = self.executors_read();
        let executor_resources = executors
            .values()
            .flat_map(|executor| executor.snapshot().resources.into_iter())
            .filter(|resource| {
                resource.availability == orion::control_plane::AvailabilityState::Available
            })
            .collect::<Vec<_>>();
        drop(executors);

        let mut local_resources = provider_snapshots
            .values()
            .flat_map(|(_, resources)| resources.iter().cloned())
            .collect::<Vec<_>>();
        local_resources.extend(executor_resources);
        local_resources.extend({
            let store = self.store_read();
            store.local_resources()
        });
        let local_resources = local_resources
            .into_iter()
            .map(|resource| (resource.resource_id.clone(), resource))
            .collect::<BTreeMap<_, _>>()
            .into_values()
            .collect::<Vec<_>>();
        let desired_local_resources = desired
            .resources
            .values()
            .filter(|resource| {
                desired
                    .providers
                    .get(&resource.provider_id)
                    .map(|provider| provider.node_id == self.config.node_id)
                    .unwrap_or(false)
                    || resource
                        .realized_by_executor_id
                        .as_ref()
                        .and_then(|executor_id| desired.executors.get(executor_id))
                        .map(|executor| executor.node_id == self.config.node_id)
                        .unwrap_or(false)
            })
            .map(|resource| (resource.resource_id.clone(), resource.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut claim_counts = BTreeMap::<ResourceId, u32>::new();

        for workload in desired.workloads.values().filter(|workload| {
            workload.desired_state == orion::control_plane::DesiredState::Running
                && workload.assigned_node_id.as_ref() == Some(&self.config.node_id)
                && {
                    let store = self.store_read();
                    store.allows_local_workload(workload)
                }
        }) {
            let mut available_resources = desired_local_resources.clone();
            for resource in &local_resources {
                available_resources.insert(resource.resource_id.clone(), resource.clone());
            }
            let mut remaining_bindings = workload.resource_bindings.clone();
            for requirement in &workload.requirements {
                for _ in 0..requirement.count {
                    let mut validation_error = None;
                    let mut selected_resource_id = None;

                    if let Some(binding_index) = remaining_bindings.iter().position(|binding| {
                        binding.node_id == self.config.node_id
                            && available_resources
                                .get(&binding.resource_id)
                                .map(|resource| resource.resource_type == requirement.resource_type)
                                .unwrap_or(false)
                    }) {
                        let binding = remaining_bindings.remove(binding_index);
                        let resource = available_resources
                            .get(&binding.resource_id)
                            .expect("binding resource existence checked above");
                        let existing_claims = claim_counts
                            .get(&resource.resource_id)
                            .copied()
                            .unwrap_or(0);
                        let validation = match provider_snapshots.get(&resource.provider_id) {
                            Some((provider, _)) => provider.validate_resource_claim(
                                resource,
                                &workload.workload_id,
                                requirement,
                                existing_claims,
                            ),
                            None => orion::runtime::validate_requirement_against_resource(
                                resource,
                                requirement,
                                existing_claims,
                            ),
                        };

                        match validation {
                            Ok(()) => {
                                *claim_counts
                                    .entry(resource.resource_id.clone())
                                    .or_default() += 1;
                                continue;
                            }
                            Err(err) => return Err(err.into()),
                        }
                    }

                    for resource in &local_resources {
                        if resource.resource_type != requirement.resource_type {
                            continue;
                        }
                        if let Some(expected_mode) = requirement.ownership_mode.as_ref()
                            && &resource.ownership_mode != expected_mode
                        {
                            continue;
                        }

                        let existing_claims = claim_counts
                            .get(&resource.resource_id)
                            .copied()
                            .unwrap_or(0);
                        let validation = match provider_snapshots.get(&resource.provider_id) {
                            Some((provider, _)) => provider.validate_resource_claim(
                                resource,
                                &workload.workload_id,
                                requirement,
                                existing_claims,
                            ),
                            None => orion::runtime::validate_requirement_against_resource(
                                resource,
                                requirement,
                                existing_claims,
                            ),
                        };

                        match validation {
                            Ok(()) => {
                                selected_resource_id = Some(resource.resource_id.clone());
                                break;
                            }
                            Err(err) => validation_error = Some(err),
                        }
                    }

                    if let Some(resource_id) = selected_resource_id {
                        *claim_counts.entry(resource_id).or_default() += 1;
                    } else if let Some(err) = validation_error {
                        return Err(err.into());
                    } else {
                        return Err(RuntimeError::UnsupportedResourceType(
                            requirement.resource_type.clone(),
                        )
                        .into());
                    }
                }
            }
        }

        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn validate_desired_state_for_test(
        &self,
        desired: &DesiredClusterState,
    ) -> Result<(), NodeError> {
        self.validate_desired_state(desired)
    }

    #[cfg(test)]
    pub(crate) async fn adopt_remote_snapshot_async_for_test(
        &self,
        snapshot: StateSnapshot,
    ) -> Result<(), NodeError> {
        self.adopt_remote_snapshot_async(snapshot).await
    }

    pub(super) fn mutation_batch_since(&self, base_revision: Revision) -> Option<MutationBatch> {
        let history = self.mutation_history_read();
        if let Some(full_replay) = self.with_desired_state_read(|desired| {
            (base_revision == Revision::ZERO
                && history.is_empty()
                && desired.revision > Revision::ZERO)
                .then(|| MutationBatch::full_state_replay(Revision::ZERO, desired))
        }) {
            return Some(full_replay);
        }

        let start = history
            .iter()
            .position(|batch| batch.base_revision == base_revision)?;
        let mut expected_base = base_revision;
        let mut mutations = Vec::new();

        for batch in history.iter().skip(start) {
            if batch.base_revision != expected_base {
                return None;
            }
            mutations.extend(batch.mutations.clone());
            expected_base = Revision::new(expected_base.get() + batch.mutations.len() as u64);
        }

        if expected_base == self.current_desired_revision() {
            Some(MutationBatch {
                base_revision,
                mutations,
            })
        } else {
            None
        }
    }

    #[cfg(test)]
    pub(crate) fn apply_observed_update(
        &self,
        update: ObservedStateUpdate,
    ) -> Result<HttpResponsePayload, NodeError> {
        self.apply_observed_update_from_peer(None, update)
    }

    pub(crate) fn apply_observed_update_from_peer(
        &self,
        peer_node_id: Option<&NodeId>,
        update: ObservedStateUpdate,
    ) -> Result<HttpResponsePayload, NodeError> {
        let mut state_changed = self.with_store_mut(|store| match peer_node_id {
            Some(peer_node_id) => merge_peer_observed_state(
                &mut store.observed,
                &store.desired,
                peer_node_id,
                update.observed,
            ),
            None => merge_observed_state(&mut store.observed, update.observed),
        });
        self.with_store_mut(|store| {
            if update.applied.revision > store.applied.revision {
                store.applied = update.applied;
                state_changed = true;
            }
        });
        if state_changed {
            let persisted = self.persist_state();
            self.request_reconcile();
            persisted?;
        }
        Ok(HttpResponsePayload::Accepted)
    }
}

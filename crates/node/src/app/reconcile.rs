use super::{
    NodeApp, NodeError,
    desired_state::{merge_observed_state, merge_peer_observed_state},
    desired_writes::WriteOrigin,
};
#[cfg(test)]
use orion::control_plane::StateSnapshot;
use orion::{
    NodeId, ResourceId,
    control_plane::{ControlMessage, DesiredClusterState, ObservedStateUpdate},
    runtime::RuntimeError,
    transport::http::HttpResponsePayload,
};
use std::{collections::BTreeMap, sync::Arc};

impl NodeApp {
    /// Serves a control message from a peer (HTTP or TCP peer surface). `peer` is the
    /// authenticated sender, when the request was signed.
    pub(crate) fn apply_control_message(
        &self,
        peer: Option<NodeId>,
        message: ControlMessage,
    ) -> Result<HttpResponsePayload, NodeError> {
        if let Some(peer) = peer.as_ref() {
            self.note_peer_heard(peer);
        }
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
                self.merge_peer_snapshot(peer, &snapshot)?;
                Ok(HttpResponsePayload::Accepted)
            }
            ControlMessage::Mutations(batch) => {
                self.apply_mutation_batch(&batch, WriteOrigin::Peer(peer))?;
                Ok(HttpResponsePayload::Accepted)
            }
            ControlMessage::QueryObservability => Ok(HttpResponsePayload::Observability(Box::new(
                self.observability_snapshot(),
            ))),
            ControlMessage::EnrollmentHello(hello) => Ok(HttpResponsePayload::EnrollmentChallenge(
                Box::new(self.answer_enrollment_hello(*hello)?),
            )),
            ControlMessage::EnrollmentConfirm(confirm) => {
                self.answer_enrollment_confirm(*confirm)?;
                Ok(HttpResponsePayload::Accepted)
            }
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
            | ControlMessage::PublishStatus(_)
            | ControlMessage::QueryStatus(_)
            | ControlMessage::WatchStatus(_)
            | ControlMessage::Status(_)
            | ControlMessage::Ping
            | ControlMessage::Pong
            | ControlMessage::Accepted
            | ControlMessage::Rejected(_)
            | ControlMessage::QueryDiscovery
            | ControlMessage::Discovery(_)
            | ControlMessage::EnrollDiscoveredPeer(_)
            | ControlMessage::RemovePeer(_)
            | ControlMessage::EnrollmentChallenge(_) => Err(NodeError::Storage(
                "local-only control message received on a peer transport".into(),
            )),
        }
    }

    pub(super) fn validate_desired_state(
        &self,
        desired: &DesiredClusterState,
    ) -> Result<(), NodeError> {
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
                    } else if self.remote_resource_could_satisfy(desired, requirement) {
                        // Cross-node binding resolves it against another node's resource.
                        continue;
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

    /// Whether a resource owned by another node matches `requirement` (type, ownership mode,
    /// capabilities), so a local workload may bind it across nodes (`docs/placement.md`).
    fn remote_resource_could_satisfy(
        &self,
        desired: &DesiredClusterState,
        requirement: &orion::control_plane::WorkloadRequirement,
    ) -> bool {
        let store = self.store_read();
        orion::cluster::leases::known_resources(desired, &store.observed).any(|resource| {
            orion::cluster::leases::resource_matches(resource, requirement)
                && orion::cluster::resource_host(desired, resource)
                    .is_some_and(|owner| owner != self.config.node_id)
        })
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
        self.merge_peer_snapshot_async(None, &snapshot)
            .await
            .map(|_| ())
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
        if let Some(peer) = peer_node_id {
            self.note_peer_heard(peer);
        }
        let mut state_changed = self.with_store_mut(|store| match peer_node_id {
            Some(peer_node_id) => merge_peer_observed_state(
                &mut store.observed,
                &store.desired,
                peer_node_id,
                update.observed,
            ),
            None => merge_observed_state(&mut store.observed, update.observed),
        });
        // A peer's applied revision counts its own commits; it means nothing here.
        self.with_store_mut(|store| {
            if peer_node_id.is_none() && update.applied.revision > store.applied.revision {
                store.applied = update.applied;
                state_changed = true;
            }
        });
        if state_changed {
            let persisted = self.persist_observed_state();
            self.request_reconcile();
            persisted?;
        }
        Ok(HttpResponsePayload::Accepted)
    }
}

//! Consumer-oriented views over control-plane records.
//!
//! Executor and provider apps usually only need a narrow slice of the cluster state: which
//! workloads are assigned to them, how to decode each workload's typed config, and which resources
//! those workloads are bound to. The helpers here derive that slice from a [`StateSnapshot`] (or
//! from the workload lists delivered by the executor watch) so apps do not have to walk
//! [`ClusterStateEnvelope`](orion_control_plane::ClusterStateEnvelope) sections by hand.

use std::collections::BTreeMap;

use orion_control_plane::{
    AvailabilityState, ConfigDecodeError, ConfigMapRef, DesiredState, ExecutorRecord, HealthState,
    LeaseState, RemoteBinding, ResourceBinding, ResourceCapability, ResourceEndpoint,
    ResourceEndpointError, ResourceOwnershipMode, ResourceRecord, ResourceState, StateSnapshot,
    TypedConfigValue, TypedResourceEndpoint, WorkloadObservedState, WorkloadPlacement,
    WorkloadRecord, WorkloadRequirement, deserialize_config,
};
use orion_core::{
    ArtifactId, ConfigSchemaId, ExecutorId, NodeId, ProviderId, ResourceId, ResourceType, Revision,
    RuntimeType, WorkloadId,
};
use serde::de::DeserializeOwned;

static EMPTY_CONFIG: BTreeMap<String, TypedConfigValue> = BTreeMap::new();

/// A workload assigned to a local executor, with typed config decoding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AssignedWorkload {
    record: WorkloadRecord,
    desired_revision: Option<Revision>,
}

impl AssignedWorkload {
    /// Wraps a workload record. `desired_revision` is the desired-state revision the record was
    /// read from, when known.
    pub fn new(record: WorkloadRecord, desired_revision: Option<Revision>) -> Self {
        Self {
            record,
            desired_revision,
        }
    }

    pub fn workload_id(&self) -> &WorkloadId {
        &self.record.workload_id
    }

    /// Desired-state revision this view was derived from.
    ///
    /// `Some` when derived from a [`StateSnapshot`]; `None` when derived from an executor watch
    /// event, which carries workload records without a cluster revision.
    pub fn desired_revision(&self) -> Option<Revision> {
        self.desired_revision
    }

    pub fn runtime_type(&self) -> &RuntimeType {
        &self.record.runtime_type
    }

    pub fn artifact_id(&self) -> &ArtifactId {
        &self.record.artifact_id
    }

    pub fn desired_state(&self) -> DesiredState {
        self.record.desired_state
    }

    pub fn observed_state(&self) -> WorkloadObservedState {
        self.record.observed_state
    }

    pub fn assigned_node_id(&self) -> Option<&NodeId> {
        self.record.assigned_node_id.as_ref()
    }

    pub fn requirements(&self) -> &[WorkloadRequirement] {
        &self.record.requirements
    }

    pub fn resource_bindings(&self) -> &[ResourceBinding] {
        &self.record.resource_bindings
    }

    /// Cross-node bindings: resources owned by another node, reached over their own endpoints.
    pub fn remote_bindings(&self) -> impl Iterator<Item = &ResourceBinding> {
        self.record
            .resource_bindings
            .iter()
            .filter(|binding| binding.is_remote())
    }

    /// Placement constraints, when the placement engine manages this workload.
    pub fn placement(&self) -> Option<&WorkloadPlacement> {
        self.record.placement.as_ref()
    }

    /// `true` when a user assigned the node explicitly (placement never moves it).
    pub fn has_explicit_assignment(&self) -> bool {
        self.record.has_explicit_assignment()
    }

    /// Ids of the resources this workload is bound to, in binding order.
    pub fn bound_resource_ids(&self) -> impl Iterator<Item = &ResourceId> {
        self.record
            .resource_bindings
            .iter()
            .map(|binding| &binding.resource_id)
    }

    pub fn has_config(&self) -> bool {
        self.record.config.is_some()
    }

    pub fn config_schema_id(&self) -> Option<&ConfigSchemaId> {
        self.record.config.as_ref().map(|config| &config.schema_id)
    }

    /// Raw typed config payload; empty when the workload carries no config.
    pub fn config_payload(&self) -> &BTreeMap<String, TypedConfigValue> {
        self.record
            .config
            .as_ref()
            .map_or(&EMPTY_CONFIG, |config| &config.payload)
    }

    /// Field-level accessor over the config payload.
    pub fn config_view(&self) -> ConfigMapRef<'_> {
        ConfigMapRef::new(self.config_payload())
    }

    /// Decodes the workload config into `T` using [`deserialize_config`].
    ///
    /// A workload without config decodes from an empty payload, so `T` must tolerate missing
    /// fields (for example via `#[serde(default)]`) for that case to succeed.
    pub fn config<T>(&self) -> Result<T, ConfigDecodeError>
    where
        T: DeserializeOwned,
    {
        deserialize_config(self.config_payload())
    }

    /// Resolves this workload's bindings against `snapshot`. See [`resources_bound_to`].
    pub fn bound_resources(&self, snapshot: &StateSnapshot) -> Vec<BoundResource> {
        bound_resources_for_bindings(snapshot, &self.record.resource_bindings)
    }

    pub fn record(&self) -> &WorkloadRecord {
        &self.record
    }

    pub fn into_record(self) -> WorkloadRecord {
        self.record
    }
}

/// A resource bound to a workload, together with the node the binding targets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundResource {
    record: ResourceRecord,
    bound_node_id: NodeId,
    remote: Option<RemoteBinding>,
}

impl BoundResource {
    pub fn new(record: ResourceRecord, bound_node_id: impl Into<NodeId>) -> Self {
        Self {
            record,
            bound_node_id: bound_node_id.into(),
            remote: None,
        }
    }

    /// A bound resource described by its `binding` (cross-node details included).
    pub fn from_binding(record: ResourceRecord, binding: &ResourceBinding) -> Self {
        Self {
            record,
            bound_node_id: binding.node_id.clone(),
            remote: binding.remote.clone(),
        }
    }

    /// `true` for a cross-node binding: the resource is owned by [`Self::bound_node_id`], another
    /// node, and is reached over its own endpoints (Orion does not proxy the data).
    pub fn is_remote(&self) -> bool {
        self.remote.is_some()
    }

    /// `false` while a cross-node binding's owner is unreachable (or reports the resource
    /// unavailable); local bindings follow the resource's availability.
    pub fn is_available(&self) -> bool {
        match self.remote.as_ref() {
            Some(remote) => remote.available,
            None => self.record.availability == AvailabilityState::Available,
        }
    }

    pub fn resource_id(&self) -> &ResourceId {
        &self.record.resource_id
    }

    pub fn resource_type(&self) -> &ResourceType {
        &self.record.resource_type
    }

    pub fn provider_id(&self) -> &ProviderId {
        &self.record.provider_id
    }

    /// Node named by the workload's [`ResourceBinding`].
    pub fn bound_node_id(&self) -> &NodeId {
        &self.bound_node_id
    }

    pub fn realized_by_executor_id(&self) -> Option<&ExecutorId> {
        self.record.realized_by_executor_id.as_ref()
    }

    pub fn ownership_mode(&self) -> &ResourceOwnershipMode {
        &self.record.ownership_mode
    }

    pub fn health(&self) -> HealthState {
        self.record.health
    }

    pub fn availability(&self) -> AvailabilityState {
        self.record.availability
    }

    pub fn lease_state(&self) -> LeaseState {
        self.record.lease_state
    }

    pub fn capabilities(&self) -> &[ResourceCapability] {
        &self.record.capabilities
    }

    pub fn labels(&self) -> &[String] {
        &self.record.labels
    }

    pub fn has_label(&self, label: &str) -> bool {
        self.record
            .labels
            .iter()
            .any(|candidate| candidate == label)
    }

    /// Raw endpoint strings as published by the provider.
    pub fn raw_endpoints(&self) -> &[String] {
        &self.record.endpoints
    }

    /// Parsed endpoints; fails if any published endpoint is malformed.
    pub fn endpoints(&self) -> Result<Vec<ResourceEndpoint>, ResourceEndpointError> {
        self.record.parsed_endpoints()
    }

    /// First endpoint of type `T` (for example `UnixEndpoint` or `TcpEndpoint`).
    pub fn endpoint<T>(&self) -> Result<T, ResourceEndpointError>
    where
        T: TypedResourceEndpoint,
    {
        self.record.endpoint::<T>()
    }

    pub fn state(&self) -> Option<&ResourceState> {
        self.record.state.as_ref()
    }

    pub fn record(&self) -> &ResourceRecord {
        &self.record
    }

    pub fn into_record(self) -> ResourceRecord {
        self.record
    }
}

/// Returns true when `workload` is assigned to `executor`.
///
/// Mirrors the daemon's executor assignment rule: the workload is placed on the executor's node
/// and its runtime type is one the executor declares.
pub fn is_assigned_to(workload: &WorkloadRecord, executor: &ExecutorRecord) -> bool {
    workload.assigned_node_id.as_ref() == Some(&executor.node_id)
        && executor.runtime_types.contains(&workload.runtime_type)
}

/// Workloads in the snapshot's desired state that are assigned to `executor`, ordered by id.
pub fn assigned_workloads(
    snapshot: &StateSnapshot,
    executor: &ExecutorRecord,
) -> Vec<AssignedWorkload> {
    let desired = &snapshot.state.desired;
    desired
        .workloads
        .values()
        .filter(|workload| is_assigned_to(workload, executor))
        .map(|workload| AssignedWorkload::new(workload.clone(), Some(desired.revision)))
        .collect()
}

/// Like [`assigned_workloads`], resolving the executor record from the snapshot's desired state.
///
/// Returns `None` when the executor is not registered in the snapshot.
pub fn assigned_workloads_for_executor(
    snapshot: &StateSnapshot,
    executor_id: &ExecutorId,
) -> Option<Vec<AssignedWorkload>> {
    snapshot
        .state
        .desired
        .executors
        .get(executor_id)
        .map(|executor| assigned_workloads(snapshot, executor))
}

/// Wraps workload records delivered by the executor watch/query APIs, which the daemon has
/// already filtered to the executor's assignments.
pub fn assigned_workloads_from_records<I>(workloads: I) -> Vec<AssignedWorkload>
where
    I: IntoIterator<Item = WorkloadRecord>,
{
    workloads
        .into_iter()
        .map(|record| AssignedWorkload::new(record, None))
        .collect()
}

/// Resources bound to `workload_id` in the snapshot's desired state, in binding order.
///
/// Each binding is resolved against desired resources first, then observed resources. Bindings
/// whose resource record is not present in either section are skipped. Returns an empty list when
/// the workload is unknown.
pub fn resources_bound_to(
    snapshot: &StateSnapshot,
    workload_id: &WorkloadId,
) -> Vec<BoundResource> {
    snapshot
        .state
        .desired
        .workloads
        .get(workload_id)
        .map(|workload| bound_resources_for_bindings(snapshot, &workload.resource_bindings))
        .unwrap_or_default()
}

fn bound_resources_for_bindings(
    snapshot: &StateSnapshot,
    bindings: &[ResourceBinding],
) -> Vec<BoundResource> {
    let state = &snapshot.state;
    bindings
        .iter()
        .filter_map(|binding| {
            state
                .desired
                .resources
                .get(&binding.resource_id)
                .or_else(|| state.observed.resources.get(&binding.resource_id))
                .map(|record| BoundResource::from_binding(record.clone(), binding))
        })
        .collect()
}

//! One-stop imports for executor and provider apps.
//!
//! `use orion_client::prelude::*;` brings in the client API together with the core identifiers
//! and control-plane records those apps commonly handle, so they can depend on `orion-client`
//! alone.

#[cfg(feature = "ipc")]
pub use crate::{
    ActionReporter, ActionRequestWatch, ActionWatch, AssignedWorkloadWatch,
    AssignedWorkloadsUpdate, ClientIdentity, ClientSession, ControlPlaneClient,
    ControlPlaneEventStream, ExecutorApp, ExecutorClient, ExecutorEventStream,
    LocalControlPlaneClient, LocalExecutorApp, LocalExecutorClient, LocalExecutorEvent,
    LocalExecutorService, LocalExecutorSubscription, LocalNodeRuntime, LocalProviderApp,
    LocalProviderClient, LocalProviderEvent, LocalProviderService, LocalProviderSubscription,
    LocalRuntimePublisher, LocalRuntimePublisherBuilder, LocalServiceRetryPolicy, ProviderApp,
    ProviderClient, ProviderEventStream, SessionConfig, StatusWatch,
};
pub use crate::{
    AssignedWorkload, BoundResource, ClientError, ClientRole, DerivedResource, ProviderResource,
    ResourceClaim, assigned_workloads, assigned_workloads_for_executor,
    assigned_workloads_from_records, is_assigned_to, resources_bound_to,
};
pub use orion_control_plane::{
    ActionQuery, ActionReport, ActionRequest, ActionResult, ActionState, ActionTarget,
    action_names, action_status_keys,
};
pub use orion_control_plane::{
    AppliedClusterState, AvailabilityState, ClusterStateEnvelope, ConfigDecodeError, ConfigMapRef,
    CustomEndpoint, CustomEndpointScheme, DesiredClusterState, DesiredState, ExecutorRecord,
    HealthState, HttpEndpoint, IpcEndpoint, LabelRequirement, LeaseHolder, LeaseRecord, LeaseState,
    ObservedClusterState, PlacementReason, ProviderRecord, RemoteBinding, ResourceBinding,
    ResourceCapability, ResourceEndpoint, ResourceEndpointError, ResourceOwnershipMode,
    ResourceRecord, ResourceState, RestartPolicy, SharedMemoryEndpoint, StateSnapshot,
    StatusChange, StatusEntry, StatusKey, StatusQuery, StatusSubject, TcpEndpoint,
    TypedConfigValue, TypedResourceEndpoint, UnixEndpoint, WorkloadConfig, WorkloadObservedState,
    WorkloadPlacement, WorkloadRecord, WorkloadRequirement, config_json_value, deserialize_config,
};
pub use orion_core::{
    ArtifactId, CapabilityId, ConfigSchemaId, ExecutorId, NodeId, ProviderId, ResourceId,
    ResourceType, Revision, RuntimeType, WorkloadId,
};

mod clock;
mod cluster;
mod config_decode;
mod host;
mod inventory;
mod placement;
mod resource_endpoints;
mod resources;
mod updates;
mod versions;
mod workloads;

pub use clock::{ClockSourceKind, NodeClockFacts};
pub use cluster::{
    AppliedClusterState, ClusterStateEnvelope, DesiredClusterState, ObservedClusterState,
};
pub use config_decode::{ConfigDecodeError, ConfigMapRef, config_json_value, deserialize_config};
pub use host::{
    HostFacts, HostFactsSource, HostMetricsSample, HostTemperature, NodeHostFacts,
    PeerHostMetricsSnapshot, ReplicatedHostMetrics,
};
pub use inventory::{
    ArtifactRecord, ArtifactRecordBuilder, ExecutorRecord, ExecutorRecordBuilder, NodeRecord,
    NodeRecordBuilder, ProviderRecord, ProviderRecordBuilder,
};
pub use placement::{
    LabelRequirement, LeaseHolder, PlacementDecision, PlacementReason, RemoteBinding,
    WorkloadPlacement, parse_node_labels, split_label,
};
pub use resource_endpoints::{
    BUILTIN_ENDPOINT_SCHEMES, CustomEndpoint, CustomEndpointScheme, HttpEndpoint, IpcEndpoint,
    ResourceEndpoint, ResourceEndpointError, SharedMemoryEndpoint, TcpEndpoint,
    TypedResourceEndpoint, UnixEndpoint, is_valid_endpoint_scheme,
};
pub use resources::{
    LeaseRecord, LeaseRecordBuilder, ResourceActionResult, ResourceActionStatus,
    ResourceCapability, ResourceConfigState, ResourceOwnershipMode, ResourceRecord,
    ResourceRecordBuilder, ResourceState,
};
pub use updates::{
    DrainPolicy, MAX_UPDATE_ARG_NAME_LEN, MAX_UPDATE_ARGS, MAX_UPDATE_COMPONENT_LEN,
    MAX_UPDATE_DIGEST_LEN, MAX_UPDATE_DRAIN_TIMEOUT_MS, MAX_UPDATE_ERROR_LEN,
    MAX_UPDATE_HANDLER_NAME_LEN, MAX_UPDATE_VERSION_LEN, UpdateActing, UpdateHandler,
    UpdateIntentRecord, UpdateIntentView, UpdatePhase, UpdateStatusRecord, update_intent_id,
    update_intent_views,
};
pub use versions::{DesiredObjectKey, DesiredObjectStamps, DesiredObjectVersion};
pub use workloads::{
    ResourceBinding, TypedConfigValue, WorkloadConfig, WorkloadRecord, WorkloadRecordBuilder,
    WorkloadRequirement,
};

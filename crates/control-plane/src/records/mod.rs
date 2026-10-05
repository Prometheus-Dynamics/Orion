mod clock;
mod cluster;
mod config_decode;
mod host;
mod inventory;
mod placement;
mod resource_endpoints;
mod resources;
mod versions;
mod workloads;

pub use clock::{ClockSourceKind, NodeClockFacts};
pub use cluster::{
    AppliedClusterState, ClusterStateEnvelope, DesiredClusterState, ObservedClusterState,
};
pub use config_decode::{ConfigDecodeError, ConfigMapRef, config_json_value, deserialize_config};
pub use host::{HostFacts, HostMetricsSample, HostTemperature, NodeHostFacts};
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
pub use versions::{DesiredObjectKey, DesiredObjectStamps, DesiredObjectVersion};
pub use workloads::{
    ResourceBinding, TypedConfigValue, WorkloadConfig, WorkloadRecord, WorkloadRecordBuilder,
    WorkloadRequirement,
};

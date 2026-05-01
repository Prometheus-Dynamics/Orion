use crate::{AvailabilityState, HealthState, LeaseState};
use orion_core::{
    CapabilityDef, CapabilityId, ExecutorId, NodeId, ProviderId, ResourceId, ResourceType,
    ResourceTypeDef, WorkloadId,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fs, io, path::PathBuf};
use thiserror::Error;

use super::config_decode::ConfigMapRef;
use super::workloads::TypedConfigValue;

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum ResourceOwnershipMode {
    Exclusive,
    SharedRead,
    SharedLimited { max_consumers: u32 },
    ExclusiveOwnerPublishesDerived,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ResourceCapability {
    pub capability_id: CapabilityId,
    pub detail: Option<String>,
}

impl ResourceCapability {
    pub fn new(capability_id: impl Into<CapabilityId>) -> Self {
        Self {
            capability_id: capability_id.into(),
            detail: None,
        }
    }

    pub fn of<T: CapabilityDef>() -> Self {
        Self::new(CapabilityId::of::<T>())
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum ResourceActionStatus {
    Applied,
    Read,
    Failed,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ResourceActionResult {
    pub action_kind: String,
    pub status: ResourceActionStatus,
    pub data: Option<TypedConfigValue>,
    pub error: Option<String>,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ResourceConfigState {
    pub payload: BTreeMap<String, TypedConfigValue>,
}

impl Default for ResourceConfigState {
    fn default() -> Self {
        Self::new()
    }
}

impl ResourceConfigState {
    pub fn new() -> Self {
        Self {
            payload: BTreeMap::new(),
        }
    }

    pub fn field(mut self, key: impl Into<String>, value: TypedConfigValue) -> Self {
        self.payload.insert(key.into(), value);
        self
    }

    pub fn view(&self) -> ConfigMapRef<'_> {
        ConfigMapRef::new(&self.payload)
    }
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ResourceState {
    pub observed_at_ms: u64,
    pub action_result: Option<ResourceActionResult>,
    pub config: Option<ResourceConfigState>,
}

impl ResourceState {
    pub fn new(observed_at_ms: u64) -> Self {
        Self {
            observed_at_ms,
            action_result: None,
            config: None,
        }
    }

    pub fn with_action_result(mut self, action_result: ResourceActionResult) -> Self {
        self.action_result = Some(action_result);
        self
    }

    pub fn with_config(mut self, config: ResourceConfigState) -> Self {
        self.config = Some(config);
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SharedMemoryEndpoint {
    pub name: String,
}

impl SharedMemoryEndpoint {
    pub fn path(&self) -> PathBuf {
        self.path_in(Self::default_root())
    }

    pub fn path_in(&self, root: impl Into<PathBuf>) -> PathBuf {
        root.into().join(&self.name)
    }

    pub fn read_bytes(&self) -> Result<Vec<u8>, io::Error> {
        self.read_bytes_from(Self::default_root())
    }

    pub fn read_bytes_from(&self, root: impl Into<PathBuf>) -> Result<Vec<u8>, io::Error> {
        fs::read(self.path_in(root))
    }

    pub fn read_string(&self) -> Result<String, io::Error> {
        self.read_string_from(Self::default_root())
    }

    pub fn read_string_from(&self, root: impl Into<PathBuf>) -> Result<String, io::Error> {
        fs::read_to_string(self.path_in(root))
    }

    fn default_root() -> PathBuf {
        std::env::var_os("ORION_SHM_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/dev/shm"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IpcEndpoint {
    pub address: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnixEndpoint {
    pub path: String,
}

impl UnixEndpoint {
    pub fn path_buf(&self) -> PathBuf {
        PathBuf::from(&self.path)
    }

    pub fn read_bytes(&self) -> Result<Vec<u8>, io::Error> {
        fs::read(self.path_buf())
    }

    pub fn read_string(&self) -> Result<String, io::Error> {
        fs::read_to_string(self.path_buf())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TcpEndpoint {
    pub address: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpEndpoint {
    pub url: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResourceEndpoint {
    SharedMemory(SharedMemoryEndpoint),
    Ipc(IpcEndpoint),
    Unix(UnixEndpoint),
    Tcp(TcpEndpoint),
    Http(HttpEndpoint),
    Https(HttpEndpoint),
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ResourceEndpointError {
    #[error("resource endpoint `{endpoint}` is missing a scheme")]
    MissingScheme { endpoint: String },
    #[error("resource endpoint `{endpoint}` has an empty payload")]
    EmptyPayload { endpoint: String },
    #[error("resource endpoint `{endpoint}` uses unsupported scheme `{scheme}`")]
    UnsupportedScheme { endpoint: String, scheme: String },
    #[error("resource `{resource_id}` has no {endpoint_type} endpoint")]
    EndpointTypeNotFound {
        resource_id: ResourceId,
        endpoint_type: &'static str,
    },
}

pub trait TypedResourceEndpoint: Sized {
    const TYPE_NAME: &'static str;

    fn from_endpoint(endpoint: &ResourceEndpoint) -> Option<Self>;
}

impl ResourceEndpoint {
    pub fn parse(endpoint: impl AsRef<str>) -> Result<Self, ResourceEndpointError> {
        let endpoint = endpoint.as_ref();
        let (scheme, payload) =
            endpoint
                .split_once("://")
                .ok_or_else(|| ResourceEndpointError::MissingScheme {
                    endpoint: endpoint.to_owned(),
                })?;
        if payload.is_empty() {
            return Err(ResourceEndpointError::EmptyPayload {
                endpoint: endpoint.to_owned(),
            });
        }

        match scheme {
            "shm" => Ok(Self::SharedMemory(SharedMemoryEndpoint {
                name: payload.to_owned(),
            })),
            "ipc" => Ok(Self::Ipc(IpcEndpoint {
                address: payload.to_owned(),
            })),
            "unix" => Ok(Self::Unix(UnixEndpoint {
                path: payload.to_owned(),
            })),
            "tcp" => Ok(Self::Tcp(TcpEndpoint {
                address: payload.to_owned(),
            })),
            "http" => Ok(Self::Http(HttpEndpoint {
                url: endpoint.to_owned(),
            })),
            "https" => Ok(Self::Https(HttpEndpoint {
                url: endpoint.to_owned(),
            })),
            _ => Err(ResourceEndpointError::UnsupportedScheme {
                endpoint: endpoint.to_owned(),
                scheme: scheme.to_owned(),
            }),
        }
    }
}

impl TypedResourceEndpoint for SharedMemoryEndpoint {
    const TYPE_NAME: &'static str = "shared memory";

    fn from_endpoint(endpoint: &ResourceEndpoint) -> Option<Self> {
        match endpoint {
            ResourceEndpoint::SharedMemory(endpoint) => Some(endpoint.clone()),
            _ => None,
        }
    }
}

impl TypedResourceEndpoint for IpcEndpoint {
    const TYPE_NAME: &'static str = "ipc";

    fn from_endpoint(endpoint: &ResourceEndpoint) -> Option<Self> {
        match endpoint {
            ResourceEndpoint::Ipc(endpoint) => Some(endpoint.clone()),
            _ => None,
        }
    }
}

impl TypedResourceEndpoint for UnixEndpoint {
    const TYPE_NAME: &'static str = "unix";

    fn from_endpoint(endpoint: &ResourceEndpoint) -> Option<Self> {
        match endpoint {
            ResourceEndpoint::Unix(endpoint) => Some(endpoint.clone()),
            _ => None,
        }
    }
}

impl TypedResourceEndpoint for TcpEndpoint {
    const TYPE_NAME: &'static str = "tcp";

    fn from_endpoint(endpoint: &ResourceEndpoint) -> Option<Self> {
        match endpoint {
            ResourceEndpoint::Tcp(endpoint) => Some(endpoint.clone()),
            _ => None,
        }
    }
}

impl TypedResourceEndpoint for HttpEndpoint {
    const TYPE_NAME: &'static str = "http/https";

    fn from_endpoint(endpoint: &ResourceEndpoint) -> Option<Self> {
        match endpoint {
            ResourceEndpoint::Http(endpoint) | ResourceEndpoint::Https(endpoint) => {
                Some(endpoint.clone())
            }
            _ => None,
        }
    }
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ResourceRecord {
    pub resource_id: ResourceId,
    pub resource_type: ResourceType,
    pub provider_id: ProviderId,
    pub realized_by_executor_id: Option<ExecutorId>,
    pub ownership_mode: ResourceOwnershipMode,
    pub realized_for_workload_id: Option<WorkloadId>,
    pub source_resource_id: Option<ResourceId>,
    pub source_workload_id: Option<WorkloadId>,
    pub health: HealthState,
    pub availability: AvailabilityState,
    pub lease_state: LeaseState,
    pub capabilities: Vec<ResourceCapability>,
    pub labels: Vec<String>,
    pub endpoints: Vec<String>,
    pub state: Option<ResourceState>,
}

impl ResourceRecord {
    pub fn builder(
        resource_id: impl Into<ResourceId>,
        resource_type: impl Into<ResourceType>,
        provider_id: impl Into<ProviderId>,
    ) -> ResourceRecordBuilder {
        ResourceRecordBuilder {
            resource_id: resource_id.into(),
            resource_type: resource_type.into(),
            provider_id: provider_id.into(),
            realized_by_executor_id: None,
            ownership_mode: ResourceOwnershipMode::Exclusive,
            realized_for_workload_id: None,
            source_resource_id: None,
            source_workload_id: None,
            health: HealthState::Unknown,
            availability: AvailabilityState::Unknown,
            lease_state: LeaseState::Unleased,
            capabilities: Vec::new(),
            labels: Vec::new(),
            endpoints: Vec::new(),
            state: None,
        }
    }

    pub fn parsed_endpoints(&self) -> Result<Vec<ResourceEndpoint>, ResourceEndpointError> {
        self.endpoints
            .iter()
            .map(ResourceEndpoint::parse)
            .collect::<Result<Vec<_>, _>>()
    }

    pub fn endpoint<T>(&self) -> Result<T, ResourceEndpointError>
    where
        T: TypedResourceEndpoint,
    {
        let endpoints = self.parsed_endpoints()?;
        endpoints.iter().find_map(T::from_endpoint).ok_or_else(|| {
            ResourceEndpointError::EndpointTypeNotFound {
                resource_id: self.resource_id.clone(),
                endpoint_type: T::TYPE_NAME,
            }
        })
    }
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct LeaseRecord {
    pub resource_id: ResourceId,
    pub lease_state: LeaseState,
    pub holder_node_id: Option<NodeId>,
    pub holder_workload_id: Option<WorkloadId>,
}

impl LeaseRecord {
    pub fn builder(resource_id: impl Into<ResourceId>) -> LeaseRecordBuilder {
        LeaseRecordBuilder {
            resource_id: resource_id.into(),
            lease_state: LeaseState::Unleased,
            holder_node_id: None,
            holder_workload_id: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceRecordBuilder {
    resource_id: ResourceId,
    resource_type: ResourceType,
    provider_id: ProviderId,
    realized_by_executor_id: Option<ExecutorId>,
    ownership_mode: ResourceOwnershipMode,
    realized_for_workload_id: Option<WorkloadId>,
    source_resource_id: Option<ResourceId>,
    source_workload_id: Option<WorkloadId>,
    health: HealthState,
    availability: AvailabilityState,
    lease_state: LeaseState,
    capabilities: Vec<ResourceCapability>,
    labels: Vec<String>,
    endpoints: Vec<String>,
    state: Option<ResourceState>,
}

impl ResourceRecordBuilder {
    pub fn with_type<T: ResourceTypeDef>(mut self) -> Self {
        self.resource_type = ResourceType::of::<T>();
        self
    }

    pub fn health(mut self, health: HealthState) -> Self {
        self.health = health;
        self
    }

    pub fn ownership_mode(mut self, ownership_mode: ResourceOwnershipMode) -> Self {
        self.ownership_mode = ownership_mode;
        self
    }

    pub fn realized_by_executor(mut self, executor_id: impl Into<ExecutorId>) -> Self {
        self.realized_by_executor_id = Some(executor_id.into());
        self
    }

    pub fn realized_for_workload(mut self, workload_id: impl Into<WorkloadId>) -> Self {
        self.realized_for_workload_id = Some(workload_id.into());
        self
    }

    pub fn source_resource(mut self, resource_id: impl Into<ResourceId>) -> Self {
        self.source_resource_id = Some(resource_id.into());
        self
    }

    pub fn source_workload(mut self, workload_id: impl Into<WorkloadId>) -> Self {
        self.source_workload_id = Some(workload_id.into());
        self
    }

    pub fn availability(mut self, availability: AvailabilityState) -> Self {
        self.availability = availability;
        self
    }

    pub fn lease_state(mut self, lease_state: LeaseState) -> Self {
        self.lease_state = lease_state;
        self
    }

    pub fn capability(mut self, capability: ResourceCapability) -> Self {
        self.capabilities.push(capability);
        self
    }

    pub fn supports_capability(mut self, capability_id: impl Into<CapabilityId>) -> Self {
        self.capabilities
            .push(ResourceCapability::new(capability_id));
        self
    }

    pub fn supports_capability_of<T: CapabilityDef>(mut self) -> Self {
        self.capabilities.push(ResourceCapability::of::<T>());
        self
    }

    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.labels.push(label.into());
        self
    }

    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoints.push(endpoint.into());
        self
    }

    pub fn state(mut self, state: ResourceState) -> Self {
        self.state = Some(state);
        self
    }

    pub fn build(self) -> ResourceRecord {
        ResourceRecord {
            resource_id: self.resource_id,
            resource_type: self.resource_type,
            provider_id: self.provider_id,
            realized_by_executor_id: self.realized_by_executor_id,
            ownership_mode: self.ownership_mode,
            realized_for_workload_id: self.realized_for_workload_id,
            source_resource_id: self.source_resource_id,
            source_workload_id: self.source_workload_id,
            health: self.health,
            availability: self.availability,
            lease_state: self.lease_state,
            capabilities: self.capabilities,
            labels: self.labels,
            endpoints: self.endpoints,
            state: self.state,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseRecordBuilder {
    resource_id: ResourceId,
    lease_state: LeaseState,
    holder_node_id: Option<NodeId>,
    holder_workload_id: Option<WorkloadId>,
}

impl LeaseRecordBuilder {
    pub fn lease_state(mut self, lease_state: LeaseState) -> Self {
        self.lease_state = lease_state;
        self
    }

    pub fn holder_node(mut self, node_id: impl Into<NodeId>) -> Self {
        self.holder_node_id = Some(node_id.into());
        self
    }

    pub fn holder_workload(mut self, workload_id: impl Into<WorkloadId>) -> Self {
        self.holder_workload_id = Some(workload_id.into());
        self
    }

    pub fn build(self) -> LeaseRecord {
        LeaseRecord {
            resource_id: self.resource_id,
            lease_state: self.lease_state,
            holder_node_id: self.holder_node_id,
            holder_workload_id: self.holder_workload_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_record_parses_typed_endpoints() {
        let resource = ResourceRecord::builder(
            ResourceId::new("resource.graph"),
            ResourceType::new("graph.payload"),
            ProviderId::new("provider.graph"),
        )
        .endpoint("shm://vision-graph")
        .endpoint("ipc://graph-control")
        .build();

        let shm = resource
            .endpoint::<SharedMemoryEndpoint>()
            .expect("shared memory endpoint should parse");
        let ipc = resource
            .endpoint::<IpcEndpoint>()
            .expect("ipc endpoint should parse");

        assert_eq!(shm.name, "vision-graph");
        assert_eq!(ipc.address, "graph-control");
    }

    #[test]
    fn resource_record_reports_missing_typed_endpoint() {
        let resource = ResourceRecord::builder(
            ResourceId::new("resource.graph"),
            ResourceType::new("graph.payload"),
            ProviderId::new("provider.graph"),
        )
        .endpoint("shm://vision-graph")
        .build();

        let error = resource
            .endpoint::<UnixEndpoint>()
            .expect_err("unix endpoint should be absent");
        assert!(matches!(
            error,
            ResourceEndpointError::EndpointTypeNotFound {
                endpoint_type: "unix",
                ..
            }
        ));
    }

    #[test]
    fn resource_record_rejects_unsupported_endpoint_scheme() {
        let resource = ResourceRecord::builder(
            ResourceId::new("resource.graph"),
            ResourceType::new("graph.payload"),
            ProviderId::new("provider.graph"),
        )
        .endpoint("serial://ttyUSB0")
        .build();

        let error = resource
            .parsed_endpoints()
            .expect_err("unsupported scheme should fail");
        assert!(matches!(
            error,
            ResourceEndpointError::UnsupportedScheme { scheme, .. } if scheme == "serial"
        ));
    }

    #[test]
    fn shared_memory_endpoint_reads_from_custom_root() {
        let root = std::env::temp_dir().join(format!("orion-shm-test-{}", std::process::id()));
        std::fs::create_dir_all(&root).expect("temp root should be creatable");
        let endpoint = SharedMemoryEndpoint {
            name: "graph-payload".to_owned(),
        };
        let path = endpoint.path_in(&root);
        std::fs::write(&path, b"{\"graph\":3}").expect("payload should be writable");

        let content = endpoint
            .read_string_from(&root)
            .expect("payload should be readable");
        assert_eq!(content, "{\"graph\":3}");

        std::fs::remove_file(&path).expect("payload should be removable");
        std::fs::remove_dir(&root).expect("temp root should be removable");
    }

    #[test]
    fn unix_endpoint_reads_file_payload() {
        let path = std::env::temp_dir().join(format!(
            "orion-unix-endpoint-test-{}.txt",
            std::process::id()
        ));
        std::fs::write(&path, b"unix-endpoint-payload").expect("payload should be writable");

        let endpoint = UnixEndpoint {
            path: path.display().to_string(),
        };
        let content = endpoint
            .read_string()
            .expect("payload should be readable through unix endpoint");
        assert_eq!(content, "unix-endpoint-payload");

        std::fs::remove_file(&path).expect("payload should be removable");
    }
}

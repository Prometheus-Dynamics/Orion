use super::*;
#[cfg(any(feature = "transport-tcp", feature = "transport-quic"))]
use orion::data_plane::{
    ChannelBinding, LinkType, PeerCapabilities, PeerLink, RemoteBinding, TransportType,
};
#[cfg(feature = "transport-quic")]
use orion::transport::quic::{
    QuicChannel, QuicEndpoint, QuicFrame, QuicFrameClient, QuicFrameHandler, QuicTransportError,
};
#[cfg(feature = "transport-tcp")]
use orion::transport::tcp::{
    TcpEndpoint, TcpFrame, TcpFrameClient, TcpFrameHandler, TcpTransportError,
};
use orion::{
    ArtifactId, CapabilityDef, CapabilityId, CompatibilityState, ConfigSchemaDef, ConfigSchemaId,
    ExecutorId, NodeId, ProviderId, ResourceType, Revision, RuntimeType, WorkloadId,
    auth::PeerRequestPayload,
    control_plane::{
        ArtifactRecord, AvailabilityState, ClientEventKind, ClientEventPoll, ClientHello,
        ClientRole, ControlMessage, DesiredClusterState, DesiredState, DesiredStateSummary,
        ExecutorRecord, ExecutorStateUpdate, HealthState, LeaseState, MutationBatch,
        PeerEnrollment, ProviderRecord, ProviderStateUpdate, ResourceCapability, ResourceRecord,
        TypedConfigValue, WorkloadConfig, WorkloadObservedState, WorkloadRecord,
    },
    encode_to_vec,
    runtime::{
        ExecutorCommand, ExecutorIntegration, ExecutorSnapshot, ProviderIntegration,
        ProviderSnapshot,
    },
    transport::{
        http::{
            ControlRoute, HttpClient, HttpControlHandler, HttpRequestPayload, HttpResponsePayload,
            HttpServer, HttpTransportError,
        },
        ipc::{ControlEnvelope, LocalAddress, UnixControlClient, UnixControlStreamClient},
    },
};
#[cfg(any(feature = "transport-tcp", feature = "transport-quic"))]
use orion::{ProtocolVersion, ResourceId};
use rcgen::generate_simple_self_signed;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::time::Duration;
use std::{fs, path::PathBuf};

#[derive(Clone)]
struct TestProvider;

fn desired_fingerprint(desired: &orion::control_plane::DesiredClusterState) -> u64 {
    let section_fingerprints = orion::control_plane::DesiredStateSectionFingerprints {
        nodes: entry_fingerprint(&desired.nodes),
        artifacts: entry_fingerprint(&desired.artifacts),
        workloads: entry_fingerprint(&(
            desired.workloads.clone(),
            desired.workload_tombstones.clone(),
        )),
        resources: entry_fingerprint(&desired.resources),
        providers: entry_fingerprint(&desired.providers),
        executors: entry_fingerprint(&desired.executors),
        leases: entry_fingerprint(&desired.leases),
    };
    let bytes = encode_to_vec(&section_fingerprints)
        .expect("desired state section fingerprints should encode");
    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

fn entry_fingerprint<T: orion::ArchiveEncode>(value: &T) -> u64 {
    let bytes = encode_to_vec(value).expect("value should encode");
    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

fn peer_control_message(payload: HttpRequestPayload) -> Result<ControlMessage, HttpTransportError> {
    match payload {
        HttpRequestPayload::Control(message) => Ok(*message),
        HttpRequestPayload::AuthenticatedPeer(request) => match request.payload {
            PeerRequestPayload::Control(message) => Ok(*message),
            PeerRequestPayload::ObservedUpdate(_) => Err(HttpTransportError::request_failed(
                "expected control message, got observed update",
            )),
        },
        HttpRequestPayload::ObservedUpdate(_) => Err(HttpTransportError::request_failed(
            "expected control message, got observed update",
        )),
    }
}

#[cfg(feature = "transport-tcp")]
fn tcp_peer_link() -> PeerLink {
    PeerCapabilities {
        node_id: NodeId::new("node-a"),
        control_versions: vec![ProtocolVersion::new(1, 0)],
        data_versions: vec![ProtocolVersion::new(1, 0)],
        transports: vec![TransportType::TcpStream],
        link_types: vec![LinkType::ReliableOrdered],
        features: Vec::new(),
    }
    .negotiate_with(&PeerCapabilities {
        node_id: NodeId::new("node-b"),
        control_versions: vec![ProtocolVersion::new(1, 0)],
        data_versions: vec![ProtocolVersion::new(1, 0)],
        transports: vec![TransportType::TcpStream],
        link_types: vec![LinkType::ReliableOrdered],
        features: Vec::new(),
    })
    .expect("tcp peer capabilities should negotiate")
}

#[cfg(feature = "transport-quic")]
fn quic_peer_link() -> PeerLink {
    PeerCapabilities {
        node_id: NodeId::new("node-a"),
        control_versions: vec![ProtocolVersion::new(1, 0)],
        data_versions: vec![ProtocolVersion::new(1, 0)],
        transports: vec![TransportType::QuicStream],
        link_types: vec![LinkType::ReliableMultiplexed],
        features: Vec::new(),
    }
    .negotiate_with(&PeerCapabilities {
        node_id: NodeId::new("node-b"),
        control_versions: vec![ProtocolVersion::new(1, 0)],
        data_versions: vec![ProtocolVersion::new(1, 0)],
        transports: vec![TransportType::QuicStream],
        link_types: vec![LinkType::ReliableMultiplexed],
        features: Vec::new(),
    })
    .expect("quic peer capabilities should negotiate")
}

#[cfg(feature = "transport-tcp")]
fn tcp_test_frame() -> TcpFrame {
    TcpFrame {
        source: TcpEndpoint::new("127.0.0.1", 4100),
        destination: TcpEndpoint::new("127.0.0.1", 4200),
        link: tcp_peer_link(),
        binding: RemoteBinding::Channel(ChannelBinding {
            remote_node_id: NodeId::new("node-b"),
            resource_id: ResourceId::new("resource.camera"),
            transport: TransportType::TcpStream,
            link_type: LinkType::ReliableOrdered,
        }),
        payload: vec![1, 2, 3, 4],
    }
}

#[cfg(feature = "transport-quic")]
fn quic_test_frame() -> QuicFrame {
    QuicFrame {
        source: QuicEndpoint::new("127.0.0.1", 5100).with_server_name("node-a.local"),
        destination: QuicEndpoint::new("127.0.0.1", 5200).with_server_name("node-b.local"),
        connection_id: 7,
        channel: QuicChannel::Stream(3),
        link: quic_peer_link(),
        binding: RemoteBinding::Channel(ChannelBinding {
            remote_node_id: NodeId::new("node-b"),
            resource_id: ResourceId::new("resource.video"),
            transport: TransportType::QuicStream,
            link_type: LinkType::ReliableMultiplexed,
        }),
        payload: vec![1, 2, 3, 4],
    }
}

impl ProviderIntegration for TestProvider {
    fn provider_record(&self) -> ProviderRecord {
        ProviderRecord::builder("provider.local", "node-a")
            .resource_type(ResourceType::new("imu.sample"))
            .build()
    }

    fn snapshot(&self) -> ProviderSnapshot {
        ProviderSnapshot {
            provider: self.provider_record(),
            resources: vec![
                ResourceRecord::builder("resource.imu-1", "imu.sample", "provider.local")
                    .health(HealthState::Healthy)
                    .availability(AvailabilityState::Available)
                    .lease_state(LeaseState::Unleased)
                    .build(),
            ],
        }
    }
}

#[derive(Clone)]
struct TestExecutor {
    commands: Arc<Mutex<Vec<ExecutorCommand>>>,
}

struct CameraControllerConfigV1;
impl ConfigSchemaDef for CameraControllerConfigV1 {
    const SCHEMA_ID: &'static str = "camera.controller.config.v1";
}

struct CaptureConfigurable;
impl CapabilityDef for CaptureConfigurable {
    const CAPABILITY_ID: &'static str = "capture.configurable";
}

impl TestExecutor {
    fn new() -> Self {
        Self {
            commands: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[derive(Clone)]
struct NamedProvider {
    provider_id: &'static str,
    resource_id: &'static str,
    resource_type: &'static str,
    ownership_mode: orion::control_plane::ResourceOwnershipMode,
    capabilities: Vec<CapabilityId>,
}

#[derive(Clone)]
struct BlockingProvider {
    provider_id: &'static str,
    resource_id: &'static str,
    resource_type: &'static str,
    started: Arc<Mutex<Option<mpsc::Sender<()>>>>,
    gate: Arc<(Mutex<bool>, Condvar)>,
}

impl BlockingProvider {
    fn new(
        provider_id: &'static str,
        resource_id: &'static str,
        resource_type: &'static str,
    ) -> (Self, mpsc::Receiver<()>) {
        let (started_tx, started_rx) = mpsc::channel();
        (
            Self {
                provider_id,
                resource_id,
                resource_type,
                started: Arc::new(Mutex::new(Some(started_tx))),
                gate: Arc::new((Mutex::new(false), Condvar::new())),
            },
            started_rx,
        )
    }

    fn release(&self) {
        let (released, wake) = &*self.gate;
        *released
            .lock()
            .expect("blocking provider release gate should not be poisoned") = true;
        wake.notify_all();
    }
}

impl ProviderIntegration for BlockingProvider {
    fn provider_record(&self) -> ProviderRecord {
        ProviderRecord::builder(self.provider_id, "node-a")
            .resource_type(ResourceType::new(self.resource_type))
            .build()
    }

    fn snapshot(&self) -> ProviderSnapshot {
        if let Some(started) = self
            .started
            .lock()
            .expect("blocking provider started sender should not be poisoned")
            .take()
        {
            let _ = started.send(());
        }

        let (released, wake) = &*self.gate;
        let mut released = released
            .lock()
            .expect("blocking provider release gate should not be poisoned");
        while !*released {
            released = wake
                .wait(released)
                .expect("blocking provider release gate should not be poisoned");
        }

        ProviderSnapshot {
            provider: self.provider_record(),
            resources: vec![
                ResourceRecord::builder(self.resource_id, self.resource_type, self.provider_id)
                    .health(HealthState::Healthy)
                    .availability(AvailabilityState::Available)
                    .lease_state(LeaseState::Unleased)
                    .build(),
            ],
        }
    }
}

impl ProviderIntegration for NamedProvider {
    fn provider_record(&self) -> ProviderRecord {
        ProviderRecord::builder(self.provider_id, "node-a")
            .resource_type(ResourceType::new(self.resource_type))
            .build()
    }

    fn snapshot(&self) -> ProviderSnapshot {
        let resource = self
            .capabilities
            .iter()
            .cloned()
            .fold(
                ResourceRecord::builder(self.resource_id, self.resource_type, self.provider_id)
                    .ownership_mode(self.ownership_mode.clone())
                    .health(HealthState::Healthy)
                    .availability(AvailabilityState::Available)
                    .lease_state(LeaseState::Unleased),
                |builder, capability_id| builder.capability(ResourceCapability::new(capability_id)),
            )
            .build();
        ProviderSnapshot {
            provider: self.provider_record(),
            resources: vec![resource],
        }
    }

    fn validate_resource_claim(
        &self,
        resource: &ResourceRecord,
        _workload_id: &WorkloadId,
        requirement: &orion::control_plane::WorkloadRequirement,
        existing_claims: u32,
    ) -> Result<(), orion::runtime::RuntimeError> {
        orion::runtime::validate_requirement_against_resource(
            resource,
            requirement,
            existing_claims,
        )
    }
}

#[derive(Clone)]
struct NamedExecutor {
    executor_id: &'static str,
    runtime_type: &'static str,
    commands: Arc<Mutex<Vec<ExecutorCommand>>>,
}

#[derive(Clone)]
struct BlockingExecutor {
    executor_id: &'static str,
    runtime_type: &'static str,
    started: Arc<Mutex<Option<mpsc::Sender<()>>>>,
    gate: Arc<(Mutex<bool>, Condvar)>,
}

impl BlockingExecutor {
    fn new(executor_id: &'static str, runtime_type: &'static str) -> (Self, mpsc::Receiver<()>) {
        let (started_tx, started_rx) = mpsc::channel();
        (
            Self {
                executor_id,
                runtime_type,
                started: Arc::new(Mutex::new(Some(started_tx))),
                gate: Arc::new((Mutex::new(false), Condvar::new())),
            },
            started_rx,
        )
    }

    fn release(&self) {
        let (released, wake) = &*self.gate;
        *released
            .lock()
            .expect("blocking executor release gate should not be poisoned") = true;
        wake.notify_all();
    }
}

impl NamedExecutor {
    fn new(executor_id: &'static str, runtime_type: &'static str) -> Self {
        Self {
            executor_id,
            runtime_type,
            commands: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl ExecutorIntegration for BlockingExecutor {
    fn executor_record(&self) -> ExecutorRecord {
        ExecutorRecord::builder(self.executor_id, "node-a")
            .runtime_type(RuntimeType::new(self.runtime_type))
            .build()
    }

    fn snapshot(&self) -> ExecutorSnapshot {
        if let Some(started) = self
            .started
            .lock()
            .expect("blocking executor started sender should not be poisoned")
            .take()
        {
            let _ = started.send(());
        }

        let (released, wake) = &*self.gate;
        let mut released = released
            .lock()
            .expect("blocking executor release gate should not be poisoned");
        while !*released {
            released = wake
                .wait(released)
                .expect("blocking executor release gate should not be poisoned");
        }

        ExecutorSnapshot {
            executor: self.executor_record(),
            workloads: Vec::new(),
            resources: Vec::new(),
        }
    }

    fn apply_command(
        &self,
        _command: &ExecutorCommand,
    ) -> Result<(), orion::runtime::RuntimeError> {
        Ok(())
    }
}

impl ExecutorIntegration for NamedExecutor {
    fn executor_record(&self) -> ExecutorRecord {
        ExecutorRecord::builder(self.executor_id, "node-a")
            .runtime_type(RuntimeType::new(self.runtime_type))
            .build()
    }

    fn snapshot(&self) -> ExecutorSnapshot {
        ExecutorSnapshot {
            executor: self.executor_record(),
            workloads: Vec::new(),
            resources: Vec::new(),
        }
    }

    fn apply_command(&self, command: &ExecutorCommand) -> Result<(), orion::runtime::RuntimeError> {
        self.commands
            .lock()
            .expect("named executor command log lock should not be poisoned")
            .push(command.clone());
        Ok(())
    }
}

impl ExecutorIntegration for TestExecutor {
    fn executor_record(&self) -> ExecutorRecord {
        ExecutorRecord::builder("executor.local", "node-a")
            .runtime_type(RuntimeType::new("graph.exec.v1"))
            .build()
    }

    fn snapshot(&self) -> ExecutorSnapshot {
        ExecutorSnapshot {
            executor: self.executor_record(),
            workloads: Vec::new(),
            resources: Vec::new(),
        }
    }

    fn apply_command(&self, command: &ExecutorCommand) -> Result<(), orion::runtime::RuntimeError> {
        self.commands
            .lock()
            .expect("test executor command log lock should not be poisoned")
            .push(command.clone());
        Ok(())
    }
}

#[derive(Clone)]
struct ValidatingExecutor;

impl ExecutorIntegration for ValidatingExecutor {
    fn executor_record(&self) -> ExecutorRecord {
        ExecutorRecord::builder("executor.camera", "node-a")
            .runtime_type(RuntimeType::new("camera.controller.v1"))
            .build()
    }

    fn snapshot(&self) -> ExecutorSnapshot {
        ExecutorSnapshot {
            executor: self.executor_record(),
            workloads: Vec::new(),
            resources: Vec::new(),
        }
    }

    fn validate_workload(
        &self,
        workload: &WorkloadRecord,
    ) -> Result<(), orion::runtime::RuntimeError> {
        let Some(config) = workload.config.as_ref() else {
            return Err(orion::runtime::RuntimeError::InvalidWorkloadConfig {
                workload_id: workload.workload_id.clone(),
                reason: "missing config".into(),
            });
        };
        if config.schema_id != ConfigSchemaId::of::<CameraControllerConfigV1>() {
            return Err(
                orion::runtime::RuntimeError::UnsupportedWorkloadConfigSchema {
                    workload_id: workload.workload_id.clone(),
                    runtime_type: workload.runtime_type.clone(),
                    schema_id: config.schema_id.clone(),
                },
            );
        }
        match config.uint("width") {
            Some(_) => Ok(()),
            _ => Err(orion::runtime::RuntimeError::InvalidWorkloadConfig {
                workload_id: workload.workload_id.clone(),
                reason: "missing width".into(),
            }),
        }
    }
}

#[derive(Clone)]
struct CameraProvider;

impl ProviderIntegration for CameraProvider {
    fn provider_record(&self) -> ProviderRecord {
        ProviderRecord::builder("provider.camera", "node-a")
            .resource_type(ResourceType::new("camera.device"))
            .build()
    }

    fn snapshot(&self) -> ProviderSnapshot {
        ProviderSnapshot {
            provider: self.provider_record(),
            resources: vec![
                ResourceRecord::builder(
                    "resource.camera.raw.front",
                    "camera.device",
                    "provider.camera",
                )
                .ownership_mode(
                    orion::control_plane::ResourceOwnershipMode::ExclusiveOwnerPublishesDerived,
                )
                .health(HealthState::Healthy)
                .availability(AvailabilityState::Available)
                .lease_state(LeaseState::Unleased)
                .build(),
            ],
        }
    }
}

#[derive(Clone)]
struct CameraControllerExecutor {
    commands: Arc<Mutex<Vec<ExecutorCommand>>>,
}

impl CameraControllerExecutor {
    fn new() -> Self {
        Self {
            commands: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl ExecutorIntegration for CameraControllerExecutor {
    fn executor_record(&self) -> ExecutorRecord {
        ExecutorRecord::builder("executor.camera-stack", "node-a")
            .runtime_type("camera.controller.v1")
            .build()
    }

    fn snapshot(&self) -> ExecutorSnapshot {
        ExecutorSnapshot {
            executor: self.executor_record(),
            workloads: Vec::new(),
            resources: Vec::new(),
        }
    }

    fn apply_command(&self, command: &ExecutorCommand) -> Result<(), orion::runtime::RuntimeError> {
        self.commands
            .lock()
            .expect("camera executor command log should not be poisoned")
            .push(command.clone());
        Ok(())
    }
}

#[derive(Clone)]
struct CameraPipelineExecutor {
    commands: Arc<Mutex<Vec<ExecutorCommand>>>,
}

impl CameraPipelineExecutor {
    fn new() -> Self {
        Self {
            commands: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl ExecutorIntegration for CameraPipelineExecutor {
    fn executor_record(&self) -> ExecutorRecord {
        ExecutorRecord::builder("executor.camera-stack", "node-a")
            .runtime_type("camera.controller.v1")
            .runtime_type("vision.consumer.v1")
            .build()
    }

    fn snapshot(&self) -> ExecutorSnapshot {
        ExecutorSnapshot {
            executor: self.executor_record(),
            workloads: vec![
                WorkloadRecord::builder(
                    "workload.camera-controller",
                    "camera.controller.v1",
                    "artifact.camera",
                )
                .desired_state(DesiredState::Running)
                .observed_state(WorkloadObservedState::Running)
                .assigned_to("node-a")
                .require_resource_with_ownership(
                    "camera.device",
                    1,
                    orion::control_plane::ResourceOwnershipMode::ExclusiveOwnerPublishesDerived,
                )
                .bind_resource("resource.camera.raw.front", "node-a")
                .build(),
            ],
            resources: vec![
                ResourceRecord::builder(
                    "resource.camera.stream.front",
                    "camera.frame_stream",
                    "provider.camera",
                )
                .realized_by_executor("executor.camera-stack")
                .ownership_mode(orion::control_plane::ResourceOwnershipMode::SharedRead)
                .realized_for_workload("workload.camera-controller")
                .source_resource("resource.camera.raw.front")
                .source_workload("workload.camera-controller")
                .health(HealthState::Healthy)
                .availability(AvailabilityState::Available)
                .build(),
            ],
        }
    }

    fn apply_command(&self, command: &ExecutorCommand) -> Result<(), orion::runtime::RuntimeError> {
        self.commands
            .lock()
            .expect("camera executor command log should not be poisoned")
            .push(command.clone());
        Ok(())
    }
}

mod runtime;
mod runtime_http;
mod state;
mod sync;

fn test_node_config(node_id: impl Into<NodeId>, ipc_label: impl AsRef<str>) -> NodeConfig {
    test_node_config_with(
        node_id,
        ipc_label,
        None,
        Vec::new(),
        crate::PeerAuthenticationMode::Optional,
    )
}

fn test_node_config_with_state_dir_and_auth(
    node_id: impl Into<NodeId>,
    ipc_label: impl AsRef<str>,
    state_dir: PathBuf,
    peer_authentication: crate::PeerAuthenticationMode,
) -> NodeConfig {
    test_node_config_with(
        node_id,
        ipc_label,
        Some(state_dir),
        Vec::new(),
        peer_authentication,
    )
}

fn test_node_config_with_auth(
    node_id: impl Into<NodeId>,
    ipc_label: impl AsRef<str>,
    peer_authentication: crate::PeerAuthenticationMode,
) -> NodeConfig {
    test_node_config_with(node_id, ipc_label, None, Vec::new(), peer_authentication)
}

fn test_node_config_for_socket_path(
    node_id: impl Into<NodeId>,
    ipc_socket_path: PathBuf,
    peer_authentication: crate::PeerAuthenticationMode,
) -> NodeConfig {
    let mut config =
        test_node_config_with(node_id, "unused", None, Vec::new(), peer_authentication);
    config.ipc_socket_path = ipc_socket_path;
    config
}

fn test_node_config_with(
    node_id: impl Into<NodeId>,
    ipc_label: impl AsRef<str>,
    state_dir: Option<PathBuf>,
    peers: Vec<PeerConfig>,
    peer_authentication: crate::PeerAuthenticationMode,
) -> NodeConfig {
    let mut config = NodeConfig::for_local_node(node_id)
        .with_http_bind_addr("127.0.0.1:0".parse().expect("socket address should parse"))
        .with_ipc_socket_path(NodeConfig::default_ipc_socket_path_for(ipc_label.as_ref()))
        .with_reconcile_interval(Duration::from_millis(50))
        .with_peers(peers)
        .with_peer_authentication(peer_authentication)
        .with_peer_sync_execution(
            NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
        )
        .with_runtime_tuning(
            NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        );
    if let Some(state_dir) = state_dir {
        config = config.with_state_dir(state_dir);
    }
    config
}

fn temp_state_dir(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "orion-node-{}-{}-{}",
        name,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos()
    ))
}

fn temp_socket_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "orion-node-ipc-{}-{}-{}.sock",
        name,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos()
    ))
}

fn temp_stream_socket_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "orion-node-ipc-stream-{}-{}-{}.sock",
        name,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos()
    ))
}

fn write_test_tls_files(name: &str) -> (PathBuf, PathBuf, PathBuf) {
    let cert_dir = temp_state_dir(name);
    fs::create_dir_all(&cert_dir).expect("TLS test directory should be created");
    let cert_path = cert_dir.join("cert.pem");
    let key_path = cert_dir.join("key.pem");
    let rcgen::CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("TLS test certificate should generate");
    fs::write(&cert_path, cert.pem()).expect("TLS certificate should write");
    fs::write(&key_path, signing_key.serialize_pem()).expect("TLS key should write");
    (cert_dir, cert_path, key_path)
}

mod ipc;

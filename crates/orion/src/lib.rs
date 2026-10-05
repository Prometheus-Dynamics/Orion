//! Facade crate for Orion.
//!
//! This crate re-exports the workspace surface behind feature gates so downstream users can pick
//! the layer they actually need instead of depending on every transport and helper crate.
//!
//! Feature guidance:
//!
//! - Default features cover the core protocol/runtime surface.
//! - `client` enables the Rust client SDK and implies `runtime`.
//! - Transport features remain opt-in.
//! - `service`, `macros`, and `cluster` are ergonomic opt-ins rather than default dependencies.
//! - `service` is ergonomic rather than strictly zero-cost because it uses dynamic dispatch.
//! - `std` (default) enables host support. With `default-features = false` the model layers
//!   (`core`, `auth`, `control-plane`, `data-plane`, `runtime`, `cluster`, `macros`) build
//!   `no_std` + `alloc`; transports, `client`, and `service` imply `std`.

#![cfg_attr(not(feature = "std"), no_std)]

pub mod build_info;

#[doc(hidden)]
pub extern crate self as orion;

#[cfg(feature = "auth")]
pub mod auth {
    pub use orion_auth::{
        AuthProtocolError, AuthenticatedPeerRequest, NodeTransportBinding,
        PEER_REQUEST_AUTH_VERSION, PEER_REQUEST_SIGNING_DOMAIN, PeerRequestAuth,
        PeerRequestPayload, TRANSPORT_BINDING_SIGNING_DOMAIN, TRANSPORT_BINDING_VERSION,
        canonical_peer_request_bytes, canonical_transport_binding_bytes,
    };
    /// `_orion._tcp` advertisement layout, key hex helpers and `orion+tcp` frame payloads.
    pub use orion_auth::{discovery, hex, peer_tcp};
}

#[cfg(feature = "cluster")]
pub mod cluster {
    pub use orion_cluster::{
        AdmissionDecision, AdmissionRejection, ClusterCoordinator, ClusterMembership, ClusterPeer,
        ClusterRole, ClusterView, Ineligibility, LeaseEdit, NodeCandidate, PlacementStatus,
        ReplicationState, choose_node, eligibility, eligible_nodes, ensure_node_present, leases,
        rendezvous_choice, rendezvous_score, resource_host,
    };
}

#[cfg(feature = "client")]
pub mod client {
    pub use orion_client::prelude;
    pub use orion_client::{ActionRequestWatch, ActionWatch};
    pub use orion_client::{
        AssignedWorkload, AssignedWorkloadWatch, AssignedWorkloadsUpdate, BoundResource,
        assigned_workloads, assigned_workloads_for_executor, assigned_workloads_from_records,
        is_assigned_to, resources_bound_to,
    };
    pub use orion_client::{
        ClientError, ClientIdentity, ClientRole, ClientSession, ControlPlaneClient,
        ControlPlaneEventStream, DEFAULT_DAEMON_ADDRESS, DerivedResource, ExecutorApp,
        ExecutorClient, ExecutorEventStream, GraphPayload, GraphPayloadSource, GraphReference,
        GraphResolutionError, LocalControlPlaneClient, LocalExecutorApp, LocalExecutorClient,
        LocalExecutorEvent, LocalExecutorService, LocalExecutorSubscription, LocalNodeRuntime,
        LocalProviderApp, LocalProviderClient, LocalProviderEvent, LocalProviderService,
        LocalProviderSubscription, LocalRuntimePublisher, LocalRuntimePublisherBuilder,
        LocalServiceRetryPolicy, ProviderApp, ProviderClient, ProviderEventStream,
        ProviderResource, ResolvedGraphPayload, ResourceClaim, SessionConfig, StatusWatch,
        default_ipc_socket_path, default_ipc_stream_socket_path, graph_reference_for_workload,
        resolve_graph_reference, resolve_workload_graph,
    };
}

/// Remote operator client (`docs/remote-operator.md`): talk to nodes over the signed `orion+tcp`
/// transport without running `orion-node`.
#[cfg(feature = "remote")]
pub mod remote {
    pub use orion_client::remote::*;
}

#[cfg(feature = "control-plane")]
pub mod control_plane {
    pub use orion_control_plane::{
        ActionQuery, ActionReport, ActionRequest, ActionResult, ActionState, ActionTarget,
        ActionTargetParseError, HostFacts, HostMetricsSample, HostTemperature, LinkStatusSnapshot,
        NodeHostFacts, action_names, action_status_keys,
    };
    pub use orion_control_plane::{
        AppliedClusterState, ArtifactRecord, ArtifactRecordBuilder, AvailabilityState, ClientEvent,
        ClientEventKind, ClientEventPoll, ClientHello, ClientRole, ClientSession,
        ClientSessionMetricsSnapshot, ClockSourceKind, ClusterStateEnvelope,
        CommunicationEndpointSnapshot, CommunicationFailureCountSnapshot, CommunicationFailureKind,
        CommunicationMetricsSnapshot, CommunicationRecentMetricsSnapshot,
        CommunicationStageMetricsSnapshot, ConfigDecodeError, ConfigMapRef, ControlMessage,
        CustomEndpoint, CustomEndpointScheme, DesiredClusterState, DesiredObjectKey,
        DesiredObjectStamps, DesiredObjectVersion, DesiredState, DesiredStateMergeSnapshot,
        DesiredStateMutation, DesiredStateObjectSelector, DesiredStateSection,
        DesiredStateSectionFingerprints, DesiredStateSummary, ExecutorRecord,
        ExecutorRecordBuilder, ExecutorStateUpdate, ExecutorWorkloadQuery, HealthState,
        HostMetricsSnapshot, HttpEndpoint, IpcEndpoint, LatencyMetricBuckets,
        LatencyMetricsSnapshot, LeaseRecord, LeaseRecordBuilder, LeaseState,
        LocalStreamUsageSnapshot, MutationApplyError, MutationBatch, MutationHistoryUsageSnapshot,
        NodeClockFacts, NodeHealthSnapshot, NodeHealthStatus, NodeObservabilitySnapshot,
        NodeReadinessSnapshot, NodeReadinessStatus, NodeRecord, NodeRecordBuilder,
        NodeResourceUsageSnapshot, ObservabilityEvent, ObservabilityEventKind,
        ObservedClusterState, ObservedPersistenceUsageSnapshot, ObservedStateUpdate,
        OperationFailureCategory, OperationMetricsSnapshot, PeerEnrollment, PeerHello,
        PeerIdentityUpdate, PeerTrustRecord, PeerTrustSnapshot, PersistenceMetricsSnapshot,
        ProcessMemorySnapshot, ProviderLeaseQuery, ProviderRecord, ProviderRecordBuilder,
        ProviderStateUpdate, RegistryUsageSnapshot, ResourceActionResult, ResourceActionStatus,
        ResourceBinding, ResourceCapability, ResourceConfigState, ResourceEndpoint,
        ResourceEndpointError, ResourceOwnershipMode, ResourceRecord, ResourceRecordBuilder,
        ResourceState, RestartPolicy, SharedMemoryEndpoint, StateSectionCounts, StateSizeSnapshot,
        StateSnapshot, StateWatch, StatusChange, StatusEntry, StatusKey, StatusLaneUsageSnapshot,
        StatusQuery, StatusSubject, StatusSubjectParseError, SyncDiffRequest, SyncRequest,
        SyncSummaryRequest, TcpEndpoint, TransportMetricsSnapshot, TypedConfigValue,
        TypedResourceEndpoint, UnixEndpoint, WorkerQueueUsageSnapshot, WorkloadConfig,
        WorkloadObservedState, WorkloadRecord, WorkloadRecordBuilder, WorkloadRequirement,
        config_json_value, deserialize_config, duration_ms_u64, estimate_wire_bytes,
    };
    pub use orion_control_plane::{
        DiscoveredPeerEnrollment, DiscoveredPeerRecord, DiscoveredPeerState,
        DiscoveryMetricsSnapshot, DiscoverySnapshot, ENROLLMENT_PROTOCOL_VERSION,
        EnrollmentChallenge, EnrollmentConfirm, EnrollmentHello, EnrollmentRole, InvalidOperatorId,
        OPERATOR_ID_PREFIX, OperatorEnrollment, OperatorEnrollmentMethod, OperatorId,
        OperatorPolicy, OperatorRecord, OperatorTrustState, OperatorWelcome, OperatorsSnapshot,
        action_pattern_matches, validate_action_patterns,
    };

    /// Prometheus text rendering is host-only and requires the `std` feature.
    #[cfg(feature = "std")]
    pub use orion_control_plane::{
        MetricsExportConfig, render_clock_metrics, render_communication_metrics,
        render_communication_metrics_with_config, render_host_facts_metrics, render_host_metrics,
        render_link_metrics, render_observability_metrics,
        render_observability_metrics_with_config, render_resource_usage_metrics,
    };
}

#[cfg(feature = "core")]
pub mod core {
    pub use orion_core::{
        ArchiveEncode, ArtifactId, CapabilityDef, CapabilityId, ClientName, CompatibilityState,
        ConfigSchemaDef, ConfigSchemaId, ExecutorId, FeatureFlag, HlcClockSkew, HlcTimestamp,
        HybridLogicalClock, NodeId, OrionError, PeerBaseUrl, ProtocolVersion, ProviderId,
        PublicKeyHex, ResourceId, ResourceType, ResourceTypeDef, Revision, RuntimeType,
        RuntimeTypeDef, SessionId, WorkloadId, decode_from_slice, encode_to_vec, hlc_node_tag,
    };
}

#[cfg(feature = "service")]
pub mod service {
    pub use orion_service::{MiddlewareNext, MiddlewareStack, RequestMiddleware, RequestService};
}

#[cfg(feature = "data-plane")]
pub mod data_plane {
    pub use orion_data_plane::{
        ChannelBinding, LinkType, NegotiationError, PeerCapabilities, PeerLink, ProxyBinding,
        RemoteBinding, TransportType,
    };
}

#[cfg(feature = "runtime")]
pub mod runtime {
    pub use orion_runtime::{
        ExecutorCommand, ExecutorDescriptor, ExecutorIntegration, ExecutorSnapshot,
        LocalRuntimeStore, ProviderDescriptor, ProviderIntegration, ProviderSnapshot,
        ReconcileReport, Runtime, RuntimeError, RuntimeSnapshot, WorkloadPlan,
        validate_requirement_against_resource,
    };
}

/// Feature-gated transport adapters grouped by carrier.
pub mod transport {
    #[cfg(feature = "transport-http")]
    pub mod http {
        pub use orion_transport_http::{
            ControlRoute, HttpClient, HttpClientTlsConfig, HttpCodec, HttpControlHandler,
            HttpMethod, HttpRequest, HttpRequestFailureKind, HttpRequestPayload, HttpResponse,
            HttpResponsePayload, HttpServer, HttpServerClientAuth, HttpServerTlsConfig,
            HttpService, HttpTlsTrustProvider, HttpTransport, HttpTransportError,
        };
    }

    #[cfg(feature = "transport-ipc")]
    pub mod ipc {
        pub use orion_transport_ipc::{
            ControlEnvelope, DEFAULT_UNIX_FD_FRAME_MAX_FDS,
            DEFAULT_UNIX_FD_FRAME_MAX_PAYLOAD_BYTES, DEFAULT_UNIX_FD_LATEST_MAX_CLIENTS,
            DEFAULT_UNIX_FD_LATEST_MAX_WAIT, DataEnvelope, IpcTransport, IpcTransportError,
            LocalAddress, LocalControlTransport, LocalDataTransport, UnixControlClient,
            UnixControlHandler, UnixControlServer, UnixControlStreamClient, UnixFdFrame,
            UnixFdLatestClient, UnixFdLatestConfig, UnixFdLatestFrame, UnixFdLatestPublisher,
            UnixFdLatestReply, UnixFdLatestServer, UnixPeerIdentity, read_control_frame,
            read_control_frame_with_limit, recv_unix_fd_frame, recv_unix_fd_frame_async,
            send_unix_fd_frame, send_unix_fd_frame_async, write_control_frame,
            write_control_frame_with_limit,
        };
    }

    #[cfg(feature = "transport-quic")]
    pub mod quic {
        pub use orion_transport_quic::{
            QuicChannel, QuicClientTlsConfig, QuicCodec, QuicCodecError, QuicEndpoint, QuicFrame,
            QuicFrameClient, QuicFrameHandler, QuicFrameServer, QuicServerClientAuth,
            QuicServerTlsConfig, QuicTransport, QuicTransportAdapter, QuicTransportError,
        };
    }

    #[cfg(feature = "transport-tcp")]
    pub mod tcp {
        pub use orion_transport_tcp::{
            TcpClientTlsConfig, TcpCodec, TcpCodecError, TcpEndpoint, TcpFrame, TcpFrameClient,
            TcpFrameHandler, TcpFrameServer, TcpServerClientAuth, TcpServerTlsConfig,
            TcpStreamTransport, TcpTransport, TcpTransportError,
        };
    }
}

/// Common Orion imports for applications that want the broader typed surface.
#[cfg(all(
    feature = "client",
    feature = "cluster",
    feature = "control-plane",
    feature = "core",
    feature = "data-plane",
    feature = "runtime",
    feature = "macros",
))]
pub mod prelude {
    pub use crate::client::{
        AssignedWorkload, AssignedWorkloadWatch, AssignedWorkloadsUpdate, BoundResource,
        assigned_workloads, assigned_workloads_for_executor, resources_bound_to,
    };
    pub use crate::client::{
        ClientIdentity, ClientRole, ClientSession, ControlPlaneClient, ControlPlaneEventStream,
        DerivedResource, ExecutorApp, ExecutorClient, ExecutorEventStream, GraphPayload,
        GraphPayloadSource, GraphReference, GraphResolutionError, LocalControlPlaneClient,
        LocalExecutorApp, LocalExecutorClient, LocalExecutorEvent, LocalExecutorService,
        LocalExecutorSubscription, LocalNodeRuntime, LocalProviderApp, LocalProviderClient,
        LocalProviderEvent, LocalProviderService, LocalProviderSubscription, LocalRuntimePublisher,
        LocalRuntimePublisherBuilder, LocalServiceRetryPolicy, ProviderApp, ProviderClient,
        ProviderEventStream, ProviderResource, ResolvedGraphPayload, ResourceClaim, SessionConfig,
        StatusWatch, default_ipc_socket_path, default_ipc_stream_socket_path,
        graph_reference_for_workload, resolve_graph_reference, resolve_workload_graph,
    };
    pub use crate::cluster::ClusterCoordinator;
    pub use crate::control_plane::{
        ConfigDecodeError, ConfigMapRef, CustomEndpoint, CustomEndpointScheme, DesiredClusterState,
        DesiredState, ExecutorRecord, ExecutorRecordBuilder, HttpEndpoint, IpcEndpoint,
        ProviderRecord, ProviderRecordBuilder, ResourceBinding, ResourceEndpoint,
        ResourceEndpointError, ResourceRecord, ResourceRecordBuilder, SharedMemoryEndpoint,
        TcpEndpoint, TypedResourceEndpoint, UnixEndpoint, WorkloadRecord, WorkloadRecordBuilder,
        WorkloadRequirement, config_json_value, deserialize_config,
    };
    pub use crate::control_plane::{
        StateSnapshot, StatusChange, StatusEntry, StatusQuery, StatusSubject, TypedConfigValue,
        WorkloadConfig,
    };
    pub use crate::core::{
        ArtifactId, ExecutorId, NodeId, ProviderId, ResourceId, ResourceType, ResourceTypeDef,
        Revision, RuntimeType, RuntimeTypeDef, WorkloadId,
    };
    pub use crate::data_plane::{LinkType, PeerLink, RemoteBinding, TransportType};
    pub use crate::runtime::{LocalRuntimeStore, Runtime};
    pub use orion_macros::{OrionExecutor, OrionProvider, orion_resource_type, orion_runtime_type};
}

#[cfg(feature = "auth")]
pub use auth::{
    AuthProtocolError, AuthenticatedPeerRequest, PEER_REQUEST_AUTH_VERSION,
    PEER_REQUEST_SIGNING_DOMAIN, PeerRequestAuth, PeerRequestPayload, canonical_peer_request_bytes,
};
#[cfg(feature = "client")]
pub use client::{
    AssignedWorkload, AssignedWorkloadWatch, AssignedWorkloadsUpdate, BoundResource,
    assigned_workloads, assigned_workloads_for_executor, resources_bound_to,
};
#[cfg(feature = "client")]
pub use client::{
    ClientError, ClientIdentity, ClientRole, ClientSession, ControlPlaneClient,
    ControlPlaneEventStream, DEFAULT_DAEMON_ADDRESS, DerivedResource, ExecutorApp, ExecutorClient,
    ExecutorEventStream, GraphPayload, GraphPayloadSource, GraphReference, GraphResolutionError,
    LocalControlPlaneClient, LocalExecutorApp, LocalExecutorClient, LocalExecutorEvent,
    LocalExecutorService, LocalExecutorSubscription, LocalNodeRuntime, LocalProviderApp,
    LocalProviderClient, LocalProviderEvent, LocalProviderService, LocalProviderSubscription,
    LocalRuntimePublisher, LocalRuntimePublisherBuilder, LocalServiceRetryPolicy, ProviderApp,
    ProviderClient, ProviderEventStream, ProviderResource, ResolvedGraphPayload, ResourceClaim,
    SessionConfig, default_ipc_socket_path, default_ipc_stream_socket_path,
    graph_reference_for_workload, resolve_graph_reference, resolve_workload_graph,
};
#[cfg(feature = "cluster")]
pub use cluster::ClusterCoordinator;
#[cfg(feature = "core")]
pub use core::{
    ArchiveEncode, ArtifactId, CapabilityDef, CapabilityId, CompatibilityState, ConfigSchemaDef,
    ConfigSchemaId, ExecutorId, FeatureFlag, NodeId, OrionError, ProtocolVersion, ProviderId,
    ResourceId, ResourceType, ResourceTypeDef, Revision, RuntimeType, RuntimeTypeDef, WorkloadId,
    decode_from_slice, encode_to_vec,
};
#[cfg(feature = "macros")]
pub use orion_macros::{OrionExecutor, OrionProvider, orion_resource_type, orion_runtime_type};
#[cfg(feature = "runtime")]
pub use runtime::{LocalRuntimeStore, Runtime};
#[cfg(feature = "service")]
pub use service::{MiddlewareNext, MiddlewareStack, RequestMiddleware, RequestService};

pub use build_info::{
    BUILD_COMMIT, BUILD_VERSION, PKG_VERSION, build_commit, build_version, pkg_version,
};

#[cfg(all(
    test,
    feature = "client",
    feature = "cluster",
    feature = "control-plane",
    feature = "core",
    feature = "data-plane",
    feature = "runtime",
    feature = "macros",
    feature = "transport-http",
    feature = "transport-ipc",
    feature = "transport-quic",
    feature = "transport-tcp",
))]
mod tests {
    use super::*;

    #[orion_runtime_type("graph.exec.v1")]
    struct GraphExecV1;

    #[orion_resource_type("imu.sample")]
    struct ImuSample;

    #[derive(OrionExecutor)]
    #[orion_executor(id = "executor.engine", runtime_types = ["graph.exec.v1"])]
    struct DerivedExecutor {
        node_id: NodeId,
    }

    #[derive(OrionProvider)]
    #[orion_provider(id = "provider.peripherals", resource_types = ["imu.sample"])]
    struct DerivedProvider {
        node_id: NodeId,
    }

    #[test]
    fn prelude_exposes_common_orion_surface() {
        let _node_id = prelude::NodeId::new("node-a");
        let _runtime = prelude::Runtime::new(prelude::NodeId::new("node-a"));
        let _coordinator = prelude::ClusterCoordinator::new(prelude::NodeId::new("node-a"), 1_000);
        let _assigned: fn(
            &prelude::StateSnapshot,
            &control_plane::ExecutorRecord,
        ) -> Vec<prelude::AssignedWorkload> = prelude::assigned_workloads;
        let _client_prelude_node = client::prelude::NodeId::new("node-a");
        let _client_prelude_revision = client::prelude::Revision::ZERO;
    }

    #[test]
    fn transport_modules_are_grouped_by_carrier() {
        let _http = transport::http::HttpTransport::new();
        let _ipc = transport::ipc::IpcTransport::new();
        let _tcp = transport::tcp::TcpTransport::new();
        let _quic = transport::quic::QuicTransport::new();
    }

    #[test]
    fn macros_define_typed_runtime_and_resource_categories() {
        assert_eq!(GraphExecV1::runtime_type().as_str(), "graph.exec.v1");
        assert_eq!(ImuSample::resource_type().as_str(), "imu.sample");
        assert_eq!(RuntimeType::of::<GraphExecV1>().as_str(), "graph.exec.v1");
        assert_eq!(ResourceType::of::<ImuSample>().as_str(), "imu.sample");
    }

    #[test]
    fn derive_macros_build_executor_and_provider_descriptors() {
        use crate::runtime::{ExecutorDescriptor, ProviderDescriptor};

        let executor = DerivedExecutor {
            node_id: NodeId::new("node-a"),
        };
        let provider = DerivedProvider {
            node_id: NodeId::new("node-a"),
        };

        assert_eq!(
            ExecutorDescriptor::executor_record(&executor).runtime_types[0].as_str(),
            "graph.exec.v1"
        );
        assert_eq!(
            ProviderDescriptor::provider_record(&provider).resource_types[0].as_str(),
            "imu.sample"
        );
    }
}

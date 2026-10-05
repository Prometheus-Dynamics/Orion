//! Orion node runtime and binary support surface.
//!
//! Preferred startup paths:
//!
//! - Use [`NodeProcessConfig::try_from_env`] in binaries that boot directly from environment.
//! - Use [`NodeApp::try_new`] for the shortest explicit-config path.
//! - Use [`NodeApp::builder`] when tests or embedded runtimes need to override transports,
//!   runtime tuning, or startup behavior.

extern crate self as orion;

pub mod actions;
mod app;
mod auth;
mod blocking;
mod clock;
mod config;
#[cfg(feature = "discovery-mdns")]
pub mod discovery;
pub mod host_facts;
#[cfg(feature = "link-gateway")]
pub mod link_gateway;
mod lock;
#[cfg(any(
    feature = "transport-http",
    feature = "transport-tcp",
    feature = "transport-quic"
))]
mod managed_transport;
mod peer;
#[cfg(feature = "peer-tcp")]
mod peer_tcp;
mod service;
mod storage;
mod storage_io;
#[cfg(all(feature = "systemd-notify", unix))]
pub mod systemd;
mod transport_security;

pub mod control_plane {
    pub use orion_control_plane::*;
}

/// Leaderless placement and cross-node lease helpers (`docs/placement.md`).
pub mod cluster {
    pub use orion_cluster::*;
}

pub mod data_plane {
    pub use orion_data_plane::*;
}

pub use orion_core::{
    ArchiveEncode, ArtifactId, CapabilityDef, CapabilityId, CompatibilityState, ConfigSchemaDef,
    ConfigSchemaId, ExecutorId, FeatureFlag, HlcClockSkew, HlcTimestamp, HybridLogicalClock,
    NodeId, OrionError, ProtocolVersion, ProviderId, ResourceId, ResourceType, ResourceTypeDef,
    Revision, RuntimeType, RuntimeTypeDef, WorkloadId, decode_from_slice, decode_from_slice_with,
    encode_to_vec, hlc_node_tag,
};

pub mod runtime {
    pub use orion_runtime::{
        ExecutorCommand, ExecutorDescriptor, ExecutorIntegration, ExecutorSnapshot,
        LocalRuntimeStore, ProviderDescriptor, ProviderIntegration, ProviderSnapshot,
        ReconcileReport, RemoteLease, Runtime, RuntimeError, RuntimeSnapshot,
        UnsatisfiedRequirement, WorkloadPlan, validate_requirement_against_resource,
    };
}

pub mod transport {
    /// HTTP control-plane protocol types.
    ///
    /// The payload, route, codec and error types are protocol data shared with the IPC control
    /// path and are always available. The network client/server and TLS configuration require the
    /// `transport-http` feature.
    pub mod http {
        pub use orion_transport_http::{
            ControlRoute, HttpCodec, HttpControlHandler, HttpMethod, HttpRequest,
            HttpRequestPayload, HttpResponse, HttpResponsePayload, HttpService, HttpTransport,
            HttpTransportError,
        };
        #[cfg(feature = "transport-http")]
        pub use orion_transport_http::{
            HttpClient, HttpClientTlsConfig, HttpServer, HttpServerClientAuth, HttpServerTlsConfig,
            HttpTlsTrustProvider,
        };
    }

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
            QuicChannel, QuicClientTlsConfig, QuicEndpoint, QuicFrame, QuicFrameClient,
            QuicFrameHandler, QuicFrameServer, QuicServerClientAuth, QuicServerTlsConfig,
            QuicTransport, QuicTransportError,
        };
    }

    #[cfg(feature = "transport-tcp")]
    pub mod tcp {
        pub use orion_transport_tcp::{
            TcpClientTlsConfig, TcpEndpoint, TcpFrame, TcpFrameClient, TcpFrameHandler,
            TcpFrameServer, TcpServerClientAuth, TcpServerTlsConfig, TcpTransport,
            TcpTransportError,
        };
    }
}

pub use app::{
    NodeApp, NodeAppBuilder, NodeError, NodeSnapshot, NodeTickReport, PeerSyncExecution,
    ReconcileLoopHandle,
};
pub use auth::{
    AuthenticatedOperator, AuthenticatedPeer, LocalAuthenticationMode, NodeSecurity,
    PeerAuthenticationMode,
    PeerSecurityMiddleware,
};
pub use clock::{ClockStatusSource, KernelClockReading, KernelClockStatusSource};
pub use config::{
    ActionTuning, HostFactsTuning, NodeConfig, NodeProcessConfig, NodeRuntimeThreads,
    PlacementTuning,
};
pub use host_facts::{HostFactsSource, LayeredHostFactsSource, LinuxHostFactsSource};
pub use peer::{
    PEER_TCP_SCHEME, PeerConfig, PeerState, PeerSyncStatus, PeerTransportKind, PeerTrustStatus,
};
#[cfg(feature = "peer-tcp")]
pub use peer_tcp::PeerTcpError;
pub use service::{
    Authenticator, AuthorizationMiddleware, Authorizer, ControlMiddleware, ControlOperation,
    ControlPrincipal, ControlRequest, ControlRequestBody, ControlRequestContext, ControlResponse,
    ControlSource, ControlSurface,
};
pub use storage::{NodeStorage, StateMigrationReport};
pub use transport_security::{
    ManagedClientTransportSecurity, ManagedNodeTransportSurface, ManagedServerTransportSecurity,
    ManagedTransportProtocol, NodeTransportSecurityManager, PeerTransportSecurityMode,
};

#[cfg(test)]
#[cfg_attr(not(feature = "transport-http"), allow(unused_imports))]
pub(crate) use app::{
    clear_test_audit_append_delay, clear_test_persist_delay, set_test_audit_append_delay,
    set_test_persist_delay,
};

#[cfg(test)]
mod tests;

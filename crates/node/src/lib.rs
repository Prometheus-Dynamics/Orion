//! Orion node runtime and binary support surface.
//!
//! Preferred startup paths:
//!
//! - Use [`NodeProcessConfig::try_from_env`] in binaries that boot directly from environment.
//! - Use [`NodeApp::try_new`] for the shortest explicit-config path.
//! - Use [`NodeApp::builder`] when tests or embedded runtimes need to override transports,
//!   runtime tuning, or startup behavior.

extern crate self as orion;

mod app;
mod auth;
mod blocking;
mod config;
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
mod service;
mod storage;
mod storage_io;
mod transport_security;

pub mod control_plane {
    pub use orion_control_plane::*;
}

pub mod data_plane {
    pub use orion_data_plane::*;
}

pub use orion_core::{
    ArchiveEncode, ArtifactId, CapabilityDef, CapabilityId, CompatibilityState, ConfigSchemaDef,
    ConfigSchemaId, ExecutorId, FeatureFlag, NodeId, OrionError, ProtocolVersion, ProviderId,
    ResourceId, ResourceType, ResourceTypeDef, Revision, RuntimeType, RuntimeTypeDef, WorkloadId,
    decode_from_slice, decode_from_slice_with, encode_to_vec,
};

pub mod runtime {
    pub use orion_runtime::{
        ExecutorCommand, ExecutorDescriptor, ExecutorIntegration, ExecutorSnapshot,
        LocalRuntimeStore, ProviderDescriptor, ProviderIntegration, ProviderSnapshot,
        ReconcileReport, Runtime, RuntimeError, RuntimeSnapshot, WorkloadPlan,
        validate_requirement_against_resource,
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
    AuthenticatedPeer, LocalAuthenticationMode, NodeSecurity, PeerAuthenticationMode,
    PeerSecurityMiddleware,
};
pub use config::{NodeConfig, NodeProcessConfig, NodeRuntimeThreads};
pub use peer::{PeerConfig, PeerState, PeerSyncStatus, PeerTrustStatus};
pub use service::{
    Authenticator, AuthorizationMiddleware, Authorizer, ControlMiddleware, ControlOperation,
    ControlPrincipal, ControlRequest, ControlRequestBody, ControlRequestContext, ControlResponse,
    ControlSource, ControlSurface,
};
pub use storage::NodeStorage;
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

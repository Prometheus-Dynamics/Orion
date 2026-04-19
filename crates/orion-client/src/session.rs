use orion_control_plane::{
    ClientHello, ClientRole, ClientSession as ProtocolClientSession, ControlMessage, MutationBatch,
    PeerHello, SyncRequest,
};
use orion_core::{NodeId, Revision};
use orion_transport_ipc::{ControlEnvelope, LocalAddress, LocalControlTransport};
use std::{env, path::PathBuf};

use crate::error::ClientError;

pub const DEFAULT_DAEMON_ADDRESS: &str = "orion";
pub const DEFAULT_NODE_ID: &str = "node.local";
pub const ORION_NODE_ID_ENV: &str = "ORION_NODE_ID";
pub const ORION_NODE_IPC_SOCKET_ENV: &str = "ORION_NODE_IPC_SOCKET";
pub const ORION_NODE_IPC_STREAM_SOCKET_ENV: &str = "ORION_NODE_IPC_STREAM_SOCKET";
const CONTROL_SOCKET_SUFFIX: &str = "control.sock";
const CONTROL_STREAM_SOCKET_SUFFIX: &str = "control-stream.sock";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientIdentity {
    pub name: String,
    pub role: ClientRole,
}

impl ClientIdentity {
    pub fn new(name: impl Into<String>, role: ClientRole) -> Self {
        let name = name.into();
        assert!(
            !name.trim().is_empty(),
            "client identities must not be empty"
        );
        Self { name, role }
    }
}

pub(crate) fn ensure_client_role(
    identity: &ClientIdentity,
    expected: ClientRole,
) -> Result<(), ClientError> {
    if identity.role != expected {
        return Err(ClientError::RoleMismatch {
            expected,
            found: identity.role.clone(),
        });
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionConfig {
    pub local_address: LocalAddress,
    pub daemon_address: LocalAddress,
}

impl SessionConfig {
    pub fn new(local_address: impl Into<LocalAddress>) -> Self {
        Self {
            local_address: local_address.into(),
            daemon_address: LocalAddress::new(DEFAULT_DAEMON_ADDRESS),
        }
    }

    pub fn with_daemon_address(mut self, daemon_address: impl Into<LocalAddress>) -> Self {
        self.daemon_address = daemon_address.into();
        self
    }

    pub fn for_identity(identity: &ClientIdentity) -> Self {
        Self::new(default_local_address_for(identity))
    }
}

pub(crate) fn local_identity_for_role(name: impl Into<String>, role: ClientRole) -> ClientIdentity {
    ClientIdentity::new(name, role)
}

pub(crate) fn local_session_config_for_role(identity: &ClientIdentity) -> SessionConfig {
    SessionConfig::for_identity(identity)
}

pub(crate) fn local_session_config_with_address(
    local_address: impl Into<LocalAddress>,
) -> SessionConfig {
    SessionConfig::new(local_address)
}

#[derive(Clone)]
pub struct ClientSession<T> {
    transport: T,
    identity: ClientIdentity,
    local_address: LocalAddress,
    daemon_address: LocalAddress,
}

impl<T> ClientSession<T>
where
    T: LocalControlTransport,
{
    pub fn connect(
        transport: T,
        identity: ClientIdentity,
        config: SessionConfig,
    ) -> Result<Self, ClientError> {
        if !transport.register_control_endpoint(config.local_address.clone()) {
            return Err(ClientError::AddressAlreadyRegistered);
        }

        Ok(Self {
            transport,
            identity,
            local_address: config.local_address,
            daemon_address: config.daemon_address,
        })
    }

    pub fn identity(&self) -> &ClientIdentity {
        &self.identity
    }

    pub fn role(&self) -> &ClientRole {
        &self.identity.role
    }

    pub fn local_address(&self) -> &LocalAddress {
        &self.local_address
    }

    pub fn daemon_address(&self) -> &LocalAddress {
        &self.daemon_address
    }

    pub fn send_control(&self, message: ControlMessage) -> Result<(), ClientError> {
        let sent = self.transport.send_control(ControlEnvelope {
            source: self.local_address.clone(),
            destination: self.daemon_address.clone(),
            message,
        });
        sent.then_some(()).ok_or(ClientError::SendFailed)
    }

    pub fn recv_control(&self) -> Result<ControlEnvelope, ClientError> {
        self.transport
            .recv_control(&self.local_address)
            .ok_or(ClientError::NoMessageAvailable)
    }

    pub fn request_control(&self, message: ControlMessage) -> Result<ControlMessage, ClientError> {
        self.send_control(message)?;
        Ok(self.recv_control()?.message)
    }

    pub(crate) fn request_control_with<Response>(
        &self,
        message: ControlMessage,
        map: impl FnOnce(ControlMessage) -> Result<Response, ClientError>,
    ) -> Result<Response, ClientError> {
        map(self.request_control(message)?)
    }

    pub fn send_control_and_expect_accepted(
        &self,
        message: ControlMessage,
    ) -> Result<(), ClientError> {
        match self.request_control(message)? {
            ControlMessage::Accepted => Ok(()),
            ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
            _ => Err(ClientError::NoMessageAvailable),
        }
    }

    pub fn send_hello(
        &self,
        node_id: NodeId,
        desired_revision: Revision,
        observed_revision: Revision,
        applied_revision: Revision,
    ) -> Result<(), ClientError> {
        self.send_control(ControlMessage::Hello(PeerHello {
            node_id,
            desired_revision,
            desired_fingerprint: 0,
            desired_section_fingerprints: orion_control_plane::DesiredStateSectionFingerprints {
                nodes: 0,
                artifacts: 0,
                workloads: 0,
                resources: 0,
                providers: 0,
                executors: 0,
                leases: 0,
            },
            observed_revision,
            applied_revision,
            transport_binding_version: None,
            transport_binding_public_key: None,
            transport_tls_cert_pem: None,
            transport_binding_signature: None,
        }))
    }

    pub fn send_client_hello(&self) -> Result<(), ClientError> {
        self.send_control(ControlMessage::ClientHello(ClientHello {
            client_name: self.identity.name.clone().into(),
            role: self.identity.role.clone(),
        }))
    }

    pub fn request_sync(
        &self,
        node_id: NodeId,
        desired_revision: Revision,
    ) -> Result<(), ClientError> {
        self.send_control(ControlMessage::SyncRequest(SyncRequest {
            node_id,
            desired_revision,
            desired_fingerprint: 0,
            desired_summary: None,
            sections: Vec::new(),
            object_selectors: Vec::new(),
        }))
    }

    pub fn publish_mutations(&self, batch: MutationBatch) -> Result<(), ClientError> {
        self.send_control(ControlMessage::Mutations(batch))
    }

    pub fn expect_welcome(&self) -> Result<ProtocolClientSession, ClientError> {
        let envelope = self.recv_control()?;
        match envelope.message {
            ControlMessage::ClientWelcome(session) => Ok(session),
            ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
            _ => Err(ClientError::NoMessageAvailable),
        }
    }
}

pub fn default_ipc_socket_path() -> PathBuf {
    env::var(ORION_NODE_IPC_SOCKET_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|_| default_ipc_socket_path_for(default_node_id_from_env()))
}

pub fn default_ipc_stream_socket_path() -> PathBuf {
    env::var(ORION_NODE_IPC_STREAM_SOCKET_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|_| default_ipc_stream_socket_path_for(default_node_id_from_env()))
}

fn default_ipc_socket_path_for(node_id: impl AsRef<str>) -> PathBuf {
    std::env::temp_dir().join(format!(
        "orion-{}-{CONTROL_SOCKET_SUFFIX}",
        node_id.as_ref()
    ))
}

fn default_ipc_stream_socket_path_for(node_id: impl AsRef<str>) -> PathBuf {
    std::env::temp_dir().join(format!(
        "orion-{}-{CONTROL_STREAM_SOCKET_SUFFIX}",
        node_id.as_ref()
    ))
}

fn default_node_id_from_env() -> NodeId {
    env::var(ORION_NODE_ID_ENV)
        .map(NodeId::new)
        .unwrap_or_else(|_| NodeId::new(DEFAULT_NODE_ID))
}

fn default_local_address_for(identity: &ClientIdentity) -> LocalAddress {
    LocalAddress::new(format!(
        "orion-client.{}.{}",
        role_slug(&identity.role),
        identity.name
    ))
}

fn role_slug(role: &ClientRole) -> &'static str {
    match role {
        ClientRole::ControlPlane => "control-plane",
        ClientRole::Provider => "provider",
        ClientRole::Executor => "executor",
    }
}

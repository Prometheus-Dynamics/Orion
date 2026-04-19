use orion_control_plane::{
    ClientEvent, ClientEventPoll, ClientRole, ControlMessage, MutationBatch, PeerEnrollment,
    PeerIdentityUpdate, PeerTrustSnapshot, StateSnapshot, StateWatch,
};
use orion_core::{NodeId, Revision};
use orion_transport_ipc::LocalControlTransport;
use std::path::{Path, PathBuf};

use crate::{
    default_ipc_socket_path, default_ipc_stream_socket_path,
    error::ClientError,
    session::{
        ClientIdentity, ClientSession, SessionConfig, ensure_client_role, local_identity_for_role,
        local_session_config_for_role, local_session_config_with_address,
    },
    stream::ClientEventStreamSession,
};
use orion_transport_ipc::{ControlEnvelope, UnixControlClient};

#[derive(Clone)]
pub struct ControlPlaneClient<T> {
    session: ClientSession<T>,
}

pub struct ControlPlaneEventStream {
    session: ClientEventStreamSession,
}

#[derive(Clone, Debug)]
pub struct LocalControlPlaneClient {
    socket_path: PathBuf,
    identity: ClientIdentity,
    config: SessionConfig,
}

impl<T> ControlPlaneClient<T>
where
    T: LocalControlTransport,
{
    pub fn new(session: ClientSession<T>) -> Result<Self, ClientError> {
        ensure_client_role(session.identity(), ClientRole::ControlPlane)?;
        Ok(Self { session })
    }

    pub fn session(&self) -> &ClientSession<T> {
        &self.session
    }

    pub fn request_sync(
        &self,
        node_id: NodeId,
        desired_revision: Revision,
    ) -> Result<(), ClientError> {
        self.session.request_sync(node_id, desired_revision)
    }

    pub fn apply_mutations(&self, batch: MutationBatch) -> Result<(), ClientError> {
        self.session
            .send_control_and_expect_accepted(ControlMessage::Mutations(batch))
    }

    pub fn publish_snapshot(&self, snapshot: StateSnapshot) -> Result<(), ClientError> {
        self.session
            .send_control(ControlMessage::Snapshot(snapshot))
    }

    pub fn subscribe_state(&self, desired_revision: Revision) -> Result<(), ClientError> {
        self.session.request_control_with(
            ControlMessage::WatchState(StateWatch { desired_revision }),
            |message| match message {
                ControlMessage::Accepted => Ok(()),
                ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
                _ => Err(ClientError::NoMessageAvailable),
            },
        )
    }

    pub fn poll_state_events(
        &self,
        after_sequence: u64,
        max_events: u32,
    ) -> Result<Vec<ClientEvent>, ClientError> {
        self.session.request_control_with(
            ControlMessage::PollClientEvents(ClientEventPoll {
                after_sequence,
                max_events,
            }),
            |message| match message {
                ControlMessage::ClientEvents(events) => Ok(events),
                ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
                _ => Err(ClientError::NoMessageAvailable),
            },
        )
    }

    pub fn enroll_peer(&self, enrollment: PeerEnrollment) -> Result<(), ClientError> {
        self.session
            .send_control_and_expect_accepted(ControlMessage::EnrollPeer(enrollment))
    }

    pub fn query_peer_trust(&self) -> Result<PeerTrustSnapshot, ClientError> {
        self.session
            .request_control_with(ControlMessage::QueryPeerTrust, |message| match message {
                ControlMessage::PeerTrust(snapshot) => Ok(snapshot),
                ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
                _ => Err(ClientError::NoMessageAvailable),
            })
    }

    pub fn revoke_peer(&self, node_id: NodeId) -> Result<(), ClientError> {
        self.session
            .send_control_and_expect_accepted(ControlMessage::RevokePeer(node_id))
    }

    pub fn replace_peer_identity(&self, update: PeerIdentityUpdate) -> Result<(), ClientError> {
        self.session
            .send_control_and_expect_accepted(ControlMessage::ReplacePeerIdentity(update))
    }

    pub fn rotate_http_tls_identity(&self) -> Result<(), ClientError> {
        self.session
            .send_control_and_expect_accepted(ControlMessage::RotateHttpTlsIdentity)
    }
}

impl ControlPlaneEventStream {
    pub async fn connect_at_with_local_address(
        socket_path: impl AsRef<std::path::Path>,
        name: impl Into<String>,
        local_address: impl Into<String>,
    ) -> Result<Self, ClientError> {
        let identity = local_identity_for_role(name, ClientRole::ControlPlane);
        Self::connect(
            socket_path,
            identity.clone(),
            local_session_config_with_address(orion_transport_ipc::LocalAddress::new(
                local_address,
            )),
        )
        .await
    }

    pub async fn connect_at(
        socket_path: impl AsRef<std::path::Path>,
        name: impl Into<String>,
    ) -> Result<Self, ClientError> {
        let identity = local_identity_for_role(name, ClientRole::ControlPlane);
        Self::connect(
            socket_path,
            identity.clone(),
            local_session_config_for_role(&identity),
        )
        .await
    }

    pub async fn connect_default(name: impl Into<String>) -> Result<Self, ClientError> {
        Self::connect_at(default_ipc_stream_socket_path(), name).await
    }

    pub async fn connect(
        socket_path: impl AsRef<std::path::Path>,
        identity: crate::session::ClientIdentity,
        config: crate::session::SessionConfig,
    ) -> Result<Self, ClientError> {
        ensure_client_role(&identity, ClientRole::ControlPlane)?;

        Ok(Self {
            session: ClientEventStreamSession::connect(
                socket_path,
                identity,
                config,
                ClientRole::ControlPlane,
            )
            .await?,
        })
    }

    pub async fn subscribe_state(&mut self, desired_revision: Revision) -> Result<(), ClientError> {
        self.session
            .subscribe_and_expect_accepted(ControlMessage::WatchState(StateWatch {
                desired_revision,
            }))
            .await
    }

    pub async fn next_events(&mut self) -> Result<Vec<ClientEvent>, ClientError> {
        self.session.next_client_events().await
    }
}

impl LocalControlPlaneClient {
    pub fn connect_at_with_local_address(
        socket_path: impl AsRef<Path>,
        name: impl Into<String>,
        local_address: impl Into<String>,
    ) -> Result<Self, ClientError> {
        let identity = local_identity_for_role(name, ClientRole::ControlPlane);
        Self::connect(
            socket_path,
            identity.clone(),
            local_session_config_with_address(orion_transport_ipc::LocalAddress::new(
                local_address,
            )),
        )
    }

    pub fn connect_at(
        socket_path: impl AsRef<Path>,
        name: impl Into<String>,
    ) -> Result<Self, ClientError> {
        let identity = local_identity_for_role(name, ClientRole::ControlPlane);
        Self::connect(
            socket_path,
            identity.clone(),
            local_session_config_for_role(&identity),
        )
    }

    pub fn connect_default(name: impl Into<String>) -> Result<Self, ClientError> {
        Self::connect_at(default_ipc_socket_path(), name)
    }

    pub fn connect(
        socket_path: impl AsRef<Path>,
        identity: ClientIdentity,
        config: SessionConfig,
    ) -> Result<Self, ClientError> {
        ensure_client_role(&identity, ClientRole::ControlPlane)?;
        Ok(Self {
            socket_path: socket_path.as_ref().to_path_buf(),
            identity,
            config,
        })
    }

    pub fn identity(&self) -> &ClientIdentity {
        &self.identity
    }

    pub async fn request_sync(
        &self,
        node_id: NodeId,
        desired_revision: Revision,
    ) -> Result<(), ClientError> {
        self.send(ControlMessage::SyncRequest(
            orion_control_plane::SyncRequest {
                node_id,
                desired_revision,
                desired_fingerprint: 0,
                desired_summary: None,
                sections: Vec::new(),
                object_selectors: Vec::new(),
            },
        ))
        .await
    }

    pub async fn apply_mutations(&self, batch: MutationBatch) -> Result<(), ClientError> {
        match self.send_request(ControlMessage::Mutations(batch)).await? {
            ControlMessage::Accepted => Ok(()),
            ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
            _ => Err(ClientError::NoMessageAvailable),
        }
    }

    pub async fn fetch_state_snapshot(&self) -> Result<StateSnapshot, ClientError> {
        match self
            .send_request(ControlMessage::QueryStateSnapshot)
            .await?
        {
            ControlMessage::Snapshot(snapshot) => Ok(snapshot),
            ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
            _ => Err(ClientError::NoMessageAvailable),
        }
    }

    pub async fn publish_snapshot(&self, snapshot: StateSnapshot) -> Result<(), ClientError> {
        self.send(ControlMessage::Snapshot(snapshot)).await
    }

    pub async fn enroll_peer(&self, enrollment: PeerEnrollment) -> Result<(), ClientError> {
        match self
            .send_request(ControlMessage::EnrollPeer(enrollment))
            .await?
        {
            ControlMessage::Accepted => Ok(()),
            ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
            _ => Err(ClientError::NoMessageAvailable),
        }
    }

    pub async fn query_peer_trust(&self) -> Result<PeerTrustSnapshot, ClientError> {
        match self.send_request(ControlMessage::QueryPeerTrust).await? {
            ControlMessage::PeerTrust(snapshot) => Ok(snapshot),
            ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
            _ => Err(ClientError::NoMessageAvailable),
        }
    }

    pub async fn revoke_peer(&self, node_id: NodeId) -> Result<(), ClientError> {
        match self
            .send_request(ControlMessage::RevokePeer(node_id))
            .await?
        {
            ControlMessage::Accepted => Ok(()),
            ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
            _ => Err(ClientError::NoMessageAvailable),
        }
    }

    pub async fn replace_peer_identity(
        &self,
        update: PeerIdentityUpdate,
    ) -> Result<(), ClientError> {
        match self
            .send_request(ControlMessage::ReplacePeerIdentity(update))
            .await?
        {
            ControlMessage::Accepted => Ok(()),
            ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
            _ => Err(ClientError::NoMessageAvailable),
        }
    }

    pub async fn rotate_http_tls_identity(&self) -> Result<(), ClientError> {
        match self
            .send_request(ControlMessage::RotateHttpTlsIdentity)
            .await?
        {
            ControlMessage::Accepted => Ok(()),
            ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
            _ => Err(ClientError::NoMessageAvailable),
        }
    }

    async fn send(&self, message: ControlMessage) -> Result<(), ClientError> {
        self.send_request(message).await.map(|_| ())
    }

    async fn send_request(&self, message: ControlMessage) -> Result<ControlMessage, ClientError> {
        let client = UnixControlClient::new(&self.socket_path);
        let response = client
            .send(ControlEnvelope {
                source: self.config.local_address.clone(),
                destination: self.config.daemon_address.clone(),
                message: ControlMessage::ClientHello(orion_control_plane::ClientHello {
                    client_name: self.identity.name.clone().into(),
                    role: ClientRole::ControlPlane,
                }),
            })
            .await?;
        match response.message {
            ControlMessage::ClientWelcome(_) => {}
            ControlMessage::Rejected(reason) => return Err(ClientError::Rejected(reason)),
            _ => return Err(ClientError::NoMessageAvailable),
        }

        let response = client
            .send(ControlEnvelope {
                source: self.config.local_address.clone(),
                destination: self.config.daemon_address.clone(),
                message,
            })
            .await?;
        Ok(response.message)
    }
}

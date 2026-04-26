use orion_control_plane::{
    ClientEvent, ClientEventPoll, ClientHello, ClientRole, CommunicationEndpointScope,
    CommunicationEndpointSnapshot, CommunicationFailureKind, CommunicationTransportKind,
    ControlMessage, HostMetricsSnapshot, MaintenanceCommand, MaintenanceStatus, MutationBatch,
    NodeObservabilitySnapshot, PeerEnrollment, PeerIdentityUpdate, PeerTrustSnapshot,
    StateSnapshot, StateWatch, duration_ms_u64,
};
use orion_core::{ClientName, NodeId, Revision};
use orion_transport_ipc::LocalControlTransport;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex as SyncMutex},
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

use crate::response::{
    expect_accepted, expect_client_events, expect_maintenance_status, expect_observability,
    expect_peer_trust, expect_state_snapshot,
};
use crate::{
    default_ipc_stream_socket_path,
    error::ClientError,
    session::{
        ClientIdentity, ClientSession, SessionConfig, ensure_client_role, local_identity_for_role,
        local_session_config_for_role, local_session_config_with_address,
    },
    stream::ClientEventStreamSession,
};
use orion_transport_ipc::{ControlEnvelope, UnixControlClient, UnixControlStreamClient};

mod metrics;

use metrics::ClientCommunicationMetrics;

#[derive(Clone)]
pub struct ControlPlaneClient<T> {
    session: ClientSession<T>,
}

pub struct ControlPlaneEventStream {
    session: ClientEventStreamSession,
}

#[derive(Clone, Debug)]
pub struct LocalControlPlaneClient {
    transport: LocalControlPlaneTransport,
    identity: ClientIdentity,
    config: SessionConfig,
    metrics: Arc<SyncMutex<ClientCommunicationMetrics>>,
}

#[derive(Clone, Debug)]
enum LocalControlPlaneTransport {
    Unary {
        socket_path: PathBuf,
    },
    Stream {
        socket_path: PathBuf,
        session: Arc<Mutex<LocalControlPlaneStreamSession>>,
    },
}

#[derive(Default)]
struct LocalControlPlaneStreamSession {
    client: Option<UnixControlStreamClient>,
    hello_completed: bool,
}

struct MeteredControlMessage {
    message: ControlMessage,
    bytes_sent: u64,
    bytes_received: u64,
}

impl std::fmt::Debug for LocalControlPlaneStreamSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalControlPlaneStreamSession")
            .field("connected", &self.client.is_some())
            .field("hello_completed", &self.hello_completed)
            .finish()
    }
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
            expect_accepted,
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
            expect_client_events,
        )
    }

    pub fn enroll_peer(&self, enrollment: PeerEnrollment) -> Result<(), ClientError> {
        self.session
            .send_control_and_expect_accepted(ControlMessage::EnrollPeer(enrollment))
    }

    pub fn query_peer_trust(&self) -> Result<PeerTrustSnapshot, ClientError> {
        self.session
            .request_control_with(ControlMessage::QueryPeerTrust, expect_peer_trust)
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

    pub fn query_maintenance(&self) -> Result<MaintenanceStatus, ClientError> {
        self.session
            .request_control_with(ControlMessage::QueryMaintenance, expect_maintenance_status)
    }

    pub fn query_observability(&self) -> Result<NodeObservabilitySnapshot, ClientError> {
        self.session
            .request_control_with(ControlMessage::QueryObservability, expect_observability)
    }

    pub fn query_host_metrics(&self) -> Result<HostMetricsSnapshot, ClientError> {
        Ok(self.query_observability()?.host)
    }

    pub fn query_communication(&self) -> Result<Vec<CommunicationEndpointSnapshot>, ClientError> {
        Ok(self.query_observability()?.communication)
    }

    pub fn update_maintenance(
        &self,
        command: MaintenanceCommand,
    ) -> Result<MaintenanceStatus, ClientError> {
        self.session.request_control_with(
            ControlMessage::UpdateMaintenance(command),
            expect_maintenance_status,
        )
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
        let identity = local_identity_for_role(name, ClientRole::ControlPlane);
        Self::connect_stream(
            default_ipc_stream_socket_path(),
            identity.clone(),
            local_session_config_for_role(&identity),
        )
    }

    pub fn connect(
        socket_path: impl AsRef<Path>,
        identity: ClientIdentity,
        config: SessionConfig,
    ) -> Result<Self, ClientError> {
        ensure_client_role(&identity, ClientRole::ControlPlane)?;
        Ok(Self {
            transport: LocalControlPlaneTransport::Unary {
                socket_path: socket_path.as_ref().to_path_buf(),
            },
            identity,
            config,
            metrics: Arc::new(SyncMutex::new(ClientCommunicationMetrics::default())),
        })
    }

    pub fn connect_stream(
        socket_path: impl AsRef<Path>,
        identity: ClientIdentity,
        config: SessionConfig,
    ) -> Result<Self, ClientError> {
        ensure_client_role(&identity, ClientRole::ControlPlane)?;
        Ok(Self {
            transport: LocalControlPlaneTransport::Stream {
                socket_path: socket_path.as_ref().to_path_buf(),
                session: Arc::new(Mutex::new(LocalControlPlaneStreamSession::default())),
            },
            identity,
            config,
            metrics: Arc::new(SyncMutex::new(ClientCommunicationMetrics::default())),
        })
    }

    pub fn identity(&self) -> &ClientIdentity {
        &self.identity
    }

    pub fn local_communication_metrics(&self) -> CommunicationEndpointSnapshot {
        let metrics = self
            .metrics
            .lock()
            .expect("local control-plane client metrics mutex should not be poisoned")
            .snapshot();
        let (id, scope, connected) = match &self.transport {
            LocalControlPlaneTransport::Unary { .. } => (
                format!(
                    "ipc/client-local-unary/{}",
                    self.config.local_address.as_str()
                ),
                CommunicationEndpointScope::ClientLocalUnary,
                true,
            ),
            LocalControlPlaneTransport::Stream { session, .. } => {
                let connected = session
                    .try_lock()
                    .map(|session| session.client.is_some() && session.hello_completed)
                    .unwrap_or(true);
                (
                    format!(
                        "ipc/client-local-stream/{}",
                        self.config.local_address.as_str()
                    ),
                    CommunicationEndpointScope::ClientLocalStream,
                    connected,
                )
            }
        };
        CommunicationEndpointSnapshot {
            id,
            transport: CommunicationTransportKind::Ipc,
            scope,
            local: Some(self.config.local_address.to_string()),
            remote: Some(self.config.daemon_address.to_string()),
            labels: BTreeMap::from([
                ("client_name".to_owned(), self.identity.name.to_string()),
                ("role".to_owned(), "control_plane".to_owned()),
            ]),
            connected,
            queued: None,
            metrics,
        }
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
        self.send_request_with(ControlMessage::Mutations(batch), expect_accepted)
            .await
    }

    pub async fn fetch_state_snapshot(&self) -> Result<StateSnapshot, ClientError> {
        self.send_request_with(ControlMessage::QueryStateSnapshot, expect_state_snapshot)
            .await
    }

    pub async fn publish_snapshot(&self, snapshot: StateSnapshot) -> Result<(), ClientError> {
        self.send(ControlMessage::Snapshot(snapshot)).await
    }

    pub async fn enroll_peer(&self, enrollment: PeerEnrollment) -> Result<(), ClientError> {
        self.send_request_with(ControlMessage::EnrollPeer(enrollment), expect_accepted)
            .await
    }

    pub async fn query_peer_trust(&self) -> Result<PeerTrustSnapshot, ClientError> {
        self.send_request_with(ControlMessage::QueryPeerTrust, expect_peer_trust)
            .await
    }

    pub async fn query_observability(&self) -> Result<NodeObservabilitySnapshot, ClientError> {
        self.send_request_with(ControlMessage::QueryObservability, expect_observability)
            .await
    }

    pub async fn query_host_metrics(&self) -> Result<HostMetricsSnapshot, ClientError> {
        Ok(self.query_observability().await?.host)
    }

    pub async fn query_communication(
        &self,
    ) -> Result<Vec<CommunicationEndpointSnapshot>, ClientError> {
        Ok(self.query_observability().await?.communication)
    }

    pub async fn revoke_peer(&self, node_id: NodeId) -> Result<(), ClientError> {
        self.send_request_with(ControlMessage::RevokePeer(node_id), expect_accepted)
            .await
    }

    pub async fn replace_peer_identity(
        &self,
        update: PeerIdentityUpdate,
    ) -> Result<(), ClientError> {
        self.send_request_with(ControlMessage::ReplacePeerIdentity(update), expect_accepted)
            .await
    }

    pub async fn rotate_http_tls_identity(&self) -> Result<(), ClientError> {
        self.send_request_with(ControlMessage::RotateHttpTlsIdentity, expect_accepted)
            .await
    }

    pub async fn query_maintenance(&self) -> Result<MaintenanceStatus, ClientError> {
        self.send_request_with(ControlMessage::QueryMaintenance, expect_maintenance_status)
            .await
    }

    pub async fn update_maintenance(
        &self,
        command: MaintenanceCommand,
    ) -> Result<MaintenanceStatus, ClientError> {
        self.send_request_with(
            ControlMessage::UpdateMaintenance(command),
            expect_maintenance_status,
        )
        .await
    }

    async fn send(&self, message: ControlMessage) -> Result<(), ClientError> {
        self.send_request(message).await.map(|_| ())
    }

    async fn send_request_with<T>(
        &self,
        message: ControlMessage,
        expect: impl FnOnce(ControlMessage) -> Result<T, ClientError>,
    ) -> Result<T, ClientError> {
        expect(self.send_request(message).await?)
    }

    async fn send_request(&self, message: ControlMessage) -> Result<ControlMessage, ClientError> {
        match &self.transport {
            LocalControlPlaneTransport::Unary { socket_path } => {
                self.send_request_unary(socket_path, message).await
            }
            LocalControlPlaneTransport::Stream {
                socket_path,
                session,
            } => {
                self.send_request_stream(socket_path, session, message)
                    .await
            }
        }
    }

    async fn send_request_unary(
        &self,
        socket_path: &Path,
        message: ControlMessage,
    ) -> Result<ControlMessage, ClientError> {
        let client = UnixControlClient::new(socket_path);
        let response = client
            .send(ControlEnvelope {
                source: self.config.local_address.clone(),
                destination: self.config.daemon_address.clone(),
                message: ControlMessage::ClientHello(ClientHello {
                    client_name: ClientName::new(self.identity.name.clone()),
                    role: ClientRole::ControlPlane,
                }),
            })
            .await?;
        match response.message {
            ControlMessage::ClientWelcome(_) => {}
            ControlMessage::Rejected(reason) => return Err(ClientError::Rejected(reason)),
            _ => return Err(ClientError::NoMessageAvailable),
        }

        let request = ControlEnvelope {
            source: self.config.local_address.clone(),
            destination: self.config.daemon_address.clone(),
            message,
        };
        let started = Instant::now();
        let response = client.send_metered(request).await.inspect_err(|err| {
            self.record_local_request_failure(0, started.elapsed(), err);
        })?;
        self.record_local_request_success(
            response.bytes_sent.min(u64::MAX as usize) as u64,
            response.bytes_received.min(u64::MAX as usize) as u64,
            started.elapsed(),
        );
        Ok(response.envelope.message)
    }

    async fn send_request_stream(
        &self,
        socket_path: &Path,
        session: &Arc<Mutex<LocalControlPlaneStreamSession>>,
        message: ControlMessage,
    ) -> Result<ControlMessage, ClientError> {
        self.send_request_stream_inner(socket_path, session, message, true)
            .await
    }

    async fn send_request_stream_inner(
        &self,
        socket_path: &Path,
        session: &Arc<Mutex<LocalControlPlaneStreamSession>>,
        message: ControlMessage,
        allow_retry: bool,
    ) -> Result<ControlMessage, ClientError> {
        let mut stream_session = session.lock().await;
        if stream_session.client.is_none() {
            stream_session.client = Some(UnixControlStreamClient::connect(socket_path).await?);
            stream_session.hello_completed = false;
            self.record_local_reconnect();
        }
        if !stream_session.hello_completed {
            self.complete_stream_hello(&mut stream_session).await?;
        }

        let request = ControlEnvelope {
            source: self.config.local_address.clone(),
            destination: self.config.daemon_address.clone(),
            message: message.clone(),
        };
        let started = Instant::now();
        let response = match self.send_stream_request(&mut stream_session, request).await {
            Ok(response) => response,
            Err(error) if allow_retry => {
                self.record_local_request_failure(0, started.elapsed(), &error);
                stream_session.client = None;
                stream_session.hello_completed = false;
                drop(stream_session);
                return Box::pin(self.send_request_stream_inner(
                    socket_path,
                    session,
                    message,
                    false,
                ))
                .await;
            }
            Err(error) => {
                self.record_local_request_failure(0, started.elapsed(), &error);
                return Err(error.into());
            }
        };
        self.record_local_request_success(
            response.bytes_sent,
            response.bytes_received,
            started.elapsed(),
        );
        Ok(response.message)
    }

    async fn complete_stream_hello(
        &self,
        session: &mut LocalControlPlaneStreamSession,
    ) -> Result<(), ClientError> {
        let client = session.client.as_mut().ok_or(ClientError::SendFailed)?;
        client
            .send(&ControlEnvelope {
                source: self.config.local_address.clone(),
                destination: self.config.daemon_address.clone(),
                message: ControlMessage::ClientHello(ClientHello {
                    client_name: ClientName::new(self.identity.name.clone()),
                    role: ClientRole::ControlPlane,
                }),
            })
            .await?;
        match self.recv_stream_message(client).await? {
            ControlMessage::ClientWelcome(_) => {
                session.hello_completed = true;
                Ok(())
            }
            ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
            _ => Err(ClientError::NoMessageAvailable),
        }
    }

    async fn send_stream_request(
        &self,
        session: &mut LocalControlPlaneStreamSession,
        request: ControlEnvelope,
    ) -> Result<MeteredControlMessage, orion_transport_ipc::IpcTransportError> {
        let client =
            session
                .client
                .as_mut()
                .ok_or(orion_transport_ipc::IpcTransportError::WriteFailed(
                    "local control-plane stream is not connected".into(),
                ))?;
        let bytes_sent = client.send_metered(&request).await?;
        loop {
            let Some((response, bytes_received)) = client.recv_metered().await? else {
                return Err(orion_transport_ipc::IpcTransportError::ReadFailed(
                    "local control-plane stream closed".into(),
                ));
            };
            match response.message {
                ControlMessage::Ping => {
                    client
                        .send(&ControlEnvelope {
                            source: self.config.local_address.clone(),
                            destination: self.config.daemon_address.clone(),
                            message: ControlMessage::Pong,
                        })
                        .await?;
                }
                message => {
                    return Ok(MeteredControlMessage {
                        message,
                        bytes_sent: bytes_sent.min(u64::MAX as usize) as u64,
                        bytes_received: bytes_received.min(u64::MAX as usize) as u64,
                    });
                }
            }
        }
    }

    async fn recv_stream_message(
        &self,
        client: &mut UnixControlStreamClient,
    ) -> Result<ControlMessage, ClientError> {
        loop {
            let Some(response) = client.recv().await? else {
                return Err(ClientError::NoMessageAvailable);
            };
            match response.message {
                ControlMessage::Ping => {
                    client
                        .send(&ControlEnvelope {
                            source: self.config.local_address.clone(),
                            destination: self.config.daemon_address.clone(),
                            message: ControlMessage::Pong,
                        })
                        .await?;
                }
                message => return Ok(message),
            }
        }
    }

    fn record_local_request_success(
        &self,
        bytes_sent: u64,
        bytes_received: u64,
        duration: Duration,
    ) {
        self.metrics
            .lock()
            .expect("local control-plane client metrics mutex should not be poisoned")
            .record_success(bytes_sent, bytes_received, now_ms(), duration);
    }

    fn record_local_request_failure(
        &self,
        bytes_sent: u64,
        duration: Duration,
        error: &orion_transport_ipc::IpcTransportError,
    ) {
        self.metrics
            .lock()
            .expect("local control-plane client metrics mutex should not be poisoned")
            .record_failure(
                bytes_sent,
                now_ms(),
                duration,
                classify_ipc_failure(error),
                error.to_string(),
            );
    }

    fn record_local_reconnect(&self) {
        self.metrics
            .lock()
            .expect("local control-plane client metrics mutex should not be poisoned")
            .record_reconnect();
    }
}

fn classify_ipc_failure(
    error: &orion_transport_ipc::IpcTransportError,
) -> CommunicationFailureKind {
    match error {
        orion_transport_ipc::IpcTransportError::ConnectionRefused(_) => {
            CommunicationFailureKind::Refused
        }
        orion_transport_ipc::IpcTransportError::ConnectFailed(_) => {
            CommunicationFailureKind::Transport
        }
        orion_transport_ipc::IpcTransportError::BindFailed(_)
        | orion_transport_ipc::IpcTransportError::AcceptFailed(_)
        | orion_transport_ipc::IpcTransportError::ReadFailed(_)
        | orion_transport_ipc::IpcTransportError::WriteFailed(_) => {
            CommunicationFailureKind::Transport
        }
        orion_transport_ipc::IpcTransportError::EncodeFailed(_) => {
            CommunicationFailureKind::Protocol
        }
        orion_transport_ipc::IpcTransportError::DecodeFailed(_) => CommunicationFailureKind::Decode,
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(duration_ms_u64)
        .unwrap_or(0)
}

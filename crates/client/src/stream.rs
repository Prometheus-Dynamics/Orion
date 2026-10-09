use crate::{
    error::ClientError,
    session::{ClientIdentity, SessionConfig, ensure_client_role},
};
use orion_control_plane::{ClientEvent, ClientHello, ClientRole, ControlMessage};
use orion_core::{ClientName, NodeId};
use orion_transport_ipc::{ControlEnvelope, LocalAddress, UnixControlStreamClient};
use std::collections::VecDeque;
use std::path::Path;

pub(crate) struct ClientEventStreamSession {
    client: UnixControlStreamClient,
    local_address: LocalAddress,
    daemon_address: LocalAddress,
    node_id: NodeId,
    /// Event batches that arrived while a subscription waited for its response; returned by
    /// `next_client_events` before anything newer.
    pending_events: VecDeque<Vec<ClientEvent>>,
}

impl ClientEventStreamSession {
    pub(crate) async fn connect(
        socket_path: impl AsRef<Path>,
        identity: ClientIdentity,
        config: SessionConfig,
        expected_role: ClientRole,
    ) -> Result<Self, ClientError> {
        ensure_client_role(&identity, expected_role.clone())?;

        let mut client = UnixControlStreamClient::connect(socket_path).await?;
        client
            .send(&ControlEnvelope {
                source: config.local_address.clone(),
                destination: config.daemon_address.clone(),
                message: ControlMessage::ClientHello(ClientHello {
                    client_name: ClientName::new(identity.name),
                    role: expected_role,
                }),
            })
            .await?;
        let Some(response) = client.recv().await? else {
            return Err(ClientError::NoMessageAvailable);
        };
        match response.message {
            ControlMessage::ClientWelcome(session) => Ok(Self {
                client,
                local_address: config.local_address,
                daemon_address: config.daemon_address,
                node_id: session.node_id,
                pending_events: VecDeque::new(),
            }),
            ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
            _ => Err(ClientError::NoMessageAvailable),
        }
    }

    /// The node that welcomed this session.
    pub(crate) fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub(crate) async fn send(&mut self, message: ControlMessage) -> Result<(), ClientError> {
        self.client
            .send(&ControlEnvelope {
                source: self.local_address.clone(),
                destination: self.daemon_address.clone(),
                message,
            })
            .await?;
        Ok(())
    }

    pub(crate) async fn subscribe_and_expect_accepted(
        &mut self,
        message: ControlMessage,
    ) -> Result<(), ClientError> {
        self.send(message).await?;
        match self.recv_response_message().await? {
            ControlMessage::Accepted => Ok(()),
            ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
            _ => Err(ClientError::NoMessageAvailable),
        }
    }

    /// The response to the request just sent. Events pushed meanwhile (for example the bootstrap
    /// of an earlier subscription on this stream, or changes racing the request) are kept for
    /// `next_client_events` instead of being mistaken for the response.
    pub(crate) async fn recv_response_message(&mut self) -> Result<ControlMessage, ClientError> {
        loop {
            let Some(response) = self.client.recv().await? else {
                return Err(ClientError::NoMessageAvailable);
            };
            match response.message {
                ControlMessage::Ping => {
                    self.send(ControlMessage::Pong).await?;
                }
                ControlMessage::ClientEvents(events) => {
                    self.pending_events.push_back(events);
                }
                message => return Ok(message),
            }
        }
    }

    pub(crate) async fn recv_event_message(&mut self) -> Result<ControlMessage, ClientError> {
        loop {
            let Some(response) = self.client.recv_wait().await? else {
                return Err(ClientError::NoMessageAvailable);
            };
            match response.message {
                ControlMessage::Ping => {
                    self.send(ControlMessage::Pong).await?;
                }
                message => return Ok(message),
            }
        }
    }

    /// The next frame from the node, as is (pings included). Cancel-safe: it may be raced in
    /// `tokio::select!`; a partially received frame is resumed by the next read.
    pub(crate) async fn recv_frame(&mut self) -> Result<Option<ControlMessage>, ClientError> {
        Ok(self
            .client
            .recv_wait()
            .await?
            .map(|envelope| envelope.message))
    }

    /// Event batches kept while a request waited for its response.
    pub(crate) fn take_pending_events(&mut self) -> Vec<Vec<ClientEvent>> {
        self.pending_events.drain(..).collect()
    }

    pub(crate) async fn next_client_events(&mut self) -> Result<Vec<ClientEvent>, ClientError> {
        if let Some(events) = self.pending_events.pop_front() {
            return Ok(events);
        }
        match self.recv_event_message().await? {
            ControlMessage::ClientEvents(events) => Ok(events),
            ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
            _ => Err(ClientError::NoMessageAvailable),
        }
    }
}

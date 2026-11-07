//! [`StreamPublisher`]: declares live streams and publishes their samples (`docs/streams.md`).

use crate::{
    error::ClientError,
    session::{
        default_ipc_stream_socket_path, local_identity_for_role, local_session_config_for_role,
    },
    stream::ClientEventStreamSession,
};
use orion_control_plane::{
    ClientEventKind, ClientRole, ControlMessage, SampleChunk, StreamDecl, StreamDemand,
    StreamPublish, StreamRef,
};
use orion_core::NodeId;
use std::collections::VecDeque;
use tokio::sync::{mpsc, oneshot};

/// One stream connection to the local node that declares streams and publishes their batches.
///
/// It connects as an executor client (streams under `node/<this node>` need no executor or
/// provider record; streams under one need the record published by this client). The node tells
/// it which series have subscribers ([`Self::next_demand`]), for series declared
/// `Sampling::WhileSubscribed`. After a reconnect, declare the streams again: the node treats the
/// new connection as a new publisher (a new epoch, new history).
pub struct StreamPublisher {
    node_id: NodeId,
    requests: mpsc::Sender<Request>,
    demand_changes: mpsc::UnboundedReceiver<StreamDemand>,
    demands: VecDeque<StreamDemand>,
    driver: tokio::task::JoinHandle<()>,
}

/// A request for the session's driver: the message, and where its answer goes.
type Request = (ControlMessage, oneshot::Sender<Result<(), ClientError>>);

impl std::fmt::Debug for StreamPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamPublisher")
            .field("node_id", &self.node_id)
            .finish()
    }
}

impl Drop for StreamPublisher {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

impl StreamPublisher {
    /// Connects to the node's stream socket at `socket_path` as client `name`.
    ///
    /// A background task owns the connection: it answers the node's heartbeats and takes in
    /// demand changes while the publisher is idle, so a publisher that publishes rarely keeps its
    /// session. Needs a Tokio runtime.
    pub async fn connect_at(
        socket_path: impl AsRef<std::path::Path>,
        name: impl Into<String>,
    ) -> Result<Self, ClientError> {
        let identity = local_identity_for_role(name, ClientRole::Executor);
        let config = local_session_config_for_role(&identity);
        let session =
            ClientEventStreamSession::connect(socket_path, identity, config, ClientRole::Executor)
                .await?;
        let node_id = session.node_id().clone();
        let (requests, request_rx) = mpsc::channel(1);
        let (demand_tx, demand_changes) = mpsc::unbounded_channel();
        let driver = tokio::spawn(drive(session, request_rx, demand_tx));
        Ok(Self {
            node_id,
            requests,
            demand_changes,
            demands: VecDeque::new(),
            driver,
        })
    }

    /// [`Self::connect_at`] the default stream socket.
    pub async fn connect_default(name: impl Into<String>) -> Result<Self, ClientError> {
        Self::connect_at(default_ipc_stream_socket_path(), name).await
    }

    /// The node this publisher is connected to (its streams under `node/<id>`).
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Declares `decl` (again: the same declaration changes nothing; other series start a new
    /// epoch).
    pub async fn declare(&mut self, decl: StreamDecl) -> Result<(), ClientError> {
        self.request(ControlMessage::DeclareStream(decl)).await
    }

    /// Publishes one batch: one chunk per series that has new samples.
    pub async fn publish(
        &mut self,
        stream: &StreamRef,
        chunks: Vec<SampleChunk>,
    ) -> Result<(), ClientError> {
        if chunks.is_empty() {
            return Ok(());
        }
        self.request(ControlMessage::PublishSamples(StreamPublish {
            stream: stream.clone(),
            chunks,
        }))
        .await
    }

    /// The demand changes that arrived so far, oldest first, without waiting. A stream's newer
    /// change replaces its older one.
    pub fn take_demands(&mut self) -> Vec<StreamDemand> {
        self.collect_pending();
        self.demands.drain(..).collect()
    }

    /// The next demand change; waits for one. Cancel-safe. An error when the session ended.
    pub async fn next_demand(&mut self) -> Result<StreamDemand, ClientError> {
        self.collect_pending();
        if let Some(demand) = self.demands.pop_front() {
            return Ok(demand);
        }
        self.demand_changes
            .recv()
            .await
            .ok_or(ClientError::NoMessageAvailable)
    }

    async fn request(&mut self, message: ControlMessage) -> Result<(), ClientError> {
        let (reply, answer) = oneshot::channel();
        self.requests
            .send((message, reply))
            .await
            .map_err(|_| ClientError::SendFailed)?;
        answer.await.map_err(|_| ClientError::NoMessageAvailable)?
    }

    fn collect_pending(&mut self) {
        while let Ok(demand) = self.demand_changes.try_recv() {
            self.demands.retain(|queued| queued.stream != demand.stream);
            self.demands.push_back(demand);
        }
    }
}

/// The connection's task: serves requests one at a time and, between them, answers heartbeats
/// and passes demand changes on. Ends when the publisher is dropped or the connection closes.
async fn drive(
    mut session: ClientEventStreamSession,
    mut requests: mpsc::Receiver<Request>,
    demands: mpsc::UnboundedSender<StreamDemand>,
) {
    let forward = |events: Vec<orion_control_plane::ClientEvent>| {
        for event in events {
            if let ClientEventKind::StreamDemand(demand) = event.event {
                let _ = demands.send(demand);
            }
        }
    };
    loop {
        tokio::select! {
            request = requests.recv() => {
                let Some((message, reply)) = request else { return };
                let result = session.subscribe_and_expect_accepted(message).await;
                for events in session.take_pending_events() {
                    forward(events);
                }
                let closed = matches!(result, Err(ClientError::NoMessageAvailable | ClientError::Ipc(_)));
                let _ = reply.send(result);
                if closed {
                    return;
                }
            }
            frame = session.recv_frame() => match frame {
                Ok(Some(ControlMessage::Ping)) => {
                    if session.send(ControlMessage::Pong).await.is_err() {
                        return;
                    }
                }
                Ok(Some(ControlMessage::ClientEvents(events))) => forward(events),
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => return,
            },
        }
    }
}

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

/// One stream connection to the local node that declares streams and publishes their batches.
///
/// It connects as an executor client (streams under `node/<this node>` need no executor or
/// provider record; streams under one need the record published by this client). The node tells
/// it which series have subscribers ([`Self::next_demand`]), for series declared
/// `Sampling::WhileSubscribed`. After a reconnect, declare the streams again: the node treats the
/// new connection as a new publisher (a new epoch, new history).
pub struct StreamPublisher {
    session: ClientEventStreamSession,
    demands: VecDeque<StreamDemand>,
}

impl std::fmt::Debug for StreamPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamPublisher")
            .field("node_id", self.session.node_id())
            .finish()
    }
}

impl StreamPublisher {
    /// Connects to the node's stream socket at `socket_path` as client `name`.
    pub async fn connect_at(
        socket_path: impl AsRef<std::path::Path>,
        name: impl Into<String>,
    ) -> Result<Self, ClientError> {
        let identity = local_identity_for_role(name, ClientRole::Executor);
        let config = local_session_config_for_role(&identity);
        Ok(Self {
            session: ClientEventStreamSession::connect(
                socket_path,
                identity,
                config,
                ClientRole::Executor,
            )
            .await?,
            demands: VecDeque::new(),
        })
    }

    /// [`Self::connect_at`] the default stream socket.
    pub async fn connect_default(name: impl Into<String>) -> Result<Self, ClientError> {
        Self::connect_at(default_ipc_stream_socket_path(), name).await
    }

    /// The node this publisher is connected to (its streams under `node/<id>`).
    pub fn node_id(&self) -> &NodeId {
        self.session.node_id()
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

    /// The demand changes that arrived so far, oldest first, without waiting.
    pub fn take_demands(&mut self) -> Vec<StreamDemand> {
        self.collect_pending();
        self.demands.drain(..).collect()
    }

    /// The next demand change; waits for one. Cancel-safe.
    pub async fn next_demand(&mut self) -> Result<StreamDemand, ClientError> {
        loop {
            self.collect_pending();
            if let Some(demand) = self.demands.pop_front() {
                return Ok(demand);
            }
            let events = self.session.next_client_events().await?;
            self.keep_demands(events);
        }
    }

    async fn request(&mut self, message: ControlMessage) -> Result<(), ClientError> {
        let result = self.session.subscribe_and_expect_accepted(message).await;
        self.collect_pending();
        result
    }

    fn collect_pending(&mut self) {
        for events in self.session.take_pending_events() {
            self.keep_demands(events);
        }
    }

    fn keep_demands(&mut self, events: Vec<orion_control_plane::ClientEvent>) {
        for event in events {
            if let ClientEventKind::StreamDemand(demand) = event.event {
                self.demands.retain(|queued| queued.stream != demand.stream);
                self.demands.push_back(demand);
            }
        }
    }
}

//! [`RemoteSession`]: a push session with one node (`docs/remote-operator.md`, "Push sessions").
//!
//! A session has its own `orion+tcp` connection. After `OpenSession` the node keeps it open and
//! pushes the events of the session's watches (status changes, action results, state
//! snapshots) as they happen; the session's requests are answered on the same connection in
//! order. Every frame from the node is signed: responses bound to their request, pushes bound to
//! the `OpenSession` request with a sequence number that must move on.

use super::{
    RemoteError, RemoteOperator,
    operator::{decode_payload, encode_signed, unexpected},
};
use orion_auth::{
    crypto::verify_peer_response,
    peer_tcp::{
        PEER_TCP_STATUS_OK, PEER_TCP_STATUS_PUSH, PeerTcpResponseFrame, peer_tcp_authority,
    },
};
use orion_control_plane::{
    ActionQuery, ClientEvent, ControlMessage, SessionOpen, SessionWelcome, StateWatch, StatusQuery,
    StreamRef, StreamSubscription,
};
use orion_core::{NodeId, decode_from_slice};
use orion_transport_http::HttpResponsePayload;
use orion_transport_ipc::{ControlFrameReadState, IpcTransportError, write_control_payload_frame};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::{TcpStream, tcp::OwnedWriteHalf},
    sync::{mpsc, oneshot},
};

/// Pushed events waiting for [`RemoteSession::next_event`] before the reader stops reading.
const EVENT_BUFFER: usize = 1024;

type Pending = VecDeque<(
    Vec<u8>,
    oneshot::Sender<Result<HttpResponsePayload, RemoteError>>,
)>;

struct Shared {
    node_id: NodeId,
    node_key: [u8; 32],
    pending: Mutex<Pending>,
}

/// A push session with one node. Dropping it closes the connection; the node forgets the
/// session's watches. A closed session does not reconnect: open a new one and watch again.
pub struct RemoteSession {
    operator: RemoteOperator,
    welcome: SessionWelcome,
    shared: Arc<Shared>,
    writer: Arc<tokio::sync::Mutex<OwnedWriteHalf>>,
    events: mpsc::Receiver<ClientEvent>,
    max_payload_bytes: usize,
    io_timeout: Duration,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl std::fmt::Debug for RemoteSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteSession")
            .field("node_id", &self.shared.node_id)
            .field("session_id", &self.welcome.session_id)
            .finish()
    }
}

impl Drop for RemoteSession {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl RemoteOperator {
    /// Opens a push session with the node on a new connection (needs an enrolled operator with
    /// read access, or allowed actions for action results). `client_name` shows in the node's
    /// observability.
    pub async fn open_session(&self, client_name: &str) -> Result<RemoteSession, RemoteError> {
        let authority = peer_tcp_authority(self.url()).map_err(RemoteError::InvalidUrl)?;
        let io_timeout = self.config().io_timeout;
        let max_payload_bytes = self.config().max_payload_bytes;
        let stream = tokio::time::timeout(io_timeout, TcpStream::connect(authority))
            .await
            .map_err(|_| RemoteError::Transport(format!("connecting to {authority} timed out")))?
            .map_err(|err| RemoteError::Transport(format!("connecting to {authority}: {err}")))?;
        let _ = stream.set_nodelay(true);
        let (mut reader, mut writer) = stream.into_split();
        let shared = Arc::new(Shared {
            node_id: self.node_id().clone(),
            node_key: self.node_public_key(),
            pending: Mutex::new(VecDeque::new()),
        });

        let open = encode_signed(
            self.identity(),
            ControlMessage::OpenSession(SessionOpen {
                client_name: client_name.to_owned(),
            }),
        )?;
        write_frame(&mut writer, &open, max_payload_bytes, io_timeout).await?;
        let mut read_state = ControlFrameReadState::new();
        let first = tokio::time::timeout(
            io_timeout,
            read_state.read_payload(&mut reader, max_payload_bytes),
        )
        .await
        .map_err(|_| RemoteError::Transport("the node did not answer OpenSession".into()))?
        .map_err(frame_error)?
        .ok_or_else(|| RemoteError::Transport("the node closed the session".into()))?;
        let welcome = match verified_response(&shared, &open, &first)? {
            HttpResponsePayload::Session(welcome) => welcome,
            other => return Err(unexpected("OpenSession", &other)),
        };

        let (events_tx, events) = mpsc::channel(EVENT_BUFFER);
        let reader_shared = shared.clone();
        let reader_task = tokio::spawn(async move {
            let mut last_seq = 0u64;
            loop {
                let payload = match read_state
                    .read_payload(&mut reader, max_payload_bytes)
                    .await
                {
                    Ok(Some(payload)) => payload,
                    Ok(None) | Err(_) => break,
                };
                let Ok(frame) = PeerTcpResponseFrame::decode(&payload) else {
                    break;
                };
                if frame.status == PEER_TCP_STATUS_PUSH {
                    match verified_push(&reader_shared, &open, &frame, last_seq) {
                        Ok((seq, pushed)) => {
                            last_seq = seq;
                            for event in pushed {
                                if events_tx.send(event).await.is_err() {
                                    return;
                                }
                            }
                        }
                        // A push that doesn't verify ends the session: nothing after it can be
                        // trusted to be complete.
                        Err(_) => break,
                    }
                } else {
                    let next = reader_shared
                        .pending
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .pop_front();
                    let Some((request, reply)) = next else {
                        break;
                    };
                    let _ = reply.send(verified_frame(&reader_shared, &request, &frame));
                }
            }
            // Fail what is still waiting.
            let waiting: Vec<_> = reader_shared
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .drain(..)
                .collect();
            for (_, reply) in waiting {
                let _ = reply.send(Err(RemoteError::Transport("the session closed".into())));
            }
        });

        let mut session = RemoteSession {
            operator: self.clone(),
            welcome,
            shared,
            writer: Arc::new(tokio::sync::Mutex::new(writer)),
            events,
            max_payload_bytes,
            io_timeout,
            tasks: vec![reader_task],
        };
        session.start_heartbeat();
        Ok(session)
    }
}

impl RemoteSession {
    /// The session's id on the node.
    pub fn session_id(&self) -> &str {
        &self.welcome.session_id
    }

    /// The node the session is with.
    pub fn node_id(&self) -> &NodeId {
        &self.shared.node_id
    }

    /// Pushes coalesced changes of the status entries matching `query`: first a bootstrap event
    /// with every current entry, then [`orion_control_plane::StatusChange`]s. A new query
    /// replaces the session's previous one.
    pub async fn watch_status(&self, query: StatusQuery) -> Result<(), RemoteError> {
        self.accepted(ControlMessage::WatchStatus(query)).await
    }

    /// Pushes the results of the actions matching `query` as they change.
    pub async fn watch_actions(&self, query: ActionQuery) -> Result<(), RemoteError> {
        self.accepted(ControlMessage::WatchActions(query)).await
    }

    /// Pushes a state snapshot when the desired state moves on (and, with
    /// `include_observed`, when the observed state changes).
    pub async fn watch_state(&self, watch: StateWatch) -> Result<(), RemoteError> {
        self.accepted(ControlMessage::WatchState(watch)).await
    }

    /// Subscribes to a live stream (`docs/streams.md`): a `StreamCatalog` event, the backfill
    /// the subscription asks for (`StreamData` with `backfill`), then a `StreamData` for every
    /// batch. A stream not declared yet starts when its publisher declares it. Resubscribing
    /// (for example `Backfill::Since` the last cursor after a gap) replaces the subscription.
    pub async fn subscribe_stream(
        &self,
        subscription: StreamSubscription,
    ) -> Result<(), RemoteError> {
        self.accepted(ControlMessage::SubscribeStream(subscription))
            .await
    }

    pub async fn unsubscribe_stream(&self, stream: StreamRef) -> Result<(), RemoteError> {
        self.accepted(ControlMessage::UnsubscribeStream(stream))
            .await
    }

    /// The next pushed event; `None` once the session has closed.
    pub async fn next_event(&mut self) -> Option<ClientEvent> {
        self.events.recv().await
    }

    async fn accepted(&self, message: ControlMessage) -> Result<(), RemoteError> {
        match self.request(message).await? {
            HttpResponsePayload::Accepted => Ok(()),
            other => Err(unexpected("session request", &other)),
        }
    }

    async fn request(&self, message: ControlMessage) -> Result<HttpResponsePayload, RemoteError> {
        request_on(
            &self.writer,
            &self.shared,
            self.operator.identity(),
            message,
            self.max_payload_bytes,
            self.io_timeout,
        )
        .await
    }

    /// Pings well within the node's idle timeout, so a quiet session stays open.
    fn start_heartbeat(&mut self) {
        let interval = Duration::from_millis(self.welcome.idle_timeout_ms.max(3_000) / 3);
        let writer = self.writer.clone();
        let shared = self.shared.clone();
        let identity = self.operator.identity().clone();
        let (max_payload_bytes, io_timeout) = (self.max_payload_bytes, self.io_timeout);
        self.tasks.push(tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.tick().await;
            loop {
                tick.tick().await;
                let pinged = request_on(
                    &writer,
                    &shared,
                    &identity,
                    ControlMessage::Ping,
                    max_payload_bytes,
                    io_timeout,
                )
                .await;
                if pinged.is_err() {
                    return;
                }
            }
        }));
    }
}

/// Sends one signed request on the session and waits for its verified answer.
async fn request_on(
    writer: &tokio::sync::Mutex<OwnedWriteHalf>,
    shared: &Shared,
    identity: &super::OperatorIdentity,
    message: ControlMessage,
    max_payload_bytes: usize,
    io_timeout: Duration,
) -> Result<HttpResponsePayload, RemoteError> {
    let request = encode_signed(identity, message)?;
    let (reply, answer) = oneshot::channel();
    {
        // Queued and written under the writer lock: the node answers in the order it reads.
        let mut writer = writer.lock().await;
        shared
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push_back((request.clone(), reply));
        write_frame(&mut writer, &request, max_payload_bytes, io_timeout).await?;
    }
    tokio::time::timeout(io_timeout, answer)
        .await
        .map_err(|_| RemoteError::Transport("the node did not answer on the session".into()))?
        .map_err(|_| RemoteError::Transport("the session closed".into()))?
}

async fn write_frame(
    writer: &mut OwnedWriteHalf,
    payload: &[u8],
    max_payload_bytes: usize,
    io_timeout: Duration,
) -> Result<(), RemoteError> {
    tokio::time::timeout(
        io_timeout,
        write_control_payload_frame(writer, payload, max_payload_bytes),
    )
    .await
    .map_err(|_| RemoteError::Transport("writing to the session timed out".into()))?
    .map(|_| ())
    .map_err(frame_error)
}

fn frame_error(err: IpcTransportError) -> RemoteError {
    match err {
        IpcTransportError::ProtocolMismatch { local, remote } => {
            RemoteError::ProtocolMismatch { local, remote }
        }
        other => RemoteError::Transport(other.to_string()),
    }
}

/// A response frame's verified payload.
fn verified_response(
    shared: &Shared,
    request: &[u8],
    payload: &[u8],
) -> Result<HttpResponsePayload, RemoteError> {
    let frame = PeerTcpResponseFrame::decode(payload)
        .map_err(|err| RemoteError::Decode(err.to_string()))?;
    verified_frame(shared, request, &frame)
}

fn verified_frame(
    shared: &Shared,
    request: &[u8],
    frame: &PeerTcpResponseFrame,
) -> Result<HttpResponsePayload, RemoteError> {
    let signature = frame.signature.as_ref().ok_or_else(|| {
        RemoteError::NodeAuthentication(format!(
            "node {} sent an unsigned response",
            shared.node_id
        ))
    })?;
    verify_peer_response(
        &shared.node_id,
        &shared.node_key,
        request,
        frame.status,
        &frame.body,
        signature,
    )
    .map_err(|err| RemoteError::NodeAuthentication(err.to_string()))?;
    if frame.status != PEER_TCP_STATUS_OK {
        return Err(RemoteError::Rejected(
            String::from_utf8_lossy(&frame.body).into_owned(),
        ));
    }
    decode_payload(&frame.body)
}

/// A push frame's sequence number and events, verified against the session's `OpenSession`
/// request; its number must move past `last_seq`.
fn verified_push(
    shared: &Shared,
    open: &[u8],
    frame: &PeerTcpResponseFrame,
    last_seq: u64,
) -> Result<(u64, Vec<ClientEvent>), RemoteError> {
    let signature = frame.signature.as_ref().ok_or_else(|| {
        RemoteError::NodeAuthentication(format!("node {} pushed an unsigned event", shared.node_id))
    })?;
    verify_peer_response(
        &shared.node_id,
        &shared.node_key,
        open,
        frame.status,
        &frame.body,
        signature,
    )
    .map_err(|err| RemoteError::NodeAuthentication(err.to_string()))?;
    let (seq, message) = frame
        .body
        .split_first_chunk::<8>()
        .ok_or_else(|| RemoteError::Decode("a push event without its sequence number".into()))?;
    let seq = u64::from_le_bytes(*seq);
    if seq <= last_seq {
        return Err(RemoteError::NodeAuthentication(format!(
            "push event {seq} replayed or out of order (after {last_seq})"
        )));
    }
    match decode_from_slice::<ControlMessage>(message)
        .map_err(|err| RemoteError::Decode(err.to_string()))?
    {
        ControlMessage::ClientEvents(events) => Ok((seq, events)),
        _ => Ok((seq, Vec::new())),
    }
}

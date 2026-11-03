//! The forwarder's push client (`docs/actions.md`, "Peer watches").
//!
//! After a forwarded `RunAction` answered that the action is still running, the node asks the
//! owner, over one `orion+tcp` push session per owner peer, to watch that action. The owner pushes
//! its result as it changes, so the forwarder does not resend the request. The session opens on
//! first use and closes once nothing has been watched for [`PUSH_IDLE_CLOSE`]. Whatever the
//! session cannot deliver (refused, dropped, a push that does not verify) falls back to the resend
//! in `forward.rs`.
//!
//! Every pushed frame is checked against the session's own `OpenSession` request (the owner signs
//! pushes over it), its sequence number must move on, and only results of actions this session
//! watches are applied. The request and response framing is the one peer sync and remote sessions
//! use; this module adds no message.

use super::{NodeApp, lock};
use crate::peer::{PeerState, peer_tcp_authority};
use crate::peer_tcp::{ResponseFrame, STATUS_OK};
use orion::{
    NodeId,
    control_plane::{ActionQuery, ClientEventKind, ControlMessage, SessionOpen},
    decode_from_slice,
    transport::http::{HttpCodec, HttpRequestPayload, HttpResponsePayload},
};
use orion_auth::peer_tcp::PEER_TCP_STATUS_PUSH;
use orion_transport_ipc::{ControlFrameReadState, write_control_payload_frame};
use std::{
    collections::{BTreeSet, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
};
use tracing::debug;

/// A push session with nothing watched for this long is closed.
pub(crate) const PUSH_IDLE_CLOSE: Duration = Duration::from_secs(60);
/// Keep-alive interval of an open session (the owner closes a silent one after 30 s).
const PING_INTERVAL: Duration = Duration::from_secs(10);
/// Most actions one session watches at once; more fall back to the resend.
const MAX_WATCHED_PER_SESSION: usize = 256;
/// Bound on queued watch requests and unread frames.
const QUEUE_CAPACITY: usize = 32;

/// The entry of one owner's session in the forwarder's session table.
pub(crate) struct PushSession {
    commands: mpsc::Sender<PushCommand>,
    /// Cleared when the session task ends; waiters on a forwarded action check it.
    alive: Arc<AtomicBool>,
}

enum PushCommand {
    /// Watch one action; `reply` gets the owner's answer to the watch request.
    Watch {
        action_id: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
}

/// A request sent on the session and the answer it waits for.
struct Awaiting {
    request: Vec<u8>,
    /// A watch's action id and reply; `None` for a keep-alive ping.
    watch: Option<(String, oneshot::Sender<Result<(), String>>)>,
}

/// What one session task knows: the owner, the request its pushes are bound to, the watched
/// actions, and the requests waiting for their answers.
struct SessionState {
    owner: NodeId,
    open_request: Vec<u8>,
    awaiting: VecDeque<Awaiting>,
    watched: BTreeSet<String>,
    last_seq: u64,
}

impl NodeApp {
    /// Asks the owner's push session (opened on first use) to watch `action_id`. Returns the
    /// session's liveness flag once the owner accepted the watch. An error means the session
    /// cannot take the watch, and the caller resends the request instead.
    pub(super) async fn push_watch(
        &self,
        owner: &NodeId,
        peer: &PeerState,
        action_id: &str,
    ) -> Result<Arc<AtomicBool>, String> {
        #[cfg(test)]
        if self.state.actions.push_disabled.load(Ordering::Relaxed) {
            return Err("push sessions are disabled for this test".to_owned());
        }
        let (commands, alive) = {
            let mut sessions = lock(&self.state.actions.push_sessions);
            match sessions.get(owner) {
                Some(session) if session.alive.load(Ordering::Acquire) => {
                    (session.commands.clone(), session.alive.clone())
                }
                _ => {
                    let (commands, receiver) = mpsc::channel(QUEUE_CAPACITY);
                    let alive = Arc::new(AtomicBool::new(true));
                    sessions.insert(
                        owner.clone(),
                        PushSession {
                            commands: commands.clone(),
                            alive: alive.clone(),
                        },
                    );
                    let app = self.clone();
                    let (owner, peer, task_alive) = (owner.clone(), peer.clone(), alive.clone());
                    tokio::spawn(async move {
                        app.run_push_session(owner, peer, receiver, task_alive)
                            .await;
                    });
                    (commands, alive)
                }
            }
        };
        let (reply, answer) = oneshot::channel();
        commands
            .send(PushCommand::Watch {
                action_id: action_id.to_owned(),
                reply,
            })
            .await
            .map_err(|_| "the push session closed".to_owned())?;
        match answer.await {
            Ok(Ok(())) => Ok(alive),
            Ok(Err(reason)) => Err(reason),
            Err(_) => Err("the push session closed".to_owned()),
        }
    }

    /// Waits until `action_id` is final (its result arrives by push and is applied to the registry
    /// on the way), the session dies, or `deadline` passes. `true` when the action is final.
    pub(super) async fn await_pushed_result(
        &self,
        action_id: &str,
        alive: &AtomicBool,
        deadline: tokio::time::Instant,
    ) -> bool {
        loop {
            // Created before the checks, so a change between them is not missed.
            let changed = self.state.actions.changed.notified();
            if !self.action_is_running(action_id) {
                return true;
            }
            if !alive.load(Ordering::Acquire) || tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::select! {
                () = changed => {}
                () = tokio::time::sleep_until(deadline) => {}
            }
        }
    }

    /// Test switch: forwarders stop opening push sessions, so forwarding uses the resend.
    #[cfg(test)]
    pub(crate) fn disable_push_for_test(&self) {
        self.state
            .actions
            .push_disabled
            .store(true, Ordering::Relaxed);
    }

    /// Test readout: `RunAction` resends made by forwarders since start.
    #[cfg(test)]
    pub(crate) fn action_forward_resends_for_test(&self) -> u64 {
        self.state.actions.resends.load(Ordering::Relaxed)
    }

    async fn run_push_session(
        &self,
        owner: NodeId,
        peer: PeerState,
        mut commands: mpsc::Receiver<PushCommand>,
        alive: Arc<AtomicBool>,
    ) {
        match self.push_session_loop(&owner, &peer, &mut commands).await {
            Ok(()) => {
                debug!(node = %self.config.node_id, owner = %owner, "push session closed when idle")
            }
            Err(reason) => {
                debug!(node = %self.config.node_id, owner = %owner, reason = %reason, "push session ended; forwarded actions resend")
            }
        }
        alive.store(false, Ordering::Release);
        self.state.actions.changed.notify_waiters();
        {
            let mut sessions = lock(&self.state.actions.push_sessions);
            if sessions
                .get(&owner)
                .is_some_and(|session| Arc::ptr_eq(&session.alive, &alive))
            {
                sessions.remove(&owner);
            }
        }
        commands.close();
        while let Ok(PushCommand::Watch { reply, .. }) = commands.try_recv() {
            let _ = reply.send(Err("the push session closed".to_owned()));
        }
    }

    async fn push_session_loop(
        &self,
        owner: &NodeId,
        peer: &PeerState,
        commands: &mut mpsc::Receiver<PushCommand>,
    ) -> Result<(), String> {
        let tuning = &self.config.runtime_tuning;
        let io_timeout = tuning.transport_io_timeout;
        let max_payload = tuning.transport_max_payload_bytes;
        let authority = peer_tcp_authority(peer.base_url.as_str())?;
        let stream = tokio::time::timeout(io_timeout, TcpStream::connect(authority))
            .await
            .map_err(|_| "connecting timed out".to_owned())?
            .map_err(|err| err.to_string())?;
        let _ = stream.set_nodelay(true);
        let (mut reader, mut writer) = stream.into_split();

        // OpenSession: the signed request the owner signs every push over.
        let open_request = self
            .sign_request(ControlMessage::OpenSession(SessionOpen {
                client_name: format!("node:{}", self.config.node_id),
            }))
            .await?;
        send_frame(&mut writer, &open_request, io_timeout, max_payload).await?;
        let mut read_state = ControlFrameReadState::new();
        let welcome = tokio::time::timeout(
            io_timeout,
            read_state.read_payload(&mut reader, max_payload),
        )
        .await
        .map_err(|_| "waiting for the session welcome timed out".to_owned())?
        .map_err(|err| err.to_string())?
        .ok_or_else(|| "the owner closed the connection before the welcome".to_owned())?;
        let frame = ResponseFrame::decode(&welcome).map_err(|err| err.to_string())?;
        self.security
            .verify_peer_response(
                owner,
                &open_request,
                frame.status,
                &frame.body,
                frame.signature.as_ref(),
            )
            .map_err(|err| err.to_string())?;
        if frame.status != STATUS_OK {
            return Err(format!(
                "the owner refused the session: {}",
                String::from_utf8_lossy(&frame.body)
            ));
        }
        match decode_from_slice::<HttpResponsePayload>(&frame.body)
            .map_err(|err| err.to_string())?
        {
            HttpResponsePayload::Session(_) => {}
            _ => return Err("OpenSession was not answered with a session".to_owned()),
        }

        // Frames are read on their own task, so a request and a push never wait on each other.
        let (frames_tx, mut frames) = mpsc::channel::<Vec<u8>>(QUEUE_CAPACITY);
        let reader_task = tokio::spawn(async move {
            while let Ok(Some(bytes)) = read_state.read_payload(&mut reader, max_payload).await {
                if frames_tx.send(bytes).await.is_err() {
                    break;
                }
            }
        });

        let mut state = SessionState {
            owner: owner.clone(),
            open_request,
            awaiting: VecDeque::new(),
            watched: BTreeSet::new(),
            last_seq: 0,
        };
        let mut last_activity = Instant::now();
        let mut keep_alive = tokio::time::interval(PING_INTERVAL);
        let outcome = loop {
            tokio::select! {
                command = commands.recv() => {
                    let Some(PushCommand::Watch { action_id, reply }) = command else {
                        break Ok(());
                    };
                    last_activity = Instant::now();
                    if state.watched.len() >= MAX_WATCHED_PER_SESSION {
                        let _ = reply.send(Err("the session watches as many actions as it may".to_owned()));
                        continue;
                    }
                    state.watched.insert(action_id.clone());
                    let message = ControlMessage::WatchActions(
                        ActionQuery::action(action_id.clone()).forwarded_by(self.config.node_id.clone()),
                    );
                    let sent = match self.sign_request(message).await {
                        Ok(request) => send_frame(&mut writer, &request, io_timeout, max_payload)
                            .await
                            .map(|()| request),
                        Err(err) => Err(err),
                    };
                    match sent {
                        Ok(request) => state.awaiting.push_back(Awaiting {
                            request,
                            watch: Some((action_id, reply)),
                        }),
                        Err(err) => {
                            state.watched.remove(&action_id);
                            let _ = reply.send(Err(err.clone()));
                            break Err(err);
                        }
                    }
                }
                frame = frames.recv() => {
                    let Some(bytes) = frame else {
                        break Err("the connection closed".to_owned());
                    };
                    if let Err(err) = self.handle_push_frame(&mut state, &bytes) {
                        break Err(err);
                    }
                    last_activity = Instant::now();
                }
                _ = keep_alive.tick() => {
                    if state.watched.is_empty()
                        && state.awaiting.is_empty()
                        && last_activity.elapsed() >= PUSH_IDLE_CLOSE
                    {
                        break Ok(());
                    }
                    let request = match self.sign_request(ControlMessage::Ping).await {
                        Ok(request) => request,
                        Err(err) => break Err(err),
                    };
                    if let Err(err) = send_frame(&mut writer, &request, io_timeout, max_payload).await {
                        break Err(err);
                    }
                    state.awaiting.push_back(Awaiting { request, watch: None });
                }
            }
        };
        reader_task.abort();
        // Watches still waiting for an answer learn it from the closed session.
        for pending in state.awaiting.drain(..) {
            if let Some((_, reply)) = pending.watch {
                let _ = reply.send(Err("the push session closed".to_owned()));
            }
        }
        outcome
    }

    /// Verifies one frame from the owner and applies it: a push (its events, bound to the open
    /// request and in sequence) or the answer to the oldest request sent on the session.
    fn handle_push_frame(&self, state: &mut SessionState, bytes: &[u8]) -> Result<(), String> {
        let frame = ResponseFrame::decode(bytes).map_err(|err| err.to_string())?;
        if frame.status == PEER_TCP_STATUS_PUSH {
            self.security
                .verify_peer_response(
                    &state.owner,
                    &state.open_request,
                    frame.status,
                    &frame.body,
                    frame.signature.as_ref(),
                )
                .map_err(|err| err.to_string())?;
            let (seq, message) = frame
                .body
                .split_first_chunk::<8>()
                .ok_or_else(|| "a push without its sequence number".to_owned())?;
            let seq = u64::from_le_bytes(*seq);
            if seq <= state.last_seq {
                return Err(format!("push {seq} replayed or out of order"));
            }
            state.last_seq = seq;
            let ControlMessage::ClientEvents(events) =
                decode_from_slice::<ControlMessage>(message).map_err(|err| err.to_string())?
            else {
                return Ok(());
            };
            for event in events {
                if let ClientEventKind::ActionResults(results) = event.event {
                    for result in results {
                        self.apply_pushed_result(state, result)?;
                    }
                }
            }
            return Ok(());
        }
        let pending = state
            .awaiting
            .pop_front()
            .ok_or_else(|| "an answer to a request nobody sent".to_owned())?;
        self.security
            .verify_peer_response(
                &state.owner,
                &pending.request,
                frame.status,
                &frame.body,
                frame.signature.as_ref(),
            )
            .map_err(|err| err.to_string())?;
        let outcome = if frame.status == STATUS_OK {
            Ok(())
        } else {
            Err(String::from_utf8_lossy(&frame.body).into_owned())
        };
        match pending.watch {
            Some((action_id, reply)) => {
                if outcome.is_err() {
                    state.watched.remove(&action_id);
                }
                let _ = reply.send(outcome);
                Ok(())
            }
            None => outcome.map_err(|reason| format!("the owner refused a keep-alive: {reason}")),
        }
    }

    /// Applies a pushed result, but only for an action this session watches: anything else is a
    /// protocol violation and ends the session.
    fn apply_pushed_result(
        &self,
        state: &mut SessionState,
        result: orion::control_plane::ActionResult,
    ) -> Result<(), String> {
        if !state.watched.contains(&result.action_id) {
            return Err(format!(
                "a push for {}, which this session does not watch",
                result.action_id
            ));
        }
        let action_id = result.action_id.clone();
        let is_final = result.state.is_terminal();
        self.apply_remote_action_result(&action_id, vec![result]);
        if is_final {
            state.watched.remove(&action_id);
        }
        Ok(())
    }

    async fn sign_request(&self, message: ControlMessage) -> Result<Vec<u8>, String> {
        let payload = self
            .security
            .wrap_http_payload_async(HttpRequestPayload::Control(Box::new(message)))
            .await
            .map_err(|err| err.to_string())?;
        HttpCodec
            .encode_request(&payload)
            .map(|request| request.body)
            .map_err(|err| err.to_string())
    }
}

async fn send_frame<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    payload: &[u8],
    io_timeout: Duration,
    max_payload: usize,
) -> Result<(), String> {
    tokio::time::timeout(
        io_timeout,
        write_control_payload_frame(writer, payload, max_payload),
    )
    .await
    .map_err(|_| "writing timed out".to_owned())?
    .map(|_| ())
    .map_err(|err| err.to_string())
}

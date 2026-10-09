//! [`ActionCaller`]: request/response actions over one control-plane stream
//! (`docs/actions.md`, "Waiting for the result").

use super::ControlPlaneEventStream;
use crate::{error::ClientError, stream::ClientEventStreamSession};
use orion_control_plane::{
    ActionRequest, ActionResult, ClientEvent, ClientEventKind, ControlMessage,
};
use orion_core::NodeId;
use std::{collections::BTreeMap, time::Duration};
use tokio::sync::{mpsc, oneshot};

type Reply = oneshot::Sender<Result<ActionResult, ClientError>>;

enum Command {
    Call {
        request: ActionRequest,
        reply: Reply,
    },
    /// The caller stopped waiting: forget its waiter and hand back the latest result.
    Forget {
        action_id: String,
        reply: oneshot::Sender<Option<ActionResult>>,
    },
}

struct Waiting {
    latest: ActionResult,
    replies: Vec<Reply>,
}

/// Runs actions and waits for their results over one control-plane stream: no polling, and any
/// number of calls in flight at once (one handle, cloned into as many tasks as needed).
///
/// Each call sends `RunAction` with `wait_ms`. The node answers at once and then pushes the
/// action's results to this stream until it is final; a background task matches them to the
/// waiting calls by action id. The action's handler (for a resource, its provider) decides
/// whether actions on one target run concurrently or one at a time.
///
/// ```no_run
/// # async fn example() -> Result<(), orion_client::ClientError> {
/// use orion_client::ActionCaller;
/// use orion_control_plane::{ActionRequest, ActionTarget, TypedConfigValue};
/// use orion_core::ResourceId;
/// use std::time::Duration;
///
/// let caller = ActionCaller::connect_default("helios-api").await?;
/// let request = ActionRequest::new(
///     "gpio-get-1",
///     ActionTarget::Resource(ResourceId::new("lemnos.raw")),
///     "gpio.get",
/// )
/// .with_arg("line", TypedConfigValue::UInt(17));
/// let result = caller.call(request, Duration::from_secs(2)).await?;
/// println!("{}: {:?}", result.state, result.output);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct ActionCaller {
    commands: mpsc::Sender<Command>,
    node_id: NodeId,
}

impl std::fmt::Debug for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Call { request, .. } => write!(f, "Call({})", request.action_id),
            Self::Forget { action_id, .. } => write!(f, "Forget({action_id})"),
        }
    }
}

impl ActionCaller {
    /// Connects a control-plane stream to `stream_socket` as `name`.
    pub async fn connect_at(
        stream_socket: impl AsRef<std::path::Path>,
        name: impl Into<String>,
    ) -> Result<Self, ClientError> {
        Ok(Self::from_stream(
            ControlPlaneEventStream::connect_at(stream_socket, name).await?,
        ))
    }

    /// Connects to the node's default stream socket.
    pub async fn connect_default(name: impl Into<String>) -> Result<Self, ClientError> {
        Ok(Self::from_stream(
            ControlPlaneEventStream::connect_default(name).await?,
        ))
    }

    /// Takes over a connected stream (it should not carry other subscriptions: their events
    /// are dropped). Needs a Tokio runtime, which runs the stream's background task.
    pub fn from_stream(stream: ControlPlaneEventStream) -> Self {
        let node_id = stream.session.node_id().clone();
        let (commands, receiver) = mpsc::channel(64);
        tokio::spawn(run(stream.session, receiver));
        Self { commands, node_id }
    }

    /// The node this caller is connected to.
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Runs `request` and waits up to `timeout` for its final result.
    ///
    /// Returns the final result (`Succeeded`, `Failed`, `Rejected` or `TimedOut`), or, when
    /// `timeout` passes first, the latest result, which is still running (check
    /// `result.state.is_terminal()`). The action itself runs until its own deadline
    /// (`deadline_ms`; set it to bound the work as well as the wait). Resubmitting the same
    /// request (same id, target, name and arguments) waits for the same action again.
    pub async fn call(
        &self,
        mut request: ActionRequest,
        timeout: Duration,
    ) -> Result<ActionResult, ClientError> {
        request.wait_ms = u64::try_from(timeout.as_millis())
            .unwrap_or(u64::MAX)
            .max(1);
        let action_id = request.action_id.clone();
        let (reply, answer) = oneshot::channel();
        self.commands
            .send(Command::Call { request, reply })
            .await
            .map_err(|_| closed())?;
        match tokio::time::timeout(timeout, answer).await {
            Ok(answer) => answer.map_err(|_| closed())?,
            Err(_) => {
                let (reply, latest) = oneshot::channel();
                self.commands
                    .send(Command::Forget {
                        action_id: action_id.clone(),
                        reply,
                    })
                    .await
                    .map_err(|_| closed())?;
                latest.await.map_err(|_| closed())?.ok_or_else(|| {
                    ClientError::Rejected(format!("action `{action_id}` has no result yet"))
                })
            }
        }
    }
}

fn closed() -> ClientError {
    ClientError::Rejected("the action caller's stream is closed".into())
}

/// The background task: owns the stream, sends calls, and routes pushed results to waiters.
async fn run(mut session: ClientEventStreamSession, mut commands: mpsc::Receiver<Command>) {
    let mut waiting: BTreeMap<String, Waiting> = BTreeMap::new();
    let failure = loop {
        tokio::select! {
            command = commands.recv() => match command {
                None => return,
                Some(Command::Call { request, reply }) => {
                    let action_id = request.action_id.clone();
                    let response = match session.send(ControlMessage::RunAction(Box::new(request))).await {
                        Ok(()) => session.recv_response_message().await,
                        Err(error) => Err(error),
                    };
                    match response {
                        Ok(ControlMessage::ActionResults(results)) => {
                            match results.into_iter().find(|result| result.action_id == action_id) {
                                Some(result) if result.state.is_terminal() => {
                                    let _ = reply.send(Ok(result));
                                }
                                Some(result) => {
                                    waiting
                                        .entry(action_id)
                                        .or_insert_with(|| Waiting { latest: result.clone(), replies: Vec::new() })
                                        .replies
                                        .push(reply);
                                }
                                None => {
                                    let _ = reply.send(Err(ClientError::NoMessageAvailable));
                                }
                            }
                        }
                        Ok(ControlMessage::Rejected(reason)) => {
                            let _ = reply.send(Err(ClientError::Rejected(reason)));
                        }
                        Ok(_) => {
                            let _ = reply.send(Err(ClientError::NoMessageAvailable));
                        }
                        Err(error) => {
                            let _ = reply.send(Err(ClientError::Rejected(error.to_string())));
                            break error.to_string();
                        }
                    }
                    for events in session.take_pending_events() {
                        deliver(&mut waiting, events);
                    }
                }
                Some(Command::Forget { action_id, reply }) => {
                    let latest = waiting.get_mut(&action_id).map(|entry| {
                        entry.replies.retain(|reply| !reply.is_closed());
                        entry.latest.clone()
                    });
                    if waiting.get(&action_id).is_some_and(|entry| entry.replies.is_empty()) {
                        waiting.remove(&action_id);
                    }
                    let _ = reply.send(latest);
                }
            },
            frame = session.recv_frame() => match frame {
                Ok(Some(ControlMessage::Ping)) => {
                    if let Err(error) = session.send(ControlMessage::Pong).await {
                        break error.to_string();
                    }
                }
                Ok(Some(ControlMessage::ClientEvents(events))) => deliver(&mut waiting, events),
                Ok(Some(_)) => {}
                Ok(None) => break "the node closed the stream".to_owned(),
                Err(error) => break error.to_string(),
            },
        }
    };
    // The stream is gone: fail the calls still waiting, then every later one.
    for (_, entry) in waiting {
        for reply in entry.replies {
            let _ = reply.send(Err(ClientError::Rejected(failure.clone())));
        }
    }
    while let Some(command) = commands.recv().await {
        match command {
            Command::Call { reply, .. } => {
                let _ = reply.send(Err(ClientError::Rejected(failure.clone())));
            }
            Command::Forget { reply, .. } => {
                let _ = reply.send(None);
            }
        }
    }
}

fn deliver(waiting: &mut BTreeMap<String, Waiting>, events: Vec<ClientEvent>) {
    for event in events {
        let ClientEventKind::ActionResults(results) = event.event else {
            continue;
        };
        for result in results {
            let Some(entry) = waiting.get_mut(&result.action_id) else {
                continue;
            };
            if result.state.is_terminal() {
                if let Some(entry) = waiting.remove(&result.action_id) {
                    for reply in entry.replies {
                        let _ = reply.send(Ok(result.clone()));
                    }
                }
            } else {
                entry.latest = result;
            }
        }
    }
}

//! Submitting and following actions (`docs/actions.md`).

use super::{ControlPlaneEventStream, LocalControlPlaneClient};
use crate::error::ClientError;
use orion_control_plane::{
    ActionQuery, ActionRequest, ActionResult, ClientEventKind, ControlMessage,
};
use std::time::{Duration, Instant};

fn expect_action_results(message: ControlMessage) -> Result<Vec<ActionResult>, ClientError> {
    match message {
        ControlMessage::ActionResults(results) => Ok(results),
        ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
        _ => Err(ClientError::NoMessageAvailable),
    }
}

impl LocalControlPlaneClient {
    /// Submits an action and returns its current result (`Accepted`, or `Rejected` when no
    /// handler can run it). Resubmitting the same `action_id` with the same request returns the
    /// existing result.
    pub async fn run_action(&self, request: ActionRequest) -> Result<ActionResult, ClientError> {
        let action_id = request.action_id.clone();
        self.send_request_with(
            ControlMessage::RunAction(Box::new(request)),
            expect_action_results,
        )
        .await?
        .into_iter()
        .find(|result| result.action_id == action_id)
        .ok_or(ClientError::NoMessageAvailable)
    }

    /// Actions the node tracks that match `query`.
    pub async fn query_actions(
        &self,
        query: ActionQuery,
    ) -> Result<Vec<ActionResult>, ClientError> {
        self.send_request_with(ControlMessage::QueryActions(query), expect_action_results)
            .await
    }

    /// Polls an action every `poll_interval` until it is final or `timeout` passes (then returns
    /// its latest, still running result).
    pub async fn wait_for_action(
        &self,
        action_id: &str,
        poll_interval: Duration,
        timeout: Duration,
    ) -> Result<ActionResult, ClientError> {
        let started = Instant::now();
        loop {
            let result = self
                .query_actions(ActionQuery::action(action_id))
                .await?
                .into_iter()
                .find(|result| result.action_id == action_id)
                .ok_or_else(|| {
                    ClientError::Rejected(format!("action `{action_id}` is not tracked"))
                })?;
            if result.state.is_terminal() || started.elapsed() >= timeout {
                return Ok(result);
            }
            tokio::time::sleep(poll_interval).await;
        }
    }
}

impl ControlPlaneEventStream {
    /// Subscribes this stream to action results matching `query`: a first event with every
    /// matching result, then updates (`ClientEventKind::ActionResults`).
    pub async fn subscribe_actions(&mut self, query: ActionQuery) -> Result<(), ClientError> {
        self.session
            .subscribe_and_expect_accepted(ControlMessage::WatchActions(query))
            .await
    }
}

/// A watch on action results, from [`ActionWatch::connect_at`].
pub struct ActionWatch {
    stream: ControlPlaneEventStream,
}

impl ActionWatch {
    /// Connects a control-plane stream to `stream_socket` and subscribes to `query`.
    pub async fn connect_at(
        stream_socket: impl AsRef<std::path::Path>,
        name: impl Into<String>,
        query: ActionQuery,
    ) -> Result<Self, ClientError> {
        let mut stream = ControlPlaneEventStream::connect_at(stream_socket, name).await?;
        stream.subscribe_actions(query).await?;
        Ok(Self { stream })
    }

    /// Waits for the next batch of updated results (newest result per action). The first batch
    /// holds every matching result.
    pub async fn next(&mut self) -> Result<Vec<ActionResult>, ClientError> {
        loop {
            let mut results = Vec::new();
            for event in self.stream.next_events().await? {
                if let ClientEventKind::ActionResults(batch) = event.event {
                    results.extend(batch);
                }
            }
            if !results.is_empty() {
                return Ok(results);
            }
        }
    }
}

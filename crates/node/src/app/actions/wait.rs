//! The reply path of `RunAction` with `wait_ms` (`docs/actions.md`, "Waiting for the result").
//!
//! The control pipeline stays synchronous and never blocks: it submits the action and answers
//! with its current result. Waiting happens around it, where the transports are async:
//!
//! - unary exchanges (the local unary socket, HTTP and `orion+tcp` peers and remote operators)
//!   hold the answer until the action is final or the wait runs out ([`NodeApp::await_action`]),
//!   capped by `ActionTuning::max_wait` so it stays below the transport I/O timeouts;
//! - a control-plane stream answers at once (its requests are served in order, so one waiting
//!   call must not hold up the next) and pushes the action's results to that stream as
//!   `ActionResults` events until it is final ([`NodeApp::register_stream_action_call`]).

use super::{NodeApp, lock};
use orion::{
    control_plane::{ActionResult, ControlMessage},
    transport::ipc::LocalAddress,
};
use std::time::Duration;

fn running(results: &[ActionResult], action_id: &str) -> bool {
    results
        .iter()
        .any(|result| result.action_id == action_id && !result.state.is_terminal())
}

impl NodeApp {
    /// The action id and wait of a `RunAction` that asks to wait for its result.
    pub(crate) fn run_action_wait(message: &ControlMessage) -> Option<(String, u64)> {
        match message {
            ControlMessage::RunAction(request) if request.wait_ms > 0 => {
                Some((request.action_id.clone(), request.wait_ms))
            }
            _ => None,
        }
    }

    /// [`Self::run_action_wait`] of a peer or remote-operator request (signed or not).
    #[cfg(peer_sync)]
    pub(crate) fn peer_action_wait(
        payload: &orion::transport::http::HttpRequestPayload,
    ) -> Option<(String, u64)> {
        use orion::{auth::PeerRequestPayload, transport::http::HttpRequestPayload};
        match payload {
            HttpRequestPayload::Control(message) => Self::run_action_wait(message),
            HttpRequestPayload::AuthenticatedPeer(request) => match &request.payload {
                PeerRequestPayload::Control(message) => Self::run_action_wait(message),
                PeerRequestPayload::ObservedUpdate(_) => None,
            },
            HttpRequestPayload::ObservedUpdate(_) => None,
        }
    }

    fn current_action_result(&self, action_id: &str) -> Option<ActionResult> {
        lock(&self.state.actions.registry)
            .get(action_id)
            .map(|entry| entry.result.clone())
    }

    /// Waits until `action_id` is final or `wait` (capped by `ActionTuning::max_wait`) passes,
    /// and returns its latest result (`None` once it is no longer tracked).
    pub(crate) async fn await_action(
        &self,
        action_id: &str,
        wait: Duration,
    ) -> Option<ActionResult> {
        let wait = wait.min(self.config.runtime_tuning.actions.max_wait);
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            // Register for the next change before reading, so a change in between is not lost.
            let changed = self.state.actions.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let current = self.current_action_result(action_id)?;
            if current.state.is_terminal() {
                return Some(current);
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return self.current_action_result(action_id).or(Some(current));
            }
        }
    }

    /// Completes a unary local `RunAction` answer: waits for the final result if asked to.
    pub(crate) async fn complete_local_action_wait(
        &self,
        wait: Option<(String, u64)>,
        response: ControlMessage,
    ) -> ControlMessage {
        let Some((action_id, wait_ms)) = wait else {
            return response;
        };
        match &response {
            ControlMessage::ActionResults(results) if running(results, &action_id) => {
                match self
                    .await_action(&action_id, Duration::from_millis(wait_ms))
                    .await
                {
                    Some(result) => ControlMessage::ActionResults(vec![result]),
                    None => response,
                }
            }
            _ => response,
        }
    }

    /// Completes a peer or remote-operator `RunAction` answer: waits for the final result if
    /// asked to.
    #[cfg(peer_sync)]
    pub(crate) async fn complete_peer_action_wait(
        &self,
        wait: Option<(String, u64)>,
        response: orion::transport::http::HttpResponsePayload,
    ) -> orion::transport::http::HttpResponsePayload {
        use orion::transport::http::HttpResponsePayload;
        let Some((action_id, wait_ms)) = wait else {
            return response;
        };
        match &response {
            HttpResponsePayload::Actions(results) if running(results, &action_id) => {
                match self
                    .await_action(&action_id, Duration::from_millis(wait_ms))
                    .await
                {
                    Some(result) => HttpResponsePayload::Actions(vec![result]),
                    None => response,
                }
            }
            _ => response,
        }
    }

    /// After a stream `RunAction` with `wait_ms` was answered: pushes the action's results to
    /// `source`'s stream until it is final. The caller flushes the stream afterwards.
    pub(crate) fn register_stream_action_call(
        &self,
        source: &LocalAddress,
        wait: Option<(String, u64)>,
        response: &ControlMessage,
    ) {
        let Some((action_id, _)) = wait else {
            return;
        };
        let ControlMessage::ActionResults(results) = response else {
            return;
        };
        let Some(answered) = results
            .iter()
            .find(|result| result.action_id == action_id && !result.state.is_terminal())
        else {
            return;
        };
        self.with_client_mut_if_present(source, |client| {
            // Read the result under the client lock: an update that lands before the call is
            // recorded is caught here, one that lands after finds the call (lock order: client
            // registry, then action registry, as nowhere takes them the other way round).
            match self.current_action_result(&action_id) {
                Some(result) if result.state.is_terminal() => {
                    super::super::local_clients::enqueue_action_results_event(client, vec![result]);
                }
                Some(result) => {
                    client.action_calls.insert(action_id.clone());
                    if result.state != answered.state || result.output != answered.output {
                        super::super::local_clients::enqueue_action_results_event(
                            client,
                            vec![result],
                        );
                    }
                }
                None => {}
            }
        });
    }
}

#[cfg(peer_sync)]
/// How long a forwarding node asks the owner to hold one answer: below the peer HTTP client's
/// request timeout, so a long action is followed by resending instead of failing the exchange.
pub(super) const FORWARD_WAIT: Duration = Duration::from_millis(750);

#[cfg(peer_sync)]
pub(super) fn forward_wait_ms(remaining: Duration) -> u64 {
    super::millis(remaining.min(FORWARD_WAIT)).max(1)
}

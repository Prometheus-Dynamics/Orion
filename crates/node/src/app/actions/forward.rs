//! Forwarding actions to the node that owns their target.
//!
//! The submitting node sends the request once as a signed `RunAction` over the peer's transport
//! (`orion+tcp` or HTTP(S)), which the owner authorizes like any peer write (authenticated and
//! enrolled), then polls the owner with `QueryActions` until the result is final or the local
//! deadline (plus a grace period) passes. Forwarding is one hop: the owner never forwards again.

use super::super::peer_transport::PeerSyncTransport;
use super::{NodeApp, lock};
use orion::{
    NodeId,
    control_plane::{ActionQuery, ActionRequest, ActionResult, ActionState, ControlMessage},
    transport::http::HttpResponsePayload,
};
use std::time::Duration;
use tracing::debug;

const FIRST_POLL_DELAY: Duration = Duration::from_millis(100);
const MAX_POLL_DELAY: Duration = Duration::from_secs(1);

impl NodeApp {
    pub(super) fn spawn_action_forward(
        &self,
        node: NodeId,
        request: ActionRequest,
    ) -> Result<tokio::task::AbortHandle, String> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| "no async runtime is available to forward the action".to_owned())?;
        if !self.peers_read().contains_key(&node) {
            return Err(format!(
                "{} is owned by node {node}, which is not a configured peer of {}",
                request.target, self.config.node_id
            ));
        }
        let app = self.clone();
        Ok(runtime
            .spawn(async move { app.forward_action(node, request).await })
            .abort_handle())
    }

    async fn forward_action(&self, node: NodeId, request: ActionRequest) {
        let action_id = request.action_id.clone();
        let Some(peer) = self.peers_read().get(&node).cloned() else {
            return self.finish_action(
                &action_id,
                ActionState::Rejected {
                    reason: format!("node {node} is no longer a configured peer"),
                },
            );
        };
        let channel = match self.open_peer_channel(&node, &peer) {
            Ok(channel) => channel,
            Err(error) => {
                return self.finish_action(
                    &action_id,
                    ActionState::Failed {
                        reason: format!("cannot reach node {node}: {error}"),
                    },
                );
            }
        };
        match channel
            .send_control(self, &node, ControlMessage::RunAction(Box::new(request)))
            .await
        {
            Ok(HttpResponsePayload::Actions(results)) => {
                self.apply_remote_action_result(&action_id, results);
            }
            Ok(_) => {
                return self.finish_action(
                    &action_id,
                    ActionState::Failed {
                        reason: format!(
                            "node {node} answered the action with an unexpected response"
                        ),
                    },
                );
            }
            Err(error) => {
                return self.finish_action(
                    &action_id,
                    ActionState::Failed {
                        reason: format!("forwarding the action to node {node} failed: {error}"),
                    },
                );
            }
        }
        let mut delay = FIRST_POLL_DELAY;
        while self.action_is_running(&action_id) {
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(MAX_POLL_DELAY);
            match channel
                .send_control(
                    self,
                    &node,
                    ControlMessage::QueryActions(ActionQuery::action(action_id.clone())),
                )
                .await
            {
                Ok(HttpResponsePayload::Actions(results)) => {
                    self.apply_remote_action_result(&action_id, results)
                }
                // Transient failures are retried until the local deadline times the action out.
                Ok(_) => {}
                Err(error) => {
                    debug!(node = %self.config.node_id, peer = %node, action_id = %action_id, error = %error, "polling a forwarded action failed");
                }
            }
        }
    }

    fn action_is_running(&self, action_id: &str) -> bool {
        lock(&self.state.actions.registry)
            .get(action_id)
            .is_some_and(|entry| !entry.result.state.is_terminal())
    }

    /// Mirrors the owner's result (state and output) into the local entry.
    fn apply_remote_action_result(&self, action_id: &str, results: Vec<ActionResult>) {
        let Some(remote) = results
            .into_iter()
            .find(|result| result.action_id == action_id)
        else {
            return;
        };
        let unchanged = lock(&self.state.actions.registry)
            .get(action_id)
            .is_some_and(|entry| {
                entry.result.state == remote.state && entry.result.output == remote.output
            });
        if !unchanged {
            self.update_node_action(action_id, remote.state, remote.output);
        }
    }
}

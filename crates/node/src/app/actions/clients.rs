//! Local clients and actions: handler registration and delivery, handler reports, and result
//! watches.

use super::super::local_clients::{
    PendingClientStreamFlush, enqueue_action_request_event, enqueue_action_results_event,
    execute_client_stream_flush, finalize_client_stream_flush, prepare_client_stream_flush,
};
use super::{ActionOrigin, ActionRoute, NodeApp, NodeError, lock};
use orion::{
    control_plane::{
        ActionQuery, ActionReport, ActionRequest, ActionResult, ActionState, ActionTarget,
        ClientRole, ControlMessage,
    },
    transport::ipc::LocalAddress,
};
use tracing::debug;

impl NodeApp {
    /// Local IPC action messages (roles are checked by the authorizer).
    pub(crate) fn apply_local_action_message(
        &self,
        source: &LocalAddress,
        message: ControlMessage,
    ) -> Result<ControlMessage, NodeError> {
        match message {
            ControlMessage::RunAction(request) => {
                let client = self
                    .clients_read()
                    .get(source)
                    .map(|client| client.session.client_name.to_string())
                    .unwrap_or_else(|| source.as_str().to_owned());
                let result = self.submit_action(*request, ActionOrigin::Local(client))?;
                Ok(ControlMessage::ActionResults(vec![result]))
            }
            ControlMessage::QueryActions(query) => {
                Ok(ControlMessage::ActionResults(self.query_actions(&query)))
            }
            ControlMessage::WatchActions(query) => {
                self.subscribe_action_watch(source, query)?;
                Ok(ControlMessage::Accepted)
            }
            ControlMessage::WatchActionRequests(targets) => {
                self.register_action_handler(source, targets)?;
                Ok(ControlMessage::Accepted)
            }
            ControlMessage::ClaimNodeActions(names) => {
                self.claim_node_actions(source, names)?;
                Ok(ControlMessage::Accepted)
            }
            ControlMessage::ReportActionResult(report) => {
                self.report_action_result(source, *report)?;
                Ok(ControlMessage::Accepted)
            }
            _ => Ok(ControlMessage::Rejected(
                "not an action control message".into(),
            )),
        }
    }

    /// The local client registered as `target`'s action handler, while its session is alive
    /// (requests reach it over its stream, or by polling its event queue).
    pub(super) fn client_action_handler(&self, target: &ActionTarget) -> Option<LocalAddress> {
        let address = lock(&self.state.actions.client_handlers)
            .get(target)
            .cloned()?;
        let registered = self.clients_read().contains_key(&address);
        registered.then_some(address)
    }

    /// `WatchActionRequests`: registers `source` as the action handler of local providers or
    /// executors (the latest registration wins, like provider and executor state) and queues
    /// requests already routed to it.
    pub(crate) fn register_action_handler(
        &self,
        source: &LocalAddress,
        targets: Vec<ActionTarget>,
    ) -> Result<(), NodeError> {
        if targets.is_empty() {
            return Err(NodeError::Action(
                "name at least one provider or executor to handle actions for".into(),
            ));
        }
        let role = self
            .clients_read()
            .get(source)
            .map(|client| client.session.role.clone())
            .ok_or_else(|| NodeError::UnknownClient(source.clone()))?;
        {
            let store = self.store_read();
            let local = &self.config.node_id;
            for target in &targets {
                let (expected, node) = match target {
                    ActionTarget::Provider(id) => (
                        ClientRole::Provider,
                        store.desired.providers.get(id).map(|p| &p.node_id),
                    ),
                    ActionTarget::Executor(id) => (
                        ClientRole::Executor,
                        store.desired.executors.get(id).map(|e| &e.node_id),
                    ),
                    other => {
                        return Err(NodeError::Action(format!(
                            "clients handle actions for providers and executors, not {other}; \
                             resource actions go to the resource's provider"
                        )));
                    }
                };
                if role != expected {
                    return Err(NodeError::Authorization(format!(
                        "a {role:?} client cannot handle actions for {target}"
                    )));
                }
                if node != Some(local) {
                    return Err(NodeError::Action(format!(
                        "{target} is not registered on node {local}; publish its state first"
                    )));
                }
            }
        }
        {
            let mut handlers = lock(&self.state.actions.client_handlers);
            for target in targets {
                debug!(node = %self.config.node_id, client = %source.as_str(), target = %target, "registered action handler");
                handlers.insert(target, source.clone());
            }
        }
        self.redeliver_pending(source);
        Ok(())
    }

    /// `ClaimNodeActions`: makes `source` the handler of node-targeted actions with these names.
    /// A name with an in-process handler, or one claimed by another live client, is refused.
    pub(crate) fn claim_node_actions(
        &self,
        source: &LocalAddress,
        names: Vec<String>,
    ) -> Result<(), NodeError> {
        if names.is_empty() {
            return Err(NodeError::Action("claim at least one action name".into()));
        }
        let local = &self.config.node_id;
        {
            let clients = self.clients_read();
            if !clients.contains_key(source) {
                return Err(NodeError::UnknownClient(source.clone()));
            }
            let claims = lock(&self.state.actions.node_claims);
            for name in &names {
                if name.trim().is_empty() || name.len() > super::registry::MAX_ACTION_TEXT_BYTES {
                    return Err(NodeError::Action(format!(
                        "action names must be 1 to {} bytes",
                        super::registry::MAX_ACTION_TEXT_BYTES
                    )));
                }
                if self.state.actions.handlers.contains_key(name) {
                    return Err(NodeError::Action(format!(
                        "action `{name}` is handled in-process on node {local}; it cannot be \
                         claimed"
                    )));
                }
                if let Some(holder) = claims.get(name)
                    && holder != source
                    && clients.contains_key(holder)
                {
                    return Err(NodeError::Action(format!(
                        "action `{name}` on node {local} is already claimed by client {}",
                        holder.as_str()
                    )));
                }
            }
        }
        {
            let mut claims = lock(&self.state.actions.node_claims);
            for name in names {
                debug!(node = %local, client = %source.as_str(), action = %name, "claimed node action");
                claims.insert(name, source.clone());
            }
        }
        self.redeliver_pending(source);
        Ok(())
    }

    /// The live client that claimed the node action `name`.
    pub(super) fn claimed_node_action(&self, name: &str) -> Option<LocalAddress> {
        let address = lock(&self.state.actions.node_claims).get(name).cloned()?;
        let registered = self.clients_read().contains_key(&address);
        registered.then_some(address)
    }

    /// Whether `source` may publish the status key `key` for this node's `node/<id>` subject:
    /// `action.*` keys while it holds any node action claim, and `<name>.*` keys for every node
    /// action `<name>` it claimed (for example `update.state` for the holder of `update`).
    pub(crate) fn node_status_key_allowed(&self, source: &LocalAddress, key: &str) -> bool {
        let claims = lock(&self.state.actions.node_claims);
        let mut held = claims
            .iter()
            .filter(|(_, holder)| *holder == source)
            .map(|(name, _)| name.as_str())
            .peekable();
        if held.peek().is_none() {
            return false;
        }
        if key.starts_with("action.") {
            return true;
        }
        held.any(|name| {
            key.strip_prefix(name)
                .is_some_and(|rest| rest.len() > 1 && rest.starts_with('.'))
        })
    }

    /// Whether `source` holds a node action claim (it may then publish `action.*` status keys
    /// for the node subject).
    pub(crate) fn client_holds_node_claim(&self, source: &LocalAddress) -> bool {
        lock(&self.state.actions.node_claims)
            .values()
            .any(|holder| holder == source)
    }

    /// Releases every handler registration and node action claim of `source` (its stream
    /// disconnected or its session expired) and fails the actions still waiting for it.
    pub(crate) fn release_action_handler(&self, source: &LocalAddress) {
        lock(&self.state.actions.client_handlers).retain(|_, holder| holder != source);
        lock(&self.state.actions.node_claims).retain(|_, holder| holder != source);
        let pending = self.action_registry().running_for_client(source);
        for action_id in pending {
            self.finish_action(
                &action_id,
                ActionState::Failed {
                    reason: "handler disconnected".into(),
                },
            );
        }
    }

    fn redeliver_pending(&self, source: &LocalAddress) {
        let pending = self.action_registry().pending_for_client(source);
        for request in pending {
            let action_id = request.action_id.clone();
            if let Err(reason) = self.deliver_action_request(source, request) {
                self.finish_action(&action_id, ActionState::Rejected { reason });
            }
        }
    }

    /// Queues `request` on the handler client's stream.
    pub(super) fn deliver_action_request(
        &self,
        address: &LocalAddress,
        request: ActionRequest,
    ) -> Result<(), String> {
        let flush = self
            .with_client_mut_if_present(address, |client| {
                enqueue_action_request_event(client, request);
                prepare_client_stream_flush(address, client)
            })
            .ok_or_else(|| format!("action handler client {} disconnected", address.as_str()))?;
        if let Some(flush) = flush {
            self.complete_flush(flush);
        }
        Ok(())
    }

    /// `ReportActionResult` from the handler client an action was delivered to.
    pub(crate) fn report_action_result(
        &self,
        source: &LocalAddress,
        report: ActionReport,
    ) -> Result<(), NodeError> {
        if matches!(report.state, ActionState::Accepted | ActionState::TimedOut) {
            return Err(NodeError::Action(
                "handlers report Running, Succeeded, Failed, or Rejected".into(),
            ));
        }
        let route = self
            .action_registry()
            .get(&report.action_id)
            .map(|entry| entry.route.clone())
            .ok_or_else(|| NodeError::Action(format!("unknown action `{}`", report.action_id)))?;
        if route != ActionRoute::Client(source.clone()) {
            return Err(NodeError::Authorization(format!(
                "client {} is not the handler of action `{}`",
                source.as_str(),
                report.action_id
            )));
        }
        let state = match report.state {
            ActionState::Running { progress } => ActionState::Running {
                progress: progress.map(|value| value.min(1000)),
            },
            state => state,
        };
        // Reports after the action finished (for example after a timeout) are ignored.
        self.update_node_action(&report.action_id, state, report.output);
        Ok(())
    }

    /// `WatchActions`: queues a bootstrap event with every matching result, then updates.
    pub(crate) fn subscribe_action_watch(
        &self,
        source: &LocalAddress,
        query: ActionQuery,
    ) -> Result<(), NodeError> {
        let current = self.query_actions(&query);
        self.with_client_mut(source, |client| {
            client.action_watch = Some(query);
            enqueue_action_results_event(client, current);
        })
        // The stream server flushes queued events right after it sends the `Accepted` response.
    }

    pub(super) fn notify_action_watchers(&self, results: Vec<ActionResult>) {
        if results.is_empty() {
            return;
        }
        self.state.actions.changed.notify_waiters();
        let mut flushes = Vec::<PendingClientStreamFlush>::new();
        self.with_client_registry_txn(|txn| {
            for (source, client) in txn.clients_mut() {
                if client.action_watch.is_none() && client.action_calls.is_empty() {
                    continue;
                }
                let matching: Vec<ActionResult> = results
                    .iter()
                    .filter(|result| {
                        client.action_calls.contains(&result.action_id)
                            || client
                                .action_watch
                                .as_ref()
                                .is_some_and(|query| query.matches(result))
                    })
                    .cloned()
                    .collect();
                if matching.is_empty() {
                    continue;
                }
                for result in &matching {
                    if result.state.is_terminal() {
                        client.action_calls.remove(&result.action_id);
                    }
                }
                enqueue_action_results_event(client, matching);
                if let Some(flush) = prepare_client_stream_flush(source, client) {
                    flushes.push(flush);
                }
            }
        });
        for flush in flushes {
            self.complete_flush(flush);
        }
    }

    fn complete_flush(&self, flush: PendingClientStreamFlush) {
        let delivered = execute_client_stream_flush(&flush);
        let _ = self.with_client_mut_if_present(&flush.source, |client| {
            finalize_client_stream_flush(client, &flush, delivered);
        });
    }
}

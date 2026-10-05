//! Generic actions: routing, lifecycle tracking, and timeouts (`docs/actions.md`).
//!
//! The node an action is submitted to tracks it in a bounded in-memory registry. It routes the
//! request by its target:
//!
//! - `Node(local)`: the node-side [`ActionHandler`] registered for the action name;
//! - `Provider` / `Executor` (local): the local client registered as the target's action handler
//!   (`WatchActionRequests`), which reports results with `ReportActionResult`;
//! - `Resource` (local): the action handler of the resource's provider (or of the executor that
//!   realizes it);
//! - any target owned by another node: forwarded once over the signed peer transport
//!   (`actions/forward.rs`), then polled until it is final.
//!
//! Nothing is persisted: a node restart forgets every tracked action.

mod clients;
#[cfg(peer_sync)]
mod forward;
pub(crate) mod registry;

use super::{NodeApp, NodeError};
use crate::actions::{ActionContext, ActionHandler};
use orion::{
    NodeId,
    control_plane::{
        ActionQuery, ActionRequest, ActionResult, ActionState, ActionTarget, TypedConfigValue,
    },
    transport::ipc::LocalAddress,
};
use registry::{ActionEntry, ActionRegistry, ActionRoute, same_request, validate_request};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};
use tokio::sync::watch;
use tracing::{debug, info};

/// Extra time a forwarding node waits past the deadline for the owner's own final result.
const REMOTE_DEADLINE_GRACE_MS: u64 = 2_000;

/// Who submitted an action.
#[derive(Clone, Debug)]
pub(crate) enum ActionOrigin {
    /// A local control-plane client, by client name.
    Local(String),
    /// An authenticated, enrolled peer forwarding the action to the target's owner.
    Peer(NodeId),
    /// An enrolled remote operator (`docs/remote-operator.md`), authorized for the action name.
    Operator(orion::control_plane::OperatorId),
}

#[derive(Default)]
pub(super) struct ActionsState {
    registry: Mutex<ActionRegistry>,
    /// Node-side handlers by action name, set once by the builder.
    pub(super) handlers: BTreeMap<String, Arc<dyn ActionHandler>>,
    /// Local clients registered as action handlers of providers and executors.
    client_handlers: Mutex<BTreeMap<ActionTarget, LocalAddress>>,
    /// Node action names claimed by out-of-process local handlers (`ClaimNodeActions`).
    node_claims: Mutex<BTreeMap<String, LocalAddress>>,
    wake: tokio::sync::Notify,
}

impl ActionsState {
    pub(super) fn with_handlers(handlers: BTreeMap<String, Arc<dyn ActionHandler>>) -> Self {
        Self {
            handlers,
            ..Self::default()
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

impl NodeApp {
    fn action_registry(&self) -> MutexGuard<'_, ActionRegistry> {
        lock(&self.state.actions.registry)
    }

    /// Names of the node-side action handlers registered on this node.
    pub fn action_handler_names(&self) -> Vec<String> {
        self.state.actions.handlers.keys().cloned().collect()
    }

    /// Tracked actions matching `query` (all of them for local callers).
    pub fn query_actions(&self, query: &ActionQuery) -> Vec<ActionResult> {
        self.sweep_actions();
        self.action_registry().query(query, None)
    }

    pub(crate) fn query_actions_for_peer(
        &self,
        query: &ActionQuery,
        peer: &NodeId,
    ) -> Vec<ActionResult> {
        self.sweep_actions();
        self.action_registry().query(query, Some(peer))
    }

    /// Submits an action as the embedder (`requested_by` becomes `local:<requested_by>`), as if a
    /// local control-plane client had sent it.
    pub fn run_action(
        &self,
        request: ActionRequest,
        requested_by: &str,
    ) -> Result<ActionResult, NodeError> {
        self.submit_action(request, ActionOrigin::Local(requested_by.to_owned()))
    }

    /// Validates, routes, records, and dispatches an action. Returns its current result (a
    /// routing failure is a recorded `Rejected` result, not an error; malformed or duplicate
    /// requests and a full registry are errors).
    pub(crate) fn submit_action(
        &self,
        mut request: ActionRequest,
        origin: ActionOrigin,
    ) -> Result<ActionResult, NodeError> {
        validate_request(&request).map_err(NodeError::Action)?;
        request.requested_by = match &origin {
            ActionOrigin::Local(client) => format!("local:{client}"),
            ActionOrigin::Peer(node) => format!("peer:{node}/{}", request.requested_by),
            ActionOrigin::Operator(operator) => operator.to_string(),
        };
        let tuning = &self.config.runtime_tuning.actions;
        request.deadline_ms = match request.deadline_ms {
            0 => millis(tuning.default_deadline),
            requested => requested.min(millis(tuning.max_deadline)),
        };
        if let Some(existing) = self.existing_action(&request)? {
            return Ok(existing);
        }

        let routed = self.route_action(&request, &origin);
        let (route, state) = match routed {
            Ok(route) => (route, ActionState::Accepted),
            Err(reason) => (ActionRoute::Rejected, ActionState::Rejected { reason }),
        };
        let now_ms = Self::current_time_ms();
        let handled_by = match &route {
            ActionRoute::Remote(node) => node.clone(),
            _ => self.config.node_id.clone(),
        };
        let grace_ms = match route {
            ActionRoute::Remote(_) => REMOTE_DEADLINE_GRACE_MS,
            _ => 0,
        };
        let result = ActionResult {
            action_id: request.action_id.clone(),
            target: request.target.clone(),
            name: request.name.clone(),
            state,
            output: BTreeMap::new(),
            handled_by,
            requested_by: request.requested_by.clone(),
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
        };
        {
            let mut registry = self.action_registry();
            registry.evict_expired(now_ms, millis(tuning.result_ttl));
            if let Some(existing) = registry.get(&request.action_id) {
                // Lost a race with an identical submission.
                return if same_request(&existing.request, &request) {
                    Ok(existing.result.clone())
                } else {
                    Err(duplicate_id(&request.action_id))
                };
            }
            if !registry.make_room(tuning.max_tracked) {
                return Err(NodeError::Action(format!(
                    "{} actions are already running on node {}; try again later",
                    registry.len(),
                    self.config.node_id
                )));
            }
            registry.insert(ActionEntry {
                request: request.clone(),
                result: result.clone(),
                deadline_at_ms: now_ms
                    .saturating_add(request.deadline_ms)
                    .saturating_add(grace_ms),
                route: route.clone(),
                origin_peer: match &origin {
                    ActionOrigin::Peer(node) => Some(node.clone()),
                    ActionOrigin::Local(_) | ActionOrigin::Operator(_) => None,
                },
                task: None,
            });
        }
        info!(
            node = %self.config.node_id,
            action_id = %request.action_id,
            target = %request.target,
            name = %request.name,
            requested_by = %request.requested_by,
            state = %result.state,
            "action submitted"
        );
        let result_snapshot = result.clone();
        self.notify_action_watchers(vec![result]);
        self.dispatch_action(route, request.clone());
        self.state.actions.wake.notify_one();
        Ok(self
            .action_registry()
            .get(&request.action_id)
            .map(|entry| entry.result.clone())
            .unwrap_or(result_snapshot))
    }

    fn existing_action(&self, request: &ActionRequest) -> Result<Option<ActionResult>, NodeError> {
        let registry = self.action_registry();
        match registry.get(&request.action_id) {
            Some(existing) if same_request(&existing.request, request) => {
                Ok(Some(existing.result.clone()))
            }
            Some(_) => Err(duplicate_id(&request.action_id)),
            None => Ok(None),
        }
    }

    /// The node that owns `target`.
    pub(super) fn action_owner_node(&self, target: &ActionTarget) -> Result<NodeId, String> {
        let store = self.store_read();
        let provider_node = |provider_id| {
            store
                .desired
                .providers
                .get(provider_id)
                .map(|provider| provider.node_id.clone())
        };
        let executor_node = |executor_id| {
            store
                .desired
                .executors
                .get(executor_id)
                .map(|executor| executor.node_id.clone())
        };
        match target {
            ActionTarget::Node(node) => Ok(node.clone()),
            ActionTarget::Provider(id) => {
                provider_node(id).ok_or_else(|| format!("unknown provider {id}"))
            }
            ActionTarget::Executor(id) => {
                executor_node(id).ok_or_else(|| format!("unknown executor {id}"))
            }
            ActionTarget::Resource(id) => {
                let resource = store
                    .observed
                    .resources
                    .get(id)
                    .or_else(|| store.desired.resources.get(id))
                    .ok_or_else(|| format!("unknown resource {id}"))?;
                provider_node(&resource.provider_id)
                    .or_else(|| {
                        resource
                            .realized_by_executor_id
                            .as_ref()
                            .and_then(executor_node)
                    })
                    .ok_or_else(|| {
                        format!(
                            "resource {id} has no known owner (provider {})",
                            resource.provider_id
                        )
                    })
            }
        }
    }

    fn route_action(
        &self,
        request: &ActionRequest,
        origin: &ActionOrigin,
    ) -> Result<ActionRoute, String> {
        let local = &self.config.node_id;
        let owner = self.action_owner_node(&request.target)?;
        if &owner != local {
            if matches!(origin, ActionOrigin::Peer(_)) {
                return Err(format!(
                    "{} is owned by node {owner}, not {local}; actions are forwarded one hop only",
                    request.target
                ));
            }
            return Ok(ActionRoute::Remote(owner));
        }
        let no_handler = |target: &ActionTarget| {
            format!("no action handler is registered for {target} on node {local}")
        };
        match &request.target {
            // In-process handlers and client claims never overlap: a claim for a name with an
            // in-process handler is refused.
            ActionTarget::Node(_) => {
                if self.state.actions.handlers.contains_key(&request.name) {
                    Ok(ActionRoute::Node)
                } else if let Some(address) = self.claimed_node_action(&request.name) {
                    Ok(ActionRoute::Client(address))
                } else {
                    Err(format!(
                        "node {local} has no handler for action `{}`",
                        request.name
                    ))
                }
            }
            target @ (ActionTarget::Provider(_) | ActionTarget::Executor(_)) => self
                .client_action_handler(target)
                .map(ActionRoute::Client)
                .ok_or_else(|| no_handler(target)),
            ActionTarget::Resource(id) => {
                let candidates = {
                    let store = self.store_read();
                    let resource = store
                        .observed
                        .resources
                        .get(id)
                        .or_else(|| store.desired.resources.get(id));
                    resource
                        .map(|resource| {
                            let mut candidates =
                                vec![ActionTarget::Provider(resource.provider_id.clone())];
                            candidates.extend(
                                resource
                                    .realized_by_executor_id
                                    .clone()
                                    .map(ActionTarget::Executor),
                            );
                            candidates
                        })
                        .unwrap_or_default()
                };
                candidates
                    .iter()
                    .find_map(|target| self.client_action_handler(target))
                    .map(ActionRoute::Client)
                    .ok_or_else(|| no_handler(&request.target))
            }
        }
    }

    fn dispatch_action(&self, route: ActionRoute, request: ActionRequest) {
        let action_id = request.action_id.clone();
        let task = match route {
            ActionRoute::Rejected => return,
            ActionRoute::Node => self.spawn_node_action(request),
            ActionRoute::Client(address) => {
                if let Err(reason) = self.deliver_action_request(&address, request) {
                    self.finish_action(&action_id, ActionState::Rejected { reason });
                }
                return;
            }
            #[cfg(peer_sync)]
            ActionRoute::Remote(node) => self.spawn_action_forward(node, request),
            #[cfg(not(peer_sync))]
            ActionRoute::Remote(node) => Err(format!(
                "{} is owned by node {node}, but this build has no peer transport to forward \
                 actions",
                request.target
            )),
        };
        match task {
            Ok(task) => {
                let mut registry = self.action_registry();
                match registry.get_mut(&action_id) {
                    Some(entry) if !entry.result.state.is_terminal() => entry.task = Some(task),
                    // Finished (or timed out) before the handle was stored.
                    _ => task.abort(),
                }
            }
            Err(reason) => self.finish_action(&action_id, ActionState::Rejected { reason }),
        }
    }

    fn spawn_node_action(
        &self,
        request: ActionRequest,
    ) -> Result<tokio::task::AbortHandle, String> {
        let handler = self
            .state
            .actions
            .handlers
            .get(&request.name)
            .cloned()
            .ok_or_else(|| format!("no handler for action `{}`", request.name))?;
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| "no async runtime is available to run the action".to_owned())?;
        let app = self.clone();
        let context = ActionContext {
            app: self.clone(),
            action_id: request.action_id.clone(),
        };
        let action_id = request.action_id.clone();
        let task = runtime.spawn(async move {
            let (state, output) = handler.run(request, context).await.into_state();
            app.update_node_action(&action_id, state, output);
        });
        Ok(task.abort_handle())
    }

    /// Progress or the final outcome of a node-side or forwarded action.
    pub(crate) fn update_node_action(
        &self,
        action_id: &str,
        state: ActionState,
        output: BTreeMap<String, TypedConfigValue>,
    ) {
        let updated =
            self.action_registry()
                .update(action_id, state, output, Self::current_time_ms());
        if let Some(result) = updated {
            self.log_action_update(&result);
            self.notify_action_watchers(vec![result]);
        }
    }

    fn finish_action(&self, action_id: &str, state: ActionState) {
        self.update_node_action(action_id, state, BTreeMap::new());
    }

    fn log_action_update(&self, result: &ActionResult) {
        if result.state.is_terminal() {
            info!(
                node = %self.config.node_id,
                action_id = %result.action_id,
                target = %result.target,
                name = %result.name,
                state = %result.state,
                reason = result.state.reason().unwrap_or(""),
                "action finished"
            );
        } else {
            debug!(node = %self.config.node_id, action_id = %result.action_id, state = ?result.state, "action progress");
        }
    }

    /// Times out overdue actions, evicts expired results, and returns the next wake-up time.
    pub(crate) fn sweep_actions(&self) -> Option<u64> {
        let now_ms = Self::current_time_ms();
        let ttl_ms = millis(self.config.runtime_tuning.actions.result_ttl);
        let (timed_out, tasks, next) = {
            let mut registry = self.action_registry();
            let (timed_out, tasks) = registry.time_out(now_ms);
            registry.evict_expired(now_ms, ttl_ms);
            (timed_out, tasks, registry.next_wakeup_ms(ttl_ms))
        };
        for task in tasks {
            task.abort();
        }
        if !timed_out.is_empty() {
            for result in &timed_out {
                self.log_action_update(result);
            }
            self.notify_action_watchers(timed_out);
        }
        next
    }

    #[cfg(test)]
    pub(crate) async fn run_action_expiry_for_test(&self, shutdown: watch::Receiver<bool>) {
        self.run_action_expiry(shutdown).await
    }

    /// Sweeper loop: sleeps until the next deadline or result expiry (or a new action).
    pub(super) async fn run_action_expiry(&self, mut shutdown: watch::Receiver<bool>) {
        loop {
            if *shutdown.borrow() {
                return;
            }
            let next = self.sweep_actions();
            let sleep = next
                .map(|at| Duration::from_millis(at.saturating_sub(Self::current_time_ms()).max(1)));
            tokio::select! {
                _ = self.state.actions.wake.notified() => {}
                _ = async {
                    match sleep {
                        Some(delay) => tokio::time::sleep(delay).await,
                        None => std::future::pending::<()>().await,
                    }
                } => {}
                changed = shutdown.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
            }
        }
    }
}

fn duplicate_id(action_id: &str) -> NodeError {
    NodeError::Action(format!(
        "action id `{action_id}` is already used by a different request; choose a unique id"
    ))
}

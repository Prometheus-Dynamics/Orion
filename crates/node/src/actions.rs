//! Node-side action handlers (`docs/actions.md`).
//!
//! An embedder registers an [`ActionHandler`] per action name with
//! `NodeAppBuilder::with_action_handler`; actions that target this node (`ActionTarget::Node`)
//! with that name run it. Orion registers none by default, so every node action is rejected
//! until the embedder provides a handler. Provider- and executor-targeted actions are handled by
//! local clients instead (`orion-client`'s action request watch).

use crate::NodeApp;
use orion::control_plane::{ActionRequest, ActionState, TypedConfigValue};
use std::{collections::BTreeMap, future::Future, pin::Pin};

/// Final outcome of a node-side action handler.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActionOutcome {
    /// The action ran; `output` is a small result map (bounded by the node).
    Succeeded(BTreeMap<String, TypedConfigValue>),
    /// The action ran and failed.
    Failed(String),
    /// The handler refused the request (for example invalid arguments) without running it.
    Rejected(String),
}

impl ActionOutcome {
    /// Success without output.
    pub fn succeeded() -> Self {
        Self::Succeeded(BTreeMap::new())
    }

    pub fn failed(reason: impl Into<String>) -> Self {
        Self::Failed(reason.into())
    }

    pub fn rejected(reason: impl Into<String>) -> Self {
        Self::Rejected(reason.into())
    }

    pub(crate) fn into_state(self) -> (ActionState, BTreeMap<String, TypedConfigValue>) {
        match self {
            Self::Succeeded(output) => (ActionState::Succeeded, output),
            Self::Failed(reason) => (ActionState::Failed { reason }, BTreeMap::new()),
            Self::Rejected(reason) => (ActionState::Rejected { reason }, BTreeMap::new()),
        }
    }
}

/// Future returned by an [`ActionHandler`].
pub type ActionFuture = Pin<Box<dyn Future<Output = ActionOutcome> + Send + 'static>>;

/// Runs one named action on this node.
///
/// `run` is called on the node's tokio runtime for every accepted request; the returned future is
/// aborted when the action's deadline passes (the action then reports `TimedOut`). Report progress
/// with [`ActionContext::progress`]. Any `Fn(ActionRequest, ActionContext) -> impl Future<Output =
/// ActionOutcome>` closure is a handler.
pub trait ActionHandler: Send + Sync + 'static {
    fn run(&self, request: ActionRequest, context: ActionContext) -> ActionFuture;
}

impl<F, Fut> ActionHandler for F
where
    F: Fn(ActionRequest, ActionContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ActionOutcome> + Send + 'static,
{
    fn run(&self, request: ActionRequest, context: ActionContext) -> ActionFuture {
        Box::pin(self(request, context))
    }
}

/// Lets a running handler report progress and partial output.
#[derive(Clone)]
pub struct ActionContext {
    pub(crate) app: NodeApp,
    pub(crate) action_id: String,
}

impl ActionContext {
    /// The action's id.
    pub fn action_id(&self) -> &str {
        &self.action_id
    }

    /// Moves the action to `Running { progress }` (per-mille, clamped to 1000) and merges
    /// `output` into its result. Ignored once the action is final.
    pub fn progress(&self, progress: Option<u16>, output: BTreeMap<String, TypedConfigValue>) {
        self.app.update_node_action(
            &self.action_id,
            ActionState::Running {
                progress: progress.map(|value| value.min(1000)),
            },
            output,
        );
    }
}

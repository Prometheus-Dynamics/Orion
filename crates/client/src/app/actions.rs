//! Handling actions in providers and executors (`docs/actions.md`).
//!
//! A provider or executor registers as the action handler of its provider or executor with
//! `watch_action_requests`, or claims node-targeted action names (an out-of-process node action
//! handler such as a device manager) with `claim_node_actions`. It receives [`ActionRequest`]s
//! and reports progress and the outcome with [`ActionRequestWatch::report`] or the
//! `progress`/`succeed`/`fail`/`reject` helpers. When the handler's stream disconnects, the node
//! releases its registrations and claims and fails the actions still waiting for it
//! (`handler disconnected`); the watch re-registers when it reconnects.

use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;

use orion_control_plane::{
    ActionReport, ActionRequest, ActionResult, ActionState, ActionTarget, ClientEventKind,
    ClientRole, ControlMessage, TypedConfigValue,
};

use super::local_unary::{LocalUnaryClient, LocalUnaryRole};
use crate::{
    ClientError, LocalExecutorClient, LocalExecutorService, LocalProviderClient,
    LocalProviderService, LocalServiceRetryPolicy,
    session::{local_identity_for_role, local_session_config_for_role},
    stream::ClientEventStreamSession,
};

impl<Role: LocalUnaryRole> LocalUnaryClient<Role> {
    async fn report_action_result(&self, result: ActionReport) -> Result<(), ClientError> {
        self.send_and_expect_accepted(ControlMessage::ReportActionResult(Box::new(result)))
            .await
    }
}

/// Reports with the same client name (and so the same local address) as the handler stream.
#[derive(Clone, Debug)]
enum Reporter {
    Provider(LocalProviderClient),
    Executor(LocalExecutorClient),
}

impl Reporter {
    async fn report(&self, result: ActionReport) -> Result<(), ClientError> {
        match self {
            Self::Provider(client) => client.inner.report_action_result(result).await,
            Self::Executor(client) => client.inner.report_action_result(result).await,
        }
    }
}

struct HandlerTarget {
    socket_path: PathBuf,
    name: String,
    role: ClientRole,
    targets: Vec<ActionTarget>,
    node_actions: Vec<String>,
    retry_policy: LocalServiceRetryPolicy,
}

impl HandlerTarget {
    async fn subscribe(&self) -> Result<ClientEventStreamSession, ClientError> {
        self.retry_policy
            .retry(|| async {
                let identity = local_identity_for_role(self.name.clone(), self.role.clone());
                let config = local_session_config_for_role(&identity);
                let mut stream = ClientEventStreamSession::connect(
                    &self.socket_path,
                    identity,
                    config,
                    self.role.clone(),
                )
                .await?;
                if !self.targets.is_empty() {
                    stream
                        .subscribe_and_expect_accepted(ControlMessage::WatchActionRequests(
                            self.targets.clone(),
                        ))
                        .await?;
                }
                if !self.node_actions.is_empty() {
                    stream
                        .subscribe_and_expect_accepted(ControlMessage::ClaimNodeActions(
                            self.node_actions.clone(),
                        ))
                        .await?;
                }
                Ok(stream)
            })
            .await
    }
}

/// Action requests for a provider or executor, from
/// [`LocalProviderService::watch_action_requests`] or
/// [`LocalExecutorService::watch_action_requests`].
pub struct ActionRequestWatch {
    target: HandlerTarget,
    stream: ClientEventStreamSession,
    reporter: Reporter,
    pending: VecDeque<ActionRequest>,
}

impl ActionRequestWatch {
    async fn connect(target: HandlerTarget, reporter: Reporter) -> Result<Self, ClientError> {
        let stream = target.subscribe().await?;
        Ok(Self {
            target,
            stream,
            reporter,
            pending: VecDeque::new(),
        })
    }

    /// The providers or executors this watch handles actions for.
    pub fn targets(&self) -> &[ActionTarget] {
        &self.target.targets
    }

    /// The node action names this watch claimed.
    pub fn node_actions(&self) -> &[String] {
        &self.target.node_actions
    }

    /// Mirrors `result` into the status lane under the `action.<action_id>.*` key convention
    /// (`ActionResult::status_entries`), for example after a restart so watchers learn the
    /// outcome of an action whose in-memory record was lost. Needs a registration or claim that
    /// covers the result's target.
    pub async fn publish_action_status(&self, result: &ActionResult) -> Result<(), ClientError> {
        let entries = result.status_entries();
        match &self.reporter {
            Reporter::Provider(client) => client.publish_status(entries).await,
            Reporter::Executor(client) => client.publish_status(entries).await,
        }
    }

    /// Waits for the next action request. Reconnects (and re-registers) per the service's retry
    /// policy when the stream drops.
    pub async fn next(&mut self) -> Result<ActionRequest, ClientError> {
        loop {
            if let Some(request) = self.pending.pop_front() {
                return Ok(request);
            }
            match self.stream.next_client_events().await {
                Ok(events) => {
                    for event in events {
                        if let ClientEventKind::ActionRequest(request) = event.event {
                            self.pending.push_back(*request);
                        }
                    }
                }
                Err(_) => {
                    self.stream = self.target.subscribe().await?;
                }
            }
        }
    }

    /// Reports progress or the outcome (`Running`, `Succeeded`, `Failed`, or `Rejected`) of an
    /// action this watch received. Reports after the action finished (for example after its
    /// deadline) are ignored by the node.
    pub async fn report(&self, result: ActionReport) -> Result<(), ClientError> {
        self.reporter.report(result).await
    }

    /// Reports `Running { progress }` (per-mille, 0 to 1000).
    pub async fn progress(
        &self,
        action_id: impl Into<String>,
        progress: Option<u16>,
    ) -> Result<(), ClientError> {
        self.report(ActionReport::new(
            action_id,
            ActionState::Running { progress },
        ))
        .await
    }

    /// Reports success with a small output map.
    pub async fn succeed(
        &self,
        action_id: impl Into<String>,
        output: BTreeMap<String, TypedConfigValue>,
    ) -> Result<(), ClientError> {
        let mut result = ActionReport::new(action_id, ActionState::Succeeded);
        result.output = output;
        self.report(result).await
    }

    /// Reports that the action ran and failed.
    pub async fn fail(
        &self,
        action_id: impl Into<String>,
        reason: impl Into<String>,
    ) -> Result<(), ClientError> {
        self.report(ActionReport::new(
            action_id,
            ActionState::Failed {
                reason: reason.into(),
            },
        ))
        .await
    }

    /// Refuses an action without running it (for example an unknown name or invalid arguments).
    pub async fn reject(
        &self,
        action_id: impl Into<String>,
        reason: impl Into<String>,
    ) -> Result<(), ClientError> {
        self.report(ActionReport::new(
            action_id,
            ActionState::Rejected {
                reason: reason.into(),
            },
        ))
        .await
    }
}

impl LocalProviderService {
    fn action_handler(
        &self,
        targets: Vec<ActionTarget>,
        node_actions: Vec<String>,
    ) -> Result<(HandlerTarget, Reporter), ClientError> {
        let name = format!("{}-actions", self.client_name());
        let reporter = Reporter::Provider(self.runtime().provider_client(name.clone())?);
        Ok((
            HandlerTarget {
                socket_path: self.runtime().ipc_stream_socket_path().to_path_buf(),
                name,
                role: ClientRole::Provider,
                targets,
                node_actions,
                retry_policy: self.retry_policy(),
            },
            reporter,
        ))
    }

    /// Registers this service as the action handler of its provider (and so of the provider's
    /// resources) and returns the request stream. Register the provider first; the latest
    /// handler registration for a provider wins.
    pub async fn watch_action_requests(&self) -> Result<ActionRequestWatch, ClientError> {
        let (target, reporter) = self.action_handler(
            vec![ActionTarget::Provider(self.provider().provider_id.clone())],
            Vec::new(),
        )?;
        ActionRequestWatch::connect(target, reporter).await
    }

    /// Claims node-targeted actions with these names (for an out-of-process node action
    /// handler such as a device manager) and returns the request stream. Refused when the node
    /// handles a name in-process or another connected client holds it; released when the stream
    /// disconnects.
    pub async fn claim_node_actions<I, S>(
        &self,
        names: I,
    ) -> Result<ActionRequestWatch, ClientError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let (target, reporter) =
            self.action_handler(Vec::new(), names.into_iter().map(Into::into).collect())?;
        ActionRequestWatch::connect(target, reporter).await
    }
}

impl LocalExecutorService {
    fn action_handler(
        &self,
        targets: Vec<ActionTarget>,
        node_actions: Vec<String>,
    ) -> Result<(HandlerTarget, Reporter), ClientError> {
        let name = format!("{}-actions", self.client_name());
        let reporter = Reporter::Executor(self.runtime().executor_client(name.clone())?);
        Ok((
            HandlerTarget {
                socket_path: self.runtime().ipc_stream_socket_path().to_path_buf(),
                name,
                role: ClientRole::Executor,
                targets,
                node_actions,
                retry_policy: self.retry_policy(),
            },
            reporter,
        ))
    }

    /// Registers this service as the action handler of its executor (and of resources it
    /// realizes whose provider has no handler) and returns the request stream. Register the
    /// executor first; the latest handler registration wins.
    pub async fn watch_action_requests(&self) -> Result<ActionRequestWatch, ClientError> {
        let (target, reporter) = self.action_handler(
            vec![ActionTarget::Executor(self.executor().executor_id.clone())],
            Vec::new(),
        )?;
        ActionRequestWatch::connect(target, reporter).await
    }

    /// Claims node-targeted actions with these names; see
    /// [`LocalProviderService::claim_node_actions`].
    pub async fn claim_node_actions<I, S>(
        &self,
        names: I,
    ) -> Result<ActionRequestWatch, ClientError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let (target, reporter) =
            self.action_handler(Vec::new(), names.into_iter().map(Into::into).collect())?;
        ActionRequestWatch::connect(target, reporter).await
    }
}

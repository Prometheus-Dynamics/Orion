//! Volatile status lane: publish, query, and watch latest-value status entries.
//!
//! Status entries live in the node's memory only, for their TTL, and are never replicated. A
//! provider or executor may publish only for subjects it owns: its own provider or executor, the
//! resources of its provider (or realized by its executor), and the workloads its executor runs.
//! It must have published its provider or executor state on the same client name first.

use std::collections::VecDeque;
use std::path::PathBuf;

use orion_control_plane::{
    ClientEventKind, ClientRole, ControlMessage, StatusChange, StatusEntry, StatusQuery,
    StatusSubject, TypedConfigValue,
};

use super::local_unary::{LocalUnaryClient, LocalUnaryRole};
use crate::{
    ClientError, LocalExecutorApp, LocalExecutorClient, LocalExecutorService, LocalProviderApp,
    LocalProviderClient, LocalProviderService, LocalServiceRetryPolicy,
    session::{local_identity_for_role, local_session_config_for_role},
    stream::ClientEventStreamSession,
};

impl<Role: LocalUnaryRole> LocalUnaryClient<Role> {
    async fn publish_status(&self, entries: Vec<StatusEntry>) -> Result<(), ClientError> {
        self.send_and_expect_accepted(ControlMessage::PublishStatus(entries))
            .await
    }

    async fn query_status(&self, query: StatusQuery) -> Result<Vec<StatusEntry>, ClientError> {
        match self.request(ControlMessage::QueryStatus(query)).await? {
            ControlMessage::Status(entries) => Ok(entries),
            ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
            _ => Err(ClientError::NoMessageAvailable),
        }
    }
}

macro_rules! status_client_methods {
    ($client:ty) => {
        impl $client {
            /// Publishes a batch of status entries; the batch is stored atomically or rejected.
            pub async fn publish_status<I>(&self, entries: I) -> Result<(), ClientError>
            where
                I: IntoIterator<Item = StatusEntry>,
            {
                self.inner
                    .publish_status(entries.into_iter().collect())
                    .await
            }

            /// Live status entries matching `query`.
            pub async fn query_status(
                &self,
                query: StatusQuery,
            ) -> Result<Vec<StatusEntry>, ClientError> {
                self.inner.query_status(query).await
            }
        }
    };
}

status_client_methods!(LocalProviderClient);
status_client_methods!(LocalExecutorClient);

impl LocalProviderApp {
    /// A status entry for this app's provider.
    pub fn status_entry(&self, key: impl Into<String>, value: TypedConfigValue) -> StatusEntry {
        StatusEntry::new(
            StatusSubject::Provider(self.provider().provider_id.clone()),
            key,
            value,
        )
    }

    /// See [`LocalProviderClient::publish_status`].
    pub async fn publish_status<I>(&self, entries: I) -> Result<(), ClientError>
    where
        I: IntoIterator<Item = StatusEntry>,
    {
        self.client.publish_status(entries).await
    }

    /// See [`LocalProviderClient::query_status`].
    pub async fn query_status(&self, query: StatusQuery) -> Result<Vec<StatusEntry>, ClientError> {
        self.client.query_status(query).await
    }
}

impl LocalExecutorApp {
    /// A status entry for this app's executor.
    pub fn status_entry(&self, key: impl Into<String>, value: TypedConfigValue) -> StatusEntry {
        StatusEntry::new(
            StatusSubject::Executor(self.executor().executor_id.clone()),
            key,
            value,
        )
    }

    /// See [`LocalExecutorClient::publish_status`].
    pub async fn publish_status<I>(&self, entries: I) -> Result<(), ClientError>
    where
        I: IntoIterator<Item = StatusEntry>,
    {
        self.client.publish_status(entries).await
    }

    /// See [`LocalExecutorClient::query_status`].
    pub async fn query_status(&self, query: StatusQuery) -> Result<Vec<StatusEntry>, ClientError> {
        self.client.query_status(query).await
    }
}

impl LocalProviderService {
    /// A status entry for this service's provider.
    pub fn status_entry(&self, key: impl Into<String>, value: TypedConfigValue) -> StatusEntry {
        StatusEntry::new(
            StatusSubject::Provider(self.provider().provider_id.clone()),
            key,
            value,
        )
    }

    /// Publishes status entries (register the provider first, with the same client name).
    pub async fn publish_status<I>(&self, entries: I) -> Result<(), ClientError>
    where
        I: IntoIterator<Item = StatusEntry>,
    {
        let entries: Vec<StatusEntry> = entries.into_iter().collect();
        self.retry_policy()
            .retry(|| async {
                self.runtime()
                    .provider(self.client_name(), self.provider().clone())?
                    .publish_status(entries.clone())
                    .await
            })
            .await
    }

    /// Live status entries matching `query` (any subject).
    pub async fn query_status(&self, query: StatusQuery) -> Result<Vec<StatusEntry>, ClientError> {
        self.retry_policy()
            .retry(|| async {
                self.runtime()
                    .provider(self.client_name(), self.provider().clone())?
                    .query_status(query.clone())
                    .await
            })
            .await
    }

    /// Watches status entries matching `query`: a bootstrap change with every matching entry,
    /// then coalesced changes (newest value per key).
    pub async fn watch_status(&self, query: StatusQuery) -> Result<StatusWatch, ClientError> {
        StatusWatch::connect(StatusWatchTarget {
            socket_path: self.runtime().ipc_stream_socket_path().to_path_buf(),
            name: format!("{}-status", self.client_name()),
            role: ClientRole::Provider,
            query,
            retry_policy: self.retry_policy(),
        })
        .await
    }
}

impl LocalExecutorService {
    /// A status entry for this service's executor.
    pub fn status_entry(&self, key: impl Into<String>, value: TypedConfigValue) -> StatusEntry {
        StatusEntry::new(
            StatusSubject::Executor(self.executor().executor_id.clone()),
            key,
            value,
        )
    }

    /// Publishes status entries (register the executor first, with the same client name).
    pub async fn publish_status<I>(&self, entries: I) -> Result<(), ClientError>
    where
        I: IntoIterator<Item = StatusEntry>,
    {
        let entries: Vec<StatusEntry> = entries.into_iter().collect();
        self.retry_policy()
            .retry(|| async {
                self.runtime()
                    .executor(self.client_name(), self.executor().clone())?
                    .publish_status(entries.clone())
                    .await
            })
            .await
    }

    /// Live status entries matching `query` (any subject).
    pub async fn query_status(&self, query: StatusQuery) -> Result<Vec<StatusEntry>, ClientError> {
        self.retry_policy()
            .retry(|| async {
                self.runtime()
                    .executor(self.client_name(), self.executor().clone())?
                    .query_status(query.clone())
                    .await
            })
            .await
    }

    /// Watches status entries matching `query`; see [`LocalProviderService::watch_status`].
    pub async fn watch_status(&self, query: StatusQuery) -> Result<StatusWatch, ClientError> {
        StatusWatch::connect(StatusWatchTarget {
            socket_path: self.runtime().ipc_stream_socket_path().to_path_buf(),
            name: format!("{}-status", self.client_name()),
            role: ClientRole::Executor,
            query,
            retry_policy: self.retry_policy(),
        })
        .await
    }
}

struct StatusWatchTarget {
    socket_path: PathBuf,
    name: String,
    role: ClientRole,
    query: StatusQuery,
    retry_policy: LocalServiceRetryPolicy,
}

impl StatusWatchTarget {
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
                stream
                    .subscribe_and_expect_accepted(ControlMessage::WatchStatus(self.query.clone()))
                    .await?;
                Ok(stream)
            })
            .await
    }
}

/// A status-lane watch from [`LocalProviderService::watch_status`] or
/// [`LocalExecutorService::watch_status`].
///
/// The node coalesces changes per watcher (newest value per key), so a slow reader never builds
/// an unbounded backlog. After a reconnect (per the service's retry policy) the next change is a
/// new bootstrap: replace the local view instead of merging into it.
pub struct StatusWatch {
    target: StatusWatchTarget,
    stream: ClientEventStreamSession,
    pending: VecDeque<StatusChange>,
}

impl StatusWatch {
    async fn connect(target: StatusWatchTarget) -> Result<Self, ClientError> {
        let stream = target.subscribe().await?;
        Ok(Self {
            target,
            stream,
            pending: VecDeque::new(),
        })
    }

    /// The query this watch follows.
    pub fn query(&self) -> &StatusQuery {
        &self.target.query
    }

    /// Waits for the next status change. The first one has `bootstrap == true` and carries every
    /// matching entry.
    pub async fn next(&mut self) -> Result<StatusChange, ClientError> {
        loop {
            if let Some(change) = self.pending.pop_front() {
                return Ok(change);
            }
            match self.stream.next_client_events().await {
                Ok(events) => {
                    for event in events {
                        if let ClientEventKind::Status(change) = event.event {
                            self.pending.push_back(change);
                        }
                    }
                }
                Err(_) => {
                    self.stream = self.target.subscribe().await?;
                }
            }
        }
    }
}

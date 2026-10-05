//! Status queries for subjects owned by another node (`docs/remote-operator.md`,
//! `docs/actions.md`).
//!
//! The status lane is volatile and node-local: each node holds only the entries published on it.
//! A `QueryStatus` whose subject another node owns (`node/<id>`, or a provider, executor,
//! resource or workload that lives there) is forwarded once over the signed peer transport, like a
//! cross-node action; the owner answers from its own lane and never forwards again. Queries
//! without a subject, for local subjects, and from peers are answered locally. A subject whose
//! owner is unknown (not in the converged state) or not a configured peer is answered locally too,
//! which normally yields no entries.

use super::{NodeApp, NodeError};
use orion::{
    NodeId,
    control_plane::{ActionTarget, StatusEntry, StatusQuery, StatusSubject},
};

impl NodeApp {
    /// The node that owns `subject`, from the converged state.
    pub(crate) fn status_owner_node(&self, subject: &StatusSubject) -> Option<NodeId> {
        let target = match subject {
            StatusSubject::Node(node) => return Some(node.clone()),
            StatusSubject::Workload(workload) => {
                let store = self.store_read();
                return store
                    .observed
                    .workloads
                    .get(workload)
                    .and_then(|workload| workload.assigned_node_id.clone())
                    .or_else(|| {
                        store
                            .desired
                            .workloads
                            .get(workload)
                            .and_then(|workload| workload.assigned_node_id.clone())
                    });
            }
            StatusSubject::Provider(id) => ActionTarget::Provider(id.clone()),
            StatusSubject::Executor(id) => ActionTarget::Executor(id.clone()),
            StatusSubject::Resource(id) => ActionTarget::Resource(id.clone()),
        };
        self.action_owner_node(&target).ok()
    }

    /// `QueryStatus` from a local client or a remote operator: answered locally, or forwarded once
    /// to the node that owns the query's subject.
    pub(crate) fn query_status_routed(
        &self,
        query: &StatusQuery,
    ) -> Result<Vec<StatusEntry>, NodeError> {
        let owner = query
            .subject
            .as_ref()
            .and_then(|subject| self.status_owner_node(subject))
            .filter(|owner| owner != &self.config.node_id);
        match owner {
            #[cfg(peer_sync)]
            Some(owner) if self.peers_read().contains_key(&owner) => {
                self.forward_status_query_blocking(owner, query.clone())
            }
            _ => Ok(self.query_status(query)),
        }
    }

    /// Runs the forward from synchronous request handling. Request handlers run on blocking
    /// threads or workers of the multi-threaded runtime `orion-node` uses, where
    /// `block_in_place` + `block_on` is allowed. On a current-thread runtime the caller may be the
    /// thread that has to drive the network I/O, so the forward is refused instead of risking a
    /// deadlock or a panic.
    #[cfg(peer_sync)]
    fn forward_status_query_blocking(
        &self,
        owner: NodeId,
        query: StatusQuery,
    ) -> Result<Vec<StatusEntry>, NodeError> {
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            NodeError::Config("forwarding a status query needs an async runtime".into())
        })?;
        if handle.runtime_flavor() != tokio::runtime::RuntimeFlavor::MultiThread {
            return Err(NodeError::Config(format!(
                "forwarding a status query to node {owner} needs the multi-threaded runtime; \
                 query that node directly"
            )));
        }
        let app = self.clone();
        tokio::task::block_in_place(move || {
            handle.block_on(async move { app.forward_status_query(owner, query).await })
        })
    }

    #[cfg(peer_sync)]
    async fn forward_status_query(
        &self,
        owner: NodeId,
        query: StatusQuery,
    ) -> Result<Vec<StatusEntry>, NodeError> {
        use super::peer_transport::PeerSyncTransport;
        use orion::{control_plane::ControlMessage, transport::http::HttpResponsePayload};

        let peer =
            self.peers_read().get(&owner).cloned().ok_or_else(|| {
                NodeError::Config(format!("node {owner} is not a configured peer"))
            })?;
        let channel = self.open_peer_channel(&owner, &peer)?;
        match channel
            .send_control(self, &owner, ControlMessage::QueryStatus(query))
            .await
        {
            Ok(HttpResponsePayload::Status(entries)) => Ok(entries),
            Ok(_) => Err(NodeError::Storage(format!(
                "node {owner} answered a status query with an unexpected response"
            ))),
            Err(error) => Err(NodeError::Storage(format!(
                "forwarding the status query to node {owner} failed: {error}"
            ))),
        }
    }
}

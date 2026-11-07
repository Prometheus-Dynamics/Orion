//! The executor role and record reads of link sessions (interface revision 3 on Unix links).
//!
//! An executor session is a local executor client of the node, registered under
//! `link:<link>/<device name>#executor` with the caller's peer identity, so the control pipeline
//! authorizes it as it does a local executor. Record reads are served from the node's state
//! here, for sessions whose client is registered (and so authorized) already.

use super::{NodeApp, NodeError};
use orion::control_plane::{
    ClientHello, ClientRole, ControlMessage, ExecutorStateUpdate, ExecutorWorkloadQuery,
    ResourceRecord,
};
use orion::transport::ipc::{LocalAddress, UnixPeerIdentity};
use orion_link::message::{ExecutorState, Record, RecordFilter, RecordKind};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Notify;

impl NodeApp {
    /// Registers the executor client of a link session and watches its executor's workloads
    /// once it knows the executor (see [`NodeApp::apply_link_executor_state`]).
    pub(crate) fn register_link_executor(
        &self,
        source: &LocalAddress,
        client_name: &str,
        identity: &UnixPeerIdentity,
        wake: Arc<Notify>,
    ) -> Result<(), NodeError> {
        self.serve_link_client_message(
            source,
            identity,
            ControlMessage::ClientHello(ClientHello {
                client_name: orion_core::ClientName::new(client_name.to_owned()),
                role: ClientRole::Executor,
            }),
        )?;
        self.with_client_mut(source, |client| client.link_wake = Some(wake))
    }

    /// Applies an executor session's state as a local executor's update: the executor on this
    /// node, and each reported workload as the node's record of it with the reported observed
    /// state. Workloads the node does not assign to this node are left out. With `watch`, the
    /// executor's workloads are watched afterwards (the first state of a session).
    pub(crate) fn apply_link_executor_state(
        &self,
        source: &LocalAddress,
        identity: &UnixPeerIdentity,
        state: ExecutorState,
        watch: bool,
    ) -> Result<(), NodeError> {
        let ExecutorState {
            mut executor,
            workloads,
            resources,
        } = state;
        executor.node_id = self.config.node_id.clone();
        let executor_id = executor.executor_id.clone();
        let workloads = {
            let store = self.store_read();
            workloads
                .into_iter()
                .filter_map(|observed| {
                    let mut record = store.desired.workloads.get(&observed.workload_id)?.clone();
                    if record.assigned_node_id.as_ref() != Some(&self.config.node_id) {
                        return None;
                    }
                    record.observed_state = observed.state;
                    Some(record)
                })
                .collect()
        };
        self.serve_link_client_message(
            source,
            identity,
            ControlMessage::ExecutorState(ExecutorStateUpdate {
                executor,
                workloads,
                resources,
            }),
        )?;
        if watch {
            self.serve_link_client_message(
                source,
                identity,
                ControlMessage::WatchExecutorWorkloads(ExecutorWorkloadQuery { executor_id }),
            )?;
        }
        Ok(())
    }

    /// A counter that moves whenever desired or observed state may have changed: a record watch
    /// recomputes its set only when it moved.
    pub(crate) fn link_state_generation(&self) -> u64 {
        *self.state.links.state_changed.borrow()
    }

    /// Wakes link tasks after a change of observed state, for record watches.
    pub(super) fn notify_link_state_change(&self) {
        self.state
            .links
            .state_changed
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }

    pub(crate) fn link_state_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.state.links.state_changed.subscribe()
    }

    /// The records `filter` selects, in id order. A workload is the node's record with its
    /// observed state; resources are the desired ones and those executors realize.
    pub(crate) fn link_records(&self, filter: &RecordFilter) -> Vec<Record> {
        let store = self.store_read();
        let (desired, observed) = (&store.desired, &store.observed);
        let id_ok = |id: &str| filter.id.as_deref().is_none_or(|wanted| wanted == id);
        let node_ok = |node: Option<&str>| {
            filter
                .node
                .as_deref()
                .is_none_or(|wanted| node == Some(wanted))
        };
        match filter.kind {
            RecordKind::Workload => desired
                .workloads
                .values()
                .filter(|w| id_ok(w.workload_id.as_str()))
                .filter(|w| node_ok(w.assigned_node_id.as_ref().map(|n| n.as_str())))
                .map(|w| {
                    let mut record = w.clone();
                    if let Some(seen) = observed.workloads.get(&w.workload_id) {
                        record.observed_state = seen.observed_state;
                    }
                    Record::Workload(record)
                })
                .collect(),
            RecordKind::Resource => {
                let mut resources: BTreeMap<&str, &ResourceRecord> = desired
                    .resources
                    .values()
                    .map(|r| (r.resource_id.as_str(), r))
                    .collect();
                resources.extend(
                    observed
                        .resources
                        .values()
                        .map(|r| (r.resource_id.as_str(), r)),
                );
                let node_of = |r: &ResourceRecord| match &r.realized_by_executor_id {
                    Some(executor) => desired.executors.get(executor).map(|e| e.node_id.as_str()),
                    None => desired
                        .providers
                        .get(&r.provider_id)
                        .map(|p| p.node_id.as_str()),
                };
                resources
                    .into_values()
                    .filter(|r| id_ok(r.resource_id.as_str()) && node_ok(node_of(r)))
                    .map(|r| Record::Resource(r.clone()))
                    .collect()
            }
            RecordKind::Provider => desired
                .providers
                .values()
                .filter(|p| id_ok(p.provider_id.as_str()) && node_ok(Some(p.node_id.as_str())))
                .map(|p| Record::Provider(p.clone()))
                .collect(),
            RecordKind::Executor => desired
                .executors
                .values()
                .filter(|e| id_ok(e.executor_id.as_str()) && node_ok(Some(e.node_id.as_str())))
                .map(|e| Record::Executor(e.clone()))
                .collect(),
        }
    }
}

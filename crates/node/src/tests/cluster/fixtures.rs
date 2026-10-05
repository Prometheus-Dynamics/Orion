//! In-process clusters over `orion+tcp` with test executors and providers, for the placement and
//! cross-node binding tests (`docs/placement.md`).

use super::*;
use crate::config::PlacementTuning;
use orion::control_plane::{
    LeaseRecord, ProviderRecord, ResourceBinding, ResourceOwnershipMode, WorkloadPlacement,
};
use std::collections::BTreeMap;

pub(super) const RUNTIME: &str = "graph.exec.v1";

/// Executor that "runs" whatever it is told and reports it back as running.
#[derive(Clone)]
pub(super) struct RecordingExecutor {
    node_id: NodeId,
    running: Arc<Mutex<BTreeMap<WorkloadId, WorkloadRecord>>>,
    pub(super) starts: Arc<Mutex<Vec<orion::runtime::WorkloadPlan>>>,
}

impl RecordingExecutor {
    pub(super) fn new(node_id: &str) -> Self {
        Self {
            node_id: NodeId::new(node_id),
            running: Arc::default(),
            starts: Arc::default(),
        }
    }

    pub(super) fn running(&self) -> BTreeMap<WorkloadId, WorkloadRecord> {
        self.running.lock().expect("lock").clone()
    }

    /// Bindings of the running workload `workload_id`, if it runs here.
    pub(super) fn bindings(&self, workload_id: &str) -> Option<Vec<ResourceBinding>> {
        self.running()
            .get(&WorkloadId::new(workload_id))
            .map(|workload| workload.resource_bindings.clone())
    }
}

impl ExecutorIntegration for RecordingExecutor {
    fn executor_record(&self) -> ExecutorRecord {
        ExecutorRecord::builder(
            ExecutorId::new(format!("executor.{}", self.node_id)),
            self.node_id.clone(),
        )
        .runtime_type(RUNTIME)
        .build()
    }

    fn snapshot(&self) -> ExecutorSnapshot {
        ExecutorSnapshot {
            executor: self.executor_record(),
            workloads: self.running().into_values().collect(),
            resources: Vec::new(),
        }
    }

    fn apply_command(&self, command: &ExecutorCommand) -> Result<(), orion::runtime::RuntimeError> {
        let mut running = self.running.lock().expect("lock");
        match command {
            ExecutorCommand::Start(plan) => {
                let mut workload = plan.workload.clone();
                workload.observed_state = WorkloadObservedState::Running;
                workload.resource_bindings = plan.resource_bindings.clone();
                running.insert(workload.workload_id.clone(), workload);
                self.starts.lock().expect("lock").push(plan.clone());
            }
            ExecutorCommand::Stop { workload_id, .. } => {
                running.remove(workload_id);
            }
        }
        Ok(())
    }
}

/// Provider whose resource list tests can change.
#[derive(Clone)]
pub(super) struct ListProvider {
    record: ProviderRecord,
    pub(super) resources: Arc<Mutex<Vec<ResourceRecord>>>,
}

impl ListProvider {
    pub(super) fn new(provider_id: &str, node_id: &str) -> Self {
        Self {
            record: ProviderRecord::builder(ProviderId::new(provider_id), NodeId::new(node_id))
                .resource_type("camera")
                .build(),
            resources: Arc::default(),
        }
    }

    pub(super) fn set(&self, resources: Vec<ResourceRecord>) {
        *self.resources.lock().expect("lock") = resources;
    }
}

impl ProviderIntegration for ListProvider {
    fn provider_record(&self) -> ProviderRecord {
        self.record.clone()
    }

    fn snapshot(&self) -> ProviderSnapshot {
        ProviderSnapshot {
            provider: self.record.clone(),
            resources: self.resources.lock().expect("lock").clone(),
        }
    }
}

/// An exclusive camera owned by `provider_id`, reachable at `endpoint`.
pub(super) fn camera(resource_id: &str, provider_id: &str, endpoint: &str) -> ResourceRecord {
    ResourceRecord::builder(
        orion::ResourceId::new(resource_id),
        "camera",
        ProviderId::new(provider_id),
    )
    .ownership_mode(ResourceOwnershipMode::Exclusive)
    .health(HealthState::Healthy)
    .availability(AvailabilityState::Available)
    .endpoint(endpoint)
    .build()
}

pub(super) fn placed_workload(id: &str, placement: WorkloadPlacement) -> WorkloadRecord {
    WorkloadRecord::builder(WorkloadId::new(id), RUNTIME, "artifact.placement")
        .desired_state(DesiredState::Running)
        .placement(placement)
        .build()
}

pub(super) struct ClusterNode {
    pub(super) app: NodeApp,
    pub(super) addr: std::net::SocketAddr,
    pub(super) executor: RecordingExecutor,
    server: crate::app::GracefulTaskHandle<NodeError>,
}

impl ClusterNode {
    pub(super) fn id(&self) -> NodeId {
        self.app.config.node_id.clone()
    }

    pub(super) fn workload(&self, id: &str) -> Option<WorkloadRecord> {
        self.app
            .state_snapshot()
            .state
            .desired
            .workloads
            .get(&WorkloadId::new(id))
            .cloned()
    }

    pub(super) fn assignee(&self, id: &str) -> Option<NodeId> {
        self.workload(id)
            .and_then(|workload| workload.assigned_node_id)
    }

    pub(super) fn lease(&self, resource_id: &str) -> Option<LeaseRecord> {
        self.app
            .state_snapshot()
            .state
            .desired
            .leases
            .get(&orion::ResourceId::new(resource_id))
            .cloned()
    }

    pub(super) async fn shutdown(self) {
        self.server.shutdown().await.expect("listener should stop");
    }
}

/// Starts a node with an `orion+tcp` listener and a recording executor.
pub(super) async fn tcp_cluster_node(
    node_id: &'static str,
    labels: &str,
    liveness: Duration,
    grace: Duration,
) -> ClusterNode {
    let mut config = test_node_config_with_auth(
        node_id,
        "node-placement",
        crate::PeerAuthenticationMode::Optional,
    );
    config.runtime_tuning.placement = PlacementTuning::default()
        .with_labels(labels)
        .with_liveness_timeout(liveness)
        .with_grace(grace);
    let app = NodeApp::builder()
        .config(config)
        .try_build()
        .expect("node app should build");
    let (addr, server) = app
        .start_peer_tcp_server("127.0.0.1:0".parse().expect("address should parse"))
        .await
        .expect("peer TCP listener should start");
    let executor = RecordingExecutor::new(node_id);
    app.register_executor(executor.clone())
        .expect("executor should register");
    ClusterNode {
        app,
        addr,
        executor,
        server,
    }
}

/// Every node knows every other as an `orion+tcp` peer with its key pinned.
pub(super) fn mesh(nodes: &[&ClusterNode]) {
    for node in nodes {
        for peer in nodes {
            if node.id() == peer.id() || node.app.peer_state_for_test(&peer.id()).is_some() {
                continue;
            }
            node.app
                .register_peer(
                    PeerConfig::new(
                        peer.id(),
                        orion_core::PeerBaseUrl::new(format!("orion+tcp://{}", peer.addr)),
                    )
                    .with_trusted_public_key_hex(peer.app.security.public_key_hex()),
                )
                .expect("peer registration should succeed");
        }
    }
}

/// One round among `nodes` (a partition when some nodes are left out): every ordered pair
/// syncs, then every node runs reconcile passes.
pub(super) async fn round(nodes: &[&ClusterNode]) {
    for local in nodes {
        for remote in nodes {
            if local.id() != remote.id() {
                local
                    .app
                    .sync_peer(&remote.id())
                    .await
                    .expect("orion+tcp sync should succeed");
            }
        }
    }
    for node in nodes {
        for _ in 0..3 {
            node.app
                .tick_async()
                .await
                .expect("reconcile should succeed");
        }
    }
}

pub(super) async fn rounds(nodes: &[&ClusterNode], count: usize) {
    for _ in 0..count {
        round(nodes).await;
    }
}

/// Runs rounds until `done` holds (or panics after `timeout`).
pub(super) async fn rounds_until(
    nodes: &[&ClusterNode],
    timeout: Duration,
    what: &str,
    mut done: impl FnMut() -> bool,
) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        round(nodes).await;
        if done() {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Asserts every node has the same assignment for `workload_id` and returns it.
pub(super) fn agreed_assignee(nodes: &[&ClusterNode], workload_id: &str) -> Option<NodeId> {
    let first = nodes[0].assignee(workload_id);
    for node in &nodes[1..] {
        assert_eq!(
            node.assignee(workload_id),
            first,
            "{} and {} disagree on {workload_id}",
            nodes[0].id(),
            node.id()
        );
    }
    first
}

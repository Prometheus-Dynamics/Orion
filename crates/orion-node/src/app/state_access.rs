use super::{
    ClientRegistry, ClientRegistryTxn, DesiredClusterState, DesiredStateMetadataCache,
    DesiredStateSummaryCache, ExecutorRegistry, LifecycleSnapshot, LocalClientState, MutationBatch,
    NodeApp, NodeError, ObservabilityState, ObservabilityTxn, PeerClientRegistry, PeerRegistry,
    PeerState, ProviderRegistry,
};
use crate::auth::{LocalSessionLookup, PeerObservedScope, PeerPolicyLookup};
use crate::lock::{read_rwlock, write_rwlock};
use orion::{
    NodeId,
    control_plane::{ClientSession, DesiredClusterState as ControlDesiredClusterState},
    runtime::LocalRuntimeStore,
    transport::ipc::LocalAddress,
};
use std::collections::BTreeSet;
use std::sync::{Arc, RwLockReadGuard, RwLockWriteGuard};

// Shared-state locking contract:
// - Keep std::sync::RwLock guards scoped to synchronous sections only; do not hold them across
//   `.await`.
// - Persisted desired-state mutations acquire locks in a fixed order:
//   store -> mutation_history -> mutation_history_baseline.
// - If a workflow must touch more than one state family, acquire persisted-state locks before
//   registry locks, and treat observability as the terminal lock so metrics/event updates never
//   block higher-level state mutation or reconciliation work.
// - Peer/client/observability helpers intentionally operate on one registry at a time so hot
//   request and sync paths avoid nested registry locks.
// - Async workflows should clone or derive the data they need, drop guards, and only then await.

impl LocalSessionLookup for AppLocalSessionLookup {
    fn session_for(&self, source: &LocalAddress) -> Option<ClientSession> {
        let clients = read_rwlock(self.state.clients.clients.read(), "node client registry");
        clients.get(source).map(|client| client.session.clone())
    }
}

impl PeerPolicyLookup for AppLocalSessionLookup {
    fn is_configured_peer(&self, node_id: &NodeId) -> bool {
        let peers = read_rwlock(self.state.peers.peers.read(), "node peer registry");
        peers.contains_key(node_id)
    }

    fn observed_scope_for(&self, node_id: &NodeId) -> PeerObservedScope {
        let store = read_rwlock(self.state.persisted.store.read(), "node runtime store");
        let provider_ids: BTreeSet<_> = store
            .desired
            .providers
            .values()
            .filter(|provider| &provider.node_id == node_id)
            .map(|provider| provider.provider_id.clone())
            .collect();
        let executor_ids: BTreeSet<_> = store
            .desired
            .executors
            .values()
            .filter(|executor| &executor.node_id == node_id)
            .map(|executor| executor.executor_id.clone())
            .collect();
        let resource_ids = store
            .desired
            .resources
            .values()
            .filter(|resource| {
                provider_ids.contains(&resource.provider_id)
                    || resource
                        .realized_by_executor_id
                        .as_ref()
                        .map(|executor_id| executor_ids.contains(executor_id))
                        .unwrap_or(false)
            })
            .map(|resource| resource.resource_id.clone())
            .collect();
        PeerObservedScope {
            executor_ids,
            resource_ids,
        }
    }
}

impl NodeApp {
    pub(super) fn store_read(&self) -> RwLockReadGuard<'_, LocalRuntimeStore> {
        read_rwlock(self.state.persisted.store.read(), "node runtime store")
    }

    pub(super) fn store_lock(&self) -> RwLockWriteGuard<'_, LocalRuntimeStore> {
        write_rwlock(self.state.persisted.store.write(), "node runtime store")
    }

    pub(super) fn with_store_mut<T>(&self, mutate: impl FnOnce(&mut LocalRuntimeStore) -> T) -> T {
        let mut store = self.store_lock();
        mutate(&mut store)
    }

    pub(super) fn providers_read(&self) -> RwLockReadGuard<'_, ProviderRegistry> {
        read_rwlock(
            self.state.runtime.providers.read(),
            "node provider registry",
        )
    }

    pub(super) fn providers_lock(&self) -> RwLockWriteGuard<'_, ProviderRegistry> {
        write_rwlock(
            self.state.runtime.providers.write(),
            "node provider registry",
        )
    }

    pub(super) fn peers_read(&self) -> RwLockReadGuard<'_, PeerRegistry> {
        read_rwlock(self.state.peers.peers.read(), "node peer registry")
    }

    pub(super) fn peers_lock(&self) -> RwLockWriteGuard<'_, PeerRegistry> {
        write_rwlock(self.state.peers.peers.write(), "node peer registry")
    }

    pub(super) fn with_peer_mut<T>(
        &self,
        node_id: &NodeId,
        mutate: impl FnOnce(&mut PeerState) -> T,
    ) -> Result<T, NodeError> {
        self.with_peer_registry_txn(|txn| {
            let peer = txn.peer_mut(node_id)?;
            Ok(mutate(peer))
        })
    }

    pub(super) fn with_peers_mut<T>(&self, mutate: impl FnOnce(&mut PeerRegistry) -> T) -> T {
        self.with_peer_registry_txn(|txn| mutate(txn.peers_mut()))
    }

    pub(super) fn with_peer_registry_txn<T>(
        &self,
        mutate: impl FnOnce(&mut PeerRegistryTxn<'_>) -> T,
    ) -> T {
        let mut peers = self.peers_lock();
        let mut txn = PeerRegistryTxn::new(&mut peers);
        mutate(&mut txn)
    }

    #[cfg(test)]
    pub(crate) fn peer_state_for_test(&self, node_id: &NodeId) -> Option<PeerState> {
        self.peers_read().get(node_id).cloned()
    }

    #[cfg(test)]
    pub(crate) fn effective_parallel_in_flight_for_test(
        requested: usize,
        peer_count: usize,
    ) -> usize {
        let config = crate::config::NodeConfig::try_from_env()
            .expect("test helper should build node config from environment");
        Self::try_new(config)
            .expect("test helper should build node app")
            .effective_parallel_in_flight(requested, peer_count)
    }

    #[cfg(test)]
    pub(crate) fn parallel_spawn_stagger_ms_for_test(slot: usize, peer_count: usize) -> u64 {
        let config = crate::config::NodeConfig::try_from_env()
            .expect("test helper should build node config from environment");
        Self::try_new(config)
            .expect("test helper should build node app")
            .parallel_spawn_stagger_ms(slot, peer_count)
    }

    pub(super) fn peer_clients_read(&self) -> RwLockReadGuard<'_, PeerClientRegistry> {
        read_rwlock(
            self.state.peers.peer_clients.read(),
            "node peer client registry",
        )
    }

    pub(super) fn peer_clients_lock(&self) -> RwLockWriteGuard<'_, PeerClientRegistry> {
        write_rwlock(
            self.state.peers.peer_clients.write(),
            "node peer client registry",
        )
    }

    pub(super) fn with_peer_clients_mut<T>(
        &self,
        mutate: impl FnOnce(&mut PeerClientRegistry) -> T,
    ) -> T {
        let mut peer_clients = self.peer_clients_lock();
        mutate(&mut peer_clients)
    }

    pub(super) fn executors_read(&self) -> RwLockReadGuard<'_, ExecutorRegistry> {
        read_rwlock(
            self.state.runtime.executors.read(),
            "node executor registry",
        )
    }

    pub(super) fn executors_lock(&self) -> RwLockWriteGuard<'_, ExecutorRegistry> {
        write_rwlock(
            self.state.runtime.executors.write(),
            "node executor registry",
        )
    }

    pub(super) fn clients_read(&self) -> RwLockReadGuard<'_, ClientRegistry> {
        read_rwlock(self.state.clients.clients.read(), "node client registry")
    }

    pub(super) fn clients_lock(&self) -> RwLockWriteGuard<'_, ClientRegistry> {
        write_rwlock(self.state.clients.clients.write(), "node client registry")
    }

    pub(super) fn with_client_mut<T>(
        &self,
        source: &LocalAddress,
        mutate: impl FnOnce(&mut LocalClientState) -> T,
    ) -> Result<T, NodeError> {
        self.with_client_registry_txn(|txn| {
            let client = txn.client_mut(source)?;
            Ok(mutate(client))
        })
    }

    pub(super) fn with_client_mut_if_present<T>(
        &self,
        source: &LocalAddress,
        mutate: impl FnOnce(&mut LocalClientState) -> T,
    ) -> Option<T> {
        self.with_client_registry_txn(|txn| txn.client_mut_if_present(source).map(mutate))
    }

    pub(super) fn with_clients_mut<T>(&self, mutate: impl FnOnce(&mut ClientRegistry) -> T) -> T {
        let mut clients = self.clients_lock();
        mutate(&mut clients)
    }

    pub(super) fn with_client_registry_txn<T>(
        &self,
        mutate: impl FnOnce(&mut ClientRegistryTxn<'_>) -> T,
    ) -> T {
        self.with_clients_mut(|clients| {
            let mut txn = ClientRegistryTxn::new(clients);
            mutate(&mut txn)
        })
    }

    pub(super) fn observability_read(&self) -> RwLockReadGuard<'_, ObservabilityState> {
        read_rwlock(self.state.observability.read(), "node observability")
    }

    pub(super) fn observability_lock(&self) -> RwLockWriteGuard<'_, ObservabilityState> {
        write_rwlock(self.state.observability.write(), "node observability")
    }

    pub(super) fn with_observability_txn<T>(
        &self,
        mutate: impl FnOnce(&mut ObservabilityTxn<'_>) -> T,
    ) -> T {
        let mut observability = self.observability_lock();
        let mut txn = ObservabilityTxn::new(&mut observability);
        mutate(&mut txn)
    }

    pub(super) fn lifecycle_snapshot(&self) -> LifecycleSnapshot {
        self.state.lifecycle.snapshot()
    }

    pub(super) fn mutation_history_read(&self) -> RwLockReadGuard<'_, Vec<MutationBatch>> {
        read_rwlock(
            self.state.persisted.mutation_history.read(),
            "node mutation history",
        )
    }

    pub(super) fn mutation_history_lock(&self) -> RwLockWriteGuard<'_, Vec<MutationBatch>> {
        write_rwlock(
            self.state.persisted.mutation_history.write(),
            "node mutation history",
        )
    }

    pub(super) fn mutation_history_baseline_read(
        &self,
    ) -> RwLockReadGuard<'_, ControlDesiredClusterState> {
        read_rwlock(
            self.state.persisted.mutation_history_baseline.read(),
            "node mutation history baseline",
        )
    }

    pub(super) fn mutation_history_baseline_lock(
        &self,
    ) -> RwLockWriteGuard<'_, ControlDesiredClusterState> {
        write_rwlock(
            self.state.persisted.mutation_history_baseline.write(),
            "node mutation history baseline",
        )
    }

    pub(super) fn with_persisted_state_read<T>(
        &self,
        read: impl FnOnce(&LocalRuntimeStore, &Vec<MutationBatch>, &ControlDesiredClusterState) -> T,
    ) -> T {
        let store = self.store_read();
        let history = self.mutation_history_read();
        let baseline = self.mutation_history_baseline_read();
        read(&store, &history, &baseline)
    }

    pub(super) fn desired_metadata_cache_read(
        &self,
    ) -> RwLockReadGuard<'_, Option<DesiredStateMetadataCache>> {
        read_rwlock(
            self.state.persisted.desired_metadata_cache.read(),
            "node desired metadata cache",
        )
    }

    pub(super) fn desired_metadata_cache_lock(
        &self,
    ) -> RwLockWriteGuard<'_, Option<DesiredStateMetadataCache>> {
        write_rwlock(
            self.state.persisted.desired_metadata_cache.write(),
            "node desired metadata cache",
        )
    }

    pub(super) fn desired_summary_cache_read(
        &self,
    ) -> RwLockReadGuard<'_, Option<DesiredStateSummaryCache>> {
        read_rwlock(
            self.state.persisted.desired_summary_cache.read(),
            "node desired state summary cache",
        )
    }

    pub(super) fn desired_summary_cache_lock(
        &self,
    ) -> RwLockWriteGuard<'_, Option<DesiredStateSummaryCache>> {
        write_rwlock(
            self.state.persisted.desired_summary_cache.write(),
            "node desired state summary cache",
        )
    }
}

pub(super) struct PersistedState {
    pub(super) store: std::sync::RwLock<LocalRuntimeStore>,
    pub(super) mutation_history: std::sync::RwLock<Vec<MutationBatch>>,
    pub(super) mutation_history_baseline: std::sync::RwLock<DesiredClusterState>,
    pub(super) desired_metadata_cache: std::sync::RwLock<Option<DesiredStateMetadataCache>>,
    pub(super) desired_summary_cache: std::sync::RwLock<Option<DesiredStateSummaryCache>>,
}

pub(super) struct PeerRegistryState {
    pub(super) peers: std::sync::RwLock<PeerRegistry>,
    pub(super) peer_clients: std::sync::RwLock<PeerClientRegistry>,
}

pub(super) struct RuntimeRegistryState {
    pub(super) providers: std::sync::RwLock<ProviderRegistry>,
    pub(super) executors: std::sync::RwLock<ExecutorRegistry>,
}

pub(super) struct ClientRegistryState {
    pub(super) clients: std::sync::RwLock<ClientRegistry>,
}

pub(super) struct NodeState {
    pub(super) persisted: PersistedState,
    pub(super) peers: PeerRegistryState,
    pub(super) runtime: RuntimeRegistryState,
    pub(super) clients: ClientRegistryState,
    pub(super) observability: std::sync::RwLock<ObservabilityState>,
    pub(super) lifecycle: super::LifecycleState,
}

#[derive(Clone)]
pub(super) struct AppLocalSessionLookup {
    pub(super) state: Arc<NodeState>,
}

pub(super) struct PeerRegistryTxn<'a> {
    peers: &'a mut PeerRegistry,
}

impl<'a> PeerRegistryTxn<'a> {
    pub(super) fn new(peers: &'a mut PeerRegistry) -> Self {
        Self { peers }
    }

    pub(super) fn peer_mut(&mut self, node_id: &NodeId) -> Result<&mut PeerState, NodeError> {
        self.peers
            .get_mut(node_id)
            .ok_or_else(|| NodeError::UnknownPeer(node_id.clone()))
    }

    pub(super) fn peers_mut(&mut self) -> &mut PeerRegistry {
        self.peers
    }
}

#[cfg(test)]
mod tests {
    use crate::lock::{read_rwlock, write_rwlock};
    use std::sync::{Arc, RwLock};

    #[test]
    fn poisoned_read_lock_recovers_inner_value() {
        let lock = Arc::new(RwLock::new(0_u8));
        let poison = Arc::clone(&lock);
        let _ = std::panic::catch_unwind(move || {
            let _guard = poison.write().expect("write lock should succeed");
            panic!("poison the lock");
        });
        let guard = read_rwlock(lock.read(), "poisoned test lock");
        assert_eq!(*guard, 0);
    }

    #[test]
    fn poisoned_write_lock_recovers_inner_value() {
        let lock = Arc::new(RwLock::new(0_u8));
        let poison = Arc::clone(&lock);
        let _ = std::panic::catch_unwind(move || {
            let _guard = poison.write().expect("write lock should succeed");
            panic!("poison the lock");
        });
        let mut guard = write_rwlock(lock.write(), "poisoned test lock");
        *guard = 1;
        assert_eq!(*guard, 1);
    }
}

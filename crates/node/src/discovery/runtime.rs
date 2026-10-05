//! The discovery runtime: drives a backend, maintains the discovered set, starts automatic
//! enrollments and answers `QueryDiscovery`.

use super::{
    advert::{Advertisement, hex, key_fingerprint},
    backend::{Announcement, DiscoveryBackend, DiscoveryEvent},
    config::DiscoveryConfig,
    enrollment::PendingChallenges,
    registry::{DiscoveredPeer, DiscoveryRegistry, Observation, duration_ms},
};
use crate::{NodeApp, NodeError, PeerAuthenticationMode};
use orion::NodeId;
use orion_control_plane::{
    DiscoveredPeerRecord, DiscoveredPeerState, DiscoveryMetricsSnapshot, DiscoverySnapshot,
};
use orion_core::CONTROL_PROTOCOL_VERSION;
use std::{
    collections::BTreeSet,
    net::IpAddr,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

/// Capacity of the backend → runtime event channel; a flood beyond it is dropped.
const EVENT_CHANNEL_CAPACITY: usize = 256;
/// Upper bound of the automatic enrollment retry delay.
const MAX_ENROLLMENT_RETRY_MS: u64 = 300_000;

/// The listeners a node advertises.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AdvertisedEndpoints {
    /// `orion+tcp` listener port (`ORION_NODE_PEER_ADDR`).
    pub peer_tcp_port: Option<u16>,
    /// HTTP peer listener port, if HTTP peer sync is enabled.
    pub http_port: Option<u16>,
    pub https: bool,
    /// Addresses to announce. Empty lets the backend announce every interface address.
    pub addresses: Vec<IpAddr>,
}

/// Shared discovery state of one node (`NodeApp::discovery`).
pub(crate) struct DiscoveryState {
    pub(crate) config: DiscoveryConfig,
    pub(crate) backend: &'static str,
    pub(crate) advertisement: Advertisement,
    registry: Mutex<DiscoveryRegistry>,
    pending: Mutex<PendingChallenges>,
    in_flight: Mutex<BTreeSet<NodeId>>,
    pub(crate) enrollment_attempts: AtomicU64,
    pub(crate) enrollment_successes: AtomicU64,
    pub(crate) enrollment_failures: AtomicU64,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl DiscoveryState {
    pub(crate) fn registry(&self) -> MutexGuard<'_, DiscoveryRegistry> {
        lock(&self.registry)
    }

    pub(crate) fn pending(&self) -> MutexGuard<'_, PendingChallenges> {
        lock(&self.pending)
    }

    pub(crate) fn record_attempt(&self) {
        self.enrollment_attempts.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_outcome(&self, success: bool) {
        let counter = if success {
            &self.enrollment_successes
        } else {
            &self.enrollment_failures
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

/// Stops the discovery runtime (withdrawing the announcement) when shut down.
pub struct DiscoveryHandle {
    shutdown: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
}

impl DiscoveryHandle {
    pub async fn shutdown(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let _ = self.task.await;
    }
}

impl NodeApp {
    pub(crate) fn discovery_state(&self) -> Option<Arc<DiscoveryState>> {
        self.discovery.get().cloned()
    }

    /// Starts advertising this node and browsing for peers of `config.cluster` on `backend`.
    ///
    /// Requires `ORION_NODE_PEER_AUTH=required` (so unenrolled peers are never accepted) and at
    /// least one advertised listener. Peers enrolled earlier through discovery are restored
    /// first.
    pub fn start_discovery(
        &self,
        config: DiscoveryConfig,
        mut backend: Box<dyn DiscoveryBackend>,
        endpoints: AdvertisedEndpoints,
    ) -> Result<DiscoveryHandle, NodeError> {
        if self.security.mode() != PeerAuthenticationMode::Required {
            return Err(NodeError::Config(
                "peer discovery requires ORION_NODE_PEER_AUTH=required: in `optional` mode any \
                 peer is pinned on first contact, which would trust discovered peers"
                    .into(),
            ));
        }
        if endpoints.peer_tcp_port.is_none() && endpoints.http_port.is_none() {
            return Err(NodeError::Config(
                "peer discovery needs a peer listener to advertise; set ORION_NODE_PEER_ADDR"
                    .into(),
            ));
        }
        let public_key: [u8; 32] = self.security.public_key_bytes();
        let advertisement = Advertisement {
            node_id: self.config.node_id.clone(),
            cluster: config.cluster.clone(),
            public_key,
            control_protocol_version: CONTROL_PROTOCOL_VERSION,
            peer_tcp_port: endpoints.peer_tcp_port,
            http_port: endpoints.http_port,
            https: endpoints.https,
        };
        let local = Announcement {
            instance: self.config.node_id.to_string(),
            addresses: endpoints.addresses.clone(),
            port: advertisement.srv_port(),
            txt: advertisement.txt(),
            ttl: Some(config.ttl),
        };
        let state = Arc::new(DiscoveryState {
            registry: Mutex::new(DiscoveryRegistry::new(
                self.config.node_id.clone(),
                config.cluster.clone(),
                config.ttl,
            )),
            config,
            backend: backend.name(),
            advertisement,
            pending: Mutex::new(PendingChallenges::default()),
            in_flight: Mutex::new(BTreeSet::new()),
            enrollment_attempts: AtomicU64::new(0),
            enrollment_successes: AtomicU64::new(0),
            enrollment_failures: AtomicU64::new(0),
        });
        if self.discovery.set(state.clone()).is_err() {
            return Err(NodeError::Startup("discovery is already running".into()));
        }
        self.restore_discovered_enrollments()?;
        let (events_tx, events) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        backend.start(&local, events_tx).map_err(|err| {
            NodeError::Startup(format!(
                "failed to start {} discovery: {err}",
                backend.name()
            ))
        })?;
        info!(
            node = %self.config.node_id,
            backend = state.backend,
            cluster = %state.config.cluster,
            key_fingerprint = %key_fingerprint(&public_key),
            enrollment_key = state.config.enrollment_key.is_some(),
            "peer discovery started"
        );
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(run_discovery(
            self.clone(),
            state,
            backend,
            events,
            shutdown_rx,
        ));
        Ok(DiscoveryHandle {
            shutdown: Some(shutdown_tx),
            task,
        })
    }

    fn handle_discovery_event(&self, state: &Arc<DiscoveryState>, event: DiscoveryEvent) {
        let now_ms = Self::current_time_ms();
        match event {
            DiscoveryEvent::Announced(announcement) => {
                let observation = state.registry().observe(&announcement, now_ms);
                match observation {
                    Observation::Ignored(reason) => {
                        debug!(node = %self.config.node_id, instance = %announcement.instance, %reason, "ignored discovery announcement");
                    }
                    Observation::New(node_id) | Observation::Changed(node_id) => {
                        let peer = state.registry().get(&node_id).cloned();
                        if let Some(peer) = peer {
                            info!(
                                node = %self.config.node_id,
                                peer = %node_id,
                                key_fingerprint = %peer.key_fingerprint(),
                                urls = ?peer.peer_urls,
                                state = %self.classify_discovered_peer(&peer),
                                "discovered peer"
                            );
                            self.follow_enrolled_peer_address(&peer);
                        }
                    }
                    Observation::Refreshed(_) => {}
                }
                self.start_due_enrollments(state, now_ms);
            }
            DiscoveryEvent::Withdrawn { instance } => {
                if let Some(node_id) = state.registry().withdraw(&instance) {
                    info!(node = %self.config.node_id, peer = %node_id, "discovered peer withdrew its announcement");
                }
            }
        }
    }

    fn discovery_tick(&self, state: &Arc<DiscoveryState>) {
        let now_ms = Self::current_time_ms();
        for node_id in state.registry().expire(now_ms) {
            info!(node = %self.config.node_id, peer = %node_id, "discovered peer expired");
        }
        self.start_due_enrollments(state, now_ms);
    }

    /// Trust state of a discovered peer, derived from the security state.
    pub(crate) fn classify_discovered_peer(&self, peer: &DiscoveredPeer) -> DiscoveredPeerState {
        if peer.advertisement.control_protocol_version != CONTROL_PROTOCOL_VERSION {
            return DiscoveredPeerState::Incompatible;
        }
        let node_id = peer.node_id();
        if self.security.is_peer_revoked(node_id).unwrap_or(false) {
            return DiscoveredPeerState::Revoked;
        }
        match self.trusted_key_hex(node_id) {
            Some(trusted)
                if !trusted.eq_ignore_ascii_case(&hex(&peer.advertisement.public_key)) =>
            {
                DiscoveredPeerState::KeyMismatch
            }
            Some(_) if self.is_registered_peer(node_id) => DiscoveredPeerState::Enrolled,
            _ => DiscoveredPeerState::Discovered,
        }
    }

    /// The key this node trusts for `node_id` (configured or in the trust store).
    pub(crate) fn trusted_key_hex(&self, node_id: &NodeId) -> Option<String> {
        let configured = self
            .security
            .configured_peer_public_key_hex(node_id)
            .ok()
            .flatten();
        configured
            .or_else(|| {
                self.security
                    .trusted_peer_public_key_hex(node_id)
                    .ok()
                    .flatten()
            })
            .map(|key| key.as_str().to_owned())
    }

    /// Starts shared-key enrollment with every discovered, unenrolled peer that is due.
    fn start_due_enrollments(&self, state: &Arc<DiscoveryState>, now_ms: u64) {
        if state.config.enrollment_key.is_none() {
            return;
        }
        let candidates: Vec<DiscoveredPeer> = state
            .registry()
            .peers()
            .filter(|peer| peer.next_enrollment_at_ms <= now_ms && peer.peer_tcp_url().is_some())
            .cloned()
            .collect();
        for peer in candidates {
            if self.classify_discovered_peer(&peer) != DiscoveredPeerState::Discovered {
                continue;
            }
            let node_id = peer.node_id().clone();
            if !lock(&state.in_flight).insert(node_id.clone()) {
                continue;
            }
            let app = self.clone();
            let state = state.clone();
            tokio::spawn(async move {
                let result = app.enroll_with_shared_key(&state, &peer).await;
                app.finish_auto_enrollment(&state, &node_id, result);
            });
        }
    }

    fn finish_auto_enrollment(
        &self,
        state: &DiscoveryState,
        node_id: &NodeId,
        result: Result<(), NodeError>,
    ) {
        lock(&state.in_flight).remove(node_id);
        let now_ms = Self::current_time_ms();
        let mut registry = state.registry();
        let Some(peer) = registry.get_mut(node_id) else {
            return;
        };
        match result {
            Ok(()) => {
                peer.enrollment_failures = 0;
                peer.last_enrollment_error = None;
                info!(node = %self.config.node_id, peer = %node_id, "enrolled discovered peer with the shared enrollment key");
            }
            Err(err) => {
                peer.enrollment_failures = peer.enrollment_failures.saturating_add(1);
                let base = duration_ms(state.config.enrollment_retry).max(1);
                let exponent = peer.enrollment_failures.saturating_sub(1).min(16);
                let delay = base
                    .saturating_mul(1u64 << exponent)
                    .min(MAX_ENROLLMENT_RETRY_MS);
                peer.next_enrollment_at_ms = now_ms.saturating_add(delay);
                peer.last_enrollment_error = Some(err.to_string());
                warn!(node = %self.config.node_id, peer = %node_id, error = %err, retry_in_ms = delay, "shared-key enrollment failed");
            }
        }
    }

    /// Discovery view for `orionctl get discovered-peers`; empty when discovery is off.
    pub(crate) fn discovery_snapshot(&self) -> DiscoverySnapshot {
        let Some(state) = self.discovery_state() else {
            return DiscoverySnapshot::default();
        };
        let peers: Vec<DiscoveredPeer> = state.registry().peers().cloned().collect();
        let peers = peers
            .into_iter()
            .map(|peer| DiscoveredPeerRecord {
                node_id: peer.node_id().clone(),
                cluster: peer.advertisement.cluster.clone(),
                public_key_hex: peer.public_key_hex(),
                key_fingerprint: peer.key_fingerprint(),
                peer_urls: peer.peer_urls.clone(),
                control_protocol_version: peer.advertisement.control_protocol_version,
                state: self.classify_discovered_peer(&peer),
                first_seen_at_ms: peer.first_seen_at_ms,
                last_seen_at_ms: peer.last_seen_at_ms,
                expires_at_ms: peer.expires_at_ms,
                last_enrollment_error: peer.last_enrollment_error.clone(),
            })
            .collect();
        DiscoverySnapshot {
            metrics: self.discovery_metrics(),
            backend: state.backend.to_owned(),
            cluster: state.config.cluster.clone(),
            local_key_fingerprint: key_fingerprint(&state.advertisement.public_key),
            enrollment_key_configured: state.config.enrollment_key.is_some(),
            peers,
        }
    }

    /// Counters for the observability snapshot; default (disabled) when discovery is off.
    pub(crate) fn discovery_metrics(&self) -> DiscoveryMetricsSnapshot {
        let Some(state) = self.discovery_state() else {
            return DiscoveryMetricsSnapshot::default();
        };
        let (counters, peers): (_, Vec<DiscoveredPeer>) = {
            let registry = state.registry();
            (registry.counters(), registry.peers().cloned().collect())
        };
        let enrolled = peers
            .iter()
            .filter(|peer| self.classify_discovered_peer(peer) == DiscoveredPeerState::Enrolled)
            .count();
        DiscoveryMetricsSnapshot {
            enabled: true,
            discovered_peers: peers.len() as u64,
            enrolled_peers: enrolled as u64,
            announcements_received: counters.announcements_received,
            announcements_ignored: counters.announcements_ignored,
            peers_expired: counters.peers_expired,
            enrollment_attempts: state.enrollment_attempts.load(Ordering::Relaxed),
            enrollment_successes: state.enrollment_successes.load(Ordering::Relaxed),
            enrollment_failures: state.enrollment_failures.load(Ordering::Relaxed),
        }
    }
}

async fn run_discovery(
    app: NodeApp,
    state: Arc<DiscoveryState>,
    mut backend: Box<dyn DiscoveryBackend>,
    mut events: mpsc::Receiver<DiscoveryEvent>,
    mut shutdown: oneshot::Receiver<()>,
) {
    let mut ticker = tokio::time::interval(state.config.tick);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut events_open = true;
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            event = events.recv(), if events_open => match event {
                Some(event) => app.handle_discovery_event(&state, event),
                None => {
                    warn!(node = %app.config.node_id, backend = state.backend, "discovery backend stopped delivering events");
                    events_open = false;
                }
            },
            _ = ticker.tick() => app.discovery_tick(&state),
        }
    }
    // Backends may block briefly while withdrawing their announcement.
    let _ = tokio::task::spawn_blocking(move || backend.stop()).await;
    info!(node = %app.config.node_id, "peer discovery stopped");
}

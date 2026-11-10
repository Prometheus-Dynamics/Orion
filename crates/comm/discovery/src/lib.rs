//! Finding the other nodes of a cluster on the local network.
//!
//! A node announces itself as a `_orion._tcp` service: its name, its public key, its cluster
//! and where it takes `orion+tcp` connections. Every node keeps the peers it has seen, each
//! until it says goodbye or its announcement is not refreshed in time.
//!
//! **Discovery only finds peers. It never makes one trusted.** An announcement proves nothing:
//! anyone on the network can announce anything. A discovered peer is listed, and that is all,
//! until it is enrolled: by a person who compared its key's fingerprint, or by both nodes
//! proving they hold the cluster's shared enrollment key (`orion_keys::enroll`). With such a
//! key set, a node enrolls with every discovered peer of its cluster by itself, trying again
//! with a growing wait when it fails.
//!
//! What the rest of the comm core gets is an event when an enrolled peer is there, or has moved
//! to another address, and when it has gone: enough to keep a connection to it.

mod advert;
mod backend;
mod enroll;
#[cfg(feature = "mdns")]
mod mdns;
mod registry;

use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use orion_keys::{SharedKeys, fingerprint};
use orion_server::Security;
use orion_tcp::TcpOptions;
use orion_types::NodeName;
use orion_wire::{INTERFACE_REVISION, PROTOCOL_VERSION};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{debug, info, warn};

pub use advert::{Advertisement, SERVICE_TYPE, hex};
pub use backend::{
    Announcement, DiscoveryBackend, DiscoveryEvent, DiscoveryEventSender, MemoryDiscoveryBackend,
    MemoryDiscoveryBus,
};
#[cfg(feature = "mdns")]
pub use mdns::MdnsDiscoveryBackend;
pub use registry::Counters;

use registry::{Observation, Registry};

/// The first wait after an enrollment that failed. It doubles up to [`ENROLL_RETRY_MAX`].
const ENROLL_RETRY_FIRST: Duration = Duration::from_secs(5);
/// The longest wait between enrollment tries.
const ENROLL_RETRY_MAX: Duration = Duration::from_secs(300);
/// Announcements waiting to be looked at. More are dropped: they come again.
const EVENTS: usize = 256;

/// How far a discovered peer is trusted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerState {
    /// Seen. Not trusted.
    Discovered,
    /// The key it announces is the key this node holds for it.
    Enrolled,
    /// Its key was revoked here. It is never enrolled again by itself.
    Revoked,
    /// It announces another key than the one held for its name: a node with a new key, or
    /// something pretending to be it.
    KeyMismatch,
    /// It speaks another version of the wire.
    Incompatible,
}

/// One discovered peer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Peer {
    /// Its name.
    pub node: NodeName,
    /// The key it announces.
    pub public_key: [u8; 32],
    /// That key's fingerprint, for people to compare.
    pub fingerprint: String,
    /// Where it can be reached, best first.
    pub addrs: Vec<SocketAddr>,
    /// How far it is trusted.
    pub state: PeerState,
    /// Milliseconds since discovery started when it was first seen.
    pub first_seen_ms: u64,
    /// And when it was last seen.
    pub last_seen_ms: u64,
    /// And when it is dropped unless it is seen again.
    pub expires_ms: u64,
    /// Why the last try to enroll with it failed.
    pub last_enrollment_error: Option<String>,
}

/// What discovery tells the rest of the comm core.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PeerEvent {
    /// An enrolled peer is there, or is now at other addresses.
    Present(Peer),
    /// A peer that was present has gone, or is no longer trusted.
    Gone(NodeName),
}

/// How discovery runs.
#[derive(Clone)]
pub struct DiscoveryConfig {
    /// This node's name.
    pub node: NodeName,
    /// Where this node takes `orion+tcp` connections.
    pub port: u16,
    /// The addresses to announce. Empty for whatever the backend finds.
    pub addresses: Vec<std::net::IpAddr>,
    /// How long a peer stays listed without a fresh announcement.
    pub ttl: Duration,
    /// How to reach peers to enroll with them, and whose keys. The cluster and the shared
    /// enrollment key are the keys'.
    pub tcp: TcpOptions,
}

impl std::fmt::Debug for DiscoveryConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DiscoveryConfig({} port {})", self.node, self.port)
    }
}

struct Inner {
    registry: Registry,
    keys: SharedKeys,
    /// The peers the rest of the comm core was told are present, and where.
    present: Vec<(NodeName, Vec<SocketAddr>)>,
}

fn state_of(keys: &SharedKeys, advert: &Advertisement) -> PeerState {
    let keys = keys.lock();
    if advert.wire != PROTOCOL_VERSION {
        PeerState::Incompatible
    } else if keys.is_revoked(&advert.public_key) {
        PeerState::Revoked
    } else {
        match keys.peer_key(&advert.node) {
            Some(key) if *key == advert.public_key => PeerState::Enrolled,
            Some(_) => PeerState::KeyMismatch,
            None => PeerState::Discovered,
        }
    }
}

impl Inner {
    fn peers(&self) -> Vec<Peer> {
        self.registry
            .peers
            .iter()
            .map(|(node, seen)| Peer {
                node: node.clone(),
                public_key: seen.advert.public_key,
                fingerprint: fingerprint(&seen.advert.public_key),
                addrs: seen.addrs.clone(),
                state: state_of(&self.keys, &seen.advert),
                first_seen_ms: seen.first_seen_ms,
                last_seen_ms: seen.last_seen_ms,
                expires_ms: seen.expires_ms,
                last_enrollment_error: seen.last_enrollment_error.clone(),
            })
            .collect()
    }

    /// The changes since the rest of the comm core was last told.
    fn changes(&mut self) -> Vec<PeerEvent> {
        let enrolled: Vec<Peer> = self
            .peers()
            .into_iter()
            .filter(|peer| peer.state == PeerState::Enrolled && !peer.addrs.is_empty())
            .collect();
        let mut events = Vec::new();
        for (node, _) in &self.present {
            if !enrolled.iter().any(|peer| peer.node == *node) {
                events.push(PeerEvent::Gone(node.clone()));
            }
        }
        for peer in &enrolled {
            let known = self
                .present
                .iter()
                .any(|(node, addrs)| *node == peer.node && *addrs == peer.addrs);
            if !known {
                events.push(PeerEvent::Present(peer.clone()));
            }
        }
        self.present = enrolled
            .into_iter()
            .map(|peer| (peer.node, peer.addrs))
            .collect();
        events
    }
}

/// Running discovery. Dropping it says goodbye and stops.
pub struct Discovery {
    inner: Rc<RefCell<Inner>>,
    backend: Box<dyn DiscoveryBackend>,
    task: JoinHandle<()>,
    look: Rc<tokio::sync::Notify>,
}

impl std::fmt::Debug for Discovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Discovery({})", self.backend.name())
    }
}

impl Discovery {
    /// Announces this node through `backend`, browses for the others of its cluster, enrolls
    /// with them if the keys hold a shared enrollment key, and tells `on_event` when an
    /// enrolled peer comes, moves or goes. Must be called inside the comm core's `LocalSet`.
    ///
    /// # Errors
    ///
    /// If the backend can't start.
    pub fn start(
        config: DiscoveryConfig,
        mut backend: Box<dyn DiscoveryBackend>,
        mut on_event: impl FnMut(PeerEvent) + 'static,
    ) -> Result<Self, String> {
        let keys = config.tcp.keys.clone();
        let (cluster, public_key, auto) = {
            let keys = keys.lock();
            (
                keys.cluster().to_owned(),
                keys.identity().public(),
                keys.enrollment_key().is_some() && config.tcp.security == Security::On,
            )
        };
        let local = Advertisement {
            node: config.node.clone(),
            cluster: cluster.clone(),
            public_key,
            wire: PROTOCOL_VERSION,
            revision: INTERFACE_REVISION,
            port: config.port,
        };
        let (events, mut incoming) = mpsc::channel(EVENTS);
        backend.start(
            &Announcement {
                instance: config.node.to_string(),
                addresses: config.addresses.clone(),
                port: config.port,
                txt: local.txt(),
                ttl: None,
            },
            events,
        )?;
        info!(backend = backend.name(), %cluster, auto_enroll = auto, "discovery started");
        let inner = Rc::new(RefCell::new(Inner {
            registry: Registry::new(config.node.clone(), cluster, config.ttl),
            keys,
            present: Vec::new(),
        }));
        let look = Rc::new(tokio::sync::Notify::new());
        let (shared, wake) = (inner.clone(), look.clone());
        let started = Instant::now();
        let (enrolled, mut results) =
            mpsc::unbounded_channel::<(NodeName, [u8; 32], Result<(), String>)>();
        let task = tokio::task::spawn_local(async move {
            let mut backend_gone = false;
            loop {
                let now_ms = started.elapsed().as_millis() as u64;
                // What is due now: peers that ran out, and enrollments to try.
                let (events, due, next_ms) = {
                    let mut inner = shared.borrow_mut();
                    inner.registry.expire(now_ms);
                    let mut next_ms: Option<u64> = None;
                    let mut due = Vec::new();
                    let keys = inner.keys.clone();
                    for (node, seen) in &mut inner.registry.peers {
                        next_ms =
                            Some(next_ms.map_or(seen.expires_ms, |next| next.min(seen.expires_ms)));
                        let wanted = auto
                            && !seen.enrolling
                            && !seen.addrs.is_empty()
                            && state_of(&keys, &seen.advert) == PeerState::Discovered;
                        if !wanted {
                            continue;
                        }
                        if seen.next_enrollment_ms <= now_ms {
                            seen.enrolling = true;
                            due.push((node.clone(), seen.advert.clone(), seen.addrs.clone()));
                        } else {
                            next_ms = Some(next_ms.map_or(seen.next_enrollment_ms, |next| {
                                next.min(seen.next_enrollment_ms)
                            }));
                        }
                    }
                    (inner.changes(), due, next_ms)
                };
                for event in events {
                    on_event(event);
                }
                for (node, advert, addrs) in due {
                    let (options, local, done) =
                        (config.tcp.clone(), config.node.clone(), enrolled.clone());
                    tokio::task::spawn_local(async move {
                        let mut result = Err("no address".to_owned());
                        for addr in addrs {
                            result = enroll::enroll(addr, &options, &local, &advert).await;
                            if result.is_ok() {
                                break;
                            }
                        }
                        let _ = done.send((node, advert.public_key, result));
                    });
                }
                // Sleep until something is due. With nothing listed, that is until something
                // is announced.
                let sleep = async {
                    match next_ms {
                        Some(at_ms) => {
                            tokio::time::sleep_until(started + Duration::from_millis(at_ms)).await
                        }
                        None => std::future::pending().await,
                    }
                };
                tokio::select! {
                    () = sleep => {}
                    () = wake.notified() => {}
                    event = incoming.recv(), if !backend_gone => {
                        let now_ms = started.elapsed().as_millis() as u64;
                        let mut inner = shared.borrow_mut();
                        match event {
                            Some(DiscoveryEvent::Announced(announcement)) => {
                                match inner.registry.observe(&announcement, now_ms) {
                                    Observation::Ignored(why) => debug!(%why, "announcement ignored"),
                                    Observation::New(node) => info!(%node, "peer discovered"),
                                    Observation::Changed(node) => info!(%node, "peer changed"),
                                    Observation::Refreshed(_) => {}
                                }
                            }
                            Some(DiscoveryEvent::Withdrawn { instance }) => {
                                if let Some(node) = inner.registry.withdraw(&instance) {
                                    info!(%node, "peer said goodbye");
                                }
                            }
                            // The backend is gone: what is listed runs out by itself.
                            None => backend_gone = true,
                        }
                    }
                    Some((node, key, result)) = results.recv() => {
                        let now_ms = started.elapsed().as_millis() as u64;
                        let mut inner = shared.borrow_mut();
                        if let Some(seen) = inner.registry.peers.get_mut(&node)
                            && seen.advert.public_key == key
                        {
                            seen.enrolling = false;
                            match result {
                                Ok(()) => {
                                    info!(%node, "peer enrolled with the shared key");
                                    seen.last_enrollment_error = None;
                                    seen.enrollment_failures = 0;
                                }
                                Err(error) => {
                                    let wait = ENROLL_RETRY_FIRST
                                        .saturating_mul(1 << seen.enrollment_failures.min(8))
                                        .min(ENROLL_RETRY_MAX);
                                    warn!(%node, %error, retry_s = wait.as_secs(), "enrolling with a peer failed");
                                    seen.enrollment_failures += 1;
                                    seen.next_enrollment_ms = now_ms + wait.as_millis() as u64;
                                    seen.last_enrollment_error = Some(error);
                                }
                            }
                        }
                    }
                }
            }
        });
        Ok(Self {
            inner,
            backend,
            task,
            look,
        })
    }

    /// The peers seen now, by name.
    #[must_use]
    pub fn peers(&self) -> Vec<Peer> {
        self.inner.borrow().peers()
    }

    /// What has been counted.
    #[must_use]
    pub fn counters(&self) -> Counters {
        self.inner.borrow().registry.counters
    }

    /// Looks at the keys again now: call it after enrolling or revoking a peer by hand, so
    /// the change is acted on at once.
    pub fn trust_changed(&self) {
        self.look.notify_one();
    }
}

impl Drop for Discovery {
    fn drop(&mut self) {
        self.task.abort();
        self.backend.stop();
    }
}

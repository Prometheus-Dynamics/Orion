//! The discovered-peer set: announcements of the local cluster with last-seen times and TTL
//! expiry. Trust state is not stored here; it is derived from the security state on demand.

use super::{
    advert::{Advertisement, key_fingerprint},
    backend::Announcement,
};
use orion::NodeId;
use orion_core::{PeerBaseUrl, PublicKeyHex};
use std::{collections::BTreeMap, time::Duration};

/// Most peers kept at once, so an announcement flood cannot grow memory without bound.
const MAX_DISCOVERED_PEERS: usize = 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DiscoveredPeer {
    pub(crate) advertisement: Advertisement,
    pub(crate) instance: String,
    pub(crate) peer_urls: Vec<PeerBaseUrl>,
    pub(crate) first_seen_at_ms: u64,
    pub(crate) last_seen_at_ms: u64,
    pub(crate) expires_at_ms: u64,
    pub(crate) last_enrollment_error: Option<String>,
    pub(crate) enrollment_failures: u32,
    pub(crate) next_enrollment_at_ms: u64,
}

impl DiscoveredPeer {
    pub(crate) fn node_id(&self) -> &NodeId {
        &self.advertisement.node_id
    }

    pub(crate) fn public_key_hex(&self) -> PublicKeyHex {
        PublicKeyHex::new(super::advert::hex(&self.advertisement.public_key))
    }

    pub(crate) fn key_fingerprint(&self) -> String {
        key_fingerprint(&self.advertisement.public_key)
    }

    /// The first `orion+tcp://` URL, the transport used for automatic enrollment.
    pub(crate) fn peer_tcp_url(&self) -> Option<&PeerBaseUrl> {
        self.peer_urls
            .iter()
            .find(|url| url.as_str().starts_with(crate::PEER_TCP_SCHEME))
    }

    /// The first URL whose transport is compiled into this build.
    pub(crate) fn preferred_url(&self) -> Option<&PeerBaseUrl> {
        self.peer_urls
            .iter()
            .find(|url| crate::PeerTransportKind::check_supported(url.as_str()).is_ok())
    }
}

/// Outcome of one announcement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Observation {
    /// Not a peer of this cluster (or malformed, or the node itself).
    Ignored(String),
    New(NodeId),
    /// Key, addresses or ports changed.
    Changed(NodeId),
    Refreshed(NodeId),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RegistryCounters {
    pub(crate) announcements_received: u64,
    pub(crate) announcements_ignored: u64,
    pub(crate) peers_expired: u64,
}

pub(crate) struct DiscoveryRegistry {
    local_node_id: NodeId,
    cluster: String,
    default_ttl: Duration,
    peers: BTreeMap<NodeId, DiscoveredPeer>,
    counters: RegistryCounters,
}

impl DiscoveryRegistry {
    pub(crate) fn new(local_node_id: NodeId, cluster: String, default_ttl: Duration) -> Self {
        Self {
            local_node_id,
            cluster,
            default_ttl,
            peers: BTreeMap::new(),
            counters: RegistryCounters::default(),
        }
    }

    pub(crate) fn counters(&self) -> RegistryCounters {
        self.counters
    }

    pub(crate) fn peers(&self) -> impl Iterator<Item = &DiscoveredPeer> {
        self.peers.values()
    }

    pub(crate) fn get(&self, node_id: &NodeId) -> Option<&DiscoveredPeer> {
        self.peers.get(node_id)
    }

    pub(crate) fn get_mut(&mut self, node_id: &NodeId) -> Option<&mut DiscoveredPeer> {
        self.peers.get_mut(node_id)
    }

    /// Records an announcement received at `now_ms`.
    pub(crate) fn observe(&mut self, announcement: &Announcement, now_ms: u64) -> Observation {
        let advertisement = match Advertisement::from_txt(&announcement.txt) {
            Ok(advertisement) => advertisement,
            Err(reason) => return self.ignore(reason),
        };
        if advertisement.node_id == self.local_node_id {
            return self.ignore("own announcement".into());
        }
        if advertisement.cluster != self.cluster {
            return self.ignore(format!("cluster `{}`", advertisement.cluster));
        }
        let node_id = advertisement.node_id.clone();
        if !self.peers.contains_key(&node_id) && self.peers.len() >= MAX_DISCOVERED_PEERS {
            return self.ignore("discovered peer limit reached".into());
        }
        self.counters.announcements_received += 1;
        let ttl_ms = duration_ms(announcement.ttl.unwrap_or(self.default_ttl));
        let peer_urls = advertisement.peer_urls(&announcement.addresses);
        let expires_at_ms = now_ms.saturating_add(ttl_ms);
        match self.peers.get_mut(&node_id) {
            Some(peer) => {
                let changed = peer.advertisement != advertisement || peer.peer_urls != peer_urls;
                if peer.advertisement.public_key != advertisement.public_key {
                    // A new key starts a new enrollment history.
                    peer.enrollment_failures = 0;
                    peer.next_enrollment_at_ms = 0;
                    peer.last_enrollment_error = None;
                }
                peer.advertisement = advertisement;
                peer.instance = announcement.instance.clone();
                peer.peer_urls = peer_urls;
                peer.last_seen_at_ms = now_ms;
                peer.expires_at_ms = expires_at_ms;
                if changed {
                    Observation::Changed(node_id)
                } else {
                    Observation::Refreshed(node_id)
                }
            }
            None => {
                self.peers.insert(
                    node_id.clone(),
                    DiscoveredPeer {
                        advertisement,
                        instance: announcement.instance.clone(),
                        peer_urls,
                        first_seen_at_ms: now_ms,
                        last_seen_at_ms: now_ms,
                        expires_at_ms,
                        last_enrollment_error: None,
                        enrollment_failures: 0,
                        next_enrollment_at_ms: 0,
                    },
                );
                Observation::New(node_id)
            }
        }
    }

    fn ignore(&mut self, reason: String) -> Observation {
        self.counters.announcements_ignored += 1;
        Observation::Ignored(reason)
    }

    /// Drops the peer announced as `instance` (goodbye or record expiry in the backend).
    pub(crate) fn withdraw(&mut self, instance: &str) -> Option<NodeId> {
        let node_id = self
            .peers
            .values()
            .find(|peer| peer.instance == instance)
            .map(|peer| peer.node_id().clone())?;
        self.peers.remove(&node_id);
        self.counters.peers_expired += 1;
        Some(node_id)
    }

    /// Drops peers whose announcement was not refreshed within its TTL.
    pub(crate) fn expire(&mut self, now_ms: u64) -> Vec<NodeId> {
        let expired: Vec<NodeId> = self
            .peers
            .values()
            .filter(|peer| peer.expires_at_ms <= now_ms)
            .map(|peer| peer.node_id().clone())
            .collect();
        for node_id in &expired {
            self.peers.remove(node_id);
        }
        self.counters.peers_expired += expired.len() as u64;
        expired
    }
}

pub(crate) fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

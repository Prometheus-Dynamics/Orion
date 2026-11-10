//! The discovered set: the announcements of this cluster, with when each was last seen and
//! when it runs out. Whether a peer is trusted is not kept here; it is asked of the keys.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use orion_types::NodeName;

use crate::advert::Advertisement;
use crate::backend::Announcement;

/// Most peers kept at once, so a flood of announcements cannot grow memory without bound.
const MAX_DISCOVERED: usize = 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Seen {
    pub(crate) advert: Advertisement,
    pub(crate) instance: String,
    pub(crate) addrs: Vec<SocketAddr>,
    pub(crate) first_seen_ms: u64,
    pub(crate) last_seen_ms: u64,
    pub(crate) expires_ms: u64,
    pub(crate) last_enrollment_error: Option<String>,
    pub(crate) enrollment_failures: u32,
    pub(crate) next_enrollment_ms: u64,
    pub(crate) enrolling: bool,
}

/// What one announcement did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Observation {
    /// Not a peer of this cluster, malformed, or the node itself.
    Ignored(String),
    New(NodeName),
    /// Its key, addresses or port changed.
    Changed(NodeName),
    Refreshed(NodeName),
}

/// What the registry has counted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counters {
    /// Announcements of this cluster's peers.
    pub received: u64,
    /// Announcements ignored: another cluster, malformed, the node's own, or over the limit.
    pub ignored: u64,
    /// Peers dropped: a goodbye, or not refreshed in time.
    pub expired: u64,
}

pub(crate) struct Registry {
    local: NodeName,
    cluster: String,
    ttl: Duration,
    pub(crate) peers: BTreeMap<NodeName, Seen>,
    pub(crate) counters: Counters,
}

impl Registry {
    pub(crate) const fn new(local: NodeName, cluster: String, ttl: Duration) -> Self {
        Self {
            local,
            cluster,
            ttl,
            peers: BTreeMap::new(),
            counters: Counters {
                received: 0,
                ignored: 0,
                expired: 0,
            },
        }
    }

    /// Records an announcement received at `now_ms`.
    pub(crate) fn observe(&mut self, announcement: &Announcement, now_ms: u64) -> Observation {
        let advert = match Advertisement::from_txt(&announcement.txt) {
            Ok(advert) => advert,
            Err(reason) => return self.ignore(reason),
        };
        if advert.node == self.local {
            return self.ignore("own announcement".into());
        }
        if advert.cluster != self.cluster {
            return self.ignore(format!("cluster `{}`", advert.cluster));
        }
        let node = advert.node.clone();
        if !self.peers.contains_key(&node) && self.peers.len() >= MAX_DISCOVERED {
            return self.ignore("discovered peer limit reached".into());
        }
        self.counters.received += 1;
        let ttl_ms = announcement.ttl.unwrap_or(self.ttl).as_millis() as u64;
        let addrs = advert.addrs(&announcement.addresses);
        let expires_ms = now_ms.saturating_add(ttl_ms);
        match self.peers.get_mut(&node) {
            Some(peer) => {
                let changed = peer.advert != advert || peer.addrs != addrs;
                if peer.advert.public_key != advert.public_key {
                    // A new key starts a new enrollment history.
                    peer.enrollment_failures = 0;
                    peer.next_enrollment_ms = 0;
                    peer.last_enrollment_error = None;
                }
                peer.advert = advert;
                peer.instance.clone_from(&announcement.instance);
                peer.addrs = addrs;
                peer.last_seen_ms = now_ms;
                peer.expires_ms = expires_ms;
                if changed {
                    Observation::Changed(node)
                } else {
                    Observation::Refreshed(node)
                }
            }
            None => {
                self.peers.insert(
                    node.clone(),
                    Seen {
                        advert,
                        instance: announcement.instance.clone(),
                        addrs,
                        first_seen_ms: now_ms,
                        last_seen_ms: now_ms,
                        expires_ms,
                        last_enrollment_error: None,
                        enrollment_failures: 0,
                        next_enrollment_ms: 0,
                        enrolling: false,
                    },
                );
                Observation::New(node)
            }
        }
    }

    fn ignore(&mut self, reason: String) -> Observation {
        self.counters.ignored += 1;
        Observation::Ignored(reason)
    }

    /// Drops the peer announced as `instance`: it said goodbye.
    pub(crate) fn withdraw(&mut self, instance: &str) -> Option<NodeName> {
        let node = self
            .peers
            .iter()
            .find(|(_, peer)| peer.instance == instance)
            .map(|(node, _)| node.clone())?;
        self.peers.remove(&node);
        self.counters.expired += 1;
        Some(node)
    }

    /// Drops peers whose announcement was not refreshed in time.
    pub(crate) fn expire(&mut self, now_ms: u64) -> Vec<NodeName> {
        let expired: Vec<NodeName> = self
            .peers
            .iter()
            .filter(|(_, peer)| peer.expires_ms <= now_ms)
            .map(|(node, _)| node.clone())
            .collect();
        for node in &expired {
            self.peers.remove(node);
        }
        self.counters.expired += expired.len() as u64;
        expired
    }
}

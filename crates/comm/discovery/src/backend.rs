//! Discovery backends: what carries announcements. mDNS on a real network, a bus in memory in
//! tests.

use std::{
    collections::BTreeMap,
    net::IpAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::mpsc;

/// One service instance as seen on the network.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Announcement {
    /// Backend-specific instance key (the mDNS full name); `Withdrawn` refers to it.
    pub instance: String,
    /// The addresses announced with it.
    pub addresses: Vec<IpAddr>,
    /// The service's port.
    pub port: u16,
    /// The TXT properties.
    pub txt: Vec<(String, String)>,
    /// How long the announcement stays valid without a refresh; `None` uses the configured TTL.
    pub ttl: Option<Duration>,
}

/// Events a backend delivers to the discovery runtime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiscoveryEvent {
    /// A new or refreshed announcement.
    Announced(Announcement),
    /// The instance said goodbye or its records expired.
    Withdrawn {
        /// The instance key of its announcement.
        instance: String,
    },
}

/// Bounded event channel; backends drop events instead of blocking when it is full.
pub type DiscoveryEventSender = mpsc::Sender<DiscoveryEvent>;

/// Transport for announcements (real mDNS, or an in-memory bus in tests).
pub trait DiscoveryBackend: Send {
    /// A short name for the log (`mdns`, `memory`).
    fn name(&self) -> &'static str;

    /// Starts advertising `local` and browsing for other instances, whose announcements are
    /// sent to `events`.
    ///
    /// # Errors
    ///
    /// Why it could not start, for the log.
    fn start(&mut self, local: &Announcement, events: DiscoveryEventSender) -> Result<(), String>;

    /// Withdraws the local announcement (goodbye) and stops browsing.
    fn stop(&mut self);
}

#[derive(Default)]
struct BusState {
    next_member: u64,
    members: BTreeMap<u64, BusMember>,
}

struct BusMember {
    announcement: Announcement,
    events: DiscoveryEventSender,
    silent: bool,
}

/// An in-memory "multicast segment": every member receives every other member's announcements.
/// Used by tests (no sockets, deterministic) and usable by embedders that discover peers by
/// other means and feed them in with [`MemoryDiscoveryBus::inject`].
#[derive(Clone, Default)]
pub struct MemoryDiscoveryBus {
    state: Arc<Mutex<BusState>>,
}

impl MemoryDiscoveryBus {
    /// An empty segment.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A backend attached to this bus.
    #[must_use]
    pub fn backend(&self) -> MemoryDiscoveryBackend {
        MemoryDiscoveryBackend {
            bus: self.clone(),
            member: None,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BusState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Delivers an announcement that no member made (another cluster, a spoofed key, ...) to
    /// every member.
    pub fn inject(&self, announcement: Announcement) {
        for member in self.lock().members.values() {
            let _ = member
                .events
                .try_send(DiscoveryEvent::Announced(announcement.clone()));
        }
    }

    /// Delivers a withdrawal of `instance` to every member.
    pub fn inject_withdrawal(&self, instance: &str) {
        for member in self.lock().members.values() {
            let _ = member.events.try_send(DiscoveryEvent::Withdrawn {
                instance: instance.to_owned(),
            });
        }
    }

    /// Re-delivers every live member's announcement to the others, like the periodic answers
    /// that keep mDNS records from expiring.
    pub fn refresh(&self) {
        let state = self.lock();
        for (id, member) in &state.members {
            if member.silent {
                continue;
            }
            for (other_id, other) in &state.members {
                if other_id != id {
                    let _ = other
                        .events
                        .try_send(DiscoveryEvent::Announced(member.announcement.clone()));
                }
            }
        }
    }

    /// Makes `instance` stop answering without a goodbye (a crashed or unplugged node): its
    /// last announcement is no longer refreshed and expires in the other members.
    pub fn silence(&self, instance: &str) {
        for member in self.lock().members.values_mut() {
            if member.announcement.instance == instance {
                member.silent = true;
            }
        }
    }
}

/// Backend side of a [`MemoryDiscoveryBus`].
pub struct MemoryDiscoveryBackend {
    bus: MemoryDiscoveryBus,
    member: Option<u64>,
}

impl DiscoveryBackend for MemoryDiscoveryBackend {
    fn name(&self) -> &'static str {
        "memory"
    }

    fn start(&mut self, local: &Announcement, events: DiscoveryEventSender) -> Result<(), String> {
        if self.member.is_some() {
            return Err("memory discovery backend already started".into());
        }
        let mut state = self.bus.lock();
        for member in state.members.values() {
            let _ = member
                .events
                .try_send(DiscoveryEvent::Announced(local.clone()));
            if !member.silent {
                let _ = events.try_send(DiscoveryEvent::Announced(member.announcement.clone()));
            }
        }
        let id = state.next_member;
        state.next_member += 1;
        state.members.insert(
            id,
            BusMember {
                announcement: local.clone(),
                events,
                silent: false,
            },
        );
        self.member = Some(id);
        Ok(())
    }

    fn stop(&mut self) {
        let Some(id) = self.member.take() else {
            return;
        };
        let mut state = self.bus.lock();
        if let Some(member) = state.members.remove(&id) {
            for other in state.members.values() {
                let _ = other.events.try_send(DiscoveryEvent::Withdrawn {
                    instance: member.announcement.instance.clone(),
                });
            }
        }
    }
}

impl std::fmt::Debug for MemoryDiscoveryBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MemoryDiscoveryBus({} members)",
            self.lock().members.len()
        )
    }
}

impl std::fmt::Debug for MemoryDiscoveryBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MemoryDiscoveryBackend({:?})", self.member)
    }
}

impl Drop for MemoryDiscoveryBackend {
    fn drop(&mut self) {
        self.stop();
    }
}

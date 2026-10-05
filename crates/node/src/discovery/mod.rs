//! Peer discovery over mDNS/DNS-SD and shared-key enrollment (feature `discovery-mdns`).
//!
//! Discovery only *finds* peers: every node of a cluster advertises `_orion._tcp` with its node
//! id, public key, peer ports and control protocol version, and browses for the others. A
//! discovered peer is never trusted and never synced with until it is enrolled, either
//!
//! - by an operator (`orionctl peers enroll <node-id>`, which pins the advertised key), or
//! - automatically, when both nodes hold the same shared enrollment key and complete the
//!   challenge-response handshake in [`enrollment`] over the `orion+tcp` peer transport.
//!
//! Enrolled peers go through the normal peer path (`NodeApp::enroll_peer`, trusted peer store)
//! and are persisted in `discovered-peers.json` in the state directory. See `docs/discovery.md`
//! for the TXT record layout, the handshake and the threat model.
//!
//! The backend is pluggable ([`DiscoveryBackend`]): [`MdnsDiscoveryBackend`] speaks real mDNS,
//! [`MemoryDiscoveryBus`] simulates a multicast segment in memory for tests.

mod advert;
mod backend;
mod config;
mod enrollment;
mod handshake;
mod mdns;
mod registry;
mod runtime;
mod store;
#[cfg(test)]
mod tests;
mod trust;

pub use advert::{Advertisement, SERVICE_TYPE, key_fingerprint};
pub use backend::{
    Announcement, DiscoveryBackend, DiscoveryEvent, DiscoveryEventSender, MemoryDiscoveryBackend,
    MemoryDiscoveryBus,
};
pub use config::{DEFAULT_CLUSTER, DiscoveryConfig, EnrollmentKey};
pub use mdns::MdnsDiscoveryBackend;
pub(crate) use runtime::DiscoveryState;
pub use runtime::{AdvertisedEndpoints, DiscoveryHandle};

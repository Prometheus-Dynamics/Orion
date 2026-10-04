//! Maps link devices (served by `crate::link_gateway`) onto the node's normal provider path.
//!
//! A device's provider snapshot goes through [`NodeApp::apply_local_provider_snapshot`], exactly
//! like a `ProviderState` from a local IPC client, so validation, persistence, reconcile
//! triggering, and observability are shared. The bridge adds what IPC clients do not need:
//!
//! - the gateway owns `ProviderRecord::node_id` (forced to the local node);
//! - a provider id belongs to one publisher: a device cannot take over a provider published by a
//!   local IPC client, another device, or another node, nor resources of another provider (and an
//!   IPC client cannot overwrite a device's provider);
//! - a lost device keeps its records, with its resources marked unavailable, so leases and history
//!   survive a reconnect.

use super::{NodeApp, NodeError};
use crate::link_gateway::LinkStatus;
use orion::{
    ProviderId, ResourceId,
    control_plane::{AvailabilityState, HealthState, LeaseRecord, ProviderRecord, ResourceRecord},
    runtime::ProviderSnapshot,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, MutexGuard};
use tracing::warn;

/// Shared bridge state, one per node.
pub(super) struct LinkBridgeState {
    owners: Mutex<BTreeMap<ProviderId, ProviderOwner>>,
    status: Mutex<BTreeMap<String, LinkStatus>>,
    desired_changed: tokio::sync::watch::Sender<u64>,
}

impl Default for LinkBridgeState {
    fn default() -> Self {
        Self {
            owners: Mutex::new(BTreeMap::new()),
            status: Mutex::new(BTreeMap::new()),
            desired_changed: tokio::sync::watch::Sender::new(0),
        }
    }
}

enum ProviderOwner {
    /// Published by a local IPC client.
    Ipc,
    /// Published by a link device.
    Device(DeviceClaim),
}

struct DeviceClaim {
    link: String,
    device: String,
    connected: bool,
    provider: ProviderRecord,
    resources: Vec<ResourceRecord>,
}

/// Why the gateway ignored a device's provider snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LinkRejection(pub(crate) String);

impl std::fmt::Display for LinkRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl NodeApp {
    /// Records that a local IPC client publishes `provider_id`. Fails if a link device owns it.
    pub(super) fn claim_provider_for_ipc(&self, provider_id: &ProviderId) -> Result<(), NodeError> {
        let mut owners = lock(&self.state.links.owners);
        match owners.get(provider_id) {
            Some(ProviderOwner::Device(claim)) => Err(NodeError::Authorization(format!(
                "provider {provider_id} belongs to link device `{}` on {}",
                claim.device, claim.link
            ))),
            Some(ProviderOwner::Ipc) => Ok(()),
            None => {
                owners.insert(provider_id.clone(), ProviderOwner::Ipc);
                Ok(())
            }
        }
    }

    /// Applies a device's provider snapshot through the local provider path. Returns the lease
    /// set to send to the device.
    pub(crate) fn link_apply_device_state(
        &self,
        link: &str,
        device: &str,
        mut provider: ProviderRecord,
        resources: Vec<ResourceRecord>,
    ) -> Result<Vec<LeaseRecord>, LinkRejection> {
        provider.node_id = self.config.node_id.clone();
        let provider_id = provider.provider_id.clone();
        self.check_device_snapshot(link, device, &provider_id, &resources)?;

        // Claim first so a concurrent IPC update cannot slip in between check and apply; the
        // previous claim is restored if the node rejects the snapshot.
        let (previous, stale) = {
            let mut owners = lock(&self.state.links.owners);
            let stale: Vec<ProviderId> = owners
                .iter()
                .filter(|(id, owner)| {
                    **id != provider_id
                        && matches!(owner, ProviderOwner::Device(c) if c.device == device)
                })
                .map(|(id, _)| id.clone())
                .collect();
            let stale: Vec<DeviceClaim> = stale
                .into_iter()
                .filter_map(|id| match owners.remove(&id) {
                    Some(ProviderOwner::Device(claim)) => Some(claim),
                    _ => None,
                })
                .collect();
            let previous = owners.insert(
                provider_id.clone(),
                ProviderOwner::Device(DeviceClaim {
                    link: link.to_owned(),
                    device: device.to_owned(),
                    connected: true,
                    provider: provider.clone(),
                    resources: resources.clone(),
                }),
            );
            (previous, stale)
        };
        // A device that switched to another provider id leaves its old provider behind,
        // unavailable, like a lost device.
        for claim in stale {
            self.mark_claim_unavailable(&claim);
        }

        let applied = self.apply_local_provider_snapshot(ProviderSnapshot {
            provider,
            resources: resources.clone(),
        });
        if let Err(error) = applied {
            let mut owners = lock(&self.state.links.owners);
            match previous {
                Some(previous) => owners.insert(provider_id, previous),
                None => owners.remove(&provider_id),
            };
            return Err(LinkRejection(format!(
                "the node rejected the snapshot: {error}"
            )));
        }
        Ok(self.link_leases_for(&provider_id, &resources))
    }

    fn check_device_snapshot(
        &self,
        link: &str,
        device: &str,
        provider_id: &ProviderId,
        resources: &[ResourceRecord],
    ) -> Result<(), LinkRejection> {
        for resource in resources {
            if &resource.provider_id != provider_id {
                return Err(LinkRejection(format!(
                    "resource {} names provider {}, not {provider_id}",
                    resource.resource_id, resource.provider_id
                )));
            }
        }
        let resource_ids: BTreeSet<&ResourceId> =
            resources.iter().map(|r| &r.resource_id).collect();
        {
            let owners = lock(&self.state.links.owners);
            match owners.get(provider_id) {
                Some(ProviderOwner::Ipc) => {
                    return Err(LinkRejection(format!(
                        "provider {provider_id} is published by a local IPC client"
                    )));
                }
                Some(ProviderOwner::Device(claim)) if claim.device != device => {
                    return Err(LinkRejection(format!(
                        "provider {provider_id} belongs to device `{}` on {}",
                        claim.device, claim.link
                    )));
                }
                Some(ProviderOwner::Device(claim)) if claim.link != link && claim.connected => {
                    return Err(LinkRejection(format!(
                        "a device named `{device}` is already connected on {}",
                        claim.link
                    )));
                }
                _ => {}
            }
        }
        let store = self.store_read();
        if let Some(existing) = store.desired.providers.get(provider_id)
            && existing.node_id != self.config.node_id
        {
            return Err(LinkRejection(format!(
                "provider {provider_id} belongs to node {}",
                existing.node_id
            )));
        }
        for resource_id in resource_ids {
            let existing = store
                .observed
                .resources
                .get(resource_id)
                .or_else(|| store.desired.resources.get(resource_id));
            if let Some(existing) = existing
                && &existing.provider_id != provider_id
            {
                return Err(LinkRejection(format!(
                    "resource {resource_id} belongs to provider {}",
                    existing.provider_id
                )));
            }
        }
        Ok(())
    }

    /// Marks the resources of `device` on `link` unavailable (the records are kept).
    pub(crate) fn link_device_lost(&self, link: &str, device: &str) {
        let lost: Vec<(ProviderRecord, Vec<ResourceRecord>)> = {
            let mut owners = lock(&self.state.links.owners);
            owners
                .values_mut()
                .filter_map(|owner| match owner {
                    ProviderOwner::Device(claim)
                        if claim.connected && claim.link == link && claim.device == device =>
                    {
                        claim.connected = false;
                        Some((claim.provider.clone(), claim.resources.clone()))
                    }
                    _ => None,
                })
                .collect()
        };
        for (provider, resources) in lost {
            self.apply_unavailable(provider, &resources);
        }
    }

    /// Marks every connected device of `link` lost (gateway shutdown).
    pub(crate) fn link_closed(&self, link: &str) {
        let devices: BTreeSet<String> = lock(&self.state.links.owners)
            .values()
            .filter_map(|owner| match owner {
                ProviderOwner::Device(claim) if claim.connected && claim.link == link => {
                    Some(claim.device.clone())
                }
                _ => None,
            })
            .collect();
        for device in devices {
            self.link_device_lost(link, &device);
        }
    }

    fn mark_claim_unavailable(&self, claim: &DeviceClaim) {
        if claim.connected {
            self.apply_unavailable(claim.provider.clone(), &claim.resources);
        }
    }

    fn apply_unavailable(&self, provider: ProviderRecord, resources: &[ResourceRecord]) {
        let provider_id = provider.provider_id.clone();
        let resources = resources
            .iter()
            .cloned()
            .map(|mut resource| {
                resource.availability = AvailabilityState::Unavailable;
                resource.health = HealthState::Unknown;
                resource
            })
            .collect();
        if let Err(error) = self.apply_local_provider_snapshot(ProviderSnapshot {
            provider,
            resources,
        }) {
            warn!(provider = %provider_id, %error, "failed to mark link device resources unavailable");
        }
    }

    /// The lease set of the provider `device` last published (on any link), if any.
    pub(crate) fn link_device_leases(&self, device: &str) -> Option<Vec<LeaseRecord>> {
        let (provider_id, resources) =
            lock(&self.state.links.owners)
                .iter()
                .find_map(|(id, owner)| match owner {
                    ProviderOwner::Device(claim) if claim.device == device => {
                        Some((id.clone(), claim.resources.clone()))
                    }
                    _ => None,
                })?;
        Some(self.link_leases_for(&provider_id, &resources))
    }

    /// Leases on the provider's resources: the same set `WatchProviderLeases` reports (leases on
    /// desired resources of the provider), plus leases on the resources the device reported.
    fn link_leases_for(
        &self,
        provider_id: &ProviderId,
        resources: &[ResourceRecord],
    ) -> Vec<LeaseRecord> {
        let store = self.store_read();
        let mut resource_ids: BTreeSet<&ResourceId> =
            resources.iter().map(|r| &r.resource_id).collect();
        resource_ids.extend(
            store
                .desired
                .resources
                .values()
                .filter(|resource| &resource.provider_id == provider_id)
                .map(|resource| &resource.resource_id),
        );
        resource_ids
            .into_iter()
            .filter_map(|id| store.desired.leases.get(id).cloned())
            .collect()
    }

    /// Wakes link tasks after a desired-state commit so they refresh lease sets.
    pub(super) fn notify_link_desired_change(&self) {
        self.state
            .links
            .desired_changed
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }

    pub(crate) fn link_desired_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.state.links.desired_changed.subscribe()
    }

    pub(crate) fn record_link_status(&self, status: LinkStatus) {
        lock(&self.state.links.status).insert(status.name.clone(), status);
    }

    /// Counters and connected devices of every configured link (feature `link-gateway`).
    pub fn link_status(&self) -> Vec<LinkStatus> {
        lock(&self.state.links.status).values().cloned().collect()
    }
}

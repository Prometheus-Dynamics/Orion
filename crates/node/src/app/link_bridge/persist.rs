//! Persists the link-owned providers (`crate::storage::LinkDeviceRecord`) and restores them when the
//! gateway starts. A provider is written when a device's accepted snapshot changes which providers
//! it owns, and is removed from the file when it stops being owned (a device switched providers,
//! or the link left the config). Connection state is not persisted: every restored claim starts
//! disconnected, so its resources are unavailable until the device reconnects.

use super::{DeviceClaim, ProviderOwner, lock};
use crate::link_gateway::LinkConfig;
use crate::storage::LinkDeviceRecord;
use crate::{NodeApp, NodeError};
use std::collections::BTreeSet;
use tracing::{info, warn};

impl NodeApp {
    /// Restores the providers link devices owned before the last shutdown or crash, and marks
    /// their resources unavailable.
    ///
    /// `orion-node` calls this at startup, before it starts the IPC servers, so that no local
    /// client can publish a persisted provider id first. [`Self::start_link_gateway`] calls it too
    /// (before any link task runs); a call after the first one does nothing.
    ///
    /// A persisted provider whose link is still configured stays reserved for its device (the
    /// device reconnects and its snapshots are accepted again). One whose link is no longer
    /// configured is forgotten: its resources stay unavailable, and the provider id is free again.
    /// A provider that is no longer local in desired state is forgotten without being recreated.
    pub fn restore_link_devices(&self, links: &[LinkConfig]) -> Result<(), NodeError> {
        let Some(storage) = self.storage.as_ref() else {
            return Ok(());
        };
        if lock(&self.state.links.persisted).is_some() {
            return Ok(());
        }
        let stored = storage.load_link_devices()?;
        let configured: BTreeSet<&str> = links.iter().map(|link| link.name.as_str()).collect();
        for record in &stored {
            let provider_id = record.provider.provider_id.clone();
            let desired = self
                .store_read()
                .desired
                .providers
                .get(&provider_id)
                .cloned()
                .filter(|provider| provider.node_id == self.config.node_id);
            let Some(provider) = desired else {
                info!(link = %record.link, device = %record.device, provider = %provider_id, "forgetting a link device provider that is not in desired state");
                continue;
            };
            if !configured.contains(record.link.as_str()) {
                info!(link = %record.link, device = %record.device, provider = %provider_id, "forgetting a link device provider: its link is no longer configured");
                self.apply_unavailable(provider, &record.resources);
                continue;
            }
            {
                let mut owners = lock(&self.state.links.owners);
                if owners.contains_key(&provider_id) {
                    // Only a client that published this id before the restore could own it:
                    // `orion-node` restores before it accepts any IPC client.
                    warn!(link = %record.link, device = %record.device, provider = %provider_id, "not restoring a link device provider: the id is already published");
                    continue;
                }
                owners.insert(
                    provider_id.clone(),
                    ProviderOwner::Device(DeviceClaim {
                        link: record.link.clone(),
                        device: record.device.clone(),
                        connected: false,
                        publisher: format!("link:{}/{}", record.link, record.device).into(),
                        provider: provider.clone(),
                        resources: record.resources.clone(),
                    }),
                );
            }
            info!(link = %record.link, device = %record.device, provider = %provider_id, "link device provider restored as unavailable until the device reconnects");
            self.apply_unavailable(provider, &record.resources);
        }
        // What is on disk is what the next write is compared against. Only restored state may
        // be written from here on: until this point the file is not ours to overwrite.
        *lock(&self.state.links.persisted) = Some(stored);
        self.persist_link_devices();
        Ok(())
    }

    /// Writes the link-owned providers if they differ from the file. Failures are logged; the
    /// next change retries the write. A no-op before [`Self::restore_link_devices`] and without a
    /// state directory.
    pub(super) fn persist_link_devices(&self) {
        let Some(storage) = self.storage.as_ref() else {
            return;
        };
        let mut persisted = lock(&self.state.links.persisted);
        if persisted.is_none() {
            return;
        }
        let devices = self.link_device_records();
        if persisted.as_ref() == Some(&devices) {
            return;
        }
        match storage.save_link_devices(&devices) {
            Ok(()) => *persisted = Some(devices),
            Err(error) => {
                warn!(%error, "failed to persist link device providers; a restart may not mark them unavailable")
            }
        }
    }

    /// The device-owned providers, in provider id order (the order of the owner map).
    fn link_device_records(&self) -> Vec<LinkDeviceRecord> {
        lock(&self.state.links.owners)
            .values()
            .filter_map(|owner| match owner {
                ProviderOwner::Device(claim) => Some(LinkDeviceRecord {
                    link: claim.link.clone(),
                    device: claim.device.clone(),
                    provider: claim.provider.clone(),
                    resources: claim.resources.clone(),
                }),
                ProviderOwner::Ipc => None,
            })
            .collect()
    }
}

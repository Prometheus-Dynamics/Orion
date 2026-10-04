//! Host-session events → node provider path, shared by serial and CAN links.

use super::LinkStatus;
use crate::NodeApp;
use orion_link::host::HostEvent;
use orion_link::message::LeaseRecord;
use std::collections::BTreeSet;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Delay before reopening a port or socket that failed.
pub(super) const REOPEN_DELAY: Duration = Duration::from_secs(1);
/// Lease sets are recomputed at least this often, even without a desired-state notification.
pub(super) const LEASE_REFRESH: Duration = Duration::from_secs(1);

/// Per-link bridge state.
pub(super) struct LinkContext {
    pub(super) app: NodeApp,
    pub(super) name: String,
    started: Instant,
    /// Devices whose current-session snapshot was accepted.
    accepted: BTreeSet<String>,
    pub(super) status: LinkStatus,
    last_logged_error: Option<String>,
}

/// What to do with a device's lease set after an event.
pub(super) enum LeaseAction {
    Keep,
    Set(Vec<LeaseRecord>),
}

impl LinkContext {
    pub(super) fn new(app: NodeApp, name: String) -> Self {
        let status = LinkStatus {
            name: name.clone(),
            ..LinkStatus::default()
        };
        Self {
            app,
            name,
            started: Instant::now(),
            accepted: BTreeSet::new(),
            status,
            last_logged_error: None,
        }
    }

    /// Monotonic milliseconds since the link started (the sessions' clock).
    pub(super) fn now_ms(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    /// Tick period for `poll`: a quarter heartbeat, between 5 and 100 ms.
    pub(super) fn tick_period(heartbeat_ms: u32) -> Duration {
        Duration::from_millis(u64::from(heartbeat_ms / 4).clamp(5, 100))
    }

    /// Applies one host event; returns what to do with the affected device's lease set.
    pub(super) fn handle(&mut self, event: HostEvent) -> LeaseAction {
        match event {
            HostEvent::DeviceConnected {
                device_name,
                session_id,
                max_frame,
                ..
            } => {
                info!(link = %self.name, device = %device_name, session_id, max_frame, "link device connected");
                self.accepted.remove(&device_name);
                // Seed the lease set of a device the node already knows, so it has its leases
                // before its first snapshot arrives.
                LeaseAction::Set(
                    self.app
                        .link_device_leases(&device_name)
                        .unwrap_or_default(),
                )
            }
            HostEvent::ProviderState {
                device_name,
                provider,
                resources,
            } => {
                let provider_id = provider.provider_id.clone();
                let count = resources.len();
                match self.app.link_apply_device_state(
                    &self.name,
                    &device_name,
                    provider,
                    resources,
                ) {
                    Ok(leases) => {
                        if self.accepted.insert(device_name.clone()) {
                            info!(link = %self.name, device = %device_name, provider = %provider_id, resources = count, "link device provider published");
                        } else {
                            debug!(link = %self.name, device = %device_name, provider = %provider_id, resources = count, "link device provider updated");
                        }
                        LeaseAction::Set(leases)
                    }
                    Err(rejection) => {
                        warn!(link = %self.name, device = %device_name, provider = %provider_id, reason = %rejection, "link device snapshot rejected");
                        self.status.snapshot_rejects += 1;
                        self.status.last_error =
                            Some(format!("device `{device_name}`: {rejection}"));
                        if self.accepted.remove(&device_name) {
                            self.app.link_device_lost(&self.name, &device_name);
                        }
                        LeaseAction::Set(Vec::new())
                    }
                }
            }
            HostEvent::DeviceLost { device_name } => {
                info!(link = %self.name, device = %device_name, "link device lost; marking its resources unavailable");
                self.accepted.remove(&device_name);
                self.app.link_device_lost(&self.name, &device_name);
                LeaseAction::Keep
            }
            HostEvent::DeviceRejected {
                device_name,
                reason,
            } => {
                warn!(link = %self.name, device = ?device_name, ?reason, "link device rejected");
                self.status.last_error = Some(format!(
                    "device {}: rejected ({reason:?})",
                    device_name.as_deref().unwrap_or("<undecodable>")
                ));
                LeaseAction::Keep
            }
            HostEvent::LeasesTooLarge {
                device_name,
                frame_len,
                max_frame,
            } => {
                warn!(link = %self.name, device = %device_name, frame_len, max_frame, "lease set does not fit the device's frame size; not sent");
                LeaseAction::Keep
            }
            other => {
                debug!(link = %self.name, event = ?other, "unhandled link event");
                LeaseAction::Keep
            }
        }
    }

    /// Current lease set of an accepted device, if it is accepted.
    pub(super) fn refreshed_leases(&self, device_name: &str) -> Option<Vec<LeaseRecord>> {
        if !self.accepted.contains(device_name) {
            return None;
        }
        self.app.link_device_leases(device_name)
    }

    pub(super) fn accepted(&self) -> impl Iterator<Item = &String> {
        self.accepted.iter()
    }

    /// Records an I/O failure (logged once per distinct message).
    pub(super) fn io_error(&mut self, what: &str, error: &std::io::Error) {
        self.status.io_errors += 1;
        let message = format!("{what}: {error}");
        if self.last_logged_error.as_deref() != Some(message.as_str()) {
            warn!(link = %self.name, error = %message, "link I/O error");
            self.last_logged_error = Some(message.clone());
        }
        self.status.last_error = Some(message);
    }

    pub(super) fn opened(&mut self) {
        info!(link = %self.name, "link opened");
        self.last_logged_error = None;
        self.status.open = true;
    }

    /// Publishes the status (`NodeApp::link_status`).
    pub(super) fn publish_status(&mut self) {
        self.status.devices = self.accepted.iter().cloned().collect();
        self.app.record_link_status(self.status.clone());
    }

    /// Gateway shutdown: mark every device of this link lost.
    pub(super) fn close(&mut self) {
        self.accepted.clear();
        self.status.open = false;
        self.app.link_closed(&self.name);
        self.publish_status();
        info!(
            link = %self.name,
            frames_rx = self.status.frames_rx,
            frames_tx = self.status.frames_tx,
            crc_errors = self.status.crc_errors,
            framing_errors = self.status.framing_errors,
            sessions = self.status.sessions,
            "link closed"
        );
    }
}

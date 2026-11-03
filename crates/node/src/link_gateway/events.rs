//! Host-session events → node provider path, shared by serial and CAN links.

use crate::NodeApp;
use crate::app::{LinkActionCommand, SerialSlot};
use crate::control_plane::LinkStatusSnapshot;
use orion::control_plane::ActionState;
use orion::control_plane::TypedConfigValue;
use orion_link::host::{ActionSendError, HostEvent};
use orion_link::message::{LeaseRecord, LinkActionState, decode_action_map, encode_action_map};
use orion_link::wire::decode_progress;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedReceiver;
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
    pub(super) status: LinkStatusSnapshot,
    last_logged_error: Option<String>,
    /// Actions sent to this link's devices, by the id the device sees.
    actions: SentActions,
}

/// Requests sent to devices of one link and not yet final. Ids are per link and wrap.
#[derive(Default)]
struct SentActions {
    next_id: u32,
    pending: BTreeMap<u32, SentAction>,
}

struct SentAction {
    device: String,
    action_id: String,
}

/// What to do with a device's lease set after an event.
pub(super) enum LeaseAction {
    Keep,
    Set(Vec<LeaseRecord>),
}

impl LinkContext {
    pub(super) fn new(app: NodeApp, name: String) -> Self {
        let status = LinkStatusSnapshot {
            name: name.clone(),
            ..LinkStatusSnapshot::default()
        };
        Self {
            app,
            name,
            started: Instant::now(),
            accepted: BTreeSet::new(),
            status,
            last_logged_error: None,
            actions: SentActions::default(),
        }
    }

    /// Registers this serial link's slot: firmware jobs and the release flag (`app/link_bridge/slots.rs`).
    pub(super) fn open_serial_slot(&self) -> SerialSlot {
        self.app.link_serial_slot_open(&self.name)
    }

    /// Registers this CAN link's slot, which refuses firmware jobs and release requests.
    pub(super) fn open_can_slot(&self) {
        self.app.link_can_slot_open(&self.name);
    }

    /// Registers this link for action requests; the receiver yields what the node routes here.
    pub(super) fn open_actions(&self) -> UnboundedReceiver<LinkActionCommand> {
        self.app.link_actions_open(&self.name)
    }

    /// Sends a routed action to its device through `send` (the link's session). A request that
    /// cannot be sent fails the action at once.
    pub(super) fn accept_action(
        &mut self,
        command: LinkActionCommand,
        send: impl FnOnce(&str, u32, &str, &[u8]) -> Result<(), ActionSendError>,
    ) {
        let LinkActionCommand {
            device,
            action_id,
            request,
        } = command;
        let payload = match encode_action_map(&request.args) {
            Ok(payload) => payload,
            Err(_) => {
                self.app.report_link_action(
                    &action_id,
                    &self.name,
                    &device,
                    ActionState::Rejected {
                        reason: "the arguments cannot be encoded for the device".into(),
                    },
                    BTreeMap::new(),
                );
                return;
            }
        };
        let id = self.actions.next_id;
        self.actions.next_id = self.actions.next_id.wrapping_add(1);
        match send(&device, id, &request.name, &payload) {
            Ok(()) => {
                debug!(link = %self.name, device = %device, action_id = %action_id, id, name = %request.name, "action sent to link device");
                self.actions
                    .pending
                    .insert(id, SentAction { device, action_id });
            }
            Err(error) => {
                let state = send_failure(&device, &self.name, error);
                self.app.report_link_action(
                    &action_id,
                    &self.name,
                    &device,
                    state,
                    BTreeMap::new(),
                );
            }
        }
    }

    /// Reports a device's `ActionResult` for an action this link sent it.
    fn on_action_result(&mut self, device: &str, id: u32, state: LinkActionState, payload: &[u8]) {
        let Some(sent) = self.actions.pending.get(&id) else {
            debug!(link = %self.name, device = %device, id, "action result for an unknown request");
            return;
        };
        if sent.device != device {
            debug!(link = %self.name, device = %device, id, "action result from another device");
            return;
        }
        let action_id = sent.action_id.clone();
        if state != LinkActionState::Running {
            self.actions.pending.remove(&id);
        }
        let (state, output) = report_state(state, payload);
        self.app
            .report_link_action(&action_id, &self.name, device, state, output);
    }

    /// Fails the actions in flight on `device` (it disconnected or restarted).
    fn fail_device_actions(&mut self, device: &str, reason: &str) {
        self.actions.pending.retain(|_, sent| sent.device != device);
        self.app.fail_link_actions(&self.name, Some(device), reason);
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
            HostEvent::Status {
                device_name,
                entries,
            } => {
                let count = entries.len();
                match self
                    .app
                    .link_publish_device_status(&self.name, &device_name, entries)
                {
                    Ok(()) => self.status.status_batches += 1,
                    Err(rejection) => {
                        debug!(link = %self.name, device = %device_name, entries = count, reason = %rejection, "link device status rejected");
                        self.status.status_rejects += 1;
                    }
                }
                LeaseAction::Keep
            }
            HostEvent::DeviceLost { device_name } => {
                info!(link = %self.name, device = %device_name, "link device lost; marking its resources unavailable");
                self.accepted.remove(&device_name);
                self.fail_device_actions(
                    &device_name,
                    "link device disconnected before the action finished",
                );
                self.app.link_device_lost(&self.name, &device_name);
                LeaseAction::Keep
            }
            HostEvent::ActionResult {
                device_name,
                id,
                state,
                payload,
            } => {
                self.on_action_result(&device_name, id, state, &payload);
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
        self.actions.pending.clear();
        self.app
            .fail_link_actions(&self.name, None, "link closed before the action finished");
        self.app.link_actions_closed(&self.name);
        self.app.link_slot_closed(&self.name);
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

/// The node-side state of a device's report, from its `ActionResult` (`docs/link-protocol.md`).
/// A `Succeeded` whose output does not decode is a failure.
fn report_state(
    state: LinkActionState,
    payload: &[u8],
) -> (ActionState, BTreeMap<String, TypedConfigValue>) {
    match state {
        LinkActionState::Running => (
            ActionState::Running {
                progress: decode_progress(payload).ok().flatten().map(|v| v.min(1000)),
            },
            BTreeMap::new(),
        ),
        LinkActionState::Succeeded => match decode_action_map(payload) {
            Ok(output) => (ActionState::Succeeded, output),
            Err(_) => (
                ActionState::Failed {
                    reason: "the device returned an output that does not decode".into(),
                },
                BTreeMap::new(),
            ),
        },
        LinkActionState::Failed => (
            ActionState::Failed {
                reason: reason_text(payload, "the device reported a failure"),
            },
            BTreeMap::new(),
        ),
        LinkActionState::Rejected => (
            ActionState::Rejected {
                reason: reason_text(payload, "the device refused the action"),
            },
            BTreeMap::new(),
        ),
    }
}

fn reason_text(payload: &[u8], empty: &str) -> String {
    if payload.is_empty() {
        empty.to_owned()
    } else {
        String::from_utf8_lossy(payload).into_owned()
    }
}

/// Why a request could not be sent to the device, as the action's state.
fn send_failure(device: &str, link: &str, error: ActionSendError) -> ActionState {
    match error {
        ActionSendError::NotConnected => ActionState::Failed {
            reason: format!("link device `{device}` on {link} is not connected"),
        },
        other => ActionState::Rejected {
            reason: format!("link device `{device}` on {link}: {other}"),
        },
    }
}

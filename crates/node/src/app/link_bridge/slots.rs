//! Per-link control slots (`docs/update-recovery.md`, "MCU firmware over the link protocol").
//!
//! Each link task registers a slot when it starts. A serial link's slot has a firmware job channel
//! and a release flag for its port (`link.release` / `link.attach` release the link). A CAN link's
//! slot has a firmware job channel and the set of its released devices: a released device is not
//! routed to, and the rest of the bus keeps working. A slot is removed when its task closes.

use super::{NodeApp, ProviderOwner, lock};
use crate::actions::ActionOutcome;
use orion_link::host::xfer::FirmwareTransfer;
use std::collections::BTreeSet;
use std::fs::File;
use tokio::sync::{mpsc, watch};

/// Where a firmware transfer stands, as the action handler sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum JobReport {
    /// Bytes the device acknowledged, of the image length.
    Running { acked: u32, total: u32 },
    /// Final: the handler returns this outcome.
    Done(ActionOutcome),
}

/// A firmware transfer for a link task: the driver, the committed image it reads, and the channel
/// the handler waits on.
pub(crate) struct FirmwareCommand {
    /// The device the image goes to (must be connected on the link when the job starts).
    pub(crate) device: String,
    pub(crate) driver: FirmwareTransfer,
    pub(crate) image: File,
    pub(crate) report: watch::Sender<JobReport>,
}

impl FirmwareCommand {
    /// Refuses the job: the handler gets `Rejected` with `reason`.
    pub(crate) fn reject(self, reason: impl Into<String>) {
        self.report
            .send_replace(JobReport::Done(ActionOutcome::rejected(reason)));
    }
}

/// What a serial link task listens on.
pub(crate) struct SerialSlot {
    pub(crate) jobs: mpsc::UnboundedReceiver<FirmwareCommand>,
    /// `true` while the link is released (its port is not served).
    pub(crate) released: watch::Receiver<bool>,
}

/// What a CAN link task listens on.
pub(crate) struct CanSlot {
    pub(crate) jobs: mpsc::UnboundedReceiver<FirmwareCommand>,
    /// The released devices, by name (a device is released until it is attached again).
    pub(crate) released: watch::Receiver<BTreeSet<String>>,
}

pub(crate) struct LinkSlot {
    jobs: mpsc::UnboundedSender<FirmwareCommand>,
    release: Release,
}

enum Release {
    /// A serial link: its port.
    Port(watch::Sender<bool>),
    /// A CAN link: its released devices.
    Devices(watch::Sender<BTreeSet<String>>),
}

impl NodeApp {
    /// Registers the slot of serial link `link` (replacing an older one) and returns the ends the
    /// task listens on.
    pub(crate) fn link_serial_slot_open(&self, link: &str) -> SerialSlot {
        let (jobs_tx, jobs) = mpsc::unbounded_channel();
        let (release_tx, released) = watch::channel(false);
        lock(&self.state.links.slots).insert(
            link.to_owned(),
            LinkSlot {
                jobs: jobs_tx,
                release: Release::Port(release_tx),
            },
        );
        SerialSlot { jobs, released }
    }

    /// Registers the slot of CAN link `link` (replacing an older one) and returns the ends the task
    /// listens on.
    pub(crate) fn link_can_slot_open(&self, link: &str) -> CanSlot {
        let (jobs_tx, jobs) = mpsc::unbounded_channel();
        let (released_tx, released) = watch::channel(BTreeSet::new());
        lock(&self.state.links.slots).insert(
            link.to_owned(),
            LinkSlot {
                jobs: jobs_tx,
                release: Release::Devices(released_tx),
            },
        );
        CanSlot { jobs, released }
    }

    /// Removes the slot of `link` (its task has stopped).
    pub(crate) fn link_slot_closed(&self, link: &str) {
        lock(&self.state.links.slots).remove(link);
    }

    /// Releases (`true`) or attaches (`false`) `device` on `link`. On a serial link the port is
    /// released, and the link has one device; on a CAN link only `device` is.
    pub(crate) fn link_set_device_released(
        &self,
        link: &str,
        device: &str,
        released: bool,
    ) -> Result<(), String> {
        let slots = lock(&self.state.links.slots);
        let slot = slots
            .get(link)
            .ok_or_else(|| format!("link {link} is not running"))?;
        match &slot.release {
            Release::Port(sender) => {
                sender.send_replace(released);
            }
            Release::Devices(sender) => {
                sender.send_if_modified(|set| {
                    if released {
                        set.insert(device.to_owned())
                    } else {
                        set.remove(device)
                    }
                });
            }
        }
        Ok(())
    }

    /// Whether `device` on `link` is released.
    pub(crate) fn link_device_released(&self, link: &str, device: &str) -> Result<bool, String> {
        let slots = lock(&self.state.links.slots);
        let slot = slots
            .get(link)
            .ok_or_else(|| format!("link {link} is not running"))?;
        Ok(match &slot.release {
            Release::Port(sender) => *sender.borrow(),
            Release::Devices(sender) => sender.borrow().contains(device),
        })
    }

    /// Hands a firmware job to the task of `link`. Refused on links that are not running.
    pub(crate) fn link_submit_firmware(
        &self,
        link: &str,
        command: FirmwareCommand,
    ) -> Result<(), String> {
        let slots = lock(&self.state.links.slots);
        let slot = slots
            .get(link)
            .ok_or_else(|| format!("link {link} is not running"))?;
        slot.jobs
            .send(command)
            .map_err(|_| format!("link {link} stopped before the transfer was started"))
    }

    /// Whether a link device of `link` named `device` has published a provider (its claim is kept
    /// while the device is disconnected).
    pub(crate) fn link_device_known(&self, link: &str, device: &str) -> bool {
        lock(&self.state.links.owners).values().any(|owner| {
            matches!(
                owner,
                ProviderOwner::Device(claim) if claim.link == link && claim.device == device
            )
        })
    }
}

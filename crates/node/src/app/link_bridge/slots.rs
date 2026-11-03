//! Per-link control slots (`docs/update-recovery.md`, "MCU firmware over the link protocol").
//!
//! Each link task registers a slot when it starts: a serial link gets a firmware job channel and a
//! release flag (`link.release` / `link.attach`); a CAN link registers an empty slot, so the
//! actions can say why they do not apply. A slot is removed when its task closes.

use super::{NodeApp, ProviderOwner, lock};
use crate::actions::ActionOutcome;
use orion_link::host::xfer::FirmwareTransfer;
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

/// A firmware transfer for a serial link's task: the driver, the committed image it reads, and
/// the channel the handler waits on.
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

#[derive(Default)]
pub(crate) struct LinkSlot {
    /// Firmware jobs (serial links only).
    jobs: Option<mpsc::UnboundedSender<FirmwareCommand>>,
    /// Release flag (serial links only).
    release: Option<watch::Sender<bool>>,
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
                jobs: Some(jobs_tx),
                release: Some(release_tx),
            },
        );
        SerialSlot { jobs, released }
    }

    /// Registers the slot of CAN link `link`: present, but without firmware or release.
    pub(crate) fn link_can_slot_open(&self, link: &str) {
        lock(&self.state.links.slots).insert(link.to_owned(), LinkSlot::default());
    }

    /// Removes the slot of `link` (its task has stopped).
    pub(crate) fn link_slot_closed(&self, link: &str) {
        lock(&self.state.links.slots).remove(link);
    }

    /// Sets the release flag of serial link `link`; returns the previous value.
    pub(crate) fn link_set_released(&self, link: &str, released: bool) -> Result<bool, String> {
        let slots = lock(&self.state.links.slots);
        let slot = slots
            .get(link)
            .ok_or_else(|| format!("link {link} is not running"))?;
        let sender = slot.release.as_ref().ok_or_else(|| {
            format!(
                "link {link} is a CAN link; link.release and link.attach support serial links only"
            )
        })?;
        Ok(sender.send_replace(released))
    }

    /// Whether serial link `link` is released.
    pub(crate) fn link_is_released(&self, link: &str) -> Result<bool, String> {
        let slots = lock(&self.state.links.slots);
        let slot = slots
            .get(link)
            .ok_or_else(|| format!("link {link} is not running"))?;
        let sender = slot.release.as_ref().ok_or_else(|| {
            format!(
                "link {link} is a CAN link; link.release and link.attach support serial links only"
            )
        })?;
        Ok(*sender.borrow())
    }

    /// Hands a firmware job to the task of serial link `link`. Refused on CAN links and on links
    /// that are not running.
    pub(crate) fn link_submit_firmware(
        &self,
        link: &str,
        command: FirmwareCommand,
    ) -> Result<(), String> {
        let slots = lock(&self.state.links.slots);
        let slot = slots
            .get(link)
            .ok_or_else(|| format!("link {link} is not running"))?;
        let jobs = slot
            .jobs
            .as_ref()
            .ok_or_else(|| "firmware transfer is not supported on CAN links yet".to_owned())?;
        jobs.send(command)
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

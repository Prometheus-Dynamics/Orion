//! Firmware over a serial link, and the link's release, as node actions (`docs/actions.md`,
//! "Link firmware"; `docs/update-recovery.md`, "MCU firmware over the link protocol").
//!
//! - `link.release { link, device }` stops routing to the device: it is marked lost (its providers
//!   become unavailable) and its traffic is dropped until `link.attach { link, device }`. On a
//!   serial link this closes the port and the link does not reopen until it is attached again. On
//!   a CAN link only that device is released; the rest of the bus keeps working (see
//!   `docs/update-recovery.md`, "MCU firmware over the link protocol").
//! - `link.firmware.update { link, device, blob, component, version, target_slot, activate }` runs a
//!   committed blob through `orion_link::host::xfer::FirmwareTransfer` on the link's session. The
//!   link task owns the transfer (it owns the session, or on a CAN bus the device's session); this
//!   handler submits the job, forwards its progress as the action's `Running` state, and returns its
//!   outcome. Serial and CAN links both take firmware.

use crate::NodeApp;
use crate::actions::{ActionContext, ActionHandler, ActionOutcome};
use crate::app::{FirmwareCommand, JobReport};
use orion::control_plane::{ActionRequest, TypedConfigValue};
use orion_link::crc32c;
use orion_link::host::HostEvent;
use orion_link::host::xfer::{DeviceInfo, Failure, FirmwareTransfer, Phase, XferRequest};
use orion_link::wire::ActivateMode;
use std::collections::BTreeMap;
use std::fs::File;
use std::future::Future;
use std::io;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::watch;

/// Longest component or version name, in bytes (it travels in `XferBegin`).
const MAX_NAME_BYTES: usize = 64;

/// The node-side handlers of the link actions, registered by the builder.
pub(crate) fn builtin_handlers() -> Vec<(String, Arc<dyn ActionHandler>)> {
    fn handler<F, Fut>(run: F) -> Arc<dyn ActionHandler>
    where
        F: Fn(ActionRequest, ActionContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ActionOutcome> + Send + 'static,
    {
        Arc::new(run)
    }
    vec![
        ("link.release".to_owned(), handler(release)),
        ("link.attach".to_owned(), handler(attach)),
        ("link.firmware.update".to_owned(), handler(firmware_update)),
    ]
}

async fn release(request: ActionRequest, context: ActionContext) -> ActionOutcome {
    set_released(&context.app, &request, true)
}

async fn attach(request: ActionRequest, context: ActionContext) -> ActionOutcome {
    set_released(&context.app, &request, false)
}

fn set_released(app: &NodeApp, request: &ActionRequest, released: bool) -> ActionOutcome {
    let (link, device) = match link_and_device(request) {
        Ok(pair) => pair,
        Err(reason) => return ActionOutcome::rejected(reason),
    };
    if !app.link_device_known(&link, &device) {
        return ActionOutcome::rejected(format!(
            "no device `{device}` has published on link {link}"
        ));
    }
    match app.link_set_device_released(&link, &device, released) {
        Ok(_) => ActionOutcome::Succeeded(BTreeMap::from([
            (
                "state".to_owned(),
                TypedConfigValue::String(if released { "released" } else { "attached" }.into()),
            ),
            ("link".to_owned(), TypedConfigValue::String(link)),
            ("device".to_owned(), TypedConfigValue::String(device)),
        ])),
        Err(reason) => ActionOutcome::rejected(reason),
    }
}

async fn firmware_update(request: ActionRequest, context: ActionContext) -> ActionOutcome {
    let app = context.app.clone();
    let args = match FirmwareArgs::from_request(&request) {
        Ok(args) => args,
        Err(reason) => return ActionOutcome::rejected(reason),
    };
    let path = match app.committed_blob_path(&args.blob) {
        Ok(path) => path,
        Err(reason) => return ActionOutcome::rejected(reason),
    };
    // A link that is not running is refused by the submission below, with the reason that applies.
    if app.link_device_released(&args.link, &args.device) == Ok(true) {
        return ActionOutcome::rejected(format!(
            "device `{}` on link {} is released; run link.attach first",
            args.device, args.link
        ));
    }

    // Digest and length of the image: one read of the file, off the runtime's threads.
    let xfer_id = transfer_id(&args);
    let prepare_args = args.clone();
    let prepared =
        tokio::task::spawn_blocking(move || prepare(&path, &prepare_args, xfer_id)).await;
    let (driver, image) = match prepared {
        Ok(Ok(pair)) => pair,
        Ok(Err(error)) => {
            return ActionOutcome::failed(format!("cannot read blob `{}`: {error}", args.blob));
        }
        Err(error) => return ActionOutcome::failed(format!("cannot read the blob: {error}")),
    };

    let total = driver.progress().1;
    let (report, mut updates) = watch::channel(JobReport::Running { acked: 0, total });
    let command = FirmwareCommand {
        device: args.device.clone(),
        driver,
        image,
        report,
    };
    if let Err(reason) = app.link_submit_firmware(&args.link, command) {
        return ActionOutcome::rejected(reason);
    }
    loop {
        let report = updates.borrow_and_update().clone();
        match report {
            JobReport::Done(outcome) => return outcome,
            JobReport::Running { acked, total } => {
                context.progress(Some(per_mille(acked, total)), BTreeMap::new());
            }
        }
        if updates.changed().await.is_err() {
            return ActionOutcome::failed("the link stopped before the firmware transfer finished");
        }
    }
}

/// The driver for the committed image at `path`, and the open image it reads chunks from.
fn prepare(path: &Path, args: &FirmwareArgs, xfer_id: u32) -> io::Result<(FirmwareTransfer, File)> {
    let mut image = File::open(path)?;
    let driver = FirmwareTransfer::from_image(
        xfer_id,
        &args.component,
        &args.version,
        args.target_slot,
        args.activate,
        &mut image,
    )?;
    Ok((driver, image))
}

/// The transfer id: a function of the blob, component and version, so a retry of the same update
/// resumes at the device's offset after a link loss or a node restart. Never 0 (reserved).
fn transfer_id(args: &FirmwareArgs) -> u32 {
    crc32c(format!("{}\0{}\0{}", args.blob, args.component, args.version).as_bytes()).max(1)
}

/// Progress in per-mille of the image (`acked` of `total` bytes).
fn per_mille(acked: u32, total: u32) -> u16 {
    let value = u64::from(acked) * 1000 / u64::from(total.max(1));
    u16::try_from(value.min(1000)).unwrap_or(1000)
}

/// The arguments of `link.firmware.update`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct FirmwareArgs {
    link: String,
    device: String,
    blob: String,
    component: String,
    version: String,
    target_slot: u8,
    activate: ActivateMode,
}

impl FirmwareArgs {
    fn from_request(request: &ActionRequest) -> Result<Self, String> {
        let (link, device) = link_and_device(request)?;
        let blob = text_arg(request, "blob")?;
        let component = text_arg(request, "component")?;
        let version = text_arg(request, "version")?;
        for (key, value) in [("component", &component), ("version", &version)] {
            if value.len() > MAX_NAME_BYTES {
                return Err(format!(
                    "argument `{key}` is longer than {MAX_NAME_BYTES} bytes"
                ));
            }
        }
        let target_slot = match request.args.get("target_slot") {
            Some(TypedConfigValue::UInt(slot)) => u8::try_from(*slot)
                .map_err(|_| "argument `target_slot` must be at most 255".to_owned())?,
            _ => return Err("argument `target_slot` (uint) is required".to_owned()),
        };
        let activate = match optional_text_arg(request, "activate")?.as_deref() {
            None | Some("trial") => ActivateMode::Trial,
            Some("handoff") => ActivateMode::Handoff,
            Some(other) => {
                return Err(format!(
                    "argument `activate` is `trial` or `handoff`, not `{other}`"
                ));
            }
        };
        Ok(Self {
            link,
            device,
            blob,
            component,
            version,
            target_slot,
            activate,
        })
    }
}

fn link_and_device(request: &ActionRequest) -> Result<(String, String), String> {
    Ok((text_arg(request, "link")?, text_arg(request, "device")?))
}

fn text_arg(request: &ActionRequest, key: &str) -> Result<String, String> {
    match request.args.get(key) {
        Some(TypedConfigValue::String(value)) if !value.trim().is_empty() => Ok(value.clone()),
        _ => Err(format!("argument `{key}` (string) is required")),
    }
}

fn optional_text_arg(request: &ActionRequest, key: &str) -> Result<Option<String>, String> {
    match request.args.get(key) {
        None => Ok(None),
        Some(TypedConfigValue::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(format!("argument `{key}` must be a string")),
    }
}

/// What the link task knows of its connected device, for a job that starts after the device
/// connected: its events were consumed before the job existed, so the job replays them.
#[derive(Debug, Default)]
pub(super) struct DeviceSeen {
    /// The connected device and its negotiated frame limit.
    connected: Option<(String, usize)>,
    /// Its last `DeviceInfo` in this session.
    info: Option<DeviceInfo>,
}

impl DeviceSeen {
    pub(super) fn on_event(&mut self, event: &HostEvent) {
        match event {
            HostEvent::DeviceConnected {
                device_name,
                max_frame,
                ..
            } => {
                self.connected = Some((device_name.clone(), *max_frame));
                self.info = None;
            }
            HostEvent::DeviceInfo { device_name, info } if self.is_connected(device_name) => {
                self.info = Some(info.clone());
            }
            HostEvent::DeviceLost { device_name } if self.is_connected(device_name) => {
                self.connected = None;
                self.info = None;
            }
            _ => {}
        }
    }

    pub(super) fn is_connected(&self, device: &str) -> bool {
        self.connected
            .as_ref()
            .is_some_and(|(name, _)| name == device)
    }
}

/// A firmware transfer in progress on a serial link's task.
pub(super) struct FirmwareJob {
    device: String,
    driver: FirmwareTransfer,
    image: File,
    report: watch::Sender<JobReport>,
    /// The progress last reported to the handler.
    reported: (u32, u32),
}

/// The end of a job: its outcome, and whether the device handed its port to the bootloader (the
/// link is then released).
pub(super) struct JobEnd {
    pub(super) outcome: ActionOutcome,
    pub(super) handoff: bool,
}

impl FirmwareJob {
    /// Starts a job for a device that is connected now, replaying the device's state from `seen`.
    pub(super) fn start(command: FirmwareCommand, seen: &DeviceSeen) -> Self {
        let FirmwareCommand {
            device,
            driver,
            image,
            report,
        } = command;
        let reported = driver.progress();
        let mut job = Self {
            device,
            driver,
            image,
            report,
            reported,
        };
        if let Some((_, max_frame)) = seen.connected.as_ref() {
            job.driver.on_connected(*max_frame);
        }
        if let Some(info) = seen.info.as_ref() {
            job.apply_info(info);
        }
        job
    }

    /// The device this job transfers to.
    pub(super) fn device(&self) -> &str {
        &self.device
    }

    /// Feeds one host event (only the events of this job's device matter).
    pub(super) fn on_event(&mut self, event: &HostEvent) {
        match event {
            HostEvent::DeviceConnected {
                device_name,
                max_frame,
                ..
            } if device_name == &self.device => self.driver.on_connected(*max_frame),
            HostEvent::DeviceInfo { device_name, info } if device_name == &self.device => {
                self.apply_info(info);
            }
            HostEvent::Xfer { device_name, reply } if device_name == &self.device => {
                self.driver.on_reply(reply);
            }
            HostEvent::DeviceLost { device_name } if device_name == &self.device => {
                self.driver.on_lost();
            }
            _ => {}
        }
    }

    /// The device reported its state. Once it runs the new version, the health gate is open and
    /// the driver may confirm the trial.
    fn apply_info(&mut self, info: &DeviceInfo) {
        self.driver
            .set_confirm_gate(info.fw_version == self.driver.plan().version);
        self.driver.on_info(info.clone());
    }

    /// One step at `now_ms`: sends the driver's next request through `send`, reports progress, and
    /// returns the end of the job when it finished or its handler went away.
    pub(super) fn drive(
        &mut self,
        now_ms: u64,
        send: impl FnOnce(&XferRequest) -> bool,
    ) -> Option<JobEnd> {
        if self.report.is_closed() {
            return Some(JobEnd {
                outcome: ActionOutcome::failed(
                    "the action ended before the firmware transfer finished",
                ),
                handoff: false,
            });
        }
        match self.driver.poll(now_ms, &mut self.image) {
            Ok(Some(request)) => {
                send(&request);
            }
            Ok(None) => {}
            Err(error) => {
                return Some(JobEnd {
                    outcome: ActionOutcome::failed(format!("cannot read the image: {error}")),
                    handoff: false,
                });
            }
        }
        let (acked, total) = self.driver.progress();
        if (acked, total) != self.reported {
            self.reported = (acked, total);
            self.report
                .send_replace(JobReport::Running { acked, total });
        }
        self.driver.is_finished().then(|| JobEnd {
            outcome: self.outcome(),
            handoff: self.driver.phase() == Phase::Handoff,
        })
    }

    /// Ends the job with `outcome` (the handler returns it).
    pub(super) fn finish(self, outcome: ActionOutcome) {
        self.report.send_replace(JobReport::Done(outcome));
    }

    fn outcome(&self) -> ActionOutcome {
        let version = self.driver.plan().version.clone();
        match self.driver.phase() {
            Phase::Succeeded => {
                let mut output = BTreeMap::from([
                    (
                        "phase".to_owned(),
                        TypedConfigValue::String("installed".into()),
                    ),
                    ("version".to_owned(), TypedConfigValue::String(version)),
                ]);
                if let Some(info) = self.driver.device_info() {
                    output.insert(
                        "active_slot".to_owned(),
                        TypedConfigValue::UInt(u64::from(info.active_slot)),
                    );
                }
                ActionOutcome::Succeeded(output)
            }
            Phase::Handoff => ActionOutcome::Succeeded(BTreeMap::from([
                (
                    "phase".to_owned(),
                    TypedConfigValue::String("handoff".into()),
                ),
                ("version".to_owned(), TypedConfigValue::String(version)),
            ])),
            Phase::Failed(failure) => ActionOutcome::failed(failure_reason(failure)),
            _ => ActionOutcome::failed("the firmware transfer did not finish"),
        }
    }
}

/// Why a transfer failed, for the action's reason.
fn failure_reason(failure: Failure) -> String {
    match failure {
        Failure::Device(status) => format!("the device refused the transfer: {status:?}"),
        Failure::Retries => {
            "the device stopped acknowledging the transfer (retry limit reached)".to_owned()
        }
        Failure::RolledBack => "rolled back: the device came back on its previous image, so its \
             bootloader reverted the new one"
            .to_owned(),
        Failure::NotActivated => {
            "the device did not come back running the new version after activation".to_owned()
        }
        Failure::Timeout => {
            "timed out waiting for the device to run the new version and confirm it".to_owned()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_is_per_mille_and_bounded() {
        assert_eq!(per_mille(0, 10), 0);
        assert_eq!(per_mille(5, 10), 500);
        assert_eq!(per_mille(10, 10), 1000);
        assert_eq!(
            per_mille(3, 0),
            1000,
            "an empty image never divides by zero"
        );
    }

    #[test]
    fn failures_name_the_cause() {
        assert!(failure_reason(Failure::RolledBack).contains("reverted"));
        assert!(failure_reason(Failure::Timeout).contains("timed out"));
        assert!(
            failure_reason(Failure::Device(orion_link::wire::XferStatus::BadDigest))
                .contains("BadDigest")
        );
    }
}

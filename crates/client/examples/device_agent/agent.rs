//! A device agent on Orion's local IPC (`docs/device-agent.md`).
//!
//! It claims the node actions `update`, `reboot` and `locate`, runs them with an [`Updater`]
//! (the device package's writer, LEDs and reboot), mirrors progress into the action and the
//! status lane, and keeps the `update.*` status keys under `node/<id>` published: once after it
//! connects (after every boot) and then every [`REPUBLISH_INTERVAL`], which also restores them
//! after `orion-node` restarts.
//!
//! The example binary and `crates/node/tests/device_agent.rs` share this file.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use orion_client::{ActionReporter, ClientError, LocalProviderService};
use orion_control_plane::{
    ActionRequest, StatusEntry, TypedConfigValue, action_names, action_status_keys, update_action,
};
use tokio::sync::{Notify, mpsc};

/// The node actions the agent claims.
pub const CLAIMED_ACTIONS: [&str; 3] = [
    action_names::UPDATE,
    action_names::REBOOT,
    action_names::LOCATE,
];

/// How often the `update.*` keys are republished. Entries are published with the node's maximum
/// TTL (`ORION_NODE_STATUS_MAX_TTL_MS`, 5 minutes by default), so this keeps them alive with a
/// wide margin and restores them within one interval after `orion-node` restarts.
pub const REPUBLISH_INTERVAL: Duration = Duration::from_secs(30);

/// What the device's updater reports (`update status`), published as the `update.*` keys.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UpdaterStatus {
    /// `idle`, `staging`, `staged`, `trying`, `confirmed`, `rolled-back`, ...
    pub state: String,
    pub slot_active: Option<String>,
    pub slot_staged: Option<String>,
    pub version_active: Option<String>,
    pub version_staged: Option<String>,
    /// Per mille, while staging.
    pub progress: Option<u16>,
    pub error: Option<String>,
    /// The kernel boot id (`/proc/sys/kernel/random/boot_id`).
    pub boot_id: Option<String>,
}

/// The arguments of an `update` action.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageRequest {
    pub image_url: String,
    pub sha256: String,
    pub size: u64,
}

impl StageRequest {
    /// Reads `image_url`, `sha256` (64 hex digits) and `size` from an `update` request.
    pub fn from_args(args: &BTreeMap<String, TypedConfigValue>) -> Result<Self, String> {
        let text = |key: &str| match args.get(key) {
            Some(TypedConfigValue::String(value)) if !value.is_empty() => Ok(value.clone()),
            _ => Err(format!("`{key}` (string) is required")),
        };
        let image_url = text(update_action::ARG_IMAGE_URL)?;
        let sha256 = text(update_action::ARG_SHA256)?.to_ascii_lowercase();
        if sha256.len() != 64 || !sha256.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err("`sha256` must be 64 hex digits".into());
        }
        let size = match args.get(update_action::ARG_SIZE) {
            Some(TypedConfigValue::UInt(size)) if *size > 0 => *size,
            Some(TypedConfigValue::Int(size)) if *size > 0 => *size as u64,
            _ => return Err("`size` (uint, bytes) is required".into()),
        };
        Ok(Self {
            image_url,
            sha256,
            size,
        })
    }
}

/// The device package's side: on Raze, `pd-device-update stage/apply/status`, `raze-leds`, and
/// `systemctl reboot`.
pub trait Updater: Send + Sync + 'static {
    /// Current state (`update status`).
    fn status(&self) -> UpdaterStatus;
    /// Downloads `image_url`, checks it and writes the inactive slot, sending progress (per
    /// mille) as it goes; returns the staged version. Must be idempotent: a request for an image
    /// that is already staged (same digest) succeeds at once.
    fn stage(
        &self,
        request: StageRequest,
        progress: mpsc::UnboundedSender<u16>,
    ) -> impl Future<Output = Result<String, String>> + Send;
    /// Points the next boot at the staged slot (trial boot) and reboots (`update apply`).
    fn apply(&self) -> impl Future<Output = Result<(), String>> + Send;
    /// Reboots after `delay_ms`.
    fn reboot(&self, delay_ms: u64) -> impl Future<Output = Result<(), String>> + Send;
    /// Starts (`enabled`) or stops identifying the device for `duration_ms`.
    fn locate(
        &self,
        enabled: bool,
        duration_ms: u64,
    ) -> impl Future<Output = Result<(), String>> + Send;
}

/// The `update.*` entries for `status` under `node/<id>` (TTL 0: the node's maximum).
pub fn update_status_entries(
    reporter: &ActionReporter,
    status: &UpdaterStatus,
) -> Vec<StatusEntry> {
    let text = |value: &Option<String>| TypedConfigValue::String(value.clone().unwrap_or_default());
    vec![
        reporter.node_status_entry(
            update_action::KEY_STATE,
            TypedConfigValue::String(status.state.clone()),
        ),
        reporter.node_status_entry(update_action::KEY_SLOT_ACTIVE, text(&status.slot_active)),
        reporter.node_status_entry(update_action::KEY_SLOT_STAGED, text(&status.slot_staged)),
        reporter.node_status_entry(
            update_action::KEY_VERSION_ACTIVE,
            text(&status.version_active),
        ),
        reporter.node_status_entry(
            update_action::KEY_VERSION_STAGED,
            text(&status.version_staged),
        ),
        reporter.node_status_entry(
            update_action::KEY_PROGRESS,
            TypedConfigValue::UInt(u64::from(status.progress.unwrap_or(0))),
        ),
        reporter.node_status_entry(update_action::KEY_ERROR, text(&status.error)),
        reporter.node_status_entry(update_action::KEY_BOOT_ID, text(&status.boot_id)),
    ]
}

/// `action.<id>.{state,progress,error}` entries under `node/<id>`.
fn action_entries(
    reporter: &ActionReporter,
    action_id: &str,
    state: &str,
    progress: Option<u16>,
    error: Option<&str>,
) -> Vec<StatusEntry> {
    let mut entries = vec![reporter.node_status_entry(
        action_status_keys::key(action_id, action_status_keys::STATE),
        TypedConfigValue::String(state.into()),
    )];
    if let Some(progress) = progress {
        entries.push(reporter.node_status_entry(
            action_status_keys::key(action_id, action_status_keys::PROGRESS),
            TypedConfigValue::UInt(u64::from(progress)),
        ));
    }
    if let Some(error) = error {
        entries.push(reporter.node_status_entry(
            action_status_keys::key(action_id, action_status_keys::ERROR),
            TypedConfigValue::String(error.into()),
        ));
    }
    entries
}

/// Publishes the updater's status; failures (no claim yet after a reconnect, node restarting)
/// are retried by the next republish.
async fn publish_update_status<U: Updater>(reporter: &ActionReporter, updater: &U) {
    let _ = reporter
        .publish_status(update_status_entries(reporter, &updater.status()))
        .await;
}

/// Claims the actions and serves them until the connection fails for good (the service's retry
/// policy gives up).
pub async fn run<U: Updater>(
    service: &LocalProviderService,
    updater: Arc<U>,
    republish_every: Duration,
) -> Result<(), ClientError> {
    let mut watch = service.claim_node_actions(CLAIMED_ACTIONS).await?;
    let reporter = watch.reporter();
    // After every boot: the durable view of the last update.
    reporter
        .publish_status(update_status_entries(&reporter, &updater.status()))
        .await?;
    let republisher = {
        let reporter = reporter.clone();
        let updater = updater.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(republish_every);
            tick.tick().await;
            loop {
                tick.tick().await;
                publish_update_status(&reporter, updater.as_ref()).await;
            }
        })
    };
    let _republisher = AbortOnDrop(republisher);
    loop {
        let request = watch.next().await?;
        handle(&reporter, updater.as_ref(), request).await;
    }
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Runs one claimed action and reports its outcome.
pub async fn handle<U: Updater>(reporter: &ActionReporter, updater: &U, request: ActionRequest) {
    let id = request.action_id.clone();
    let uint = |key: &str, default: u64| match request.args.get(key) {
        Some(TypedConfigValue::UInt(value)) => *value,
        _ => default,
    };
    let outcome = match request.name.as_str() {
        action_names::UPDATE => return handle_update(reporter, updater, request).await,
        action_names::REBOOT => {
            // Report first: the reboot ends this process and the node's action record.
            let _ = reporter
                .succeed(
                    &id,
                    BTreeMap::from([(
                        update_action::OUTPUT_PHASE.to_owned(),
                        TypedConfigValue::String(update_action::PHASE_REBOOTING.into()),
                    )]),
                )
                .await;
            updater.reboot(uint("delay_ms", 0)).await.err()
        }
        action_names::LOCATE => {
            let enabled = !matches!(
                request.args.get("enabled"),
                Some(TypedConfigValue::Bool(false))
            );
            match updater.locate(enabled, uint("duration_ms", 10_000)).await {
                Ok(()) => {
                    let _ = reporter.succeed(&id, BTreeMap::new()).await;
                    None
                }
                Err(error) => Some(error),
            }
        }
        other => {
            let _ = reporter
                .reject(&id, format!("unsupported action `{other}`"))
                .await;
            None
        }
    };
    if let Some(error) = outcome {
        // For `reboot` the result is already final; the error only reaches the logs.
        let _ = reporter.fail(&id, error).await;
    }
}

async fn handle_update<U: Updater>(reporter: &ActionReporter, updater: &U, request: ActionRequest) {
    let id = request.action_id.as_str();
    let stage = match StageRequest::from_args(&request.args) {
        Ok(stage) => stage,
        Err(reason) => {
            let _ = reporter
                .publish_status(action_entries(
                    reporter,
                    id,
                    "rejected",
                    None,
                    Some(&reason),
                ))
                .await;
            let _ = reporter.reject(id, reason).await;
            return;
        }
    };
    let _ = reporter.progress(id, Some(0)).await;
    let _ = reporter
        .publish_status(action_entries(reporter, id, "running", Some(0), None))
        .await;
    publish_update_status(reporter, updater).await;

    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
    let staging = updater.stage(stage, progress_tx);
    tokio::pin!(staging);
    let staged = loop {
        tokio::select! {
            result = &mut staging => break result,
            Some(progress) = progress_rx.recv() => {
                let progress = progress.min(1000);
                let _ = reporter.progress(id, Some(progress)).await;
                let mut entries = action_entries(reporter, id, "running", Some(progress), None);
                entries.push(reporter.node_status_entry(
                    update_action::KEY_PROGRESS,
                    TypedConfigValue::UInt(u64::from(progress)),
                ));
                let _ = reporter.publish_status(entries).await;
            }
        }
    };
    match staged {
        Ok(version) => {
            publish_update_status(reporter, updater).await;
            let _ = reporter
                .publish_status(action_entries(reporter, id, "succeeded", Some(1000), None))
                .await;
            // "Staged and apply issued": report before `apply` reboots into the trial slot.
            let _ = reporter
                .succeed(
                    id,
                    BTreeMap::from([
                        (
                            update_action::OUTPUT_PHASE.to_owned(),
                            TypedConfigValue::String(update_action::PHASE_REBOOTING.into()),
                        ),
                        (
                            update_action::OUTPUT_VERSION_STAGED.to_owned(),
                            TypedConfigValue::String(version),
                        ),
                    ]),
                )
                .await;
            if let Err(error) = updater.apply().await {
                // Still staged (for example a `pre-reboot` hook refused); the keys say so.
                let mut status = updater.status();
                status.error = Some(error);
                let _ = reporter
                    .publish_status(update_status_entries(reporter, &status))
                    .await;
            }
        }
        Err(error) => {
            publish_update_status(reporter, updater).await;
            let _ = reporter
                .publish_status(action_entries(reporter, id, "failed", None, Some(&error)))
                .await;
            let _ = reporter.fail(id, error).await;
        }
    }
}

/// An in-memory updater for the example and tests: staging takes `steps` progress steps of
/// `step` each, `apply` and `reboot` only record that they were called, and staging can be held
/// at its first step with [`FakeUpdater::hold_staging`].
pub struct FakeUpdater {
    status: Mutex<UpdaterStatus>,
    steps: u16,
    step: Duration,
    hold: Mutex<Option<Arc<Notify>>>,
    calls: Mutex<Vec<String>>,
}

impl FakeUpdater {
    pub fn new(status: UpdaterStatus, steps: u16, step: Duration) -> Self {
        Self {
            status: Mutex::new(status),
            steps: steps.max(1),
            step,
            hold: Mutex::new(None),
            calls: Mutex::new(Vec::new()),
        }
    }

    /// Makes the next stages wait after their first progress step until the returned notify
    /// fires.
    pub fn hold_staging(&self) -> Arc<Notify> {
        let notify = Arc::new(Notify::new());
        *self.hold.lock().expect("fake updater") = Some(notify.clone());
        notify
    }

    /// `stage <url>`, `apply`, `reboot <delay>`, `locate <enabled> <duration>`, in call order.
    pub fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("fake updater").clone()
    }

    pub fn set_status(&self, status: UpdaterStatus) {
        *self.status.lock().expect("fake updater") = status;
    }

    fn record(&self, call: String) {
        self.calls.lock().expect("fake updater").push(call);
    }

    fn update(&self, change: impl FnOnce(&mut UpdaterStatus)) {
        change(&mut self.status.lock().expect("fake updater"));
    }
}

impl Updater for FakeUpdater {
    fn status(&self) -> UpdaterStatus {
        self.status.lock().expect("fake updater").clone()
    }

    async fn stage(
        &self,
        request: StageRequest,
        progress: mpsc::UnboundedSender<u16>,
    ) -> Result<String, String> {
        self.record(format!("stage {}", request.image_url));
        if request.image_url.contains("corrupt") {
            self.update(|status| {
                status.state = "idle".into();
                status.error = Some("sha256 mismatch".into());
            });
            return Err("sha256 mismatch".into());
        }
        self.update(|status| {
            status.state = "staging".into();
            status.error = None;
        });
        let hold = self.hold.lock().expect("fake updater").clone();
        for step in 1..=self.steps {
            let per_mille =
                u16::try_from(u32::from(step) * 1000 / u32::from(self.steps)).unwrap_or(1000);
            self.update(|status| status.progress = Some(per_mille));
            let _ = progress.send(per_mille);
            if step == 1
                && let Some(hold) = &hold
            {
                hold.notified().await;
            }
            tokio::time::sleep(self.step).await;
        }
        let version = request
            .image_url
            .rsplit('/')
            .next()
            .and_then(|file| file.strip_prefix("image-"))
            .and_then(|file| file.strip_suffix(".img.xz"))
            .unwrap_or("unknown")
            .to_owned();
        self.update(|status| {
            status.state = "staged".into();
            status.slot_staged = Some(match status.slot_active.as_deref() {
                Some("A") => "B".into(),
                _ => "A".into(),
            });
            status.version_staged = Some(version.clone());
            status.progress = Some(1000);
        });
        Ok(version)
    }

    async fn apply(&self) -> Result<(), String> {
        self.record("apply".into());
        self.update(|status| status.state = "trying".into());
        Ok(())
    }

    async fn reboot(&self, delay_ms: u64) -> Result<(), String> {
        self.record(format!("reboot {delay_ms}"));
        Ok(())
    }

    async fn locate(&self, enabled: bool, duration_ms: u64) -> Result<(), String> {
        self.record(format!("locate {enabled} {duration_ms}"));
        Ok(())
    }
}

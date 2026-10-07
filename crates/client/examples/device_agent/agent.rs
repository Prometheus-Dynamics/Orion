//! A device agent on Orion's local IPC (`docs/device-agent.md`).
//!
//! It claims the node actions `update`, `update.cancel`, `update.rollback`, `reboot` and
//! `locate`, and runs them with an [`Updater`] (the device package's writer, LEDs and reboot).
//! `update` is asynchronous: the action succeeds with `phase = "staging"` once the download and
//! stage have started, and the stage then runs in the background, reporting only through the
//! `update.*` status keys under `node/<id>`. The agent keeps those keys published: once after it
//! connects (after every boot), on every change, and every [`REPUBLISH_INTERVAL`], which also
//! restores them after `orion-node` restarts.
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
use tokio::sync::{Notify, mpsc, oneshot};

/// The node actions the agent claims.
pub const CLAIMED_ACTIONS: [&str; 5] = [
    action_names::UPDATE,
    action_names::UPDATE_CANCEL,
    action_names::UPDATE_ROLLBACK,
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
    /// One of the `update_action::STATE_*` values.
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
    /// mille) as it goes; returns the staged version. The agent drops the future to cancel it.
    fn stage(
        &self,
        request: StageRequest,
        progress: mpsc::UnboundedSender<u16>,
    ) -> impl Future<Output = Result<String, String>> + Send;
    /// Points the next boot at the staged slot (trial boot) and reboots (`update apply`).
    fn apply(&self) -> impl Future<Output = Result<(), String>> + Send;
    /// Cleans up after a cancelled stage, or forgets a staged update; `Ok(false)` when there was
    /// nothing to cancel. Leaves the state `cancelled` when it cancelled something.
    fn cancel(&self) -> impl Future<Output = Result<bool, String>> + Send;
    /// Points the next boot at the previous confirmed slot; `Err` when there is none.
    fn prepare_rollback(&self) -> impl Future<Output = Result<(), String>> + Send;
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

fn phase(value: &str) -> BTreeMap<String, TypedConfigValue> {
    BTreeMap::from([(
        update_action::OUTPUT_PHASE.to_owned(),
        TypedConfigValue::String(value.into()),
    )])
}

/// A stage running in the background.
struct Staging {
    generation: u64,
    sha256: String,
    cancel: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

/// The agent: one per connection to the node.
pub struct Agent<U: Updater> {
    updater: Arc<U>,
    reporter: ActionReporter,
    staging: Mutex<Option<Staging>>,
    generation: Mutex<u64>,
}

impl<U: Updater> Agent<U> {
    pub fn new(updater: Arc<U>, reporter: ActionReporter) -> Arc<Self> {
        Arc::new(Self {
            updater,
            reporter,
            staging: Mutex::new(None),
            generation: Mutex::new(0),
        })
    }

    /// Publishes `status` as the `update.*` keys. Failures (no claim yet after a reconnect, node
    /// restarting) are retried by the next republish.
    async fn publish(&self, status: &UpdaterStatus) {
        let _ = self
            .reporter
            .publish_status(update_status_entries(&self.reporter, status))
            .await;
    }

    /// Publishes the updater's current status.
    pub async fn publish_current(&self) {
        self.publish(&self.updater.status()).await;
    }

    /// Mirrors a final action state into `action.<id>.*`.
    async fn publish_action(&self, action_id: &str, state: &str, error: Option<&str>) {
        let mut entries = vec![self.reporter.node_status_entry(
            action_status_keys::key(action_id, action_status_keys::STATE),
            TypedConfigValue::String(state.into()),
        )];
        if let Some(error) = error {
            entries.push(self.reporter.node_status_entry(
                action_status_keys::key(action_id, action_status_keys::ERROR),
                TypedConfigValue::String(error.into()),
            ));
        }
        let _ = self.reporter.publish_status(entries).await;
    }

    async fn reject(&self, action_id: &str, reason: String) {
        self.publish_action(action_id, "rejected", Some(&reason))
            .await;
        let _ = self.reporter.reject(action_id, reason).await;
    }

    async fn succeed(&self, action_id: &str, phase_value: &str) {
        self.publish_action(action_id, "succeeded", None).await;
        let _ = self.reporter.succeed(action_id, phase(phase_value)).await;
    }

    /// Runs one claimed action and reports its outcome.
    pub async fn handle(self: &Arc<Self>, request: ActionRequest) {
        let id = request.action_id.clone();
        let uint = |key: &str, default: u64| match request.args.get(key) {
            Some(TypedConfigValue::UInt(value)) => *value,
            _ => default,
        };
        match request.name.as_str() {
            action_names::UPDATE => self.start_update(&id, &request.args).await,
            action_names::UPDATE_CANCEL => self.cancel_update(&id).await,
            action_names::UPDATE_ROLLBACK => self.rollback(&id).await,
            action_names::REBOOT => {
                // Report first: the reboot ends this process and the node's action record.
                self.succeed(&id, update_action::PHASE_REBOOTING).await;
                let _ = self.updater.reboot(uint("delay_ms", 0)).await;
            }
            action_names::LOCATE => {
                let enabled = !matches!(
                    request.args.get("enabled"),
                    Some(TypedConfigValue::Bool(false))
                );
                match self
                    .updater
                    .locate(enabled, uint("duration_ms", 10_000))
                    .await
                {
                    Ok(()) => {
                        let _ = self.reporter.succeed(&id, BTreeMap::new()).await;
                    }
                    Err(error) => {
                        let _ = self.reporter.fail(&id, error).await;
                    }
                }
            }
            other => {
                self.reject(&id, format!("unsupported action `{other}`"))
                    .await
            }
        }
    }

    /// `update`: starts the stage in the background and succeeds with `phase = "staging"`.
    async fn start_update(self: &Arc<Self>, id: &str, args: &BTreeMap<String, TypedConfigValue>) {
        let request = match StageRequest::from_args(args) {
            Ok(request) => request,
            Err(reason) => return self.reject(id, reason).await,
        };
        let refused = {
            let mut staging = self.staging.lock().expect("agent staging");
            match staging.as_ref() {
                // A retry of the running update (same image): already started.
                Some(running) if running.sha256 == request.sha256 => false,
                Some(_) => true,
                None => {
                    let generation = {
                        let mut next = self.generation.lock().expect("agent generation");
                        *next += 1;
                        *next
                    };
                    let (cancel, cancelled) = oneshot::channel();
                    let sha256 = request.sha256.clone();
                    let task = tokio::spawn(self.clone().run_stage(generation, request, cancelled));
                    *staging = Some(Staging {
                        generation,
                        sha256,
                        cancel,
                        task,
                    });
                    false
                }
            }
        };
        if refused {
            return self
                .reject(
                    id,
                    "another update is staging; cancel it with `update.cancel` first".into(),
                )
                .await;
        }
        let mut status = self.updater.status();
        status.state = update_action::STATE_STAGING.into();
        status.error = None;
        status.progress = Some(status.progress.unwrap_or(0));
        self.publish(&status).await;
        self.succeed(id, update_action::PHASE_STAGING).await;
    }

    /// The background stage: progress into `update.progress`, then `staged`, `rebooting` and
    /// `apply`, or `error`. Returns early (without touching the state) when cancelled; the
    /// canceller reports that.
    async fn run_stage(
        self: Arc<Self>,
        generation: u64,
        request: StageRequest,
        mut cancelled: oneshot::Receiver<()>,
    ) {
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
        let updater = self.updater.clone();
        let staging = updater.stage(request, progress_tx);
        tokio::pin!(staging);
        let result = loop {
            tokio::select! {
                // A cancel request, or the agent went away (the sender was dropped).
                _ = &mut cancelled => return,
                result = &mut staging => break result,
                Some(progress) = progress_rx.recv() => {
                    let mut status = self.updater.status();
                    status.state = update_action::STATE_STAGING.into();
                    status.progress = Some(progress.min(1000));
                    self.publish(&status).await;
                }
            }
        };
        {
            let mut staging = self.staging.lock().expect("agent staging");
            if staging
                .as_ref()
                .is_some_and(|running| running.generation == generation)
            {
                *staging = None;
            }
        }
        match result {
            Ok(_version) => {
                let mut status = self.updater.status();
                status.state = update_action::STATE_STAGED.into();
                self.publish(&status).await;
                status.state = update_action::STATE_REBOOTING.into();
                self.publish(&status).await;
                if let Err(error) = self.updater.apply().await {
                    // Still staged (for example a `pre-reboot` hook refused).
                    let mut status = self.updater.status();
                    status.state = update_action::STATE_STAGED.into();
                    status.error = Some(error);
                    self.publish(&status).await;
                }
            }
            Err(error) => {
                let mut status = self.updater.status();
                status.state = update_action::STATE_ERROR.into();
                status.error = Some(error);
                self.publish(&status).await;
            }
        }
    }

    /// `update.cancel`: stops a running stage, or forgets a staged update.
    async fn cancel_update(&self, id: &str) {
        let running = self.staging.lock().expect("agent staging").take();
        let had_stage = running.is_some();
        if let Some(running) = running {
            let _ = running.cancel.send(());
            let _ = running.task.await;
        }
        match self.updater.cancel().await {
            Ok(cancelled) => {
                self.publish_current().await;
                let phase_value = if cancelled || had_stage {
                    update_action::PHASE_CANCELLED
                } else {
                    update_action::PHASE_IDLE
                };
                self.succeed(id, phase_value).await;
            }
            Err(error) => {
                self.publish_action(id, "failed", Some(&error)).await;
                let _ = self.reporter.fail(id, error).await;
            }
        }
    }

    /// `update.rollback`: switch to the previous confirmed slot and reboot.
    async fn rollback(&self, id: &str) {
        let staging = self.staging.lock().expect("agent staging").is_some();
        if staging {
            return self
                .reject(
                    id,
                    "an update is staging; cancel it with `update.cancel` first".into(),
                )
                .await;
        }
        if let Err(reason) = self.updater.prepare_rollback().await {
            return self.reject(id, reason).await;
        }
        let mut status = self.updater.status();
        status.state = update_action::STATE_REBOOTING.into();
        self.publish(&status).await;
        // Report first: the reboot ends this process and the node's action record.
        self.succeed(id, update_action::PHASE_REBOOTING).await;
        let _ = self.updater.reboot(0).await;
    }
}

/// Claims the actions and serves them until the connection fails for good (the service's retry
/// policy gives up). Dropping the returned future stops a running stage too, like the agent
/// process exiting.
pub async fn run<U: Updater>(
    service: &LocalProviderService,
    updater: Arc<U>,
    republish_every: Duration,
) -> Result<(), ClientError> {
    let mut watch = service.claim_node_actions(CLAIMED_ACTIONS).await?;
    let agent = Agent::new(updater, watch.reporter());
    // After every boot: the durable view of the last update.
    watch
        .publish_status(update_status_entries(
            &watch.reporter(),
            &agent.updater.status(),
        ))
        .await?;
    let republisher = {
        let agent = agent.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(republish_every);
            tick.tick().await;
            loop {
                tick.tick().await;
                agent.publish_current().await;
            }
        })
    };
    let _stop = StopOnDrop {
        republisher,
        agent: agent.clone(),
    };
    loop {
        let request = watch.next().await?;
        agent.handle(request).await;
    }
}

struct StopOnDrop<U: Updater> {
    republisher: tokio::task::JoinHandle<()>,
    agent: Arc<Agent<U>>,
}

impl<U: Updater> Drop for StopOnDrop<U> {
    fn drop(&mut self) {
        self.republisher.abort();
        let running = self.agent.staging.lock().expect("agent staging").take();
        if let Some(running) = running {
            running.task.abort();
        }
    }
}

/// An in-memory updater for the example and tests: staging takes `steps` progress steps of
/// `step` each (an `image_url` containing `corrupt` fails the checksum), `apply`, `reboot` and
/// `prepare_rollback` only record that they were called, and staging or locating can be held
/// with [`FakeUpdater::hold_staging`] / [`FakeUpdater::hold_locate`].
pub struct FakeUpdater {
    status: Mutex<UpdaterStatus>,
    steps: u16,
    step: Duration,
    hold_stage: Mutex<Option<Arc<Notify>>>,
    hold_locate: Mutex<Option<Arc<Notify>>>,
    calls: Mutex<Vec<String>>,
}

impl FakeUpdater {
    pub fn new(status: UpdaterStatus, steps: u16, step: Duration) -> Self {
        Self {
            status: Mutex::new(status),
            steps: steps.max(1),
            step,
            hold_stage: Mutex::new(None),
            hold_locate: Mutex::new(None),
            calls: Mutex::new(Vec::new()),
        }
    }

    /// Makes later stages wait after their first progress step until the notify fires.
    pub fn hold_staging(&self) -> Arc<Notify> {
        let notify = Arc::new(Notify::new());
        *self.hold_stage.lock().expect("fake updater") = Some(notify.clone());
        notify
    }

    /// Makes later `locate` calls wait until the notify fires.
    pub fn hold_locate(&self) -> Arc<Notify> {
        let notify = Arc::new(Notify::new());
        *self.hold_locate.lock().expect("fake updater") = Some(notify.clone());
        notify
    }

    /// `stage <url>`, `apply`, `cancel`, `rollback`, `reboot <delay>`, `locate <enabled>
    /// <duration>`, in call order.
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
        self.update(|status| {
            status.state = update_action::STATE_STAGING.into();
            status.error = None;
            status.progress = Some(0);
        });
        if request.image_url.contains("corrupt") {
            self.update(|status| {
                status.state = update_action::STATE_ERROR.into();
                status.error = Some("sha256 mismatch".into());
            });
            return Err("sha256 mismatch".into());
        }
        let hold = self.hold_stage.lock().expect("fake updater").clone();
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
            status.state = update_action::STATE_STAGED.into();
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
        // A real updater reboots here; the fake jumps to the trial boot.
        self.update(|status| status.state = update_action::STATE_TRYING.into());
        Ok(())
    }

    async fn cancel(&self) -> Result<bool, String> {
        self.record("cancel".into());
        let mut cancelled = false;
        self.update(|status| {
            if matches!(
                status.state.as_str(),
                update_action::STATE_STAGING | update_action::STATE_STAGED
            ) {
                status.state = update_action::STATE_CANCELLED.into();
                status.slot_staged = None;
                status.version_staged = None;
                status.progress = None;
                cancelled = true;
            }
        });
        Ok(cancelled)
    }

    async fn prepare_rollback(&self) -> Result<(), String> {
        if self.status().state == update_action::STATE_ROLLED_BACK {
            return Err("no previous confirmed slot".into());
        }
        self.record("rollback".into());
        Ok(())
    }

    async fn reboot(&self, delay_ms: u64) -> Result<(), String> {
        self.record(format!("reboot {delay_ms}"));
        Ok(())
    }

    async fn locate(&self, enabled: bool, duration_ms: u64) -> Result<(), String> {
        self.record(format!("locate {enabled} {duration_ms}"));
        let hold = self.hold_locate.lock().expect("fake updater").clone();
        if let Some(hold) = hold {
            hold.notified().await;
        }
        Ok(())
    }
}

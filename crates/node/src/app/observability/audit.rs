use crate::app::NodeError;
use crate::blocking::run_possibly_blocking;
use crate::config::AuditLogOverloadPolicy;
use serde::Serialize;
#[cfg(test)]
use std::time::Duration;
use std::{
    fs::OpenOptions,
    io::Write,
    path::PathBuf,
    sync::Mutex,
    sync::atomic::{AtomicU64, Ordering},
    sync::mpsc::{self, SyncSender, TrySendError},
    thread::JoinHandle,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(test)]
static TEST_AUDIT_APPEND_DELAY_MS: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AuditEventKind {
    TransportSecurityFailure,
    PeerEnrolled,
    PeerRevoked,
    PeerIdentityReplaced,
    PeerTlsPretrusted,
    HttpTlsRotated,
}

#[derive(Debug, Serialize)]
struct AuditEventRecord {
    timestamp_ms: u64,
    node_id: String,
    kind: AuditEventKind,
    subject: Option<String>,
    message: String,
}

#[derive(Debug)]
pub(crate) struct AuditLogSink {
    pub(super) path: PathBuf,
    sender: SyncSender<AuditEventRecord>,
    overload_policy: AuditLogOverloadPolicy,
    dropped_records: AtomicU64,
    join_handle: Mutex<Option<JoinHandle<()>>>,
}

impl AuditLogSink {
    pub(crate) fn dropped_records(&self) -> u64 {
        self.dropped_records.load(Ordering::Relaxed)
    }

    pub(crate) fn new(
        path: PathBuf,
        queue_capacity: usize,
        overload_policy: AuditLogOverloadPolicy,
    ) -> Result<Self, NodeError> {
        let (sender, receiver) = mpsc::sync_channel(queue_capacity);
        let worker_path = path.clone();
        let join_handle = std::thread::Builder::new()
            .name("orion-audit-log".into())
            .spawn(move || {
                while let Ok(record) = receiver.recv() {
                    if let Err(err) = append_audit_record(&worker_path, &record) {
                        tracing::warn!(
                            path = %worker_path.display(),
                            error = %err,
                            "failed to append audit log event"
                        );
                    }
                }
            })
            .map_err(|err| NodeError::StartupSpawn {
                context: "audit log worker",
                message: err.to_string(),
            })?;
        Ok(Self {
            path,
            sender,
            overload_policy,
            dropped_records: AtomicU64::new(0),
            join_handle: Mutex::new(Some(join_handle)),
        })
    }

    fn append(&self, record: &AuditEventRecord) -> Result<(), NodeError> {
        append_audit_record(&self.path, record)
    }

    fn enqueue(&self, record: AuditEventRecord) -> Result<(), NodeError> {
        match self.overload_policy {
            AuditLogOverloadPolicy::Block => {
                match run_possibly_blocking(|| self.sender.send(record)) {
                    Ok(()) => Ok(()),
                    Err(mpsc::SendError(_record)) => Err(NodeError::Storage(format!(
                        "audit log worker disconnected for `{}`",
                        self.path.display()
                    ))),
                }
            }
            AuditLogOverloadPolicy::DropNewest => match self.sender.try_send(record) {
                Ok(()) => Ok(()),
                Err(TrySendError::Full(_record)) => {
                    let dropped_total = self.dropped_records.fetch_add(1, Ordering::Relaxed) + 1;
                    if should_emit_audit_drop_warning(dropped_total) {
                        tracing::warn!(
                            path = %self.path.display(),
                            dropped_total,
                            overload_policy = "drop_newest",
                            "audit log queue full; dropping newest event"
                        );
                    }
                    Ok(())
                }
                Err(TrySendError::Disconnected(_record)) => Err(NodeError::Storage(format!(
                    "audit log worker disconnected for `{}`",
                    self.path.display()
                ))),
            },
        }
    }

    fn append_for_current_runtime(&self, record: AuditEventRecord) -> Result<(), NodeError> {
        if should_offload_audit_write() {
            self.enqueue(record)
        } else {
            self.append(&record)
        }
    }

    #[cfg(test)]
    pub(crate) fn enqueue_test_record(
        &self,
        timestamp_ms: u64,
        node_id: &str,
        kind: AuditEventKind,
        subject: Option<String>,
        message: &str,
    ) -> Result<(), NodeError> {
        self.enqueue(AuditEventRecord {
            timestamp_ms,
            node_id: node_id.to_owned(),
            kind,
            subject,
            message: message.to_owned(),
        })
    }
}

impl Drop for AuditLogSink {
    fn drop(&mut self) {
        let (replacement, _receiver) = mpsc::sync_channel(1);
        let sender = std::mem::replace(&mut self.sender, replacement);
        drop(sender);
        if let Ok(mut join_handle) = self.join_handle.lock()
            && let Some(join_handle) = join_handle.take()
        {
            let _ = join_handle.join();
        }
    }
}

fn append_audit_record(path: &std::path::Path, record: &AuditEventRecord) -> Result<(), NodeError> {
    #[cfg(test)]
    maybe_test_delay_before_audit_append();
    let parent = path
        .parent()
        .ok_or_else(|| NodeError::Storage("audit log path must have a parent directory".into()))?;
    std::fs::create_dir_all(parent).map_err(|err| {
        NodeError::Storage(format!(
            "failed to create audit log directory `{}`: {err}",
            parent.display()
        ))
    })?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|err| {
            NodeError::Storage(format!(
                "failed to open audit log `{}`: {err}",
                path.display()
            ))
        })?;
    let mut line = serde_json::to_vec(record)
        .map_err(|err| NodeError::Storage(format!("failed to encode audit log record: {err}")))?;
    line.push(b'\n');
    file.write_all(&line).map_err(|err| {
        NodeError::Storage(format!(
            "failed to append audit log `{}`: {err}",
            path.display()
        ))
    })
}

#[cfg(test)]
fn maybe_test_delay_before_audit_append() {
    let delay_ms = TEST_AUDIT_APPEND_DELAY_MS.load(Ordering::Relaxed);
    if delay_ms > 0 {
        std::thread::sleep(Duration::from_millis(delay_ms));
    }
}

#[cfg(test)]
pub(crate) fn set_test_audit_append_delay(delay: Duration) {
    TEST_AUDIT_APPEND_DELAY_MS.store(
        delay.as_millis().min(u128::from(u64::MAX)) as u64,
        Ordering::Relaxed,
    );
}

#[cfg(test)]
pub(crate) fn clear_test_audit_append_delay() {
    TEST_AUDIT_APPEND_DELAY_MS.store(0, Ordering::Relaxed);
}

fn should_offload_audit_write() -> bool {
    matches!(
        tokio::runtime::Handle::try_current(),
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
    )
}

pub(crate) fn should_emit_audit_drop_warning(dropped_total: u64) -> bool {
    dropped_total == 1 || dropped_total.is_power_of_two()
}

pub(crate) fn write_audit_record(
    audit_log: &AuditLogSink,
    node_id: &str,
    kind: AuditEventKind,
    subject: Option<String>,
    message: String,
) -> Result<(), NodeError> {
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0);
    let record = AuditEventRecord {
        timestamp_ms,
        node_id: node_id.to_owned(),
        kind,
        subject,
        message,
    };
    audit_log.append_for_current_runtime(record)
}

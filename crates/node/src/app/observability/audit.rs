use crate::app::NodeError;
use crate::blocking::run_possibly_blocking;
use crate::config::AuditLogOverloadPolicy;
use serde::Serialize;
#[cfg(test)]
use std::time::Duration;
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    sync::mpsc::{self, SyncSender, TrySendError},
    sync::{Arc, Mutex, PoisonError},
    thread::JoinHandle,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(test)]
static TEST_AUDIT_APPEND_DELAY_MS: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AuditEventKind {
    #[cfg_attr(not(any(test, feature = "transport-http")), allow(dead_code))]
    TransportSecurityFailure,
    PeerEnrolled,
    PeerRevoked,
    PeerRemoved,
    PeerIdentityReplaced,
    PeerTlsPretrusted,
    #[cfg_attr(not(feature = "transport-http"), allow(dead_code))]
    HttpTlsRotated,
    OperatorEnrolled,
    OperatorRemoved,
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
    writer: Arc<Mutex<AuditFileWriter>>,
    overload_policy: AuditLogOverloadPolicy,
    dropped_records: AtomicU64,
    queued_records: Arc<AtomicU64>,
    join_handle: Mutex<Option<JoinHandle<()>>>,
}

impl AuditLogSink {
    pub(crate) fn dropped_records(&self) -> u64 {
        self.dropped_records.load(Ordering::Relaxed)
    }

    /// Records accepted by the queue that the writer thread has not picked up yet.
    pub(crate) fn queued_records(&self) -> u64 {
        self.queued_records.load(Ordering::Relaxed)
    }

    pub(crate) fn new(
        path: PathBuf,
        queue_capacity: usize,
        overload_policy: AuditLogOverloadPolicy,
        max_bytes: usize,
    ) -> Result<Self, NodeError> {
        let (sender, receiver) = mpsc::sync_channel(queue_capacity);
        let writer = Arc::new(Mutex::new(AuditFileWriter::new(path.clone(), max_bytes)));
        let worker_writer = Arc::clone(&writer);
        let worker_path = path.clone();
        let queued_records = Arc::new(AtomicU64::new(0));
        let worker_queued_records = Arc::clone(&queued_records);
        let join_handle = std::thread::Builder::new()
            .name("orion-audit-log".into())
            .spawn(move || {
                while let Ok(record) = receiver.recv() {
                    saturating_decrement(&worker_queued_records);
                    if let Err(err) = append_audit_record(&worker_writer, &record) {
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
            writer,
            overload_policy,
            dropped_records: AtomicU64::new(0),
            queued_records,
            join_handle: Mutex::new(Some(join_handle)),
        })
    }

    fn append(&self, record: &AuditEventRecord) -> Result<(), NodeError> {
        append_audit_record(&self.writer, record)
    }

    fn enqueue(&self, record: AuditEventRecord) -> Result<(), NodeError> {
        // Count before sending so the writer thread can never observe the record first.
        self.queued_records.fetch_add(1, Ordering::Relaxed);
        let result = self.enqueue_counted(record);
        if !matches!(result, Ok(true)) {
            saturating_decrement(&self.queued_records);
        }
        result.map(|_| ())
    }

    fn enqueue_counted(&self, record: AuditEventRecord) -> Result<bool, NodeError> {
        match self.overload_policy {
            AuditLogOverloadPolicy::Block => {
                match run_possibly_blocking(|| self.sender.send(record)) {
                    Ok(()) => Ok(true),
                    Err(mpsc::SendError(_record)) => Err(NodeError::Storage(format!(
                        "audit log worker disconnected for `{}`",
                        self.path.display()
                    ))),
                }
            }
            AuditLogOverloadPolicy::DropNewest => match self.sender.try_send(record) {
                Ok(()) => Ok(true),
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
                    Ok(false)
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

fn append_audit_record(
    writer: &Mutex<AuditFileWriter>,
    record: &AuditEventRecord,
) -> Result<(), NodeError> {
    #[cfg(test)]
    maybe_test_delay_before_audit_append();
    let mut line = serde_json::to_vec(record)
        .map_err(|err| NodeError::Storage(format!("failed to encode audit log record: {err}")))?;
    line.push(b'\n');
    writer
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .append(&line)
}

/// The audit log file, kept open between records. Before a record would grow the file past
/// `max_bytes`, the file is renamed to `<path>.1` (replacing the previous rotation) and a new file
/// is started, so the log keeps at most about two caps of history. A single record larger than
/// the cap is still written.
#[derive(Debug)]
struct AuditFileWriter {
    path: PathBuf,
    max_bytes: u64,
    file: Option<File>,
    len: u64,
}

impl AuditFileWriter {
    fn new(path: PathBuf, max_bytes: usize) -> Self {
        Self {
            path,
            max_bytes: u64::try_from(max_bytes).unwrap_or(u64::MAX),
            file: None,
            len: 0,
        }
    }

    fn open(&mut self) -> Result<(), NodeError> {
        let parent = self.path.parent().ok_or_else(|| {
            NodeError::Storage("audit log path must have a parent directory".into())
        })?;
        std::fs::create_dir_all(parent).map_err(|err| {
            NodeError::Storage(format!(
                "failed to create audit log directory `{}`: {err}",
                parent.display()
            ))
        })?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|err| {
                NodeError::Storage(format!(
                    "failed to open audit log `{}`: {err}",
                    self.path.display()
                ))
            })?;
        self.len = file.metadata().map_or(0, |metadata| metadata.len());
        self.file = Some(file);
        Ok(())
    }

    fn append(&mut self, line: &[u8]) -> Result<(), NodeError> {
        if self.file.is_none() {
            self.open()?;
        }
        let line_len = u64::try_from(line.len()).unwrap_or(u64::MAX);
        if self.len > 0 && self.len.saturating_add(line_len) > self.max_bytes {
            self.rotate()?;
        }
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| NodeError::Storage("audit log file is not open".into()))?;
        match file.write_all(line) {
            Ok(()) => {
                self.len = self.len.saturating_add(line_len);
                Ok(())
            }
            Err(err) => {
                // Drop the handle so the next record reopens the file.
                self.file = None;
                Err(NodeError::Storage(format!(
                    "failed to append audit log `{}`: {err}",
                    self.path.display()
                )))
            }
        }
    }

    fn rotate(&mut self) -> Result<(), NodeError> {
        // Close the handle before renaming it.
        self.file = None;
        match std::fs::rename(&self.path, rotated_audit_path(&self.path)) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(NodeError::Storage(format!(
                    "failed to rotate audit log `{}`: {err}",
                    self.path.display()
                )));
            }
        }
        self.open()
    }
}

/// `<path>.1`: the single rotated audit log file.
fn rotated_audit_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".1");
    path.with_file_name(name)
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

// `fetch_update` is deprecated in favour of `try_update` (Rust 1.99), which is newer than the
// workspace MSRV (1.94).
#[allow(deprecated)]
fn saturating_decrement(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_sub(1))
    });
}

#[cfg(test)]
mod tests {
    use super::{AuditFileWriter, rotated_audit_path};
    use std::{
        fs,
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    };

    fn temp_audit_dir(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("orion-audit-{name}-{unique}"))
    }

    fn read(path: &Path) -> String {
        fs::read_to_string(path).unwrap_or_default()
    }

    #[test]
    fn audit_writer_rotates_once_the_cap_would_be_exceeded() {
        let dir = temp_audit_dir("rotate");
        let path = dir.join("audit.log");
        let mut writer = AuditFileWriter::new(path.clone(), 40);

        for line in [b"aaaaaaaaaa\n", b"bbbbbbbbbb\n", b"cccccccccc\n"] {
            writer.append(line).expect("append should succeed");
        }
        assert_eq!(read(&path), "aaaaaaaaaa\nbbbbbbbbbb\ncccccccccc\n");
        assert!(!rotated_audit_path(&path).exists());

        writer
            .append(b"dddddddddd\n")
            .expect("append should rotate");
        writer
            .append(b"eeeeeeeeee\n")
            .expect("append should succeed");
        assert_eq!(read(&path), "dddddddddd\neeeeeeeeee\n");
        assert_eq!(
            read(&rotated_audit_path(&path)),
            "aaaaaaaaaa\nbbbbbbbbbb\ncccccccccc\n"
        );

        // Only one rotated file is kept: the next rotation replaces `.1`.
        writer
            .append(b"ffffffffff\n")
            .expect("append should succeed");
        writer
            .append(b"gggggggggg\n")
            .expect("append should rotate again");
        assert_eq!(read(&path), "gggggggggg\n");
        assert_eq!(
            read(&rotated_audit_path(&path)),
            "dddddddddd\neeeeeeeeee\nffffffffff\n"
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn audit_writer_keeps_a_record_larger_than_the_cap() {
        let dir = temp_audit_dir("oversized");
        let path = dir.join("audit.log");
        let mut writer = AuditFileWriter::new(path.clone(), 10);

        writer
            .append(b"0123456789abcdefghij\n")
            .expect("oversized record should still be written");
        assert_eq!(read(&path), "0123456789abcdefghij\n");
        assert!(!rotated_audit_path(&path).exists());

        writer.append(b"next\n").expect("append should rotate");
        assert_eq!(read(&path), "next\n");
        assert_eq!(read(&rotated_audit_path(&path)), "0123456789abcdefghij\n");

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn audit_writer_counts_existing_file_size_on_reopen() {
        let dir = temp_audit_dir("reopen");
        let path = dir.join("audit.log");
        let mut first = AuditFileWriter::new(path.clone(), 15);
        first
            .append(b"0123456789\n")
            .expect("append should succeed");
        drop(first);

        // A restarted node starts from the size already on disk, not from zero.
        let mut second = AuditFileWriter::new(path.clone(), 15);
        second
            .append(b"abcdefghij\n")
            .expect("append should rotate");
        assert_eq!(read(&path), "abcdefghij\n");
        assert_eq!(read(&rotated_audit_path(&path)), "0123456789\n");

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn rotated_audit_path_appends_one_suffix() {
        assert_eq!(
            rotated_audit_path(Path::new("/var/lib/orion/audit.log")),
            PathBuf::from("/var/lib/orion/audit.log.1")
        );
    }
}

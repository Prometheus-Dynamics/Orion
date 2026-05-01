use super::EncodedPersistedStateBundle;
use crate::blocking::WorkerThread;
use crate::storage::NodeStorage;
use orion::control_plane::ArtifactRecord;
use std::io::Read;
use tokio::sync::{mpsc, oneshot};

#[cfg(test)]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicU64 as SharedAtomicU64;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use super::persist_bundle_to_storage;
use crate::NodeError;

#[cfg(test)]
static TEST_PERSIST_DELAY_MS: AtomicU64 = AtomicU64::new(0);

enum PersistenceCommand {
    Persist {
        bundle: Box<EncodedPersistedStateBundle>,
        reply: oneshot::Sender<Result<(), NodeError>>,
    },
    PutArtifactStream {
        artifact: ArtifactRecord,
        content: Box<dyn Read + Send + 'static>,
        reply: oneshot::Sender<Result<(), NodeError>>,
    },
}

struct CoalescedPersistCommand {
    bundle: Box<EncodedPersistedStateBundle>,
    replies: Vec<oneshot::Sender<Result<(), NodeError>>>,
    pending_command: Option<PersistenceCommand>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PersistenceWorkerMetricsSnapshot {
    pub(crate) operation_count: u64,
    pub(crate) queue_wait_count: u64,
    pub(crate) queue_wait_ms_total: u64,
    pub(crate) queue_wait_ms_max: u64,
    pub(crate) reply_wait_count: u64,
    pub(crate) reply_wait_ms_total: u64,
    pub(crate) reply_wait_ms_max: u64,
    pub(crate) operation_ms_total: u64,
    pub(crate) operation_ms_max: u64,
    pub(crate) enqueue_count: u64,
    pub(crate) enqueue_wait_count: u64,
    pub(crate) enqueue_wait_ms_total: u64,
    pub(crate) enqueue_wait_ms_max: u64,
}

pub(crate) struct PersistenceWorker {
    thread: WorkerThread<PersistenceCommand>,
    operation_count: SharedAtomicU64,
    queue_wait_count: SharedAtomicU64,
    queue_wait_ms_total: SharedAtomicU64,
    queue_wait_ms_max: SharedAtomicU64,
    reply_wait_count: SharedAtomicU64,
    reply_wait_ms_total: SharedAtomicU64,
    reply_wait_ms_max: SharedAtomicU64,
    operation_ms_total: SharedAtomicU64,
    operation_ms_max: SharedAtomicU64,
}

impl PersistenceWorker {
    pub(crate) fn new(storage: NodeStorage, queue_capacity: usize) -> Result<Self, NodeError> {
        Ok(Self {
            thread: WorkerThread::spawn(
                "orion-persistence",
                queue_capacity,
                "persistence worker",
                move |receiver| run_persistence_worker(storage, receiver),
            )?,
            operation_count: SharedAtomicU64::new(0),
            queue_wait_count: SharedAtomicU64::new(0),
            queue_wait_ms_total: SharedAtomicU64::new(0),
            queue_wait_ms_max: SharedAtomicU64::new(0),
            reply_wait_count: SharedAtomicU64::new(0),
            reply_wait_ms_total: SharedAtomicU64::new(0),
            reply_wait_ms_max: SharedAtomicU64::new(0),
            operation_ms_total: SharedAtomicU64::new(0),
            operation_ms_max: SharedAtomicU64::new(0),
        })
    }

    fn record_request_metrics(
        &self,
        queue_wait: Duration,
        reply_wait: Option<Duration>,
        operation_duration: Duration,
    ) {
        self.operation_count.fetch_add(1, Ordering::Relaxed);
        record_nonzero_duration(
            queue_wait,
            &self.queue_wait_count,
            &self.queue_wait_ms_total,
            &self.queue_wait_ms_max,
        );
        if let Some(reply_wait) = reply_wait {
            record_nonzero_duration(
                reply_wait,
                &self.reply_wait_count,
                &self.reply_wait_ms_total,
                &self.reply_wait_ms_max,
            );
        }
        let operation_ms = duration_ms(operation_duration);
        self.operation_ms_total
            .fetch_add(operation_ms, Ordering::Relaxed);
        update_max(&self.operation_ms_max, operation_ms);
    }

    pub(crate) fn metrics_snapshot(&self) -> PersistenceWorkerMetricsSnapshot {
        let operation_count = self.operation_count.load(Ordering::Relaxed);
        let queue_wait_count = self.queue_wait_count.load(Ordering::Relaxed);
        let queue_wait_ms_total = self.queue_wait_ms_total.load(Ordering::Relaxed);
        let queue_wait_ms_max = self.queue_wait_ms_max.load(Ordering::Relaxed);
        PersistenceWorkerMetricsSnapshot {
            operation_count,
            queue_wait_count,
            queue_wait_ms_total,
            queue_wait_ms_max,
            reply_wait_count: self.reply_wait_count.load(Ordering::Relaxed),
            reply_wait_ms_total: self.reply_wait_ms_total.load(Ordering::Relaxed),
            reply_wait_ms_max: self.reply_wait_ms_max.load(Ordering::Relaxed),
            operation_ms_total: self.operation_ms_total.load(Ordering::Relaxed),
            operation_ms_max: self.operation_ms_max.load(Ordering::Relaxed),
            enqueue_count: operation_count,
            enqueue_wait_count: queue_wait_count,
            enqueue_wait_ms_total: queue_wait_ms_total,
            enqueue_wait_ms_max: queue_wait_ms_max,
        }
    }

    async fn request_async(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<(), NodeError>>) -> PersistenceCommand,
    ) -> Result<(), NodeError> {
        let operation_started = Instant::now();
        let (reply_tx, reply_rx) = oneshot::channel();
        let send_started = Instant::now();
        let send_result = self.thread.sender().send(build(reply_tx)).await;
        let queue_wait = send_started.elapsed();
        let Ok(()) = send_result else {
            self.record_request_metrics(queue_wait, None, operation_started.elapsed());
            return Err(NodeError::PersistenceWorkerUnavailable);
        };
        let reply_started = Instant::now();
        let result = reply_rx
            .await
            .unwrap_or(Err(NodeError::PersistenceWorkerTerminated));
        self.record_request_metrics(
            queue_wait,
            Some(reply_started.elapsed()),
            operation_started.elapsed(),
        );
        result
    }

    fn request_blocking(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<(), NodeError>>) -> PersistenceCommand + Send,
    ) -> Result<(), NodeError> {
        let (result, queue_wait, reply_wait, operation_duration) =
            match tokio::runtime::Handle::try_current() {
                Ok(_) => {
                    perform_blocking_request_on_helper_thread(self.thread.sender().clone(), build)
                }
                Err(_) => perform_blocking_request(self.thread.sender(), build),
            };
        self.record_request_metrics(queue_wait, reply_wait, operation_duration);
        result
    }

    pub(super) async fn persist_bundle_async(
        &self,
        bundle: EncodedPersistedStateBundle,
    ) -> Result<(), NodeError> {
        self.request_async(|reply| PersistenceCommand::Persist {
            bundle: Box::new(bundle),
            reply,
        })
        .await
    }

    pub(super) fn persist_bundle_blocking(
        &self,
        bundle: EncodedPersistedStateBundle,
    ) -> Result<(), NodeError> {
        self.request_blocking(|reply| PersistenceCommand::Persist {
            bundle: Box::new(bundle),
            reply,
        })
    }

    pub(super) async fn put_artifact_stream_async(
        &self,
        artifact: ArtifactRecord,
        content: Box<dyn Read + Send + 'static>,
    ) -> Result<(), NodeError> {
        self.request_async(|reply| PersistenceCommand::PutArtifactStream {
            artifact,
            content,
            reply,
        })
        .await
    }

    pub(super) fn put_artifact_stream_blocking(
        &self,
        artifact: ArtifactRecord,
        content: Box<dyn Read + Send + 'static>,
    ) -> Result<(), NodeError> {
        self.request_blocking(|reply| PersistenceCommand::PutArtifactStream {
            artifact,
            content,
            reply,
        })
    }
}

fn perform_blocking_request(
    sender: &mpsc::Sender<PersistenceCommand>,
    build: impl FnOnce(oneshot::Sender<Result<(), NodeError>>) -> PersistenceCommand,
) -> (Result<(), NodeError>, Duration, Option<Duration>, Duration) {
    let operation_started = Instant::now();
    let (reply_tx, reply_rx) = oneshot::channel();
    let send_started = Instant::now();
    let send_result = sender.blocking_send(build(reply_tx));
    let queue_wait = send_started.elapsed();
    if send_result.is_err() {
        return (
            Err(NodeError::PersistenceWorkerUnavailable),
            queue_wait,
            None,
            operation_started.elapsed(),
        );
    }
    let reply_started = Instant::now();
    let result = reply_rx
        .blocking_recv()
        .unwrap_or(Err(NodeError::PersistenceWorkerTerminated));
    (
        result,
        queue_wait,
        Some(reply_started.elapsed()),
        operation_started.elapsed(),
    )
}

fn perform_blocking_request_on_helper_thread(
    sender: mpsc::Sender<PersistenceCommand>,
    build: impl FnOnce(oneshot::Sender<Result<(), NodeError>>) -> PersistenceCommand + Send,
) -> (Result<(), NodeError>, Duration, Option<Duration>, Duration) {
    std::thread::scope(|scope| {
        scope
            .spawn(move || perform_blocking_request(&sender, build))
            .join()
            .unwrap_or((
                Err(NodeError::Storage(
                    "persistence worker request helper thread panicked".into(),
                )),
                Duration::default(),
                None,
                Duration::default(),
            ))
    })
}

fn record_nonzero_duration(
    duration: Duration,
    count: &SharedAtomicU64,
    total: &SharedAtomicU64,
    max: &SharedAtomicU64,
) {
    let duration_ms = duration_ms(duration);
    if duration_ms == 0 {
        return;
    }
    count.fetch_add(1, Ordering::Relaxed);
    total.fetch_add(duration_ms, Ordering::Relaxed);
    update_max(max, duration_ms);
}

fn update_max(max: &SharedAtomicU64, value: u64) {
    let _ = max.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.max(value))
    });
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn run_persistence_worker(storage: NodeStorage, mut receiver: mpsc::Receiver<PersistenceCommand>) {
    let mut pending_command = None;
    while let Some(command) = pending_command.take().or_else(|| receiver.blocking_recv()) {
        match command {
            PersistenceCommand::Persist { bundle, reply } => {
                let coalesced = coalesce_queued_persist_commands(bundle, reply, &mut receiver);
                pending_command = coalesced.pending_command;
                send_persist_result(
                    coalesced.replies,
                    persist_bundle_to_storage(&storage, &coalesced.bundle),
                );
            }
            PersistenceCommand::PutArtifactStream {
                artifact,
                content,
                reply,
            } => {
                let _ = reply.send(storage.put_artifact_stream(&artifact, content));
            }
        }
    }
}

fn coalesce_queued_persist_commands(
    mut bundle: Box<EncodedPersistedStateBundle>,
    reply: oneshot::Sender<Result<(), NodeError>>,
    receiver: &mut mpsc::Receiver<PersistenceCommand>,
) -> CoalescedPersistCommand {
    let mut replies = vec![reply];
    let mut pending_command = None;
    while let Ok(command) = receiver.try_recv() {
        match command {
            PersistenceCommand::Persist {
                bundle: next_bundle,
                reply,
            } => {
                bundle = next_bundle;
                replies.push(reply);
            }
            command => {
                pending_command = Some(command);
                break;
            }
        }
    }
    CoalescedPersistCommand {
        bundle,
        replies,
        pending_command,
    }
}

fn send_persist_result(
    replies: Vec<oneshot::Sender<Result<(), NodeError>>>,
    result: Result<(), NodeError>,
) {
    match result {
        Ok(()) => {
            for reply in replies {
                let _ = reply.send(Ok(()));
            }
        }
        Err(err) => {
            let message = err.to_string();
            let mut replies = replies.into_iter();
            if let Some(reply) = replies.next() {
                let _ = reply.send(Err(err));
            }
            for reply in replies {
                let _ = reply.send(Err(NodeError::Storage(message.clone())));
            }
        }
    }
}

#[cfg(test)]
fn maybe_test_delay_before_persist() {
    let delay_ms = TEST_PERSIST_DELAY_MS.load(Ordering::Relaxed);
    if delay_ms > 0 {
        std::thread::sleep(std::time::Duration::from_millis(delay_ms));
    }
}

#[cfg(test)]
pub(crate) fn set_test_persist_delay(delay: std::time::Duration) {
    TEST_PERSIST_DELAY_MS.store(
        delay.as_millis().min(u128::from(u64::MAX)) as u64,
        Ordering::Relaxed,
    );
}

#[cfg(test)]
pub(crate) fn clear_test_persist_delay() {
    TEST_PERSIST_DELAY_MS.store(0, Ordering::Relaxed);
}

pub(super) fn delay_before_persist_for_tests() {
    #[cfg(test)]
    maybe_test_delay_before_persist();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{desired_snapshot_revision_only, encode_archive_to_vec};
    use orion::{
        Revision,
        control_plane::{
            AppliedClusterState, DesiredClusterState, MutationBatch, ObservedClusterState,
        },
    };
    use std::io::Cursor;

    #[test]
    fn coalesce_queued_persist_commands_keeps_latest_bundle_before_next_artifact() {
        let (sender, mut receiver) = mpsc::channel(8);
        let (first_reply, _first_rx) = oneshot::channel();
        let (second_reply, _second_rx) = oneshot::channel();
        let (third_reply, _third_rx) = oneshot::channel();
        let (artifact_reply, _artifact_rx) = oneshot::channel();
        sender
            .try_send(PersistenceCommand::Persist {
                bundle: Box::new(test_bundle(Revision::new(2))),
                reply: second_reply,
            })
            .expect("queued persist should send");
        sender
            .try_send(PersistenceCommand::Persist {
                bundle: Box::new(test_bundle(Revision::new(3))),
                reply: third_reply,
            })
            .expect("queued persist should send");
        sender
            .try_send(PersistenceCommand::PutArtifactStream {
                artifact: ArtifactRecord::builder("artifact.coalesce").build(),
                content: Box::new(Cursor::new(Vec::<u8>::new())),
                reply: artifact_reply,
            })
            .expect("queued artifact should send");

        let coalesced = coalesce_queued_persist_commands(
            Box::new(test_bundle(Revision::new(1))),
            first_reply,
            &mut receiver,
        );

        assert_eq!(coalesced.bundle.observed_revision, Revision::new(3));
        assert_eq!(coalesced.replies.len(), 3);
        assert!(matches!(
            coalesced.pending_command,
            Some(PersistenceCommand::PutArtifactStream { .. })
        ));
    }

    fn test_bundle(revision: Revision) -> EncodedPersistedStateBundle {
        let desired = DesiredClusterState {
            revision,
            ..Default::default()
        };
        let observed = ObservedClusterState {
            revision,
            ..Default::default()
        };
        let applied = AppliedClusterState { revision };
        let history = Vec::<MutationBatch>::new();
        EncodedPersistedStateBundle {
            desired: desired_snapshot_revision_only(&desired),
            observed_revision: observed.revision,
            observed_bytes: Some(encode_archive_to_vec(&observed).expect("observed should encode")),
            applied_revision: applied.revision,
            applied_bytes: Some(encode_archive_to_vec(&applied).expect("applied should encode")),
            history_bytes: Some(encode_archive_to_vec(&history).expect("history should encode")),
            baseline_revision: Revision::ZERO,
            baseline_bytes: None,
            snapshot_rewrite_cadence: 1,
        }
    }
}

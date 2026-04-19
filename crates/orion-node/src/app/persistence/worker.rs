use super::EncodedPersistedStateBundle;
use crate::blocking::{
    WorkerThread, request_worker_operation_async, request_worker_operation_blocking,
};
use crate::storage::NodeStorage;
use orion::control_plane::ArtifactRecord;
use tokio::sync::{mpsc, oneshot};

#[cfg(test)]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicU64 as SharedAtomicU64;
use std::sync::atomic::Ordering;

use super::persist_bundle_to_storage;
use crate::NodeError;

#[cfg(test)]
static TEST_PERSIST_DELAY_MS: AtomicU64 = AtomicU64::new(0);

enum PersistenceCommand {
    Persist {
        bundle: Box<EncodedPersistedStateBundle>,
        reply: oneshot::Sender<Result<(), NodeError>>,
    },
    PutArtifact {
        artifact: ArtifactRecord,
        content: Vec<u8>,
        reply: oneshot::Sender<Result<(), NodeError>>,
    },
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PersistenceWorkerMetricsSnapshot {
    pub(crate) enqueue_count: u64,
    pub(crate) enqueue_wait_count: u64,
    pub(crate) enqueue_wait_ms_total: u64,
    pub(crate) enqueue_wait_ms_max: u64,
}

pub(crate) struct PersistenceWorker {
    thread: WorkerThread<PersistenceCommand>,
    enqueue_count: SharedAtomicU64,
    enqueue_wait_count: SharedAtomicU64,
    enqueue_wait_ms_total: SharedAtomicU64,
    enqueue_wait_ms_max: SharedAtomicU64,
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
            enqueue_count: SharedAtomicU64::new(0),
            enqueue_wait_count: SharedAtomicU64::new(0),
            enqueue_wait_ms_total: SharedAtomicU64::new(0),
            enqueue_wait_ms_max: SharedAtomicU64::new(0),
        })
    }

    fn record_enqueue_wait(&self, duration: std::time::Duration) {
        self.enqueue_count.fetch_add(1, Ordering::Relaxed);
        let wait_ms = duration.as_millis().min(u128::from(u64::MAX)) as u64;
        if wait_ms == 0 {
            return;
        }
        self.enqueue_wait_count.fetch_add(1, Ordering::Relaxed);
        self.enqueue_wait_ms_total
            .fetch_add(wait_ms, Ordering::Relaxed);
        let _ = self.enqueue_wait_ms_max.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.max(wait_ms)),
        );
    }

    pub(crate) fn metrics_snapshot(&self) -> PersistenceWorkerMetricsSnapshot {
        PersistenceWorkerMetricsSnapshot {
            enqueue_count: self.enqueue_count.load(Ordering::Relaxed),
            enqueue_wait_count: self.enqueue_wait_count.load(Ordering::Relaxed),
            enqueue_wait_ms_total: self.enqueue_wait_ms_total.load(Ordering::Relaxed),
            enqueue_wait_ms_max: self.enqueue_wait_ms_max.load(Ordering::Relaxed),
        }
    }

    pub(super) async fn persist_bundle_async(
        &self,
        bundle: EncodedPersistedStateBundle,
    ) -> Result<(), NodeError> {
        let started = std::time::Instant::now();
        let result = request_worker_operation_async(
            self.thread.sender(),
            |reply| PersistenceCommand::Persist {
                bundle: Box::new(bundle),
                reply,
            },
            NodeError::PersistenceWorkerUnavailable,
            NodeError::PersistenceWorkerTerminated,
        )
        .await;
        self.record_enqueue_wait(started.elapsed());
        result
    }

    pub(super) fn persist_bundle_blocking(
        &self,
        bundle: EncodedPersistedStateBundle,
    ) -> Result<(), NodeError> {
        let started = std::time::Instant::now();
        let result = request_worker_operation_blocking(
            self.thread.sender(),
            |reply| PersistenceCommand::Persist {
                bundle: Box::new(bundle),
                reply,
            },
            NodeError::PersistenceWorkerUnavailable,
            NodeError::PersistenceWorkerTerminated,
        );
        self.record_enqueue_wait(started.elapsed());
        result
    }

    pub(super) async fn put_artifact_async(
        &self,
        artifact: ArtifactRecord,
        content: Vec<u8>,
    ) -> Result<(), NodeError> {
        let started = std::time::Instant::now();
        let result = request_worker_operation_async(
            self.thread.sender(),
            |reply| PersistenceCommand::PutArtifact {
                artifact,
                content,
                reply,
            },
            NodeError::PersistenceWorkerUnavailable,
            NodeError::PersistenceWorkerTerminated,
        )
        .await;
        self.record_enqueue_wait(started.elapsed());
        result
    }

    pub(super) fn put_artifact_blocking(
        &self,
        artifact: ArtifactRecord,
        content: Vec<u8>,
    ) -> Result<(), NodeError> {
        let started = std::time::Instant::now();
        let result = request_worker_operation_blocking(
            self.thread.sender(),
            |reply| PersistenceCommand::PutArtifact {
                artifact,
                content,
                reply,
            },
            NodeError::PersistenceWorkerUnavailable,
            NodeError::PersistenceWorkerTerminated,
        );
        self.record_enqueue_wait(started.elapsed());
        result
    }
}

fn run_persistence_worker(storage: NodeStorage, mut receiver: mpsc::Receiver<PersistenceCommand>) {
    while let Some(command) = receiver.blocking_recv() {
        match command {
            PersistenceCommand::Persist { bundle, reply } => {
                let _ = reply.send(persist_bundle_to_storage(&storage, &bundle));
            }
            PersistenceCommand::PutArtifact {
                artifact,
                content,
                reply,
            } => {
                let _ = reply.send(storage.put_artifact(&artifact, &content));
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

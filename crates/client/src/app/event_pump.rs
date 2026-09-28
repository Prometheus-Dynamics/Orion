//! Cancel-safe adapter over client event streams.
//!
//! Frame reads on a Unix control stream (`read_u32_le` followed by `read_exact`) are not
//! cancel-safe: dropping the read future mid-frame discards the bytes already consumed and leaves
//! the stream desynchronised. [`EventPump`] moves each stream into a dedicated reader task that
//! always runs reads to completion and forwards the results over an mpsc channel. Receiving from
//! the channel *is* cancel-safe, so callers can freely race several pumps in `tokio::select!`.

use orion_control_plane::ClientEvent;
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{ClientError, ControlPlaneEventStream, ProviderEventStream};

/// Number of event batches buffered per stream before the reader task applies backpressure.
const EVENT_PUMP_CAPACITY: usize = 16;

pub(crate) trait EventSource: Send + 'static {
    fn next_events(
        &mut self,
    ) -> impl Future<Output = Result<Vec<ClientEvent>, ClientError>> + Send + '_;
}

impl EventSource for ProviderEventStream {
    fn next_events(
        &mut self,
    ) -> impl Future<Output = Result<Vec<ClientEvent>, ClientError>> + Send + '_ {
        ProviderEventStream::next_events(self)
    }
}

impl EventSource for ControlPlaneEventStream {
    fn next_events(
        &mut self,
    ) -> impl Future<Output = Result<Vec<ClientEvent>, ClientError>> + Send + '_ {
        ControlPlaneEventStream::next_events(self)
    }
}

/// Owns a reader task for one event stream. Dropping the pump aborts the task.
pub(crate) struct EventPump {
    receiver: mpsc::Receiver<Result<Vec<ClientEvent>, ClientError>>,
    task: JoinHandle<()>,
}

impl EventPump {
    pub(crate) fn spawn<S: EventSource>(mut source: S) -> Self {
        let (sender, receiver) = mpsc::channel(EVENT_PUMP_CAPACITY);
        let task = tokio::spawn(async move {
            loop {
                let result = source.next_events().await;
                let failed = result.is_err();
                if sender.send(result).await.is_err() || failed {
                    return;
                }
            }
        });
        Self { receiver, task }
    }

    /// Returns the next batch of events. Cancel-safe: if the future is dropped before completing,
    /// no batch is lost. A reader task that ended without reporting an error is surfaced as
    /// [`ClientError::NoMessageAvailable`] so callers reconnect.
    pub(crate) async fn recv(&mut self) -> Result<Vec<ClientEvent>, ClientError> {
        self.receiver
            .recv()
            .await
            .unwrap_or(Err(ClientError::NoMessageAvailable))
    }
}

impl Drop for EventPump {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::Arc, time::Duration};

    /// Yields one scripted result per call, then pends forever while holding `alive`.
    struct ScriptedSource {
        script: Vec<Result<Vec<ClientEvent>, ClientError>>,
        _alive: Arc<()>,
    }

    impl EventSource for ScriptedSource {
        async fn next_events(&mut self) -> Result<Vec<ClientEvent>, ClientError> {
            if self.script.is_empty() {
                std::future::pending::<()>().await;
            }
            self.script.remove(0)
        }
    }

    #[tokio::test]
    async fn dropping_pump_aborts_reader_task_and_releases_source() {
        let alive = Arc::new(());
        let pump = EventPump::spawn(ScriptedSource {
            script: Vec::new(),
            _alive: Arc::clone(&alive),
        });
        tokio::task::yield_now().await;
        assert_eq!(Arc::strong_count(&alive), 2);
        drop(pump);
        tokio::time::timeout(Duration::from_secs(1), async {
            while Arc::strong_count(&alive) > 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reader task should be aborted and drop its source");
    }

    #[tokio::test]
    async fn pump_forwards_batches_then_error_then_reports_closed() {
        let mut pump = EventPump::spawn(ScriptedSource {
            script: vec![Ok(Vec::new()), Err(ClientError::NoMessageAvailable)],
            _alive: Arc::new(()),
        });
        assert!(pump.recv().await.expect("first batch").is_empty());
        assert!(matches!(
            pump.recv().await,
            Err(ClientError::NoMessageAvailable)
        ));
        // Reader task stops after an error; further receives report a closed stream.
        assert!(matches!(
            pump.recv().await,
            Err(ClientError::NoMessageAvailable)
        ));
    }
}

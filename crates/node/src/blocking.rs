use crate::NodeError;
use std::{sync::Mutex, thread::JoinHandle};
use tokio::sync::{mpsc, oneshot};

pub(crate) fn run_possibly_blocking<F, T>(work: F) -> T
where
    F: FnOnce() -> T,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(work)
        }
        _ => work(),
    }
}

pub(crate) async fn request_worker_operation_async<Command>(
    sender: &mpsc::Sender<Command>,
    build: impl FnOnce(oneshot::Sender<Result<(), NodeError>>) -> Command,
    unavailable_error: NodeError,
    terminated_error: NodeError,
) -> Result<(), NodeError> {
    let (reply_tx, reply_rx) = oneshot::channel();
    sender
        .send(build(reply_tx))
        .await
        .map_err(|_| unavailable_error)?;
    reply_rx.await.unwrap_or(Err(terminated_error))
}

pub(crate) fn request_worker_operation_blocking<Command>(
    sender: &mpsc::Sender<Command>,
    build: impl FnOnce(oneshot::Sender<Result<(), NodeError>>) -> Command + Send,
    unavailable_error: NodeError,
    terminated_error: NodeError,
) -> Result<(), NodeError>
where
    Command: Send + 'static,
{
    fn perform_request<Command>(
        sender: &mpsc::Sender<Command>,
        build: impl FnOnce(oneshot::Sender<Result<(), NodeError>>) -> Command,
        unavailable_error: NodeError,
        terminated_error: NodeError,
    ) -> Result<(), NodeError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        sender
            .blocking_send(build(reply_tx))
            .map_err(|_| unavailable_error)?;
        reply_rx.blocking_recv().unwrap_or(Err(terminated_error))
    }

    fn perform_request_on_helper_thread<Command>(
        sender: mpsc::Sender<Command>,
        build: impl FnOnce(oneshot::Sender<Result<(), NodeError>>) -> Command + Send,
        unavailable_error: NodeError,
        terminated_error: NodeError,
    ) -> Result<(), NodeError>
    where
        Command: Send + 'static,
    {
        std::thread::scope(|scope| {
            scope
                .spawn(move || perform_request(&sender, build, unavailable_error, terminated_error))
                .join()
                .unwrap_or(Err(NodeError::Storage(
                    "worker request helper thread panicked".into(),
                )))
        })
    }

    match tokio::runtime::Handle::try_current() {
        Ok(_) => perform_request_on_helper_thread(
            sender.clone(),
            build,
            unavailable_error,
            terminated_error,
        ),
        Err(_) => perform_request(sender, build, unavailable_error, terminated_error),
    }
}

pub(crate) struct WorkerThread<Command> {
    sender: mpsc::Sender<Command>,
    join_handle: Mutex<Option<JoinHandle<()>>>,
}

impl<Command: Send + 'static> WorkerThread<Command> {
    pub(crate) fn spawn(
        thread_name: &str,
        queue_capacity: usize,
        startup_context: &'static str,
        run: impl FnOnce(mpsc::Receiver<Command>) + Send + 'static,
    ) -> Result<Self, NodeError> {
        let (sender, receiver) = mpsc::channel(queue_capacity);
        let join_handle = std::thread::Builder::new()
            .name(thread_name.into())
            .spawn(move || run(receiver))
            .map_err(|err| NodeError::StartupSpawn {
                context: startup_context,
                message: err.to_string(),
            })?;
        Ok(Self {
            sender,
            join_handle: Mutex::new(Some(join_handle)),
        })
    }

    pub(crate) fn sender(&self) -> &mpsc::Sender<Command> {
        &self.sender
    }
}

impl<Command> Drop for WorkerThread<Command> {
    fn drop(&mut self) {
        let (replacement, _receiver) = mpsc::channel(1);
        let sender = std::mem::replace(&mut self.sender, replacement);
        drop(sender);
        if let Ok(mut join_handle) = self.join_handle.lock()
            && let Some(join_handle) = join_handle.take()
        {
            let _ = join_handle.join();
        }
    }
}

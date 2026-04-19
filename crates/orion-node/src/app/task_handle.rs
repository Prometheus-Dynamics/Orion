use super::NodeError;
use orion::transport::{http::HttpTransportError, ipc::IpcTransportError};
use tokio::sync::oneshot;

pub struct GracefulTaskHandle<E> {
    pub(crate) shutdown: Option<oneshot::Sender<()>>,
    pub(crate) handle: tokio::task::JoinHandle<Result<(), E>>,
}

impl<E> GracefulTaskHandle<E> {
    pub(crate) fn new(
        shutdown: oneshot::Sender<()>,
        handle: tokio::task::JoinHandle<Result<(), E>>,
    ) -> Self {
        Self {
            shutdown: Some(shutdown),
            handle,
        }
    }

    pub async fn shutdown(mut self) -> Result<(), E>
    where
        E: FromShutdownJoinError,
    {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        match self.handle.await {
            Ok(result) => result,
            Err(err) => Err(E::from_shutdown_join_error(err)),
        }
    }
}

pub trait FromShutdownJoinError {
    fn from_shutdown_join_error(err: tokio::task::JoinError) -> Self;
}

impl FromShutdownJoinError for HttpTransportError {
    fn from_shutdown_join_error(err: tokio::task::JoinError) -> Self {
        HttpTransportError::ServeFailed(format!("graceful shutdown join failed: {err}"))
    }
}

impl FromShutdownJoinError for IpcTransportError {
    fn from_shutdown_join_error(err: tokio::task::JoinError) -> Self {
        IpcTransportError::WriteFailed(format!("graceful shutdown join failed: {err}"))
    }
}

impl FromShutdownJoinError for NodeError {
    fn from_shutdown_join_error(err: tokio::task::JoinError) -> Self {
        NodeError::Storage(format!("graceful shutdown join failed: {err}"))
    }
}

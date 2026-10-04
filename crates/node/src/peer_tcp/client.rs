//! Client side of the `orion+tcp` transport: one cached connection per peer, sequential
//! request/response exchanges, one transparent reconnect when the cached connection went away.

use super::PeerTcpError;
use orion_transport_ipc::{ControlFrameReadState, write_control_payload_frame};
use std::{future::Future, time::Duration};
use tokio::{net::TcpStream, sync::Mutex};

struct Connection {
    stream: TcpStream,
    read_state: ControlFrameReadState,
}

/// Bytes moved by one exchange.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ExchangeBytes {
    pub(crate) sent: u64,
    pub(crate) received: u64,
}

pub(crate) struct PeerTcpClient {
    authority: String,
    io_timeout: Duration,
    max_payload_bytes: usize,
    connection: Mutex<Option<Connection>>,
}

impl PeerTcpClient {
    pub(crate) fn new(authority: &str, io_timeout: Duration, max_payload_bytes: usize) -> Self {
        Self {
            authority: authority.to_owned(),
            io_timeout,
            max_payload_bytes,
            connection: Mutex::new(None),
        }
    }

    pub(crate) fn authority(&self) -> &str {
        &self.authority
    }

    /// Sends one request payload and returns the response payload.
    ///
    /// Peer requests are idempotent (signed with a fresh nonce, merged with the last-writer-wins
    /// rule), so a request that failed on a reused connection is retried once on a new one.
    pub(crate) async fn exchange(
        &self,
        request: &[u8],
    ) -> Result<(Vec<u8>, ExchangeBytes), PeerTcpError> {
        let mut connection = self.connection.lock().await;
        let reused = connection.is_some();
        match self.exchange_on(&mut connection, request).await {
            Ok(response) => Ok(response),
            Err(err) => {
                *connection = None;
                if reused && err.is_retryable_connection_error() {
                    let retried = self.exchange_on(&mut connection, request).await;
                    if retried.is_err() {
                        *connection = None;
                    }
                    retried
                } else {
                    Err(err)
                }
            }
        }
    }

    async fn exchange_on(
        &self,
        connection: &mut Option<Connection>,
        request: &[u8],
    ) -> Result<(Vec<u8>, ExchangeBytes), PeerTcpError> {
        if connection.is_none() {
            *connection = Some(self.connect().await?);
        }
        let Some(connection) = connection.as_mut() else {
            return Err(PeerTcpError::Closed {
                addr: self.authority.clone(),
            });
        };
        let sent = self
            .timed(
                "write",
                write_control_payload_frame(
                    &mut connection.stream,
                    request,
                    self.max_payload_bytes,
                ),
            )
            .await??;
        let response = self
            .timed(
                "read",
                connection
                    .read_state
                    .read_payload(&mut connection.stream, self.max_payload_bytes),
            )
            .await??
            .ok_or_else(|| PeerTcpError::Closed {
                addr: self.authority.clone(),
            })?;
        let bytes = ExchangeBytes {
            sent: sent as u64,
            received: response.len() as u64,
        };
        Ok((response, bytes))
    }

    async fn connect(&self) -> Result<Connection, PeerTcpError> {
        let stream = self
            .timed("connect", TcpStream::connect(self.authority.as_str()))
            .await?
            .map_err(|err| PeerTcpError::Connect {
                addr: self.authority.clone(),
                message: err.to_string(),
            })?;
        let _ = stream.set_nodelay(true);
        Ok(Connection {
            stream,
            read_state: ControlFrameReadState::new(),
        })
    }

    async fn timed<T>(
        &self,
        operation: &'static str,
        future: impl Future<Output = T>,
    ) -> Result<T, PeerTcpError> {
        tokio::time::timeout(self.io_timeout, future)
            .await
            .map_err(|_| PeerTcpError::Timeout {
                addr: self.authority.clone(),
                operation,
                timeout_ms: self.io_timeout.as_millis().min(u128::from(u64::MAX)) as u64,
            })
    }
}

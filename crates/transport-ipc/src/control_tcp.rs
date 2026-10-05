//! Client side of the `orion+tcp` control transport: one cached TCP connection, sequential
//! request/response exchanges of control frames, one transparent reconnect when the cached
//! connection went away.
//!
//! The frames are the local IPC stream frames (`[b"OC"][version u16 LE][len u32 LE][payload]`),
//! so a protocol skew is reported as [`IpcTransportError::ProtocolMismatch`] before any payload
//! is decoded. The payloads (signed requests, signed responses) are opaque here; their layout is
//! `orion_auth::peer_tcp`. Used by `orion-node` for peer sync and by the remote operator client
//! in `orion-client`.

use crate::{ControlFrameReadState, IpcTransportError, write_control_payload_frame};
use std::{future::Future, time::Duration};
use thiserror::Error;
use tokio::{net::TcpStream, sync::Mutex};

/// Errors of one `orion+tcp` exchange.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ControlTcpError {
    #[error("failed to connect to {addr}: {message}")]
    Connect { addr: String, message: String },
    #[error("orion+tcp {operation} with {addr} timed out after {timeout_ms}ms")]
    Timeout {
        addr: String,
        operation: &'static str,
        timeout_ms: u64,
    },
    #[error("orion+tcp connection to {addr} failed: {message}")]
    Connection { addr: String, message: String },
    #[error("orion+tcp connection to {addr} was closed before a response arrived")]
    Closed { addr: String },
    #[error(transparent)]
    Frame(#[from] IpcTransportError),
}

impl ControlTcpError {
    /// Errors that a fresh connection may fix (the cached connection was closed by the remote).
    pub fn is_retryable_connection_error(&self) -> bool {
        matches!(
            self,
            Self::Connection { .. }
                | Self::Closed { .. }
                | Self::Frame(IpcTransportError::ReadFailed(_) | IpcTransportError::WriteFailed(_))
        )
    }
}

/// Bytes moved by one exchange.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ControlTcpExchangeBytes {
    pub sent: u64,
    pub received: u64,
}

struct Connection {
    stream: TcpStream,
    read_state: ControlFrameReadState,
}

/// A client for one `orion+tcp` endpoint (`host:port`).
pub struct ControlTcpClient {
    authority: String,
    io_timeout: Duration,
    max_payload_bytes: usize,
    connection: Mutex<Option<Connection>>,
}

impl ControlTcpClient {
    /// `authority` is `host:port`; every connect, write and read is bounded by `io_timeout`, and
    /// frames by `max_payload_bytes` in both directions.
    pub fn new(authority: &str, io_timeout: Duration, max_payload_bytes: usize) -> Self {
        Self {
            authority: authority.to_owned(),
            io_timeout,
            max_payload_bytes,
            connection: Mutex::new(None),
        }
    }

    pub fn authority(&self) -> &str {
        &self.authority
    }

    /// Drops the cached connection; the next exchange connects again.
    pub async fn disconnect(&self) {
        *self.connection.lock().await = None;
    }

    /// Sends one request payload and returns the response payload.
    ///
    /// Callers must only send idempotent requests (every signed request carries a fresh nonce
    /// and is safe to repeat): a request that failed on a reused connection is retried once on a
    /// new one.
    pub async fn exchange(
        &self,
        request: &[u8],
    ) -> Result<(Vec<u8>, ControlTcpExchangeBytes), ControlTcpError> {
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
    ) -> Result<(Vec<u8>, ControlTcpExchangeBytes), ControlTcpError> {
        if connection.is_none() {
            *connection = Some(self.connect().await?);
        }
        let Some(connection) = connection.as_mut() else {
            return Err(ControlTcpError::Closed {
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
            .ok_or_else(|| ControlTcpError::Closed {
                addr: self.authority.clone(),
            })?;
        let bytes = ControlTcpExchangeBytes {
            sent: sent as u64,
            received: response.len() as u64,
        };
        Ok((response, bytes))
    }

    async fn connect(&self) -> Result<Connection, ControlTcpError> {
        let stream = self
            .timed("connect", TcpStream::connect(self.authority.as_str()))
            .await?
            .map_err(|err| ControlTcpError::Connect {
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
    ) -> Result<T, ControlTcpError> {
        tokio::time::timeout(self.io_timeout, future)
            .await
            .map_err(|_| ControlTcpError::Timeout {
                addr: self.authority.clone(),
                operation,
                timeout_ms: self.io_timeout.as_millis().min(u128::from(u64::MAX)) as u64,
            })
    }
}

// Runs on every target (including the Windows CI job): the remote operator client depends on it.
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    /// Echoes payload frames back: up to `frames` per connection, for `connections` connections.
    async fn echo_server(
        connections: usize,
        frames: usize,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener addr").to_string();
        let task = tokio::spawn(async move {
            for _ in 0..connections {
                let (mut stream, _) = listener.accept().await.expect("accept");
                let mut read_state = ControlFrameReadState::new();
                for _ in 0..frames {
                    let Some(payload) = read_state
                        .read_payload(&mut stream, 1024)
                        .await
                        .expect("request frame")
                    else {
                        break;
                    };
                    write_control_payload_frame(&mut stream, &payload, 1024)
                        .await
                        .expect("response frame");
                }
            }
        });
        (addr, task)
    }

    #[tokio::test]
    async fn exchange_reconnects_once_after_the_server_closed_the_connection() {
        // The first connection answers one request and closes; the second answers the next.
        let (addr, server) = echo_server(2, 1).await;
        let client = ControlTcpClient::new(&addr, Duration::from_secs(5), 1024);
        assert_eq!(client.authority(), addr);

        let (response, bytes) = client.exchange(b"one").await.expect("first exchange");
        assert_eq!(response, b"one");
        assert_eq!(bytes.received, 3);
        assert!(bytes.sent > 3, "sent bytes include the frame header");

        let (response, _) = client.exchange(b"two").await.expect("retried exchange");
        assert_eq!(response, b"two");
        server.await.expect("server should finish");
    }

    #[tokio::test]
    async fn oversized_requests_and_refused_connections_are_errors() {
        let (addr, server) = echo_server(1, 1).await;
        let client = ControlTcpClient::new(&addr, Duration::from_secs(5), 4);
        let err = client
            .exchange(b"too large")
            .await
            .expect_err("oversized request should fail");
        assert!(matches!(
            err,
            ControlTcpError::Frame(IpcTransportError::EncodeFailed(_))
        ));
        server.abort();

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let closed = listener.local_addr().expect("addr").to_string();
        drop(listener);
        let client = ControlTcpClient::new(&closed, Duration::from_secs(5), 1024);
        assert!(matches!(
            client.exchange(b"x").await,
            Err(ControlTcpError::Connect { .. } | ControlTcpError::Timeout { .. })
        ));
    }
}

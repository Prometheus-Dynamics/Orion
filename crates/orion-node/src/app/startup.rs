use super::startup_loops::record_stale_stream_eviction;
use super::task_handle::GracefulTaskHandle;
use super::{NodeApp, NodeError};
use crate::managed_transport::{ManagedSurfaceLaunchRequest, ManagedTransportBinding};
use crate::service::ControlSurface;
use orion::{
    control_plane::ControlMessage,
    transport::{
        http::HttpTransportError,
        ipc::{ControlEnvelope, IpcTransportError, LocalAddress, UnixControlServer},
    },
};
use orion_transport_ipc::{
    read_control_frame_with_limit_metered, write_control_frame_with_limit_metered,
};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{sync::oneshot, task::JoinSet};
use tracing::info;

impl NodeApp {
    pub async fn start_http_server_graceful(
        &self,
        addr: SocketAddr,
    ) -> Result<(SocketAddr, GracefulTaskHandle<HttpTransportError>), NodeError> {
        let (binding, handle) = self
            .start_managed_surface_with_shutdown(ManagedSurfaceLaunchRequest::PeerHttpControl {
                addr,
                handler: Arc::new(self.http_control_handler()),
            })
            .await?;
        self.state.lifecycle.mark_http_bound();
        match binding {
            ManagedTransportBinding::Socket(addr) => {
                Ok((addr, map_managed_http_shutdown_handle(self.clone(), handle)))
            }
            #[cfg(feature = "transport-quic")]
            ManagedTransportBinding::Quic(_) => Err(NodeError::Storage(
                "managed HTTP adapter returned QUIC binding".into(),
            )),
        }
    }

    #[cfg(test)]
    pub(crate) async fn start_http_server(
        &self,
        addr: SocketAddr,
    ) -> Result<
        (
            SocketAddr,
            tokio::task::JoinHandle<Result<(), HttpTransportError>>,
        ),
        NodeError,
    > {
        let (binding, handle) = self
            .start_managed_surface(ManagedSurfaceLaunchRequest::PeerHttpControl {
                addr,
                handler: Arc::new(self.http_control_handler()),
            })
            .await?;
        self.state.lifecycle.mark_http_bound();
        match binding {
            ManagedTransportBinding::Socket(addr) => {
                Ok((addr, map_managed_http_handle(self.clone(), handle)))
            }
            #[cfg(feature = "transport-quic")]
            ManagedTransportBinding::Quic(_) => Err(NodeError::Storage(
                "managed HTTP adapter returned QUIC binding".into(),
            )),
        }
    }

    #[cfg(feature = "transport-tcp")]
    pub async fn start_tcp_data_server(
        &self,
        addr: SocketAddr,
        handler: Arc<dyn orion::transport::tcp::TcpFrameHandler>,
    ) -> Result<
        (
            SocketAddr,
            tokio::task::JoinHandle<Result<(), orion::transport::tcp::TcpTransportError>>,
        ),
        NodeError,
    > {
        let (binding, handle) = self
            .start_managed_surface(ManagedSurfaceLaunchRequest::PeerTcpData { addr, handler })
            .await?;
        match binding {
            ManagedTransportBinding::Socket(addr) => Ok((addr, map_managed_tcp_handle(handle))),
            #[cfg(feature = "transport-quic")]
            ManagedTransportBinding::Quic(_) => Err(NodeError::Storage(
                "managed TCP adapter returned non-socket binding".into(),
            )),
        }
    }

    #[cfg(feature = "transport-quic")]
    pub async fn start_quic_data_server(
        &self,
        addr: SocketAddr,
        server_name: Option<String>,
        handler: Arc<dyn orion::transport::quic::QuicFrameHandler>,
    ) -> Result<
        (
            orion::transport::quic::QuicEndpoint,
            tokio::task::JoinHandle<Result<(), orion::transport::quic::QuicTransportError>>,
        ),
        NodeError,
    > {
        let (binding, handle) = self
            .start_managed_surface(ManagedSurfaceLaunchRequest::PeerQuicData {
                addr,
                handler,
                server_name,
            })
            .await?;
        match binding {
            ManagedTransportBinding::Quic(endpoint) => {
                Ok((endpoint, map_managed_quic_handle(handle)))
            }
            ManagedTransportBinding::Socket(_) => Err(NodeError::Storage(
                "managed QUIC adapter returned socket binding".into(),
            )),
        }
    }

    pub async fn start_http_probe_server_graceful(
        &self,
        addr: SocketAddr,
    ) -> Result<(SocketAddr, GracefulTaskHandle<HttpTransportError>), NodeError> {
        let (binding, handle) = self
            .start_managed_surface_with_shutdown(ManagedSurfaceLaunchRequest::HttpProbe {
                addr,
                handler: Arc::new(self.http_probe_handler()),
            })
            .await?;
        self.state.lifecycle.mark_http_bound();
        match binding {
            ManagedTransportBinding::Socket(addr) => {
                Ok((addr, map_managed_http_shutdown_handle(self.clone(), handle)))
            }
            #[cfg(feature = "transport-quic")]
            ManagedTransportBinding::Quic(_) => Err(NodeError::Storage(
                "managed HTTP probe adapter returned QUIC binding".into(),
            )),
        }
    }

    #[cfg(any(test, feature = "transport-tcp", feature = "transport-quic"))]
    pub(crate) async fn start_managed_surface(
        &self,
        request: ManagedSurfaceLaunchRequest,
    ) -> Result<
        (
            ManagedTransportBinding,
            tokio::task::JoinHandle<Result<(), NodeError>>,
        ),
        NodeError,
    > {
        request.start(self.clone()).await
    }

    pub(crate) async fn start_managed_surface_with_shutdown(
        &self,
        request: ManagedSurfaceLaunchRequest,
    ) -> Result<(ManagedTransportBinding, GracefulTaskHandle<NodeError>), NodeError> {
        request.start_with_shutdown(self.clone()).await
    }

    #[cfg(test)]
    pub(crate) async fn start_ipc_server(
        &self,
        socket_path: impl AsRef<Path>,
    ) -> Result<
        (
            PathBuf,
            tokio::task::JoinHandle<Result<(), IpcTransportError>>,
        ),
        NodeError,
    > {
        let server =
            UnixControlServer::bind(socket_path.as_ref(), Arc::new(self.unix_control_handler()))
                .await?
                .with_max_payload_bytes(self.config.runtime_tuning.transport_max_payload_bytes)
                .with_io_timeout(self.config.runtime_tuning.transport_io_timeout)
                .with_max_connections(
                    self.config
                        .runtime_tuning
                        .transport_max_concurrent_connections,
                );
        let path = server.socket_path().to_path_buf();
        let app = self.clone();
        self.state.lifecycle.mark_ipc_bound();
        let handle = tokio::spawn(async move {
            let result = server.serve().await;
            app.state.lifecycle.clear_ipc_bound();
            result
        });
        Ok((path, handle))
    }

    pub async fn start_ipc_server_graceful(
        &self,
        socket_path: impl AsRef<Path>,
    ) -> Result<(PathBuf, GracefulTaskHandle<IpcTransportError>), NodeError> {
        let server =
            UnixControlServer::bind(socket_path.as_ref(), Arc::new(self.unix_control_handler()))
                .await?
                .with_max_payload_bytes(self.config.runtime_tuning.transport_max_payload_bytes)
                .with_io_timeout(self.config.runtime_tuning.transport_io_timeout)
                .with_max_connections(
                    self.config
                        .runtime_tuning
                        .transport_max_concurrent_connections,
                );
        let path = server.socket_path().to_path_buf();
        self.state.lifecycle.mark_ipc_bound();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let shutdown_path = path.clone();
        let app = self.clone();
        let handle = tokio::spawn(async move {
            let result = server
                .serve_with_shutdown(async {
                    let _ = (&mut shutdown_rx).await;
                })
                .await;
            let _ = tokio::fs::remove_file(&shutdown_path).await;
            app.state.lifecycle.clear_ipc_bound();
            result
        });
        Ok((path, GracefulTaskHandle::new(shutdown_tx, handle)))
    }

    #[cfg(test)]
    pub(crate) async fn start_ipc_stream_server(
        &self,
        socket_path: impl AsRef<Path>,
    ) -> Result<
        (
            PathBuf,
            tokio::task::JoinHandle<Result<(), IpcTransportError>>,
        ),
        NodeError,
    > {
        let socket_path = socket_path.as_ref().to_path_buf();
        if socket_path.exists() {
            let _ = tokio::fs::remove_file(&socket_path).await;
        }
        let listener = tokio::net::UnixListener::bind(&socket_path)
            .map_err(|err| IpcTransportError::BindFailed(err.to_string()))?;
        let app = self.clone();
        let path = socket_path.clone();
        self.state.lifecycle.mark_ipc_stream_bound();
        let handle = tokio::spawn(async move {
            let result = async {
                loop {
                    let (stream, _) = listener
                        .accept()
                        .await
                        .map_err(|err| IpcTransportError::AcceptFailed(err.to_string()))?;
                    let app = app.clone();
                    tokio::spawn(async move {
                        let _ = app.handle_ipc_stream_connection(stream).await;
                    });
                }
            }
            .await;
            app.state.lifecycle.clear_ipc_stream_bound();
            result
        });
        Ok((path, handle))
    }

    pub async fn start_ipc_stream_server_graceful(
        &self,
        socket_path: impl AsRef<Path>,
    ) -> Result<(PathBuf, GracefulTaskHandle<IpcTransportError>), NodeError> {
        let socket_path = socket_path.as_ref().to_path_buf();
        if socket_path.exists() {
            let _ = tokio::fs::remove_file(&socket_path).await;
        }
        let listener = tokio::net::UnixListener::bind(&socket_path)
            .map_err(|err| IpcTransportError::BindFailed(err.to_string()))?;
        let app = self.clone();
        let path = socket_path.clone();
        self.state.lifecycle.mark_ipc_stream_bound();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            let mut connection_tasks = JoinSet::new();
            let result = loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted
                            .map_err(|err| IpcTransportError::AcceptFailed(err.to_string()))?;
                        let app = app.clone();
                        connection_tasks.spawn(async move {
                            let _ = app.handle_ipc_stream_connection(stream).await;
                        });
                    }
                    _ = &mut shutdown_rx => {
                        connection_tasks.abort_all();
                        while connection_tasks.join_next().await.is_some() {}
                        let _ = tokio::fs::remove_file(&socket_path).await;
                        break Ok(());
                    }
                }
            };
            app.state.lifecycle.clear_ipc_stream_bound();
            result
        });
        Ok((path, GracefulTaskHandle::new(shutdown_tx, handle)))
    }

    async fn handle_ipc_stream_connection(
        &self,
        stream: tokio::net::UnixStream,
    ) -> Result<(), IpcTransportError> {
        info!(node = %self.config.node_id, "client stream session opened");
        let peer_identity = stream
            .peer_cred()
            .map(|cred| orion::transport::ipc::UnixPeerIdentity {
                pid: cred.pid().and_then(|pid| u32::try_from(pid).ok()),
                uid: cred.uid(),
                gid: cred.gid(),
            })
            .ok();
        let max_payload_bytes = self.config.runtime_tuning.transport_max_payload_bytes;
        let (mut reader, mut writer) = stream.into_split();
        let (hello, hello_bytes) =
            match read_control_frame_with_limit_metered(&mut reader, max_payload_bytes).await? {
                Some((envelope, bytes)) => (envelope, bytes_len_u64(bytes)),
                None => return Ok(()),
            };

        let (source, destination, welcome): (LocalAddress, LocalAddress, ControlMessage) =
            match hello.message {
                ControlMessage::ClientHello(client_hello) => {
                    let response = self
                        .serve_local_control_message_async(
                            ControlSurface::LocalIpcStream,
                            hello.source.clone(),
                            hello.destination.clone(),
                            ControlMessage::ClientHello(client_hello),
                        )
                        .await
                        .map_err(|err| IpcTransportError::WriteFailed(err.to_string()))?;
                    (hello.source, hello.destination, response)
                }
                other => {
                    let rejected = ControlEnvelope {
                        source: hello.destination,
                        destination: hello.source,
                        message: ControlMessage::Rejected(format!(
                            "stream session requires ClientHello as first message, got {other:?}"
                        )),
                    };
                    let rejected_bytes = write_control_frame_with_limit_metered(
                        &mut writer,
                        &rejected,
                        max_payload_bytes,
                    )
                    .await?;
                    self.record_local_stream_sent(
                        &rejected.destination,
                        bytes_len_u64(rejected_bytes),
                    );
                    return Ok(());
                }
            };
        self.record_local_stream_received(&source, hello_bytes);

        if matches!(welcome, ControlMessage::Rejected(_)) {
            let rejected = ControlEnvelope {
                source: destination,
                destination: source,
                message: welcome,
            };
            let rejected_bytes =
                write_control_frame_with_limit_metered(&mut writer, &rejected, max_payload_bytes)
                    .await?;
            self.record_local_stream_sent(&rejected.destination, bytes_len_u64(rejected_bytes));
            return Ok(());
        }

        let (tx, mut rx) = tokio::sync::mpsc::channel::<ControlEnvelope>(
            self.config.runtime_tuning.local_stream_send_queue_capacity,
        );
        self.attach_local_client_stream(&source, tx.clone())
            .map_err(|err| IpcTransportError::WriteFailed(err.to_string()))?;
        self.record_local_stream_peer_identity(&source, peer_identity);

        let welcome_envelope = ControlEnvelope {
            source: destination.clone(),
            destination: source.clone(),
            message: welcome,
        };
        tx.send(welcome_envelope)
            .await
            .map_err(|_| IpcTransportError::WriteFailed("failed to queue welcome frame".into()))?;
        self.flush_client_stream_for_source(&source);

        let writer_app = self.clone();
        let writer_source = source.clone();
        let writer_task = tokio::spawn(async move {
            while let Some(envelope) = rx.recv().await {
                let bytes = match write_control_frame_with_limit_metered(
                    &mut writer,
                    &envelope,
                    max_payload_bytes,
                )
                .await
                {
                    Ok(bytes) => bytes,
                    Err(err) => {
                        writer_app.record_local_stream_failure(
                            &writer_source,
                            None,
                            err.to_string(),
                        );
                        return Err(err);
                    }
                };
                writer_app.record_local_stream_sent(&writer_source, bytes_len_u64(bytes));
            }
            Ok::<(), IpcTransportError>(())
        });

        let mut heartbeat = tokio::time::interval_at(
            tokio::time::Instant::now() + self.config.ipc_stream_heartbeat_interval,
            self.config.ipc_stream_heartbeat_interval,
        );
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut awaiting_pong_since: Option<tokio::time::Instant> = None;

        loop {
            tokio::select! {
                frame = read_control_frame_with_limit_metered(&mut reader, max_payload_bytes) => {
                    let (envelope, bytes_received) = match frame {
                        Ok(Some((envelope, bytes_received))) => (envelope, bytes_len_u64(bytes_received)),
                        Ok(None) => break,
                        Err(err) => {
                            self.record_local_stream_failure(&source, None, err.to_string());
                            return Err(err);
                        }
                    };
                    self.record_local_stream_received(&source, bytes_received);
                    let source = envelope.source.clone();
                    let destination = envelope.destination.clone();
                    match envelope.message {
                        ControlMessage::Pong => {
                            self.touch_local_client_activity(&source, Self::current_time_ms());
                            if let Some(since) = awaiting_pong_since {
                                self.record_local_stream_success(&source, since.elapsed());
                            }
                            awaiting_pong_since = None;
                            continue;
                        }
                        message => {
                            let response = match self.serve_local_control_message_async(
                                ControlSurface::LocalIpcStream,
                                source.clone(),
                                destination.clone(),
                                message,
                            )
                            .await
                            {
                                Ok(response) => response,
                                Err(err) => ControlMessage::Rejected(err.to_string()),
                            };
                            tx.send(ControlEnvelope {
                                source: destination,
                                destination: source.clone(),
                                message: response,
                            })
                            .await
                            .map_err(|_| IpcTransportError::WriteFailed("failed to queue stream response".into()))?;
                            self.flush_client_stream_for_source(&source);
                        }
                    }
                }
                _ = heartbeat.tick() => {
                    let now = tokio::time::Instant::now();
                    if let Some(since) = awaiting_pong_since {
                        if now.duration_since(since) >= self.config.ipc_stream_heartbeat_timeout {
                            record_stale_stream_eviction(self, &source, now.duration_since(since));
                            self.record_local_stream_failure(
                                &source,
                                Some(now.duration_since(since)),
                                "ipc stream heartbeat timed out",
                            );
                            break;
                        }
                    } else {
                        tx.send(ControlEnvelope {
                            source: destination.clone(),
                            destination: source.clone(),
                            message: ControlMessage::Ping,
                        })
                        .await
                        .map_err(|_| IpcTransportError::WriteFailed("failed to queue stream heartbeat".into()))?;
                        awaiting_pong_since = Some(now);
                    }
                }
            }
        }

        self.detach_local_client_stream(&source);
        drop(tx);
        match writer_task.await {
            Ok(result) => result?,
            Err(err) => {
                return Err(IpcTransportError::WriteFailed(format!(
                    "stream writer task failed: {err}"
                )));
            }
        }
        Ok(())
    }
}

fn map_managed_task_result<E>(
    result: Result<Result<(), NodeError>, tokio::task::JoinError>,
    extract: impl FnOnce(NodeError) -> Result<E, NodeError>,
    wrap: impl FnOnce(String) -> E,
) -> Result<(), E> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => match extract(err) {
            Ok(err) => Err(err),
            Err(err) => Err(wrap(err.to_string())),
        },
        Err(err) => Err(wrap(format!("managed transport task join failed: {err}"))),
    }
}

fn bytes_len_u64(bytes: usize) -> u64 {
    bytes.min(u64::MAX as usize) as u64
}

#[cfg(test)]
fn map_managed_http_handle(
    app: NodeApp,
    handle: tokio::task::JoinHandle<Result<(), NodeError>>,
) -> tokio::task::JoinHandle<Result<(), HttpTransportError>> {
    tokio::spawn(async move {
        let result = map_managed_task_result(
            handle.await,
            |err| match err {
                NodeError::HttpTransport(err) => Ok(err),
                other => Err(other),
            },
            HttpTransportError::ServeFailed,
        );
        app.state.lifecycle.clear_http_bound();
        result
    })
}

fn map_managed_http_shutdown_handle(
    app: NodeApp,
    handle: GracefulTaskHandle<NodeError>,
) -> GracefulTaskHandle<HttpTransportError> {
    let GracefulTaskHandle { shutdown, handle } = handle;
    let mapped = tokio::spawn(async move {
        let result = map_managed_task_result(
            handle.await,
            |err| match err {
                NodeError::HttpTransport(err) => Ok(err),
                other => Err(other),
            },
            HttpTransportError::ServeFailed,
        );
        app.state.lifecycle.clear_http_bound();
        result
    });
    GracefulTaskHandle {
        shutdown,
        handle: mapped,
    }
}

#[cfg(feature = "transport-tcp")]
fn map_managed_tcp_handle(
    handle: tokio::task::JoinHandle<Result<(), NodeError>>,
) -> tokio::task::JoinHandle<Result<(), orion::transport::tcp::TcpTransportError>> {
    tokio::spawn(async move {
        map_managed_task_result(
            handle.await,
            |err| match err {
                NodeError::TcpTransport(err) => Ok(err),
                other => Err(other),
            },
            orion::transport::tcp::TcpTransportError::WriteFailed,
        )
    })
}

#[cfg(feature = "transport-quic")]
fn map_managed_quic_handle(
    handle: tokio::task::JoinHandle<Result<(), NodeError>>,
) -> tokio::task::JoinHandle<Result<(), orion::transport::quic::QuicTransportError>> {
    tokio::spawn(async move {
        map_managed_task_result(
            handle.await,
            |err| match err {
                NodeError::QuicTransport(err) => Ok(err),
                other => Err(other),
            },
            orion::transport::quic::QuicTransportError::WriteFailed,
        )
    })
}

//! Link gateway (feature `link-gateway`): serves microcontroller links configured with
//! `ORION_NODE_LINKS` and bridges every device into the node as an ordinary provider.
//!
//! Each link runs in its own task: a serial port drives one `orion_link::host::HostSession`
//! (COBS byte stream), a SocketCAN interface drives one `orion_link::host::HostBus` (many devices,
//! demultiplexed by identifier), and a Unix socket drives one `HostSession` per connection. Device events go through the node's local provider path (see
//! `app/link_bridge.rs`); lease sets are refreshed after every desired-state commit. Serial and
//! SocketCAN I/O are Linux-only and implemented directly on `libc` with tokio's `AsyncFd`.

#[cfg(target_os = "linux")]
mod can_link;
mod config;
#[cfg(target_os = "linux")]
mod events;
#[cfg(target_os = "linux")]
pub(crate) mod firmware;
#[cfg(target_os = "linux")]
mod remote_link;
#[cfg(target_os = "linux")]
mod serial;
#[cfg(target_os = "linux")]
mod serial_link;
#[cfg(target_os = "linux")]
mod socketcan;
#[cfg(all(test, target_os = "linux"))]
mod tests;
#[cfg(target_os = "linux")]
mod unix_client;
#[cfg(target_os = "linux")]
mod unix_executor;
#[cfg(target_os = "linux")]
mod unix_link;

pub use config::{
    CanLinkConfig, DEFAULT_BAUD, DEFAULT_CAN_ADDRESSES, DEFAULT_CAN_DEVICE_BASE,
    DEFAULT_CAN_HOST_BASE, DEFAULT_HEARTBEAT_MS, DEFAULT_MISSED_HEARTBEATS,
    DEFAULT_UNIX_MAX_CONNECTIONS, DEFAULT_UNIX_MAX_FRAMES_PER_S, LINKS_ENV, LinkConfig,
    LinkTransport, MAX_CAN_ADDRESSES, MAX_UNIX_CONNECTIONS, MAX_UNIX_FRAMES_PER_S, REMOTE_AUTH_ENV,
    SUPPORTED_BAUD_RATES, SerialLinkConfig, TcpLinkConfig, UnixLinkConfig,
};

use crate::{NodeApp, NodeError};
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// Running link tasks, from [`NodeApp::start_link_gateway`].
pub struct LinkGatewayHandle {
    shutdown_tx: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
}

impl LinkGatewayHandle {
    /// Number of links served.
    #[must_use]
    pub fn link_count(&self) -> usize {
        self.tasks.len()
    }

    /// Stops every link task. Connected devices are marked lost (their resources unavailable)
    /// and ports and sockets are closed before this returns.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        for task in self.tasks {
            let _ = task.await;
        }
    }
}

impl NodeApp {
    /// Starts one task per link. Must be called inside a Tokio runtime. Ports and sockets that
    /// cannot be opened yet are retried in the background (and reported in
    /// [`crate::control_plane::LinkStatusSnapshot`]).
    pub fn start_link_gateway(
        &self,
        links: Vec<LinkConfig>,
    ) -> Result<LinkGatewayHandle, NodeError> {
        // Before any link task: providers of a previous run are unavailable until their device
        // reconnects, and links no longer configured are forgotten (`docs/link-protocol.md`).
        // `orion-node` has restored them already, before its IPC servers accept clients; this is
        // then a no-op, and it keeps library callers that start the gateway directly safe.
        self.restore_link_devices(&links)?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        #[cfg(not(target_os = "linux"))]
        {
            let _ = shutdown_rx;
            if !links.is_empty() {
                return Err(NodeError::Config(format!(
                    "{LINKS_ENV}: serial, SocketCAN and Unix links are only supported on Linux"
                )));
            }
            Ok(LinkGatewayHandle {
                shutdown_tx,
                tasks: Vec::new(),
            })
        }
        #[cfg(target_os = "linux")]
        {
            let session_seed = session_seed();
            let tasks = links
                .into_iter()
                .map(|link| {
                    let host = link.host_config(self.config.node_id.clone(), session_seed);
                    let ctx = events::LinkContext::new(self.clone(), link.name.clone());
                    let shutdown = shutdown_rx.clone();
                    match link.transport {
                        LinkTransport::Serial(serial) => {
                            tokio::spawn(serial_link::run(ctx, serial, host, shutdown))
                        }
                        LinkTransport::Can(can) => {
                            let opener = socketcan::opener(can.clone());
                            tokio::spawn(can_link::run(ctx, can, host, opener, shutdown))
                        }
                        LinkTransport::Unix(unix) => {
                            let limits = unix_link::ConnLimits {
                                max_frames_per_s: unix.max_frames_per_s,
                            };
                            let listen = unix_link::Listen {
                                endpoint: unix_link::Endpoint::Unix(unix.path),
                                max_connections: unix.max_connections,
                            };
                            tokio::spawn(unix_link::run(ctx, listen, host, limits, shutdown))
                        }
                        LinkTransport::Tcp(tcp) => {
                            let limits = unix_link::ConnLimits {
                                max_frames_per_s: tcp.max_frames_per_s,
                            };
                            let listen = unix_link::Listen {
                                endpoint: unix_link::Endpoint::Tcp(tcp.addr),
                                max_connections: tcp.max_connections,
                            };
                            tokio::spawn(unix_link::run(ctx, listen, host, limits, shutdown))
                        }
                    }
                })
                .collect();
            Ok(LinkGatewayHandle { shutdown_tx, tasks })
        }
    }
}

/// Session ids that differ across gateway restarts, so devices notice a restarted host.
#[cfg(target_os = "linux")]
fn session_seed() -> u32 {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(1);
    // Leave room below u32::MAX for many sessions before wrapping.
    u32::try_from(secs & 0x7FFF_FFFF).unwrap_or(1).max(1)
}

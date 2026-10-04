//! Link gateway (feature `link-gateway`): serves microcontroller links configured with
//! `ORION_NODE_LINKS` and bridges every device into the node as an ordinary provider.
//!
//! Each link runs in its own task: a serial port drives one `orion_link::host::HostSession`
//! (COBS byte stream), a SocketCAN interface drives one `orion_link::host::HostBus` (many devices,
//! demultiplexed by identifier). Device events go through the node's local provider path (see
//! `app/link_bridge.rs`); lease sets are refreshed after every desired-state commit. Serial and
//! SocketCAN I/O are Linux-only and implemented directly on `libc` with tokio's `AsyncFd`.

#[cfg(target_os = "linux")]
mod can_link;
mod config;
#[cfg(target_os = "linux")]
mod events;
#[cfg(target_os = "linux")]
mod serial;
#[cfg(target_os = "linux")]
mod serial_link;
#[cfg(target_os = "linux")]
mod socketcan;
#[cfg(all(test, target_os = "linux"))]
mod tests;

pub use config::{
    CanLinkConfig, DEFAULT_BAUD, DEFAULT_CAN_ADDRESSES, DEFAULT_CAN_DEVICE_BASE,
    DEFAULT_CAN_HOST_BASE, DEFAULT_HEARTBEAT_MS, DEFAULT_MISSED_HEARTBEATS, LINKS_ENV, LinkConfig,
    LinkTransport, MAX_CAN_ADDRESSES, SUPPORTED_BAUD_RATES, SerialLinkConfig,
};

use crate::{NodeApp, NodeError};
use serde::Serialize;
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// Counters and connected devices of one link, from [`NodeApp::link_status`]. Counters are
/// cumulative since the gateway started.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct LinkStatus {
    /// `<kind>:<target>` as configured.
    pub name: String,
    /// Whether the serial port or CAN socket is currently open.
    pub open: bool,
    /// Devices with an accepted provider snapshot in the current session.
    pub devices: Vec<String>,
    /// Valid link frames received.
    pub frames_rx: u64,
    /// Link frames sent (for CAN: CAN frames).
    pub frames_tx: u64,
    /// Bytes (serial) or CAN payload bytes received.
    pub bytes_rx: u64,
    /// Bytes (serial) or CAN payload bytes sent.
    pub bytes_tx: u64,
    /// Frames dropped for a bad CRC.
    pub crc_errors: u64,
    /// Frames dropped for bad framing (COBS errors, short frames).
    pub framing_errors: u64,
    /// Every frame or partial message dropped by the transport decoder (includes CRC and framing
    /// errors, overflows, and CAN sequence errors).
    pub dropped: u64,
    /// Frames whose message body did not decode.
    pub decode_errors: u64,
    /// Device sessions opened.
    pub sessions: u64,
    /// Devices lost to missed heartbeats.
    pub device_timeouts: u64,
    /// `Hello`s refused (allowlist, version, roles, frame size).
    pub hello_rejects: u64,
    /// Provider snapshots refused by the gateway (ownership collisions, invalid records).
    pub snapshot_rejects: u64,
    /// Status batches accepted into the node's status lane.
    pub status_batches: u64,
    /// Status batches refused (no accepted provider snapshot yet, invalid entries, lane caps).
    pub status_rejects: u64,
    /// Open, read, or write failures of the port or socket.
    pub io_errors: u64,
    /// Most recent I/O error or rejection.
    pub last_error: Option<String>,
}

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
    /// cannot be opened yet are retried in the background (and reported in [`LinkStatus`]).
    pub fn start_link_gateway(
        &self,
        links: Vec<LinkConfig>,
    ) -> Result<LinkGatewayHandle, NodeError> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        #[cfg(not(target_os = "linux"))]
        {
            let _ = shutdown_rx;
            if !links.is_empty() {
                return Err(NodeError::Config(format!(
                    "{LINKS_ENV}: serial and SocketCAN links are only supported on Linux"
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

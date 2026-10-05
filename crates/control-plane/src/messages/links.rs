//! Link gateway counters (`orion-node` feature `link-gateway`), carried in the observability
//! snapshot so `orionctl get links` and Prometheus can read them. See `docs/link-protocol.md`.

use alloc::{string::String, vec::Vec};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

/// Counters and connected devices of one microcontroller link. Counters are cumulative since the
/// gateway started.
#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct LinkStatusSnapshot {
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

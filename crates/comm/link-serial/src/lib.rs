//! Links over byte streams: a serial port, or anything else that moves bytes in order.
//!
//! A link is one more carrier of the one wire. Frames are COBS-encoded and end in `0x00`
//! (`orion_wire::stream`), so a receiver finds the next frame after any noise. What is on the
//! other end says hello like any client, with the role `device`.
//!
//! **A link never gives up.** The port is opened again a fixed, short time after it fails, for
//! as long as the program runs: a link is a way into the machine, and ways in are not backed
//! off. Between tries the task sleeps until that deadline.
//!
//! **A silent link costs nothing.** The carrier hands the server a connection only when a whole
//! valid frame has arrived. Until then one task waits for the port to become readable: no
//! session, no timer, no allocation.
//!
//! [`StreamCarrier`] is the carrier, over anything that implements [`Open`]. [`Serial`] opens a
//! serial port.

#![cfg(target_os = "linux")]

mod carrier;
#[cfg(feature = "sealed")]
pub mod sealed;
mod serial;

pub use carrier::{
    ByteStream, LinkStats, MAX_FRAME, Open, StreamCarrier, StreamConnection, StreamRead,
    StreamWrite,
};
pub use serial::{Serial, SerialLink, SerialPort};

//! Links over a CAN bus.
//!
//! A frame of the one wire is cut into segments, one per CAN frame (`orion_wire::packet`): 7
//! bytes each on classic CAN, up to 63 on CAN FD. A bus carries many devices. Each has an
//! address, and the address picks its pair of identifiers: one for what it sends, one for what
//! it is sent, both the bus's base identifier plus the address. Lower identifiers win
//! arbitration, so a deployment ranks its devices by address.
//!
//! Each device is its own session. It says hello like any client, and the server knows it as
//! the device `<bus>.<address>`. One that stops talking loses its session and nothing else;
//! the others on the bus are not touched.
//!
//! The bus is opened again a short, fixed time after it fails, for as long as the program
//! runs. A bus where nobody says anything is one task waiting on the socket.

mod carrier;
#[cfg(all(feature = "socketcan", target_os = "linux"))]
mod socket;

pub use carrier::{
    BusStats, CanBus, CanCarrier, CanConnection, CanFrame, CanIo, CanRead, CanWrite, MAX_FRAME,
    OpenCan,
};
#[cfg(all(feature = "socketcan", target_os = "linux"))]
pub use socket::{SocketCan, SocketCanBus};

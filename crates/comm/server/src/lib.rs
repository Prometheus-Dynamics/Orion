//! The comm core's server: everything a connection goes through before a service sees it.
//!
//! - A [`Carrier`] brings connections: the Unix socket, `orion+tcp`, a WebSocket, a link. It
//!   says who is on the other end and moves whole frames. Nothing else about it is known here.
//! - A connection becomes a **session** with `Hello` and `Welcome`. Each session has a bounded
//!   outbox and three deadlines: to say hello, to be heard from, and to take what it is sent.
//! - A [`Service`] answers a range of kinds. It can only be called with a
//!   [`Checked`]`<`[`Incoming`]`>`, and only the server can make one, after the one check. A
//!   service that forgets the check does not compile, because it has nothing to forget.
//!
//! Everything runs on one thread. A service call is a plain function that returns at once: it
//! reads the request, changes its state and queues frames. That is what makes "one bad session
//! cannot stall another" checkable:
//!
//! - a session that floods requests is one task taking turns with the others;
//! - a session that does not read fills its own outbox, is refused more, and is closed at its
//!   write deadline;
//! - a session that goes quiet is closed at its idle deadline;
//! - a request that makes a service panic closes that session, and the service stays up.
//!
//! Nothing allocates per request once a session is open: the read buffer, the outbox and the
//! timers belong to the session.

mod carrier;
mod driver;
mod limits;
pub mod mem;
mod outbox;
mod server;
mod service;
mod sessions;

pub use carrier::{Carrier, Connection, FrameRead, FrameWrite};
pub use driver::Comm;
pub use limits::Limits;
pub use orion_check::{Admits, Checked, Grants, Policy, Principal, Security, What};
pub use orion_fdpass::FdSet;
pub use orion_msg_session::Roles;
pub use orion_types::Error;
pub use server::Stats;
pub use service::{Cx, Incoming, Service};
pub use sessions::{Delivery, SessionId, SessionInfo, Sessions};

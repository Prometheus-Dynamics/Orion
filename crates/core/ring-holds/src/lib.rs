//! Hold marks: the one new thing a buffer topic adds to a ring.
//!
//! A reader that is looking at a frame marks the frame's buffers in a page of shared memory
//! that only that reader writes. The publisher maps every reader's page read-only, and a buffer
//! is free when no reader marks it. That is all a hold is: there is no message, no reply and no
//! system call, for the reader or the publisher.
//!
//! - [`Marks`] is a reader's page. It plugs into a `BufferReader` as its marker.
//! - [`HoldTable`] is the publisher's set of readers' pages. It plugs into a `BufferWriter` as
//!   its holders, and applies each reader's [`Limits`].
//!
//! **A mark and a reuse cannot cross.** The reader marks a buffer and *then* checks the buffer's
//! ticket; the publisher raises the ticket and *then* looks at the marks; each side fences in
//! between ([`orion_ring_buffers::page`]). Whichever goes first, either the publisher sees the
//! mark and leaves the buffer alone, or the reader sees the new ticket and never reads it.
//!
//! **No reader can starve the others or the publisher.**
//!
//! - The publisher never waits. With every buffer held it drops the frame.
//! - A reader may hold at most `max_held` buffers. An honest one stops taking frames at its
//!   limit; one that marks more anyway is cut off.
//! - A reader may hold a buffer at most `max_hold`. One that keeps it longer is stuck, and is
//!   cut off.
//! - A reader that is cut off no longer counts: its marks are ignored, its buffers come back,
//!   and it is told the next time it looks.
//! - A reader that dies leaves its marks behind in its page. They go when the publisher drops
//!   the page ([`HoldTable::remove`]), which the comm core asks for when the connection closes;
//!   until then the hold-time limit covers it.
//!
//! A reader's page is its own. It cannot write another reader's, so it cannot release or pin
//! another reader's frames. The worst it can do with its own is hold what its limits allow.

#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]

mod marks;
mod table;

pub use marks::Marks;
pub use table::{HoldTable, Limits};

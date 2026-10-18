//! Remote push sessions (`docs/remote-operator.md`, "Push sessions").
//!
//! A remote operator opens a session on its own `orion+tcp` connection with
//! [`ControlMessage::OpenSession`](super::ControlMessage::OpenSession). The node answers with a
//! [`SessionWelcome`], and from then on the connection carries the operator's signed requests
//! (watches, pings) one way and, the other way, their responses interleaved with the events the
//! watches produce, pushed as they happen instead of polled.

use alloc::string::String;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

/// The local client address prefix of remote sessions. Reserved: local IPC clients may not use
/// it, so nothing on the node's socket can act as a remote session.
pub const REMOTE_SESSION_PREFIX: &str = "remote-session/";

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct SessionOpen {
    /// Shown in the node's observability (the client's name, e.g. `operator-console`).
    pub client_name: String,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct SessionWelcome {
    /// The session's id on the node (its local client address).
    pub session_id: String,
    /// The node closes a session that sends nothing for this long; the client pings well within.
    pub idle_timeout_ms: u64,
}

//! NetworkTables 4.1 (FRC NT4) for Orion tools: a client that subscribes to and publishes topics
//! of a robot, a camera or a coprocessor, and a server that a host app can run with values it
//! edits itself.
//!
//! The crate knows nothing of Orion's topics. It speaks the wire protocol directly: a WebSocket
//! per client (the comm core's own WebSocket code, `orion-ws`), JSON control messages in text
//! frames, and MessagePack values in binary frames. See the crate README for scope and usage.
//!
//! * [`Client`] / [`ClientHandle`]: connect, subscribe by prefix ([`Subscription`] yields
//!   [`TopicEvent`]s), publish ([`Publisher`]), and estimate the server clock.
//! * [`Server`] / [`ServerHandle`]: accept clients, serve the topic table, relay values, and let
//!   the host create, edit and delete topics with [`ServerHandle`]'s local API.
//! * [`Value`]: every NT4 value type; topic type strings map to ids with [`type_id_for_name`], and
//!   the numeric type ids live in [`value`].

#[cfg(feature = "client")]
mod client;
mod codec;
mod error;
mod mailbox;
mod message;
mod msgpack;
#[cfg(any(feature = "client", feature = "server"))]
mod net;
#[cfg(feature = "server")]
mod server;
mod time;
pub mod value;

#[cfg(feature = "client")]
pub use client::{
    Client, ClientConfig, ClientEvent, ClientHandle, Publisher, Subscription, TopicEvent, TopicInfo,
};
pub use codec::{
    Frame, decode_frame, decode_frames, encode_frame, encode_frame_into, encode_frames,
    for_each_frame,
};
pub use error::{Error, Result};
pub use message::{Control, Properties, SubscribeOptions};
#[cfg(feature = "server")]
pub use server::{
    ClientInfo, Server, ServerConfig, ServerEvent, ServerHandle, Taker, TopicOwner, TopicSnapshot,
};
pub use time::now_micros;
pub use value::{Value, type_id_for_name};

/// The longest WebSocket message taken. ntcore batches values into one message, and a value
/// may be a long string or a block of bytes; past this the connection is dropped.
pub const MAX_MESSAGE: usize = 4 * 1024 * 1024;
/// Default NT4 WebSocket port.
pub const DEFAULT_PORT: u16 = 5810;
/// The NT4.1 WebSocket subprotocol. Preferred by both client and server.
pub const SUBPROTOCOL_V4_1: &str = "v4.1.networktables.first.wpi.edu";
/// The NT4.0 subprotocol, accepted as a fallback. Same message set as implemented here.
pub const SUBPROTOCOL_V4_0: &str = "networktables.first.wpi.edu";

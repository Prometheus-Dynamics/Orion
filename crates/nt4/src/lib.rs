//! NetworkTables 4.1 (FRC NT4) for Orion tools: a client that subscribes to and publishes topics
//! of a robot, a camera or a coprocessor, and a server that Atlas and HeliOS can host with values
//! they edit themselves.
//!
//! The crate is independent of `orion-node`. It needs only Tokio and speaks the wire protocol
//! directly: a WebSocket per client, JSON control messages in text frames, and MessagePack values
//! in binary frames. See the crate README for scope and usage.
//!
//! * [`Client`] / [`ClientHandle`]: connect, subscribe by prefix ([`Subscription`] yields
//!   [`TopicEvent`]s), publish ([`Publisher`]), and estimate the server clock.
//! * [`Server`] / [`ServerHandle`]: accept clients, serve the topic table, relay values, and let
//!   the host create, edit and delete topics with [`ServerHandle`]'s local API.
//! * [`Value`]: every NT4 value type; topic type strings map to ids with [`type_id_for_name`], and
//!   the numeric type ids live in [`value`].

mod client;
mod codec;
mod error;
mod message;
mod server;
mod time;
pub mod value;

pub use client::{
    Client, ClientConfig, ClientEvent, ClientHandle, Publisher, Subscription, TopicEvent, TopicInfo,
};
pub use codec::{
    Frame, decode_frame, decode_frames, encode_frame, encode_frame_into, encode_frames,
};
pub use error::{Error, Result};
pub use message::{Control, Properties, SubscribeOptions};
pub use server::{
    ClientInfo, Server, ServerConfig, ServerEvent, ServerHandle, TopicOwner, TopicSnapshot,
};
pub use time::now_micros;
pub use value::{Value, type_id_for_name};

/// Default NT4 WebSocket port.
pub const DEFAULT_PORT: u16 = 5810;
/// The NT4.1 WebSocket subprotocol. Preferred by both client and server.
pub const SUBPROTOCOL_V4_1: &str = "v4.1.networktables.first.wpi.edu";
/// The NT4.0 subprotocol, accepted as a fallback. Same message set as implemented here.
pub const SUBPROTOCOL_V4_0: &str = "networktables.first.wpi.edu";

//! NT4 client: connects to a NetworkTables server (a robot, a coprocessor, or `nt4-server`),
//! keeps subscriptions and publications across reconnects, and syncs its clock to the server.
//!
//! Topic data arrives on [`Subscription`] streams, one per [`ClientHandle::subscribe`] call, so a
//! consumer only sees the prefixes it asked for. Connection state (connect, disconnect, clock sync,
//! protocol warnings) arrives on the [`Client`] event stream.
//!
//! ```no_run
//! # async fn demo() -> orion_nt4::Result<()> {
//! use orion_nt4::{Client, ClientConfig, ClientEvent, SubscribeOptions, TopicEvent, Value};
//!
//! let mut client = Client::start(ClientConfig::new("10.0.0.2", "atlas"));
//! let handle = client.handle();
//! let mut cameras = handle.subscribe(&["/CameraPublisher/"], SubscribeOptions {
//!     prefix: true,
//!     ..Default::default()
//! })?;
//! let speed = handle.publish("/HeliOS/speed", "double")?; // cheap to clone and to call set on
//! speed.set(Value::Double(1.5))?;
//! while let Some(event) = cameras.next().await {
//!     if let TopicEvent::Value { name, value, .. } = event {
//!         println!("{name} = {value:?}");
//!     }
//! }
//! # let _ = client.next_event().await; Ok(()) }
//! ```

mod session;
mod subscription;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
use std::time::Duration;

use tokio::sync::mpsc;

use crate::DEFAULT_PORT;
use crate::error::{Error, Result};
use crate::message::{Properties, SubscribeOptions};
use crate::time::now_micros;
use crate::value::{Value, type_id_for_name};

pub use subscription::{Subscription, TopicEvent, TopicInfo};

/// Where and how a client connects.
#[derive(Clone, Debug)]
pub struct ClientConfig {
    /// Server host name or address.
    pub host: String,
    /// Server port (5810 for NT4).
    pub port: u16,
    /// Client name, sent as the WebSocket path `/nt/<name>`.
    pub name: String,
    /// First delay before reconnecting; doubles up to `reconnect_max`.
    pub reconnect_min: Duration,
    /// Longest delay between reconnect attempts.
    pub reconnect_max: Duration,
    /// Time allowed for the TCP and WebSocket handshake.
    pub connect_timeout: Duration,
}

impl ClientConfig {
    /// A config for `host` on the default NT4 port, with 0.25 s to 5 s reconnect backoff.
    pub fn new(host: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            port: DEFAULT_PORT,
            name: name.into(),
            reconnect_min: Duration::from_millis(250),
            reconnect_max: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(5),
        }
    }
}

/// Connection-level events, delivered by [`Client::next_event`]. Topic data is on [`Subscription`]s.
#[derive(Clone, Debug, PartialEq)]
pub enum ClientEvent {
    /// The WebSocket handshake succeeded. Subscriptions and publications were sent again.
    Connected,
    /// The connection ended. The client reconnects on its own until it is dropped.
    Disconnected {
        /// Why the connection ended.
        reason: String,
    },
    /// The clock estimate improved from a better RTT sample.
    TimeSync {
        /// `server_time = local_time + offset_us`.
        offset_us: i64,
        /// Round-trip time of the sample that produced the estimate.
        rtt_us: i64,
    },
    /// A message the client could not use (bad frame, unknown topic id, unknown method).
    /// The connection stays up.
    ProtocolWarning(String),
}

/// Requests from [`ClientHandle`] to the connection task.
#[derive(Debug)]
pub(crate) enum Command {
    Subscribe {
        subuid: u32,
        topics: Vec<String>,
        options: SubscribeOptions,
        events: mpsc::UnboundedSender<TopicEvent>,
    },
    Unsubscribe {
        subuid: u32,
    },
    Publish {
        pubuid: u32,
        name: String,
        type_name: String,
    },
    Unpublish {
        pubuid: u32,
    },
    Set {
        pubuid: u32,
        timestamp_us: i64,
        value: Value,
    },
    SetProperties {
        name: String,
        update: Properties,
    },
}

/// Uid counters and the clock estimate, shared by every handle of one client.
#[derive(Debug, Default)]
pub(crate) struct Shared {
    next_pubuid: AtomicU32,
    next_subuid: AtomicU32,
    offset_us: AtomicI64,
    synced: AtomicBool,
}

impl Shared {
    pub(crate) fn set_offset(&self, offset_us: i64) {
        self.offset_us.store(offset_us, Ordering::Release);
        self.synced.store(true, Ordering::Release);
    }
}

/// A running client. Owns the connection-event stream; use [`ClientHandle`] for commands.
pub struct Client {
    handle: ClientHandle,
    events: mpsc::UnboundedReceiver<ClientEvent>,
}

impl Client {
    /// Starts the connection task on the current Tokio runtime. Connecting happens in the
    /// background and is retried until the client (and every handle) is dropped.
    pub fn start(config: ClientConfig) -> Self {
        let shared = Arc::new(Shared::default());
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        tokio::spawn(session::run(config, Arc::clone(&shared), cmd_rx, event_tx));
        Self {
            handle: ClientHandle { tx: cmd_tx, shared },
            events: event_rx,
        }
    }

    /// A handle for sending commands. Cheap to clone.
    pub fn handle(&self) -> ClientHandle {
        self.handle.clone()
    }

    /// The next connection event, or `None` if the connection task has stopped.
    pub async fn next_event(&mut self) -> Option<ClientEvent> {
        self.events.recv().await
    }

    /// Splits into the command handle and the event stream.
    pub fn split(self) -> (ClientHandle, mpsc::UnboundedReceiver<ClientEvent>) {
        (self.handle, self.events)
    }
}

/// Sends commands to a client's connection task. Commands made while disconnected are kept and
/// sent after the next connect. Cheap to clone; all clones share one connection.
#[derive(Clone, Debug)]
pub struct ClientHandle {
    tx: mpsc::UnboundedSender<Command>,
    shared: Arc<Shared>,
}

impl ClientHandle {
    /// Subscribes to topics (names, or prefixes when `options.prefix`) and returns a stream of
    /// their announces, values and property changes. Dropping the stream unsubscribes.
    pub fn subscribe(&self, topics: &[&str], options: SubscribeOptions) -> Result<Subscription> {
        let subuid = self.shared.next_subuid.fetch_add(1, Ordering::Relaxed) + 1;
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let topics = topics.iter().map(|t| (*t).to_owned()).collect();
        self.send(Command::Subscribe {
            subuid,
            topics,
            options,
            events: events_tx,
        })?;
        Ok(Subscription::new(self.clone(), subuid, events_rx))
    }

    /// Publishes `name` with NT4 type `type_name` and returns a [`Publisher`] for its values.
    /// `type_name` must be a type this crate models (see [`crate::type_id_for_name`]).
    pub fn publish(&self, name: &str, type_name: &str) -> Result<Publisher> {
        let type_id = type_id_for_name(type_name)
            .ok_or_else(|| Error::UnsupportedType(type_name.to_owned()))?;
        let pubuid = self.shared.next_pubuid.fetch_add(1, Ordering::Relaxed) + 1;
        self.send(Command::Publish {
            pubuid,
            name: name.to_owned(),
            type_name: type_name.to_owned(),
        })?;
        Ok(Publisher {
            handle: self.clone(),
            pubuid,
            type_id,
            name: Arc::from(name),
            type_name: Arc::from(type_name),
        })
    }

    /// Merges `update` into a topic's properties (`null` removes a key).
    pub fn set_properties(&self, name: &str, update: Properties) -> Result<()> {
        self.send(Command::SetProperties {
            name: name.to_owned(),
            update,
        })
    }

    /// The server's clock estimate in microseconds. Before the first RTT sample this is the local
    /// monotonic clock; see [`ClientHandle::time_offset_us`].
    pub fn server_time_us(&self) -> i64 {
        let local = now_micros();
        match self.time_offset_us() {
            Some(offset) => local + offset,
            None => local,
        }
    }

    /// `server_time - local_time` from the best RTT sample so far, or `None` before the first.
    pub fn time_offset_us(&self) -> Option<i64> {
        self.shared
            .synced
            .load(Ordering::Acquire)
            .then(|| self.shared.offset_us.load(Ordering::Acquire))
    }

    pub(crate) fn unsubscribe(&self, subuid: u32) -> Result<()> {
        self.send(Command::Unsubscribe { subuid })
    }

    pub(crate) fn send(&self, command: Command) -> Result<()> {
        self.tx.send(command).map_err(|_| Error::Closed)
    }
}

/// A published topic. Cheap to clone and to call [`Publisher::set`] on from any thread: a set is
/// one channel send. Dropping it does not unpublish; call [`Publisher::unpublish`].
#[derive(Clone, Debug)]
pub struct Publisher {
    handle: ClientHandle,
    pubuid: u32,
    type_id: u8,
    name: Arc<str>,
    type_name: Arc<str>,
}

impl Publisher {
    /// The topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The NT4 type string the topic was published with.
    pub fn type_name(&self) -> &str {
        &self.type_name
    }

    /// Sets the value, timestamped with the server clock estimate. Fails if the value does not
    /// fit the topic's type (for example a `Double` on an `int` topic).
    pub fn set(&self, value: Value) -> Result<()> {
        if value.type_id() != self.type_id {
            return Err(Error::TypeMismatch {
                topic: self.name.to_string(),
                expected: self.type_name.to_string(),
                got: value.default_type_name(),
            });
        }
        let timestamp_us = self.handle.server_time_us();
        self.handle.send(Command::Set {
            pubuid: self.pubuid,
            timestamp_us,
            value,
        })
    }

    /// Stops publishing the topic.
    pub fn unpublish(&self) -> Result<()> {
        self.handle.send(Command::Unpublish {
            pubuid: self.pubuid,
        })
    }
}

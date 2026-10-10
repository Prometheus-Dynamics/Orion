//! NT4 server: a WebSocket listener with a topic table, value relay, retained and persistent
//! topics, and a local API so the host app can create, edit and delete topics itself.
//!
//! ```no_run
//! # async fn demo() -> orion_nt4::Result<()> {
//! use orion_nt4::{Server, ServerConfig, Value};
//! use orion_nt4::Properties;
//!
//! let server = Server::start(ServerConfig::default()).await?; // 0.0.0.0:5810
//! let host = server.handle();
//! host.publish("/Camera/exposure", "double", Properties::new())?;
//! host.set_value("/Camera/exposure", Value::Double(20.0))?; // relayed to subscribers
//! # Ok(()) }
//! ```

mod conn;
mod persist;
mod state;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::error::{Error, Result};
use crate::message::Properties;
use crate::time::now_micros;
use crate::value::Value;
use state::{Owner, State};

/// Where the server listens and whether it persists.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    /// Listen address. The default is `0.0.0.0:5810`.
    pub bind: SocketAddr,
    /// JSON file holding every `persistent` topic. Off by default; written on each change to such
    /// a topic and restored on start.
    pub persist_path: Option<PathBuf>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from(([0, 0, 0, 0], crate::DEFAULT_PORT)),
            persist_path: None,
        }
    }
}

/// A running server. Dropping it stops the listener and closes every connection.
pub struct Server {
    handle: ServerHandle,
    local_addr: SocketAddr,
    task: JoinHandle<()>,
}

impl Server {
    /// Binds the listener and starts accepting on the current Tokio runtime.
    pub async fn start(config: ServerConfig) -> Result<Self> {
        let (events, _) = broadcast::channel(256);
        let mut state = State::new(config.persist_path.clone(), events);
        state.load_persisted()?;
        let listener = TcpListener::bind(config.bind).await?;
        let local_addr = listener.local_addr()?;
        let inner = Arc::new(Mutex::new(state));
        let task = tokio::spawn(conn::accept_loop(listener, Arc::clone(&inner)));
        Ok(Self {
            handle: ServerHandle { inner },
            local_addr,
            task,
        })
    }

    /// The local API. Cheap to clone.
    pub fn handle(&self) -> ServerHandle {
        self.handle.clone()
    }

    /// The bound address (useful with port 0).
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Who owns a topic's values, as reported by [`TopicSnapshot`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopicOwner {
    /// Retained or persistent with no publisher now.
    Unpublished,
    /// Created through the local API.
    Local,
    /// Published by the connected client with this id.
    Client(u64),
}

/// A copy of a topic's state.
#[derive(Clone, Debug, PartialEq)]
pub struct TopicSnapshot {
    /// Topic id as announced to clients.
    pub id: i32,
    /// Topic name.
    pub name: String,
    /// NT4 type string.
    pub type_name: String,
    /// Properties.
    pub properties: Properties,
    /// Last value, if any.
    pub value: Option<Value>,
    /// Server time of the last value, in microseconds.
    pub timestamp_us: Option<i64>,
    /// Who publishes the topic.
    pub owner: TopicOwner,
}

/// A connected client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientInfo {
    /// Connection id.
    pub id: u64,
    /// Name from the WebSocket path `/nt/<name>`.
    pub name: String,
    /// Active subscriptions.
    pub subscriptions: usize,
    /// Active publications.
    pub publications: usize,
}

/// Something that happened on the server, for the host app to observe.
#[derive(Clone, Debug, PartialEq)]
pub enum ServerEvent {
    /// A client connected.
    ClientConnected {
        /// Connection id.
        id: u64,
        /// Name from the WebSocket path.
        name: String,
    },
    /// A client disconnected.
    ClientDisconnected {
        /// Connection id.
        id: u64,
        /// Name from the WebSocket path.
        name: String,
    },
    /// A topic was created or taken over by a publisher.
    TopicPublished {
        /// Topic name.
        name: String,
        /// NT4 type string.
        type_name: String,
    },
    /// A topic lost its publisher or was deleted.
    TopicUnpublished {
        /// Topic name.
        name: String,
    },
    /// A new value (from the local API or a client).
    ValueChanged {
        /// Topic name.
        name: String,
        /// The value.
        value: Value,
        /// Server time, in microseconds.
        timestamp_us: i64,
        /// The client that sent it, or `None` for the local API.
        client: Option<u64>,
    },
    /// Topic properties changed.
    PropertiesChanged {
        /// Topic name.
        name: String,
        /// Changed keys.
        update: Properties,
    },
    /// A message from a client was rejected, or a frame could not be decoded.
    ProtocolWarning {
        /// The client, if known.
        client: Option<u64>,
        /// What went wrong.
        message: String,
    },
    /// The persistence file could not be written. The in-memory table is unaffected.
    PersistFailed {
        /// What went wrong.
        message: String,
    },
}

/// The local API: create, edit and delete topics and set values, from the host app.
/// Cheap to clone and safe to call from any thread; calls are short and never block on I/O
/// (except the persistence write when a persistence file is configured).
#[derive(Clone)]
pub struct ServerHandle {
    inner: Arc<Mutex<State>>,
}

impl ServerHandle {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        conn::lock(&self.inner)
    }

    /// Creates a topic owned by the host, or takes over an existing one (clients are re-announced).
    pub fn publish(&self, name: &str, type_name: &str, properties: Properties) -> Result<()> {
        self.lock()
            .publish(name, type_name, properties, Owner::Local)
    }

    /// Sets a value. Creates the topic (type from the value) if it does not exist; otherwise the
    /// value must fit the topic's type. Subscribed clients get it immediately.
    pub fn set_value(&self, name: &str, value: Value) -> Result<()> {
        let mut state = self.lock();
        let existing = state
            .topics
            .get(name)
            .map(|t| (t.type_id, t.type_name.clone()));
        match existing {
            Some((type_id, type_name)) => {
                if type_id != value.type_id() {
                    return Err(Error::TypeMismatch {
                        topic: name.to_owned(),
                        expected: type_name,
                        got: value.default_type_name(),
                    });
                }
            }
            None => {
                state.publish(
                    name,
                    value.default_type_name(),
                    Properties::new(),
                    Owner::Local,
                )?;
            }
        }
        state.store_value(name, value, None)
    }

    /// Merges into a topic's properties (`null` removes a key).
    pub fn set_properties(&self, name: &str, update: Properties) -> Result<()> {
        self.lock().update_properties(name, update)
    }

    /// Gives up the topic's publisher (kept if retained or persistent, removed otherwise).
    pub fn unpublish(&self, name: &str) -> Result<()> {
        let mut state = self.lock();
        if !state.topics.contains_key(name) {
            return Err(Error::UnknownTopic(name.to_owned()));
        }
        state.unpublish(name);
        Ok(())
    }

    /// Removes a topic and its value for everyone.
    pub fn delete(&self, name: &str) -> Result<()> {
        let mut state = self.lock();
        if !state.topics.contains_key(name) {
            return Err(Error::UnknownTopic(name.to_owned()));
        }
        state.delete(name);
        Ok(())
    }

    /// The last value of a topic.
    pub fn value(&self, name: &str) -> Option<Value> {
        self.lock()
            .topics
            .get(name)
            .and_then(|t| t.value.as_ref().map(|(_, v)| v.clone()))
    }

    /// A copy of one topic's state.
    pub fn topic(&self, name: &str) -> Option<TopicSnapshot> {
        let state = self.lock();
        state.topics.get(name).map(|t| state.snapshot(name, t))
    }

    /// Copies of every topic, sorted by name.
    pub fn topics(&self) -> Vec<TopicSnapshot> {
        let state = self.lock();
        state
            .topics
            .iter()
            .map(|(name, t)| state.snapshot(name, t))
            .collect()
    }

    /// The connected clients.
    pub fn clients(&self) -> Vec<ClientInfo> {
        self.lock().client_infos()
    }

    /// A stream of [`ServerEvent`]s. Only events after the call are delivered; a receiver that
    /// falls behind by more than 256 events gets a lag error.
    pub fn events(&self) -> broadcast::Receiver<ServerEvent> {
        self.lock().event_receiver()
    }

    /// The server's clock in microseconds, the same clock as value timestamps.
    pub fn now_us(&self) -> i64 {
        now_micros()
    }
}

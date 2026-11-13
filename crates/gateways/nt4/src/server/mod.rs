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
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::error::{Error, Result};
use crate::message::Properties;
use crate::time::now_micros;
use crate::value::Value;
use state::{Owner, State};

/// How long a change to a persistent topic may wait before the file is rewritten. A burst of
/// changes inside one window costs one write.
const PERSIST_DEBOUNCE: Duration = Duration::from_secs(1);

/// Where the server listens and whether it persists.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    /// Listen address. The default is `0.0.0.0:5810`.
    pub bind: SocketAddr,
    /// Whether the server has a port of its own at [`ServerConfig::bind`]. Without one it
    /// serves only the connections handed to [`Server::take`] by a listener it shares.
    /// Default on.
    pub listen: bool,
    /// JSON file holding every `persistent` topic. Off by default. Changes to such a topic are
    /// written (compact JSON, synced to disk) within about a second, and any pending change is
    /// written when the [`Server`] is dropped. Restored on start.
    pub persist_path: Option<PathBuf>,
    /// A client connection that sends nothing for this long is closed. The server sends a
    /// WebSocket ping every [`ServerConfig::ping_interval`], which every client answers, so
    /// silence means the client's link is gone. It also bounds unacknowledged writes
    /// (`TCP_USER_TIMEOUT` on Linux). Default 3 s.
    pub client_timeout: Duration,
    /// How often each client is sent a WebSocket ping. Default 1 s.
    pub ping_interval: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from(([0, 0, 0, 0], crate::DEFAULT_PORT)),
            listen: true,
            persist_path: None,
            client_timeout: Duration::from_secs(3),
            ping_interval: Duration::from_secs(1),
        }
    }
}

/// A running server. Dropping it stops the listener and closes every connection.
pub struct Server {
    handle: ServerHandle,
    local_addr: SocketAddr,
    /// Connections upgraded elsewhere, on their way to the accept loop.
    taken: tokio::sync::mpsc::UnboundedSender<orion_ws::WsSocket>,
    task: JoinHandle<()>,
    persister: Option<JoinHandle<()>>,
    /// Serialises file writes between the persister and the drop-time flush.
    writes: Arc<Mutex<()>>,
}

/// Hands connections upgraded elsewhere to a [`Server`]: see [`Server::taker`]. Once the
/// server is gone, what is handed is dropped.
#[derive(Clone, Debug)]
pub struct Taker(tokio::sync::mpsc::UnboundedSender<orion_ws::WsSocket>);

impl Taker {
    /// As [`Server::take`].
    pub fn take(&self, socket: orion_ws::WsSocket) {
        let _ = self.0.send(socket);
    }
}

impl Server {
    /// Binds the listener and starts accepting on the current Tokio runtime.
    pub async fn start(config: ServerConfig) -> Result<Self> {
        let (events, _) = broadcast::channel(256);
        let mut state = State::new(config.persist_path.clone(), events);
        state.load_persisted()?;
        let listener = if config.listen {
            Some(TcpListener::bind(config.bind).await?)
        } else {
            None
        };
        let local_addr = match &listener {
            Some(listener) => listener.local_addr()?,
            None => config.bind,
        };
        let persist_enabled = config.persist_path.is_some();
        let inner = Arc::new(Mutex::new(state));
        let writes = Arc::new(Mutex::new(()));
        let liveness = conn::Liveness {
            timeout: config.client_timeout.max(Duration::from_millis(10)),
            ping_every: config.ping_interval.max(Duration::from_millis(10)),
        };
        let (taken, sockets) = tokio::sync::mpsc::unbounded_channel();
        let task = tokio::spawn(conn::accept_loop(
            listener,
            sockets,
            Arc::clone(&inner),
            liveness,
        ));
        let persister = persist_enabled
            .then(|| tokio::spawn(persist_loop(Arc::clone(&inner), Arc::clone(&writes))));
        Ok(Self {
            handle: ServerHandle { inner },
            local_addr,
            taken,
            task,
            persister,
            writes,
        })
    }

    /// The local API. Cheap to clone.
    pub fn handle(&self) -> ServerHandle {
        self.handle.clone()
    }

    /// Serves a client whose connection was upgraded elsewhere: by a WebSocket listener this
    /// server shares with other protocols, which hands over the connections that speak
    /// [`crate::SUBPROTOCOL_V4_1`] or [`crate::SUBPROTOCOL_V4_0`]. The client's name is the
    /// path it asked for after `/nt/`.
    pub fn take(&self, socket: orion_ws::WsSocket) {
        // The accept loop lives as long as the server does.
        let _ = self.taken.send(socket);
    }

    /// What hands connections to this server from a task of its own: [`Server::take`] that
    /// can be cloned and kept.
    pub fn taker(&self) -> Taker {
        Taker(self.taken.clone())
    }

    /// The bound address (useful with port 0). With no port of its own, the address it was
    /// given.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
        if let Some(persister) = &self.persister {
            persister.abort();
        }
        // Flush a change the persister has not written yet. The state lock is released before the
        // write gate is taken, as in the persister, so the two never wait on each other.
        let job = conn::lock(&self.handle.inner).take_persist_job();
        if let Some((path, bytes)) = job {
            let _gate = self.writes.lock().unwrap_or_else(PoisonError::into_inner);
            if let Err(e) = persist::write_atomic(&path, &bytes) {
                conn::lock(&self.handle.inner).persist_failed(e.to_string());
            }
        }
    }
}

/// Rewrites the persistence file once per [`PERSIST_DEBOUNCE`] while a change is pending.
async fn persist_loop(inner: Arc<Mutex<State>>, writes: Arc<Mutex<()>>) {
    let mut ticker = tokio::time::interval(PERSIST_DEBOUNCE);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let Some((path, bytes)) = conn::lock(&inner).take_persist_job() else {
            continue;
        };
        let gate = Arc::clone(&writes);
        let result = tokio::task::spawn_blocking(move || {
            let _gate = gate.lock().unwrap_or_else(PoisonError::into_inner);
            persist::write_atomic(&path, &bytes)
        })
        .await;
        let failure = match result {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(e.to_string()),
            Err(e) => Some(e.to_string()),
        };
        if let Some(message) = failure {
            conn::lock(&inner).persist_failed(message);
        }
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

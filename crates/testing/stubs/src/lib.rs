//! A comm core for tests.
//!
//! The client is built before node logic is, and its tests need something to talk to.
//! [`Board::start`] runs a real comm core in its own thread: the real server, the real topic
//! broker with its add-ons, the real hello and keys, on a real socket and a real network
//! port. What belongs to node logic is stood in for by the smallest thing that speaks the
//! wire: a record [`store`], an action [`router`], a file [`receiver`] and a workload
//! [`tracker`]. Each keeps to what the wire and the client's briefs say of the real one.

#![cfg(target_os = "linux")]

pub mod older;
pub mod receiver;
pub mod router;
pub mod store;
pub mod tracker;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::{Arc, mpsc};
use std::time::Duration;

use orion_broker::{Broker, BrokerConfig};
use orion_broker_buffers::BrokerBuffers;
use orion_broker_proxy::BrokerProxy;
use orion_device_gateway::DeviceGateway;
use orion_keys::{Keys, KeysConfig, KeysService, SharedKeys};
use orion_link_serial::{Serial, StreamCarrier};
use orion_server::{Admits, Comm, Limits, Policy};
use orion_tcp::{TcpCarrier, TcpOptions};
use orion_unix::UnixCarrier;

pub use orion_server::Security;
pub use receiver::Shelf;

/// How a [`Board`] starts: [`Board::start_with`].
#[derive(Clone, Debug)]
pub struct Options {
    /// The security switch. Off by default.
    pub security: Security,
    /// The board's name. `board` by default.
    pub name: String,
    /// The network port, if it is to come back where it was. Any free one by default.
    pub port: Option<u16>,
    /// No stand-ins for node logic: the test runs node logic, or is refused. Off by default:
    /// the store, router, receiver and tracker are stood in for, as [`Board::start`] does.
    pub for_node: bool,
    /// Serial links: a device on the other end of one is a resource of the board.
    pub links: Vec<PathBuf>,
    /// The board's key and trust file kept in `dir/state`, where node logic reads them.
    pub on_disk: bool,
    /// `comm.heartbeat_ms`: how often a session is asked to ping. 2000 by default.
    pub heartbeat_ms: u32,
    /// `comm.idle_ms`: how long a session may go unheard, either way, before it is over. Zero,
    /// the default, is three heartbeats; otherwise at least two.
    pub idle_ms: u32,
    /// `enrollment.key`: what an operator or a node proves it knows to enroll itself. None by
    /// default: only someone on the board enrolls anyone.
    pub enrollment_key: Option<Vec<u8>>,
    /// The additive level its welcome names: this build's by default, lower to stand in for
    /// an older node.
    pub level: u16,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            security: Security::Off,
            name: "board".to_owned(),
            port: None,
            for_node: false,
            links: Vec::new(),
            on_disk: false,
            heartbeat_ms: Limits::default().heartbeat_ms,
            idle_ms: 0,
            enrollment_key: None,
            level: Limits::default().level,
        }
    }
}

/// A comm core running in its own thread until it is dropped.
#[derive(Debug)]
pub struct Board {
    socket: PathBuf,
    network: SocketAddr,
    websocket: SocketAddr,
    shelf: Arc<Shelf>,
    keys: SharedKeys,
    dir: PathBuf,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Board {
    /// Starts a board named `board` in a directory of its own under the system's temporary
    /// one, with security off. `name` keeps tests apart.
    ///
    /// # Panics
    ///
    /// If the board cannot start: a test has nothing to do then.
    #[must_use]
    pub fn start(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("orion-stubs-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("the board's directory");
        Self::start_in(&dir, Security::Off, None)
    }

    /// Starts a board in `dir`, on `port` if it is to come back where it was.
    ///
    /// # Panics
    ///
    /// If the board cannot start.
    #[must_use]
    pub fn start_in(dir: &Path, security: Security, port: Option<u16>) -> Self {
        Self::start_as(dir, security, port, false)
    }

    /// Starts a board with no stand-in for the record store, the action router, the file receiver or the workload
    /// tracker: those are passed to node
    /// logic, which the user running the test may be. Without node logic they are refused.
    ///
    /// # Panics
    ///
    /// If the board cannot start.
    #[must_use]
    pub fn start_for_node(dir: &Path) -> Self {
        Self::start_as(dir, Security::Off, None, true)
    }

    /// As [`Board::start_for_node`], with the security switch where the test wants it.
    ///
    /// # Panics
    ///
    /// If the board cannot start.
    #[must_use]
    pub fn start_for_node_with(dir: &Path, security: Security) -> Self {
        Self::start_as(dir, security, None, true)
    }

    /// As [`Board::start_for_node_with`], for a board with another name than `board`, on
    /// `port` if it is to come back where it was: one of several in a test. Its key and trust
    /// file are kept in `dir/state`, where node logic reads them.
    ///
    /// # Panics
    ///
    /// If the board cannot start.
    #[must_use]
    pub fn start_named(dir: &Path, security: Security, name: &str, port: Option<u16>) -> Self {
        Self::start_with(
            dir,
            Options {
                security,
                name: name.to_owned(),
                port,
                for_node: true,
                on_disk: true,
                ..Options::default()
            },
        )
    }

    /// Starts a board in `dir` as `options` say: any of the others, with the heartbeat and how
    /// long a session may go unheard set too. A test on a loaded machine sets these long, so a
    /// slow moment is not taken for a lost session.
    ///
    /// # Panics
    ///
    /// If the board cannot start.
    #[must_use]
    pub fn start_with(dir: &Path, options: Options) -> Self {
        Self::start_all(dir, options)
    }

    /// As [`Board::start_for_node`], with a serial link on each of `links`: a device on the
    /// other end of one is a resource of the board.
    ///
    /// # Panics
    ///
    /// If the board cannot start.
    #[must_use]
    pub fn start_for_node_with_links(dir: &Path, links: &[PathBuf]) -> Self {
        Self::start_all(
            dir,
            Options {
                links: links.to_vec(),
                for_node: true,
                ..Options::default()
            },
        )
    }

    fn start_as(dir: &Path, security: Security, port: Option<u16>, for_node: bool) -> Self {
        Self::start_all(
            dir,
            Options {
                security,
                port,
                for_node,
                ..Options::default()
            },
        )
    }

    fn start_all(dir: &Path, options: Options) -> Self {
        let Options {
            security,
            name,
            port,
            for_node,
            links,
            on_disk,
            heartbeat_ms,
            idle_ms,
            enrollment_key,
            level,
        } = options;
        use std::os::unix::fs::MetadataExt;
        // Whoever owns the directory runs the test, and may be the node logic.
        let node_uids: Vec<u32> = std::fs::metadata(dir)
            .ok()
            .filter(|_| for_node)
            .map(|metadata| metadata.uid())
            .into_iter()
            .collect();
        let socket = dir.join("orion.sock");
        let shelf = Arc::new(Shelf::default());
        // A named board keeps its key and trust file in `dir/state`, as a comm core does: node
        // logic reads them there, and the key stays the same across a restart.
        let keys_config = KeysConfig {
            state_dir: on_disk.then(|| dir.join("state")),
            enrollment_key,
            ..KeysConfig::in_memory("lab")
        };
        let keys = SharedKeys::new(Keys::open(keys_config, security).expect("keys"));
        let (ready, started) = mpsc::channel();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let (path, on_shelf, board_keys) = (socket.clone(), shelf.clone(), keys.clone());
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            tokio::task::LocalSet::new().block_on(&runtime, async move {
                let node: orion_types::NodeName = name.parse().expect("name");
                let limits = Limits {
                    outbox_bytes: 8 << 20,
                    heartbeat_ms,
                    idle_ms,
                    level,
                    ..Limits::default()
                };
                let comm = Comm::new(
                    node.clone(),
                    security,
                    Some(Rc::new(board_keys.clone())),
                    limits,
                );
                // Devices on links, as resources.
                let devices = DeviceGateway::start(&comm, node.clone());
                let anyone = Policy::new(Admits::ANYONE).without_grant();
                let config = BrokerConfig {
                    declare: anyone,
                    subscribe: anyone,
                    ..BrokerConfig::default()
                };
                let broker = Broker::new(node.clone(), config);
                // Samples for subscribers on the network, which cannot map a ring.
                let _outbound = orion_outbound::Outbound::start(
                    &comm,
                    &broker,
                    orion_outbound::OutboundLimits::default(),
                )
                .expect("outbound");
                comm.add_service(BrokerProxy::new(&broker, anyone));
                comm.add_service(BrokerBuffers::new(&broker, anyone));
                comm.add_service(orion_hello::Hello::new(
                    node.clone(),
                    board_keys.clone(),
                    Some(broker.topics()),
                ));
                comm.add_service(KeysService::new(node.clone(), board_keys.clone()));
                comm.add_service(devices.watch(broker));
                // The clock is answered for, and nothing more: no sync, nothing served.
                let _time = orion_time::TimeSync::start(
                    &comm,
                    None,
                    orion_time::TimeConfig::default(),
                    None,
                )
                .expect("time");
                if for_node {
                    comm.add_service(
                        devices.watch(orion_relay::Relay::new(&orion_relay::routes())),
                    );
                } else {
                    comm.add_service(store::Store::default());
                    comm.add_service(devices.watch(router::Router::default()));
                }
                if !for_node {
                    comm.add_service(receiver::Receiver::new(on_shelf));
                }
                if !for_node {
                    comm.add_service(tracker::Tracker::new(node));
                }
                let unix = UnixCarrier::bind(&path, Some(0o600), &node_uids).expect("socket");
                let options = TcpOptions {
                    security,
                    keys: board_keys,
                    max_frame: 64 * 1024,
                    handshake: Duration::from_secs(5),
                };
                let listen = SocketAddr::from(([127, 0, 0, 1], port.unwrap_or(0)));
                let tcp = TcpCarrier::bind(listen, options).await.expect("listen");
                // And the way in for browsers.
                let mut pages = orion_ws::WsListener::bind(
                    SocketAddr::from(([127, 0, 0, 1], 0)),
                    orion_ws::WsOptions::default(),
                )
                .await
                .expect("listen for browsers");
                let ws = orion_ws::WsCarrier::on(&mut pages);
                ready
                    .send((
                        tcp.local_addr().expect("addr"),
                        pages.local_addr().expect("addr"),
                    ))
                    .expect("ready");
                tokio::task::spawn_local(pages.run());
                let (on_socket, on_network, on_pages) = (comm.clone(), comm.clone(), comm);
                for (n, path) in links.into_iter().enumerate() {
                    let name = format!("link{n}").parse().expect("a link's name");
                    let carrier = StreamCarrier::new(
                        name,
                        Serial {
                            path,
                            baud: 115_200,
                        },
                    );
                    let serving = on_socket.clone();
                    tokio::task::spawn_local(async move { serving.serve(carrier).await });
                }
                tokio::task::spawn_local(async move { on_socket.serve(unix).await });
                tokio::task::spawn_local(async move { on_network.serve(tcp).await });
                tokio::task::spawn_local(async move { on_pages.serve(ws).await });
                let _ = stopped.await;
            });
        });
        let (network, websocket) = started.recv().expect("the board starts");
        Self {
            socket,
            network,
            websocket,
            shelf,
            keys,
            dir: dir.to_owned(),
            stop: Some(stop),
            thread: Some(thread),
        }
    }

    /// The board's socket.
    #[must_use]
    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// Where the board listens on the network.
    #[must_use]
    pub const fn network(&self) -> SocketAddr {
        self.network
    }

    /// Where the board listens for browsers: a WebSocket.
    #[must_use]
    pub const fn websocket(&self) -> SocketAddr {
        self.websocket
    }

    /// The board's directory.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// What the file receiver has in place and can fetch.
    #[must_use]
    pub fn shelf(&self) -> &Shelf {
        &self.shelf
    }

    /// The board's identity and whom it trusts.
    #[must_use]
    pub const fn keys(&self) -> &SharedKeys {
        &self.keys
    }

    /// Stops the comm core. Its directory stays, for a board started in it again.
    pub fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for Board {
    fn drop(&mut self) {
        self.stop();
    }
}

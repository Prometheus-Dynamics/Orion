//! The comm core, assembled.
//!
//! The comm core is the always-up half of Orion: the only program with a door to the outside.
//! It owns the board's socket, the network way in, the links, the topic broker, the keys, the
//! hello document and the clock. Node logic and every other program connect to it, on the same
//! wire as everyone else. It runs with nothing else on the board.
//!
//! [`CommCore::start`] puts the parts together from one [`CommSettings`]. Each part beyond the
//! server, the socket, the broker, the keys and hello is a cargo feature, so an image carries
//! only what it uses. [`run`] is the whole program: it starts the comm core, tells systemd it
//! is ready, keeps saying it is alive, and stops when asked.

#![cfg(target_os = "linux")]

#[cfg(feature = "ssh")]
mod helper;
#[cfg(feature = "recovery")]
mod recover;
mod settings;
#[cfg(feature = "systemd")]
pub mod systemd;

use std::cell::RefCell;
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::rc::Rc;
#[cfg(any(feature = "tcp", feature = "systemd"))]
use std::time::Duration;

use orion_broker::{Broker, Topics};
use orion_hello::Hello;
use orion_keys::{Keys, KeysService, SharedKeys};
use orion_server::Comm;
use orion_unix::UnixCarrier;
use tokio::task::JoinHandle;
use tracing::info;

#[cfg(feature = "ssh")]
pub use helper::helper;
#[cfg(feature = "recovery")]
pub use recover::{RecoverWith, Started, recover, run_or_recover, start_or_recover};
pub use settings::CommSettings;
#[cfg(feature = "link-sealed")]
pub use settings::SealedLink;

/// Why the comm core could not start.
#[derive(Debug)]
pub struct StartError(String);

impl fmt::Display for StartError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for StartError {}

fn failed(what: impl fmt::Display, why: impl fmt::Display) -> StartError {
    StartError(format!("{what}: {why}"))
}

/// A running comm core. Dropping it stops it.
pub struct CommCore {
    comm: Comm,
    keys: SharedKeys,
    topics: Rc<RefCell<Topics>>,
    socket: PathBuf,
    peer_addr: Option<SocketAddr>,
    tasks: Vec<JoinHandle<()>>,
    parts: Vec<&'static str>,
    /// Kept so that it runs for as long as the comm core does.
    #[cfg(feature = "outbound")]
    _outbound: orion_outbound::Outbound,
    /// The SSH way in, if it is open.
    #[cfg(feature = "ssh")]
    ssh: Option<SshServer>,
    /// The NT4 gateway, if it runs.
    #[cfg(feature = "nt4")]
    nt4: Option<orion_nt4_gateway::Gateway>,
    /// The NT4 gateway in its client form, if it runs.
    #[cfg(feature = "nt4-client")]
    nt4_client: Option<orion_nt4_client_gateway::ClientGateway>,
    /// Kept for the same reason.
    #[cfg(feature = "topic-peers")]
    _topic_peers: orion_topic_peers::TopicPeers,
    #[cfg(feature = "self-status")]
    status: orion_self_status::SelfStatus,
    #[cfg(feature = "time")]
    time: Option<orion_time::TimeSync>,
    #[cfg(feature = "discovery")]
    discovery: Option<orion_discovery::Discovery>,
}

/// The SSH way in, as it was asked for.
#[cfg(feature = "ssh")]
enum SshServer {
    /// In the comm core's own process.
    Plain(orion_ssh::Ssh),
    /// With a child for each connection.
    #[cfg(feature = "ssh-sandbox")]
    Sandboxed(orion_ssh_sandbox::SandboxedSsh),
}

impl fmt::Debug for CommCore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CommCore({})", self.summary())
    }
}

impl CommCore {
    /// Starts every part the settings ask for. Must be called inside a `LocalSet` on a
    /// current-thread runtime: the comm core is one thread.
    ///
    /// # Errors
    ///
    /// The first part that could not start, and why. Nothing is left running.
    #[allow(clippy::too_many_lines)]
    pub async fn start(settings: CommSettings) -> Result<Self, StartError> {
        let node = settings.config.node_name.clone();
        let security = settings.config.security;
        let keys = SharedKeys::new(
            Keys::open(settings.keys.clone(), security).map_err(|error| failed("keys", error))?,
        );
        let comm = Comm::new(
            node.clone(),
            security,
            Some(Rc::new(keys.clone())),
            settings.limits,
        );
        let broker = Broker::new(node.clone(), settings.broker);
        let topics = broker.topics();
        let mut tasks = Vec::new();
        // Which of these grow depends on the build.
        #[allow(unused_mut)]
        let mut parts = vec!["socket", "topics", "keys", "hello"];
        let serve = |tasks: &mut Vec<JoinHandle<()>>, carrier| {
            let comm = comm.clone();
            tasks.push(tokio::task::spawn_local(async move {
                comm.serve(carrier).await
            }));
        };

        #[cfg(feature = "self-status")]
        let status = {
            parts.push("status");
            orion_self_status::SelfStatus::start(
                &comm,
                &broker,
                orion_self_status::ProcSource::new(),
                settings.status_interval,
            )
            .map_err(|error| failed("status topics", error))?
        };
        #[cfg(feature = "time")]
        let time = if settings.time.enabled() {
            parts.push("time");
            let started = orion_time::TimeSync::start(
                &comm,
                Some(&topics),
                settings.time.clone(),
                Some(&settings.config.state_dir),
            );
            Some(started.map_err(|error| failed("time sync", error))?)
        } else {
            None
        };
        #[cfg(feature = "outbound")]
        let outbound = {
            parts.push("outbound");
            orion_outbound::Outbound::start(&comm, &broker, settings.outbound)
                .map_err(|error| failed("outbound samples", error))?
        };
        #[cfg(feature = "proxy")]
        comm.add_service(orion_broker_proxy::BrokerProxy::new(
            &broker,
            settings.broker.declare,
        ));
        #[cfg(feature = "buffers")]
        comm.add_service(orion_broker_buffers::BrokerBuffers::new(
            &broker,
            settings.broker.declare,
        ));
        // Devices on links, each a resource with what it declared and registered.
        #[cfg(feature = "devices")]
        let devices = {
            parts.push("devices");
            orion_device_gateway::DeviceGateway::start(&comm, node.clone())
        };
        #[cfg(feature = "node")]
        {
            parts.push("node");
            let relay = orion_relay::Relay::new(&orion_relay::routes());
            #[cfg(feature = "devices")]
            comm.add_service(devices.watch(relay));
            #[cfg(not(feature = "devices"))]
            comm.add_service(relay);
        }
        comm.add_service(Hello::new(node.clone(), keys.clone(), Some(topics.clone())));
        comm.add_service(KeysService::new(node.clone(), keys.clone()));
        #[cfg(feature = "devices")]
        comm.add_service(devices.watch(broker));
        #[cfg(not(feature = "devices"))]
        comm.add_service(broker);

        // Where a link says how it is doing: a topic when there are status topics.
        #[cfg(any(feature = "link-serial", feature = "link-can"))]
        let link_state = |name: &str| -> Result<LinkState, StartError> {
            #[cfg(feature = "self-status")]
            return status
                .part(&format!("link.{name}"))
                .map(LinkState::Topic)
                .map_err(|error| failed(format!("link {name}"), error));
            #[cfg(not(feature = "self-status"))]
            {
                let _ = name;
                Ok(LinkState::Log)
            }
        };
        #[cfg(feature = "link-serial")]
        for link in &settings.serial_links {
            parts.push("serial link");
            let carrier =
                orion_link_serial::StreamCarrier::new(link.name.clone(), link.serial.clone())
                    .reporting(link_state(link.name.as_str())?);
            serve(&mut tasks, Carriers::Serial(carrier));
        }
        #[cfg(feature = "link-sealed")]
        for link in &settings.sealed_links {
            parts.push("sealed link");
            let listener = orion_link_serial::sealed::SealedListener::bind(
                link.listen,
                node.as_str(),
                keys.clone(),
            )
            .map_err(|error| failed(format!("links.sealed {}", link.listen), error))?;
            let carrier = orion_link_serial::StreamCarrier::new(link.name.clone(), listener)
                .reporting(link_state(link.name.as_str())?);
            serve(&mut tasks, Carriers::Sealed(carrier));
        }
        #[cfg(feature = "link-usb")]
        if !settings.usb_links.is_empty() {
            let links = orion_link_usb::UsbLinks::new();
            comm.add_service(links.service());
            for name in &settings.usb_links {
                parts.push("usb link");
                let device = name
                    .parse()
                    .map_err(|error| failed(format!("links.usb {name}"), error))?;
                let carrier = orion_link_serial::StreamCarrier::new(device, links.link(name))
                    .reporting(link_state(name)?);
                serve(&mut tasks, Carriers::Usb(carrier));
            }
        }
        #[cfg(feature = "link-can")]
        for bus in &settings.can_links {
            parts.push("can bus");
            let open = orion_link_can::SocketCanBus {
                interface: bus.interface.clone(),
                fd: bus.fd,
            };
            let carrier = orion_link_can::CanCarrier::new(bus, open)
                .map_err(|error| failed("links.can", error))?
                .reporting(link_state(&bus.name)?);
            serve(&mut tasks, Carriers::Can(carrier));
        }

        // The board's own socket: where every program on it connects.
        let socket = settings.config.socket.clone();
        if let Some(dir) = socket.parent() {
            // Under systemd the runtime directory is already there, with its own owner.
            let _ = std::fs::create_dir_all(dir);
        }
        let unix = UnixCarrier::bind(&socket, Some(settings.socket_mode), &settings.node_uids)
            .map_err(|error| failed(format!("socket {}", socket.display()), error))?;
        serve(&mut tasks, Carriers::Unix(unix));

        // The network way in.
        #[allow(unused_mut)]
        let mut peer_addr = None;
        #[cfg(feature = "tcp")]
        let tcp_options = orion_tcp::TcpOptions {
            security,
            keys: keys.clone(),
            max_frame: settings.limits.max_frame,
            handshake: Duration::from_millis(u64::from(settings.limits.hello_ms)),
        };
        // Other nodes' topics, for programs here that ask for them.
        #[cfg(feature = "topic-peers")]
        let topic_peers = {
            parts.push("topics of other nodes");
            let pinned = keys.clone();
            let table = orion_topic_peers::Table::new(settings.peer_nodes.clone(), move |node| {
                pinned.lock().peer_key(node).copied()
            });
            orion_topic_peers::TopicPeers::start(&comm, &topics, tcp_options.clone(), table)
        };
        #[cfg(feature = "tcp")]
        if let Some(listen) = settings.config.peer_listen {
            parts.push("network");
            let carrier = orion_tcp::TcpCarrier::bind(listen, tcp_options.clone())
                .await
                .map_err(|error| failed(format!("peer.listen {listen}"), error))?;
            peer_addr = carrier.local_addr().ok();
            serve(&mut tasks, Carriers::Tcp(carrier));
        }
        #[cfg(not(feature = "tcp"))]
        if settings.config.peer_listen.is_some() {
            return Err(failed("peer.listen", "this build has no network way in"));
        }

        // The WebSocket way in, for browsers. Its listener is one other WebSocket protocols
        // can share.
        #[cfg(feature = "ws")]
        if let Some(listen) = settings.ws_listen {
            parts.push("websocket");
            let options = orion_ws::WsOptions {
                origins: settings.ws_origins.clone(),
                handshake: Duration::from_millis(u64::from(settings.limits.hello_ms)),
                send_buffer: settings.ws_send_buffer,
            };
            let mut listener = orion_ws::WsListener::bind(listen, options)
                .await
                .map_err(|error| failed(format!("ws.listen {listen}"), error))?;
            let carrier = orion_ws::WsCarrier::on(&mut listener);
            tasks.push(tokio::task::spawn_local(listener.run()));
            serve(&mut tasks, Carriers::Ws(carrier));
        }

        // The SSH way in: logins decided by the same check, with the same trust store.
        #[cfg(feature = "ssh")]
        let ssh = match settings.ssh.clone() {
            Some(config) => {
                parts.push("ssh");
                let listen = config.listen;
                // In a child for each connection, unless that is turned off.
                #[cfg(feature = "ssh-sandbox")]
                let server = match settings.ssh_sandbox.clone() {
                    Some(sandbox) => {
                        let sandboxed = orion_ssh_sandbox::SandboxedSsh::start(
                            config.clone(),
                            sandbox,
                            security,
                            keys.clone(),
                        )
                        .await;
                        match sandboxed {
                            Ok(server) => Ok(SshServer::Sandboxed(server)),
                            // An image with no account for the children to become.
                            // Reading in this process lets in nobody it would not have:
                            // a way in is worth more than none. An address that cannot
                            // be listened on is not helped by trying again without.
                            Err(error) if error.kind() == std::io::ErrorKind::Other => {
                                tracing::warn!(
                                    "ssh: no child processes for connections ({error}): they \
                                     are read in the comm core itself"
                                );
                                orion_ssh::Ssh::start(config, security, keys.clone())
                                    .await
                                    .map(SshServer::Plain)
                            }
                            Err(error) => Err(error),
                        }
                    }
                    None => orion_ssh::Ssh::start(config, security, keys.clone())
                        .await
                        .map(SshServer::Plain),
                };
                #[cfg(not(feature = "ssh-sandbox"))]
                let server = orion_ssh::Ssh::start(config, security, keys.clone())
                    .await
                    .map(SshServer::Plain);
                Some(server.map_err(|error| failed(format!("ssh.listen {listen}"), error))?)
            }
            None => None,
        };

        // The NT4 gateway: its own port, and a reader of the topics its mapping names.
        #[cfg(feature = "nt4")]
        let nt4 = match settings.nt4.clone() {
            Some(config) => {
                parts.push("nt4");
                let listen = config.listen;
                Some(
                    orion_nt4_gateway::Gateway::start(&topics, config)
                        .await
                        .map_err(|error| failed(format!("nt4.listen {listen}"), error))?,
                )
            }
            None => None,
        };

        // The same mapping against an NT4 server somewhere else.
        #[cfg(feature = "nt4-client")]
        let nt4_client = match settings.nt4_client.clone() {
            Some(config) => {
                parts.push("nt4 client");
                let server = format!("nt4.server {}:{}", config.host, config.port);
                Some(
                    orion_nt4_client_gateway::ClientGateway::start(&topics, config)
                        .map_err(|error| failed(server, error))?,
                )
            }
            None => None,
        };

        #[cfg(feature = "discovery")]
        let discovery = match (settings.discovery, peer_addr) {
            (true, Some(addr)) => {
                parts.push("discovery");
                let config = orion_discovery::DiscoveryConfig {
                    node: node.clone(),
                    port: addr.port(),
                    addresses: if addr.ip().is_unspecified() {
                        Vec::new()
                    } else {
                        vec![addr.ip()]
                    },
                    ttl: settings.discovery_ttl,
                    tcp: tcp_options.clone(),
                };
                let started = orion_discovery::Discovery::start(
                    config,
                    discovery_backend(&settings)?,
                    |event| match event {
                        orion_discovery::PeerEvent::Present(peer) => {
                            info!(node = %peer.node, addrs = ?peer.addrs, "peer present");
                        }
                        orion_discovery::PeerEvent::Gone(node) => info!(%node, "peer gone"),
                    },
                );
                Some(started.map_err(|error| failed("discovery", error))?)
            }
            _ => None,
        };

        let core = Self {
            comm,
            keys,
            topics,
            socket,
            peer_addr,
            tasks,
            parts,
            #[cfg(feature = "outbound")]
            _outbound: outbound,
            #[cfg(feature = "ssh")]
            ssh,
            #[cfg(feature = "nt4")]
            nt4,
            #[cfg(feature = "nt4-client")]
            nt4_client,
            #[cfg(feature = "topic-peers")]
            _topic_peers: topic_peers,
            #[cfg(feature = "self-status")]
            status,
            #[cfg(feature = "time")]
            time,
            #[cfg(feature = "discovery")]
            discovery,
        };
        info!(node = %node, security = ?security, "comm core started: {}", core.summary());
        Ok(core)
    }

    /// The server, for adding what this assembly does not know about.
    #[must_use]
    pub const fn comm(&self) -> &Comm {
        &self.comm
    }

    /// The node's identity and whom it trusts.
    #[must_use]
    pub const fn keys(&self) -> &SharedKeys {
        &self.keys
    }

    /// The topic table.
    #[must_use]
    pub const fn topics(&self) -> &Rc<RefCell<Topics>> {
        &self.topics
    }

    /// The board's socket.
    #[must_use]
    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// Where other nodes and people's tools connect, if anywhere.
    #[must_use]
    pub const fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer_addr
    }

    /// The SSH way in, if it is open.
    #[cfg(feature = "ssh")]
    #[must_use]
    pub fn ssh_addr(&self) -> Option<SocketAddr> {
        self.ssh.as_ref().map(|server| match server {
            SshServer::Plain(server) => server.local_addr(),
            #[cfg(feature = "ssh-sandbox")]
            SshServer::Sandboxed(server) => server.local_addr(),
        })
    }

    /// The NT4 gateway, if it runs.
    #[cfg(feature = "nt4")]
    #[must_use]
    pub const fn nt4(&self) -> Option<&orion_nt4_gateway::Gateway> {
        self.nt4.as_ref()
    }

    /// The NT4 gateway in its client form, if it runs.
    #[cfg(feature = "nt4-client")]
    #[must_use]
    pub const fn nt4_client(&self) -> Option<&orion_nt4_client_gateway::ClientGateway> {
        self.nt4_client.as_ref()
    }

    /// The board's status topics.
    #[cfg(feature = "self-status")]
    #[must_use]
    pub const fn status(&self) -> &orion_self_status::SelfStatus {
        &self.status
    }

    /// The time sync, if it runs.
    #[cfg(feature = "time")]
    #[must_use]
    pub const fn time(&self) -> Option<&orion_time::TimeSync> {
        self.time.as_ref()
    }

    /// Discovery, if it runs.
    #[cfg(feature = "discovery")]
    #[must_use]
    pub const fn discovery(&self) -> Option<&orion_discovery::Discovery> {
        self.discovery.as_ref()
    }

    /// What is running, in a line: for the log and for `systemctl status`.
    #[must_use]
    pub fn summary(&self) -> String {
        let mut line = format!("serving {}", self.socket.display());
        if let Some(addr) = self.peer_addr {
            line.push_str(&format!(" and {addr}"));
        }
        line.push_str(&format!(" ({})", self.parts.join(", ")));
        line
    }
}

impl Drop for CommCore {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// Where a link's state goes.
#[cfg(any(feature = "link-serial", feature = "link-can"))]
enum LinkState {
    #[cfg(feature = "self-status")]
    Topic(orion_self_status::Part),
    #[cfg(not(feature = "self-status"))]
    Log,
}

#[cfg(any(feature = "link-serial", feature = "link-can"))]
impl orion_supervise::Report for LinkState {
    fn report(&mut self, status: &orion_supervise::Status) {
        match self {
            #[cfg(feature = "self-status")]
            Self::Topic(part) => part.report(status),
            #[cfg(not(feature = "self-status"))]
            Self::Log => tracing::debug!(?status, "link state"),
        }
    }
}

#[cfg(feature = "discovery")]
fn discovery_backend(
    settings: &CommSettings,
) -> Result<Box<dyn orion_discovery::DiscoveryBackend>, StartError> {
    #[cfg(feature = "mdns")]
    return Ok(Box::new(orion_discovery::MdnsDiscoveryBackend::new(
        settings.discovery_interfaces.clone(),
    )));
    #[cfg(not(feature = "mdns"))]
    {
        let _ = settings;
        Err(failed("discovery", "this build has no mDNS"))
    }
}

/// Every carrier this build can serve, as one type the server is generic over.
macro_rules! carriers {
    ($( $(#[$cfg:meta])* $variant:ident($carrier:ty, $connection:ty) ),* $(,)?) => {
        // One of each per carrier or connection, made once: their size is what they hold.
        #[allow(clippy::large_enum_variant)]
        enum Carriers { $( $(#[$cfg])* $variant($carrier), )* }
        #[allow(clippy::large_enum_variant)]
        enum Connections { $( $(#[$cfg])* $variant($connection), )* }
        #[allow(clippy::large_enum_variant)]
        enum Reads { $( $(#[$cfg])* $variant(<$connection as orion_server::Connection>::Read), )* }
        #[allow(clippy::large_enum_variant)]
        enum Writes { $( $(#[$cfg])* $variant(<$connection as orion_server::Connection>::Write), )* }

        impl orion_server::Carrier for Carriers {
            type Connection = Connections;

            async fn accept(&mut self) -> std::io::Result<Connections> {
                match self {
                    $( $(#[$cfg])* Self::$variant(carrier) => carrier.accept().await.map(Connections::$variant), )*
                }
            }
        }

        impl orion_server::Connection for Connections {
            type Read = Reads;
            type Write = Writes;

            fn principal(&self) -> orion_server::Principal {
                match self { $( $(#[$cfg])* Self::$variant(connection) => connection.principal(), )* }
            }

            fn offers(&self) -> orion_server::Roles {
                match self { $( $(#[$cfg])* Self::$variant(connection) => connection.offers(), )* }
            }

            fn passes_descriptors(&self) -> bool {
                match self { $( $(#[$cfg])* Self::$variant(connection) => connection.passes_descriptors(), )* }
            }

            fn split(self) -> (Reads, Writes) {
                match self {
                    $( $(#[$cfg])* Self::$variant(connection) => {
                        let (read, write) = connection.split();
                        (Reads::$variant(read), Writes::$variant(write))
                    } )*
                }
            }
        }

        impl orion_server::FrameRead for Reads {
            async fn recv(&mut self, buf: &mut [u8], fds: &mut orion_server::FdSet) -> std::io::Result<Option<usize>> {
                match self { $( $(#[$cfg])* Self::$variant(read) => read.recv(buf, fds).await, )* }
            }
        }

        impl orion_server::FrameWrite for Writes {
            async fn send(&mut self, frame: &[u8], fds: &[std::os::fd::OwnedFd]) -> std::io::Result<()> {
                match self { $( $(#[$cfg])* Self::$variant(write) => write.send(frame, fds).await, )* }
            }
        }
    };
}

carriers! {
    Unix(UnixCarrier, orion_unix::UnixConnection),
    #[cfg(feature = "tcp")]
    Tcp(orion_tcp::TcpCarrier, orion_tcp::TcpConnection),
    #[cfg(feature = "ws")]
    Ws(orion_ws::WsCarrier, orion_ws::WsConnection),
    #[cfg(feature = "link-serial")]
    Serial(
        orion_link_serial::StreamCarrier<orion_link_serial::Serial>,
        orion_link_serial::StreamConnection<orion_link_serial::SerialPort>
    ),
    #[cfg(feature = "link-sealed")]
    Sealed(
        orion_link_serial::StreamCarrier<orion_link_serial::sealed::SealedListener>,
        orion_link_serial::StreamConnection<orion_link_serial::sealed::SealedStream>
    ),
    #[cfg(feature = "link-usb")]
    Usb(
        orion_link_serial::StreamCarrier<orion_link_usb::UsbLink>,
        orion_link_serial::StreamConnection<orion_link_usb::UsbStream>
    ),
    #[cfg(feature = "link-can")]
    Can(
        orion_link_can::CanCarrier<orion_link_can::SocketCanBus>,
        orion_link_can::CanConnection<orion_link_can::SocketCan>
    ),
}

/// Runs the comm core until `stop` finishes: starts it, tells `notifier` it is ready, says it
/// is alive every `watchdog`, and says it is stopping at the end. Must be called inside a
/// `LocalSet` on a current-thread runtime.
///
/// The "alive" message comes from the comm core's own thread. If that thread is stuck, the
/// messages stop, and that is the point: the service manager restarts a comm core that no
/// longer turns.
///
/// # Errors
///
/// If the comm core could not start.
#[cfg(feature = "systemd")]
pub async fn run_until(
    settings: CommSettings,
    notifier: Option<systemd::Notifier>,
    watchdog: Option<Duration>,
    stop: impl Future<Output = ()>,
) -> Result<(), StartError> {
    let core = CommCore::start(settings).await?;
    let notifier = notifier.map(Rc::new);
    if let Some(notifier) = &notifier {
        notifier.ready(&core.summary());
    }
    let alive = match (&notifier, watchdog) {
        (Some(notifier), Some(interval)) => {
            let notifier = notifier.clone();
            Some(tokio::task::spawn_local(async move {
                loop {
                    notifier.alive();
                    tokio::time::sleep(interval).await;
                }
            }))
        }
        _ => None,
    };
    stop.await;
    if let Some(notifier) = &notifier {
        notifier.stopping("stopping");
    }
    if let Some(alive) = alive {
        alive.abort();
    }
    drop(core);
    Ok(())
}

/// The comm core as a program: reads the service manager's variables, runs until `SIGTERM` or
/// `SIGINT`, and returns. Settings nobody read are logged, so a typo is seen.
///
/// # Errors
///
/// If the runtime or the comm core could not start.
#[cfg(feature = "systemd")]
pub fn run(settings: CommSettings, unknown: &[orion_config::Warning]) -> Result<(), StartError> {
    use tokio::signal::unix::{SignalKind, signal};
    for warning in unknown {
        tracing::warn!("{warning}");
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| failed("runtime", error))?;
    tokio::task::LocalSet::new().block_on(&runtime, async {
        let mut terminate =
            signal(SignalKind::terminate()).map_err(|error| failed("signals", error))?;
        let mut interrupt =
            signal(SignalKind::interrupt()).map_err(|error| failed("signals", error))?;
        let stop = async {
            tokio::select! {
                _ = terminate.recv() => info!("asked to stop"),
                _ = interrupt.recv() => info!("interrupted"),
            }
        };
        run_until(
            settings,
            systemd::Notifier::from_env(),
            systemd::watchdog_interval_from_env(),
            stop,
        )
        .await
    })
}

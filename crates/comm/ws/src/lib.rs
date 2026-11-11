//! The WebSocket carrier.
//!
//! A browser cannot open a socket of its own, so it reaches the comm core over a WebSocket
//! and speaks the same frames as everyone else: each Orion frame is one binary message.
//! Nothing above the carrier knows the difference. A browser is a subscriber without a ring,
//! and a publisher on behalf, like any program on another board.
//!
//! - **No HTTP framework.** A WebSocket starts as one HTTP request, which is read and
//!   answered here: the upgrade and nothing else. Any other request is turned away.
//! - **One listener, several protocols.** A connection names the subprotocol it speaks.
//!   The listener hands it to whoever asked for that subprotocol: the comm core for
//!   [`ORION`], and later the NT4 gateway for its own, on the same port.
//! - **Who a browser is.** Nobody the comm core knows: it is anonymous, and may do what the
//!   policies let anyone do. The page it came from is checked against a list, if one is
//!   given, so that a page on another site cannot use a visitor's browser to reach the
//!   board.
//! - **Bounded.** The request must come within the handshake time and fit 8 KiB. A message
//!   longer than the largest frame is refused without being read.

#![cfg(target_os = "linux")]

mod sha1;
mod socket;

use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use orion_server::{Carrier, Connection, FdSet, FrameRead, FrameWrite, Principal, Roles};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::debug;

pub use socket::{Message, WsRead, WsSocket, WsWrite};

/// The subprotocol that carries Orion's frames.
pub const ORION: &str = "orion.v1";

/// The roles a browser may ask for: it subscribes, runs actions, and may publish.
pub const WS_ROLES: Roles = Roles::CLIENT.and(Roles::PROVIDER);

/// The most a request may be before its blank line.
const MAX_REQUEST: usize = 8 * 1024;

/// What the listener is told.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WsOptions {
    /// The pages that may connect, by origin (`https://panel.example`). Empty for any. A
    /// connection that names no origin is not from a browser and is always taken.
    pub origins: Vec<String>,
    /// How long the upgrade may take.
    pub handshake: Duration,
}

impl Default for WsOptions {
    fn default() -> Self {
        Self {
            origins: Vec::new(),
            handshake: Duration::from_secs(5),
        }
    }
}

/// One TCP port that takes WebSocket connections and hands each to whoever speaks its
/// subprotocol.
#[derive(Debug)]
pub struct WsListener {
    listener: TcpListener,
    options: WsOptions,
    protocols: BTreeMap<String, mpsc::Sender<WsSocket>>,
}

/// The connections of one subprotocol, as they are made.
#[derive(Debug)]
pub struct Incoming(mpsc::Receiver<WsSocket>);

impl Incoming {
    /// The next connection. `None` when the listener is gone.
    pub async fn next(&mut self) -> Option<WsSocket> {
        self.0.recv().await
    }
}

/// What a request for an upgrade says.
#[derive(Debug, Default)]
struct Request {
    path: String,
    key: Option<String>,
    protocols: Vec<String>,
    origin: Option<String>,
    upgrade: bool,
    connection: bool,
    version: bool,
}

async fn read_request(
    stream: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
) -> io::Result<Request> {
    let mut request = Request::default();
    let mut line = String::new();
    let mut taken = 0;
    let mut first = true;
    loop {
        line.clear();
        let read = stream.read_line(&mut line).await?;
        taken += read;
        if read == 0 || taken > MAX_REQUEST {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "not a request"));
        }
        let text = line.trim_end();
        if first {
            first = false;
            let mut parts = text.split(' ');
            match (parts.next(), parts.next(), parts.next()) {
                (Some("GET"), Some(path), Some(version)) if version.starts_with("HTTP/1.1") => {
                    path.clone_into(&mut request.path);
                }
                _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "not a GET")),
            }
            continue;
        }
        if text.is_empty() {
            return Ok(request);
        }
        let Some((name, value)) = text.split_once(':') else {
            continue;
        };
        let value = value.trim();
        match name.trim().to_ascii_lowercase().as_str() {
            "upgrade" => request.upgrade = value.eq_ignore_ascii_case("websocket"),
            "connection" => {
                request.connection = value
                    .split(',')
                    .any(|part| part.trim().eq_ignore_ascii_case("upgrade"));
            }
            "sec-websocket-version" => request.version = value == "13",
            "sec-websocket-key" => request.key = Some(value.to_owned()),
            "sec-websocket-protocol" => request
                .protocols
                .extend(value.split(',').map(|part| part.trim().to_owned())),
            "origin" => request.origin = Some(value.to_owned()),
            _ => {}
        }
    }
}

impl WsListener {
    /// Listens on `addr`.
    ///
    /// # Errors
    ///
    /// If the address can't be bound.
    pub async fn bind(addr: SocketAddr, options: WsOptions) -> io::Result<Self> {
        Ok(Self {
            listener: TcpListener::bind(addr).await?,
            options,
            protocols: BTreeMap::new(),
        })
    }

    /// The address it listens on.
    ///
    /// # Errors
    ///
    /// If the socket can't say.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Says who takes the connections that speak `protocol`. Call it before
    /// [`WsListener::run`].
    pub fn protocol(&mut self, protocol: &str) -> Incoming {
        let (hand, taken) = mpsc::channel(16);
        self.protocols.insert(protocol.to_owned(), hand);
        Incoming(taken)
    }

    /// Takes connections for as long as it is polled: each is upgraded in a task of its own
    /// and handed to its subprotocol's taker.
    pub async fn run(self) {
        let Self {
            listener,
            options,
            protocols,
        } = self;
        let (options, protocols) = (std::rc::Rc::new(options), std::rc::Rc::new(protocols));
        loop {
            let (stream, from) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(error) => {
                    debug!(%error, "a connection could not be accepted");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            };
            let (options, protocols) = (options.clone(), protocols.clone());
            tokio::task::spawn_local(async move {
                let upgraded = tokio::time::timeout(
                    options.handshake,
                    upgrade(stream, from, &options, &protocols),
                )
                .await;
                match upgraded {
                    Ok(Ok(Some((socket, taker)))) => {
                        // A taker that is not taking is a connection that is dropped.
                        let _ = taker.send(socket).await;
                    }
                    Ok(Ok(None)) => {}
                    Ok(Err(error)) => debug!(%from, %error, "not a WebSocket upgrade"),
                    Err(_) => debug!(%from, "a connection took too long to ask for its upgrade"),
                }
            });
        }
    }
}

async fn refuse(mut stream: tokio::net::tcp::OwnedWriteHalf, status: &str) -> io::Result<()> {
    let answer = format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
    stream.write_all(answer.as_bytes()).await
}

/// Reads one request and, if it asks for a subprotocol someone takes from a page that may
/// ask, answers the upgrade. `None` if it was turned away.
async fn upgrade(
    stream: TcpStream,
    from: SocketAddr,
    options: &WsOptions,
    protocols: &BTreeMap<String, mpsc::Sender<WsSocket>>,
) -> io::Result<Option<(WsSocket, mpsc::Sender<WsSocket>)>> {
    stream.set_nodelay(true)?;
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);
    let Ok(request) = read_request(&mut read).await else {
        refuse(write, "400 Bad Request").await?;
        return Ok(None);
    };
    let Some(key) = request
        .key
        .as_ref()
        .filter(|_| request.upgrade && request.connection && request.version)
    else {
        refuse(write, "426 Upgrade Required").await?;
        return Ok(None);
    };
    if let Some(origin) = &request.origin
        && !options.origins.is_empty()
        && !options.origins.iter().any(|allowed| allowed == origin)
    {
        debug!(%from, %origin, "a page that may not connect");
        refuse(write, "403 Forbidden").await?;
        return Ok(None);
    }
    // The first subprotocol the other end offers that someone here takes.
    let Some((protocol, taker)) = request
        .protocols
        .iter()
        .find_map(|offered| protocols.get_key_value(offered))
    else {
        refuse(write, "404 Not Found").await?;
        return Ok(None);
    };
    let answer = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Accept: {}\r\nSec-WebSocket-Protocol: {protocol}\r\n\r\n",
        sha1::accept_key(key)
    );
    write.write_all(answer.as_bytes()).await?;
    let socket = WsSocket::new(
        read,
        write,
        true,
        from,
        request.path,
        request.origin,
        protocol.clone(),
    );
    Ok(Some((socket, taker.clone())))
}

/// Connects to a WebSocket server at `addr` and asks for `protocol`: the other end of
/// [`WsListener`], for programs and tests. A browser does this itself.
///
/// # Errors
///
/// If nothing answers there, or it does not take the upgrade.
pub async fn connect(
    addr: SocketAddr,
    path: &str,
    protocol: &str,
    origin: Option<&str>,
) -> io::Result<WsSocket> {
    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);
    // The key only has to be sixteen bytes in base64. Nothing is kept secret by it.
    let key = sha1::base64(&sha1::sha1(addr.to_string().as_bytes())[..16]);
    let origin = origin.map_or(String::new(), |origin| format!("Origin: {origin}\r\n"));
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\
         Sec-WebSocket-Protocol: {protocol}\r\n{origin}\r\n"
    );
    write.write_all(request.as_bytes()).await?;
    let mut line = String::new();
    read.read_line(&mut line).await?;
    if !line.starts_with("HTTP/1.1 101") {
        return Err(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            line.trim_end().to_owned(),
        ));
    }
    let mut accepted = false;
    loop {
        line.clear();
        if read.read_line(&mut line).await? == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "half an answer",
            ));
        }
        let text = line.trim_end();
        if text.is_empty() {
            break;
        }
        if let Some((name, value)) = text.split_once(':')
            && name.eq_ignore_ascii_case("sec-websocket-accept")
        {
            accepted = value.trim() == sha1::accept_key(&key);
        }
    }
    if !accepted {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "the server did not answer the key",
        ));
    }
    Ok(WsSocket::new(
        read,
        write,
        false,
        addr,
        path.to_owned(),
        None,
        protocol.to_owned(),
    ))
}

/// The carrier: Orion's frames over the WebSocket connections that speak [`ORION`].
#[derive(Debug)]
pub struct WsCarrier(Incoming);

impl WsCarrier {
    /// The carrier for `listener`'s connections that speak [`ORION`].
    pub fn on(listener: &mut WsListener) -> Self {
        Self(listener.protocol(ORION))
    }
}

/// One browser's connection.
#[derive(Debug)]
pub struct WsConnection(WsSocket);

/// The reading half of a [`WsConnection`].
#[derive(Debug)]
pub struct WsFrameRead(WsRead);

/// The writing half of a [`WsConnection`].
#[derive(Debug)]
pub struct WsFrameWrite(WsWrite);

impl FrameRead for WsFrameRead {
    async fn recv(&mut self, buf: &mut [u8], _fds: &mut FdSet) -> io::Result<Option<usize>> {
        match self.0.recv(buf).await? {
            Some(Message::Binary(len)) => Ok(Some(len)),
            // Orion's frames are bytes.
            Some(Message::Text(_)) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "a text message where a frame belongs",
            )),
            None => Ok(None),
        }
    }
}

impl FrameWrite for WsFrameWrite {
    async fn send(&mut self, frame: &[u8], fds: &[std::os::fd::OwnedFd]) -> io::Result<()> {
        if !fds.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "descriptors do not travel over a WebSocket",
            ));
        }
        self.0.send_binary(frame).await
    }
}

impl Connection for WsConnection {
    type Read = WsFrameRead;
    type Write = WsFrameWrite;

    fn principal(&self) -> Principal {
        Principal::Anonymous
    }

    fn offers(&self) -> Roles {
        WS_ROLES
    }

    fn passes_descriptors(&self) -> bool {
        false
    }

    fn split(self) -> (WsFrameRead, WsFrameWrite) {
        (WsFrameRead(self.0.read), WsFrameWrite(self.0.write))
    }
}

impl Carrier for WsCarrier {
    type Connection = WsConnection;

    async fn accept(&mut self) -> io::Result<WsConnection> {
        match self.0.next().await {
            Some(socket) => Ok(WsConnection(socket)),
            // The listener is gone: there will be no more. Wait rather than spin.
            None => std::future::pending().await,
        }
    }
}

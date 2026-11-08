//! The task serving one Unix link: a socket the node listens on, one `HostSession<Stream>` per
//! connection (COBS framed, as on a serial port).
//!
//! Each connection gets a small I/O task (`serve_conn`) that reads into a shared channel and writes
//! what this task hands it; this task owns every session, so the provider path sees the link as one
//! link with many devices, like a CAN bus.
//!
//! - **Admission.** The caller's `SO_PEERCRED` identity is judged at accept by the node's local
//!   policy (`ORION_NODE_LOCAL_AUTH`, `ORION_NODE_LOCAL_AUTH_ALLOW`,
//!   `ORION_NODE_LOCAL_PROVIDER_ALLOW`). A caller admitted to nothing is disconnected at once; the
//!   provider role needs the provider lane, which full admission includes.
//! - **One session per device.** A connection whose `Hello` names a device already connected on
//!   another connection of the link is closed; the first keeps it.
//! - **Rate limit.** Frames from a connection are read within its own budget
//!   (`max_frames_per_s`, a token bucket): past it, a read waits for the next frame's turn.
//!   Nothing is refused, so an action's final result is never lost.
//! - **Queues.** Output waiting for a connection is bounded in bytes ([`QUEUE_BYTES`]); past it, frames
//!   stay in the session, which bounds and counts its own queue. A connection that takes no output
//!   for [`WRITE_TIMEOUT`] is closed. Answers the session's client has not taken yet are bounded
//!   in bytes ([`ANSWER_BYTES`]); past it, queries are refused with `limit` and the connection is
//!   not read until they drain.
//! - **Held is heard.** A connection the node does not read (budget or answers) tells the link
//!   task every [`HELD_NOTICE`], so its session is not timed out. Writes run in their own task,
//!   so a peer slow to take output still has its pings read.
//! - Firmware transfers and `link.release` / `link.attach` do not apply: a Unix link has no port to
//!   hand over.

use super::events::{LEASE_REFRESH, LeaseAction, LinkContext, LinkSend, REOPEN_DELAY};
use super::remote_link::{self, ConnRead, ConnWrite, HANDSHAKE_TIMEOUT};
use super::unix_client::{ANSWER_BYTES, LinkClient};
use crate::app::LinkCaller;
use crate::auth::{LocalAdmission, LocalAdmissionPolicy};
use orion_link::Stream;
use orion_link::host::{ActionSendError, HostConfig, HostEvent, HostSession};
use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::os::unix::fs::FileTypeExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
use tokio::sync::{Notify, mpsc, oneshot, watch};
use tracing::{debug, info, warn};

/// Output bytes waiting for one connection above which no more frames are pulled from its session.
pub(super) const QUEUE_BYTES: usize = 64 * 1024;
/// A connection whose pending output does not move for this long is closed.
pub(super) const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
/// Inbound chunks buffered for the link task, across its connections.
const INBOUND_CHUNKS: usize = 64;
/// Read buffer per connection.
pub(super) const READ_BUF: usize = 4096;

/// While a connection is held back by its frame budget, the link task hears of it at least this
/// often, so the session counts as heard and never times out for frames the node made wait.
const HELD_NOTICE: Duration = Duration::from_millis(100);

/// Per-connection limits, from the link's config (`max_frames_per_s=`).
#[derive(Clone, Copy, Debug)]
pub(super) struct ConnLimits {
    /// Frames read per second, sustained. A burst of one second's worth reads at once.
    pub(super) max_frames_per_s: u32,
}

/// A token bucket of frames: refilled continuously at the rate, holding one second's worth. A
/// connection past it waits for the next frame's token (1 / rate), not for a window to end.
struct FrameBudget {
    rate: f64,
    tokens: f64,
    refilled: tokio::time::Instant,
}

impl FrameBudget {
    fn new(limits: ConnLimits, now: tokio::time::Instant) -> Self {
        let rate = f64::from(limits.max_frames_per_s.max(1));
        Self {
            rate,
            tokens: rate,
            refilled: now,
        }
    }

    /// When reading may go on, or `None` if it may now.
    fn wait_until(&mut self, now: tokio::time::Instant) -> Option<tokio::time::Instant> {
        let elapsed = now.saturating_duration_since(self.refilled).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate).min(self.rate);
        self.refilled = now;
        (self.tokens < 1.0).then(|| now + Duration::from_secs_f64((1.0 - self.tokens) / self.rate))
    }

    /// Frames read: a read may take more than the tokens left, and the debt is waited off.
    fn spend(&mut self, frames: u32) {
        self.tokens -= f64::from(frames);
    }
}

/// What a connection's I/O task tells the link task.
enum Inbound {
    Bytes(u64, Vec<u8>),
    /// The connection's reads wait for its frame budget: its device is alive.
    Held(u64),
    Closed(u64, Option<io::Error>),
}

/// Byte buffers shared by a connection's reader, writer and link task: a read chunk or an
/// outgoing frame travels in one of them and comes back when done, so steady traffic allocates
/// none.
#[derive(Clone, Default)]
struct Buffers(Arc<std::sync::Mutex<Vec<Vec<u8>>>>);

impl Buffers {
    /// Buffers kept for reuse: enough for the reads and frames in flight at once.
    const KEPT: usize = 16;

    fn take(&self) -> Vec<u8> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop()
            .unwrap_or_default()
    }

    fn give(&self, mut buffer: Vec<u8>) {
        let mut kept = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if kept.len() < Self::KEPT && buffer.capacity() <= 64 * 1024 {
            buffer.clear();
            kept.push(buffer);
        }
    }
}

struct Conn {
    session: HostSession<Stream>,
    out: mpsc::UnboundedSender<Vec<u8>>,
    buffers: Buffers,
    pending: Arc<AtomicUsize>,
    /// The session's answers waiting for output, in bytes, as last counted; the connection is
    /// not read while it is at [`ANSWER_BYTES`] or more.
    backlog: Arc<AtomicUsize>,
    /// Wakes the connection's reader when the backlog falls below [`ANSWER_BYTES`].
    room: Arc<Notify>,
    /// Who the session's client requests are for.
    caller: LinkCaller,
    /// When an operator's enrollment was last checked ([`still_admitted`]).
    checked: Instant,
    /// The current session's client and stream publisher.
    client: Option<LinkClient>,
}

/// Session counters of connections already closed, so the link's totals never go back.
#[derive(Default)]
struct Retired {
    frames_rx: u64,
    decode_errors: u64,
    sessions: u64,
    device_timeouts: u64,
    hello_rejects: u64,
    crc_errors: u64,
    framing_errors: u64,
    dropped: u64,
}

/// Where a link listens: a Unix socket path, or a TCP address (`tcp:` links, remote operators).
#[derive(Clone, Debug)]
pub(super) enum Endpoint {
    Unix(PathBuf),
    Tcp(SocketAddr),
}

/// What the link task listens with.
pub(super) struct Listen {
    pub(super) endpoint: Endpoint,
    pub(super) max_connections: u32,
}

enum Listener {
    Unix(UnixListener),
    Tcp(TcpListener),
}

enum Incoming {
    Unix(UnixStream),
    Tcp(TcpStream, SocketAddr),
}

pub(super) async fn run(
    mut ctx: LinkContext,
    config: Listen,
    host: HostConfig,
    limits: ConnLimits,
    mut shutdown: watch::Receiver<bool>,
) {
    if matches!(config.endpoint, Endpoint::Tcp(_)) && orion_auth::link_channel::PORTABLE_CHACHA20 {
        warn!(
            link = %ctx.name,
            "this build seals tcp: link records with ChaCha20's portable code, about 2.3x the CPU of \
             its NEON backend; build aarch64 with RUSTFLAGS=\"--cfg chacha20_force_neon\" \
             (docs/link-protocol.md, \"Remote links\")"
        );
    }
    let policy = ctx.app.local_admission.clone();
    // Connection ids of one turn, kept across turns so a turn does not allocate.
    let mut ids: Vec<u64> = Vec::new();
    // One turn's events of one connection, reused across turns.
    let mut events: Vec<HostEvent> = Vec::new();
    let mut desired = ctx.app.link_desired_changes();
    let mut state_changes = ctx.app.link_state_changes();
    let mut listener: Option<Listener> = None;
    // `tcp:` connections whose handshake finished, and how many are still in one.
    let (ready_tx, mut ready) = mpsc::channel::<remote_link::Accepted>(8);
    let handshaking = Arc::new(AtomicUsize::new(0));
    let mut reopen_at = Instant::now();
    let mut lease_refresh_at = Instant::now() + LEASE_REFRESH;
    let mut commands = ctx.open_actions();
    let mut commands_open = true;
    let (inbound_tx, mut inbound) = mpsc::channel::<Inbound>(INBOUND_CHUNKS);
    let drained = Arc::new(Notify::new());
    // Notified when the node queues an event for one of the link's clients.
    let client_wake = Arc::new(Notify::new());
    let mut conns: BTreeMap<u64, Conn> = BTreeMap::new();
    let mut next_id: u64 = 0;
    let mut retired = Retired::default();

    loop {
        if listener.is_none() && Instant::now() >= reopen_at {
            match bind(&config.endpoint).await {
                Ok(bound) => {
                    listener = Some(bound);
                    ctx.opened();
                }
                Err(error) => {
                    ctx.io_error(&format!("bind {}", endpoint_name(&config.endpoint)), &error);
                    reopen_at = Instant::now() + REOPEN_DELAY;
                }
            }
        }

        let mut refresh = false;
        let mut closed: Vec<u64> = Vec::new();
        let reopen = listener.is_none().then_some(reopen_at);
        let rechecks = conns
            .values()
            .filter(|conn| matches!(conn.caller, LinkCaller::Operator { .. }))
            .map(|conn| conn.checked + OPERATOR_RECHECK);
        let wake = ctx.wake_at(
            conns
                .values()
                .filter_map(|conn| conn.session.next_poll_ms())
                .min(),
            [lease_refresh_at].into_iter().chain(reopen).chain(rechecks),
        );
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            event = inbound.recv() => match event {
                Some(Inbound::Bytes(id, bytes)) => {
                    if let Some(conn) = conns.get_mut(&id) {
                        ctx.status.bytes_rx += bytes.len() as u64;
                        conn.session.receive(&bytes);
                        conn.buffers.give(bytes);
                    }
                }
                Some(Inbound::Held(id)) => {
                    if let Some(conn) = conns.get_mut(&id) {
                        conn.session.mark_heard();
                    }
                }
                Some(Inbound::Closed(id, error)) => {
                    if let Some(error) = error {
                        debug!(link = %ctx.name, conn = id, %error, "unix link connection failed");
                    }
                    closed.push(id);
                }
                None => {}
            },
            Some(accepted) = ready.recv() => {
                handshaking.fetch_sub(1, Ordering::AcqRel);
                if conns.len() >= config.max_connections as usize {
                    warn!(link = %ctx.name, max_connections = config.max_connections, "link is full; closing the new connection");
                } else {
                    let conn = start_conn(
                        next_id,
                        accepted.reader,
                        accepted.writer,
                        accepted.caller,
                        &host,
                        limits,
                        &inbound_tx,
                        &drained,
                    );
                    conns.insert(next_id, conn);
                    next_id += 1;
                }
            }
            accepted = accept(listener.as_ref()) => match accepted {
                Ok(Incoming::Unix(stream)) => {
                    if let Some(conn) = admit(
                        &ctx.name,
                        &policy,
                        stream,
                        conns.len(),
                        config.max_connections,
                        next_id,
                        &host,
                        limits,
                        &inbound_tx,
                        &drained,
                    ) {
                        conns.insert(next_id, conn);
                        next_id += 1;
                    }
                }
                Ok(Incoming::Tcp(stream, peer)) => {
                    // Handshakes count against the link's connections, so a flood of them cannot
                    // grow past it.
                    let open = conns.len() + handshaking.load(Ordering::Acquire);
                    if open >= config.max_connections as usize {
                        warn!(link = %ctx.name, %peer, max_connections = config.max_connections, "link is full; closing the new connection");
                    } else {
                        handshaking.fetch_add(1, Ordering::AcqRel);
                        let (app, ready_tx, handshaking, link) =
                            (ctx.app.clone(), ready_tx.clone(), handshaking.clone(), ctx.name.clone());
                        tokio::spawn(async move {
                            match tokio::time::timeout(
                                HANDSHAKE_TIMEOUT,
                                remote_link::handshake(&app, stream, peer),
                            )
                            .await
                            {
                                Ok(Ok(accepted)) => {
                                    if ready_tx.send(accepted).await.is_ok() {
                                        return;
                                    }
                                }
                                Ok(Err(error)) => debug!(%link, %peer, %error, "tcp link handshake failed"),
                                Err(_) => debug!(%link, %peer, "tcp link handshake timed out"),
                            }
                            handshaking.fetch_sub(1, Ordering::AcqRel);
                        });
                    }
                }
                Err(error) => {
                    // Per-connection failures (the peer gave up) are not the listener's; anything
                    // else rebinds.
                    if !matches!(
                        error.kind(),
                        io::ErrorKind::ConnectionAborted | io::ErrorKind::ConnectionReset
                    ) {
                        ctx.io_error("accept", &error);
                        listener = None;
                        ctx.status.open = false;
                        reopen_at = Instant::now() + REOPEN_DELAY;
                    }
                }
            },
            () = drained.notified() => {}
            () = client_wake.notified() => {}
            changed = desired.changed() => refresh = changed.is_ok(),
            // Record watches and executors' workloads see the change at the pump below.
            _ = state_changes.changed() => {}
            command = commands.recv(), if commands_open => match command {
                Some(command) => ctx.accept_action(command, |device, send| {
                    let conn = conns
                        .values_mut()
                        .find(|conn| conn.session.device_name() == Some(device));
                    match conn {
                        Some(conn) => match send {
                            LinkSend::Request { id, name, payload } => {
                                conn.session.send_action(id, name, payload)
                            }
                            LinkSend::Cancel { id } => conn.session.send_action_cancel(id),
                        },
                        None => Err(ActionSendError::NotConnected),
                    }
                }),
                None => commands_open = false,
            },
            () = tokio::time::sleep_until(wake) => {}
        }

        for id in closed {
            if let Some(conn) = conns.remove(&id) {
                drop_conn(&mut ctx, &mut retired, conn);
            }
        }

        let now = ctx.now_ms();
        ids.clear();
        ids.extend(conns.keys().copied());
        for &id in &ids {
            events.clear();
            if let Some(conn) = conns.get_mut(&id) {
                conn.session.poll(now);
                while let Some(event) = conn.session.next_event() {
                    events.push(event);
                }
            }
            let mut duplicate = false;
            for event in events.drain(..) {
                if let HostEvent::DeviceConnected { device_name, .. } = &event
                    && taken_elsewhere(&conns, id, device_name)
                {
                    warn!(
                        link = %ctx.name,
                        device = %device_name,
                        "device already connected on another connection of this link; closing the new one"
                    );
                    duplicate = true;
                    break;
                }
                let Some(conn) = conns.get_mut(&id) else {
                    break;
                };
                match &event {
                    HostEvent::ClientRequest { .. }
                    | HostEvent::StreamDeclare { .. }
                    | HostEvent::StreamPublish { .. }
                    | HostEvent::ExecutorState { .. } => {
                        if let Some(client) = conn.client.as_mut() {
                            match event {
                                HostEvent::ClientRequest { request, .. } => {
                                    client.request(&ctx.app, request);
                                }
                                HostEvent::StreamDeclare {
                                    req, id, series, ..
                                } => client.declare(&ctx.app, req, id, series),
                                HostEvent::StreamPublish { id, mut chunks, .. } => {
                                    client.publish(&ctx.app, &id, &mut chunks);
                                    conn.session.recycle_chunks(chunks);
                                }
                                HostEvent::ExecutorState { state, .. } => {
                                    client.executor_state(&ctx.app, *state);
                                }
                                _ => {}
                            }
                        }
                        continue;
                    }
                    HostEvent::DeviceConnected {
                        device_name, roles, ..
                    } => {
                        // A new session: the client of the previous one (if any) ends.
                        if let Some(client) = conn.client.take() {
                            client.close(&ctx.app);
                        }
                        conn.client = Some(LinkClient::open(
                            &ctx.app,
                            &ctx.name,
                            device_name,
                            conn.caller.clone(),
                            client_wake.clone(),
                            *roles,
                        ));
                    }
                    HostEvent::DeviceLost { .. } => {
                        if let Some(client) = conn.client.take() {
                            client.close(&ctx.app);
                        }
                    }
                    _ => {}
                }
                let leases = ctx.handle(event);
                if let Some(conn) = conns.get_mut(&id) {
                    if let LeaseAction::Set(leases) = leases {
                        conn.session.set_leases(leases);
                    }
                    if let Some(entries) = ctx.take_status_spare() {
                        conn.session.recycle_status(entries);
                    }
                    if let Some(chunks) = ctx.take_chunk_spare() {
                        conn.session.recycle_chunks(chunks);
                    }
                }
            }
            if duplicate {
                if let Some(conn) = conns.remove(&id) {
                    // The device stays with its first connection: nothing of this one reached the
                    // provider path, so it is dropped without a `DeviceLost`.
                    retire(&mut retired, &conn.session);
                }
                continue;
            }
            // An operator removed while connected loses the connection, so its watches stop.
            if conns
                .get_mut(&id)
                .is_some_and(|conn| !still_admitted(&ctx, conn))
            {
                if let Some(conn) = conns.remove(&id) {
                    drop_conn(&mut ctx, &mut retired, conn);
                }
                continue;
            }
            let Some(conn) = conns.get_mut(&id) else {
                continue;
            };
            // Client answers are queued while the connection has room, and again each time frames
            // move on, so a long answer does not wait for the next tick.
            loop {
                let more = conn.pending.load(Ordering::Acquire) < QUEUE_BYTES
                    && conn
                        .client
                        .as_mut()
                        .is_some_and(|client| client.pump(&ctx.app, &mut conn.session));
                let moved = transmit(&mut ctx, conn);
                if !more || moved == 0 {
                    break;
                }
            }
            let backlog = conn.client.as_ref().map_or(0, LinkClient::answer_bytes);
            let before = conn.backlog.swap(backlog, Ordering::AcqRel);
            if before >= ANSWER_BYTES && backlog < ANSWER_BYTES {
                conn.room.notify_one();
            }
        }

        let periodic = Instant::now() >= lease_refresh_at;
        if periodic {
            lease_refresh_at = Instant::now() + LEASE_REFRESH;
        }
        if ctx.leases_due(refresh, periodic) {
            for conn in conns.values_mut() {
                if let Some(leases) = conn
                    .session
                    .device_name()
                    .and_then(|name| ctx.refreshed_leases(name))
                {
                    conn.session.set_leases(leases);
                }
            }
        }
        update_stats(&mut ctx, &retired, &conns);
        ctx.publish_status();
    }
    for (_, conn) in std::mem::take(&mut conns) {
        drop_conn(&mut ctx, &mut retired, conn);
    }
    if let (Some(Listener::Unix(_)), Endpoint::Unix(path)) = (listener.take(), &config.endpoint) {
        let _ = std::fs::remove_file(path);
    }
    update_stats(&mut ctx, &retired, &conns);
    ctx.close();
}

/// Hands the session's frames to the connection while its queue has room; the rest wait in the
/// session. Returns the frames handed over.
fn transmit(ctx: &mut LinkContext, conn: &mut Conn) -> usize {
    let mut moved = 0;
    while conn.pending.load(Ordering::Acquire) < QUEUE_BYTES {
        let mut bytes = conn.buffers.take();
        if !conn.session.transmit_into(&mut bytes) {
            conn.buffers.give(bytes);
            break;
        }
        ctx.status.frames_tx += 1;
        ctx.status.bytes_tx += bytes.len() as u64;
        conn.pending.fetch_add(bytes.len(), Ordering::AcqRel);
        moved += 1;
        if conn.out.send(bytes).is_err() {
            break;
        }
    }
    moved
}

/// Whether a connection other than `id` has a session for `device`.
fn taken_elsewhere(conns: &BTreeMap<u64, Conn>, id: u64, device: &str) -> bool {
    conns
        .iter()
        .any(|(other, conn)| *other != id && conn.session.device_name() == Some(device))
}

/// Binds the socket, replacing a stale socket file at the path (never any other kind of file).
fn endpoint_name(endpoint: &Endpoint) -> String {
    match endpoint {
        Endpoint::Unix(path) => path.display().to_string(),
        Endpoint::Tcp(addr) => addr.to_string(),
    }
}

async fn bind(endpoint: &Endpoint) -> io::Result<Listener> {
    match endpoint {
        Endpoint::Unix(path) => bind_unix(path).map(Listener::Unix),
        Endpoint::Tcp(addr) => TcpListener::bind(addr).await.map(Listener::Tcp),
    }
}

fn bind_unix(path: &std::path::Path) -> io::Result<UnixListener> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_socket() => std::fs::remove_file(path)?,
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "the path exists and is not a socket",
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    UnixListener::bind(path)
}

async fn accept(listener: Option<&Listener>) -> io::Result<Incoming> {
    match listener {
        Some(Listener::Unix(listener)) => listener
            .accept()
            .await
            .map(|(stream, _)| Incoming::Unix(stream)),
        Some(Listener::Tcp(listener)) => listener
            .accept()
            .await
            .map(|(stream, peer)| Incoming::Tcp(stream, peer)),
        None => std::future::pending().await,
    }
}

/// Admits a new connection and starts its I/O task, or closes it (dropped here).
#[allow(clippy::too_many_arguments)]
fn admit(
    link: &str,
    policy: &LocalAdmissionPolicy,
    stream: UnixStream,
    open: usize,
    max_connections: u32,
    id: u64,
    host: &HostConfig,
    limits: ConnLimits,
    inbound: &mpsc::Sender<Inbound>,
    drained: &Arc<Notify>,
) -> Option<Conn> {
    let Some(identity) = orion_transport_ipc::unix_peer_identity(&stream) else {
        warn!(link, "unix link caller without peer credentials; closing");
        return None;
    };
    let admission = policy.admit(&identity);
    if admission == LocalAdmission::Refused {
        info!(
            link,
            uid = identity.uid,
            gid = identity.gid,
            "unix link caller not admitted by the local policy; closing (see docs/node-env.md)"
        );
        return None;
    }
    if open >= max_connections as usize {
        warn!(
            link,
            max_connections, "unix link is full; closing the new connection"
        );
        return None;
    }
    debug!(
        link,
        conn = id,
        uid = identity.uid,
        ?admission,
        "unix link connection"
    );
    let (reader, writer) = stream.into_split();
    Some(start_conn(
        id,
        reader,
        writer,
        LinkCaller::Local(identity),
        host,
        limits,
        inbound,
        drained,
    ))
}

/// Starts the I/O task of an admitted connection.
#[allow(clippy::too_many_arguments)]
fn start_conn<R: ConnRead, W: ConnWrite>(
    id: u64,
    reader: R,
    writer: W,
    caller: LinkCaller,
    host: &HostConfig,
    limits: ConnLimits,
    inbound: &mpsc::Sender<Inbound>,
    drained: &Arc<Notify>,
) -> Conn {
    let (out, out_rx) = mpsc::unbounded_channel();
    let pending = Arc::new(AtomicUsize::new(0));
    let backlog = Arc::new(AtomicUsize::new(0));
    let room = Arc::new(Notify::new());
    let buffers = Buffers::default();
    tokio::spawn(serve_conn(
        id,
        reader,
        writer,
        out_rx,
        Shared {
            pending: pending.clone(),
            backlog: backlog.clone(),
            room: room.clone(),
            drained: drained.clone(),
            buffers: buffers.clone(),
        },
        inbound.clone(),
        limits,
    ));
    Conn {
        session: HostSession::stream(host.clone()),
        out,
        buffers,
        pending,
        backlog,
        room,
        caller,
        checked: Instant::now(),
        client: None,
    }
}

/// How often an operator's connection checks that its operator is still enrolled.
const OPERATOR_RECHECK: Duration = Duration::from_secs(1);

/// Whether the connection may stay: an operator's (a `tcp:` link) is closed within
/// [`OPERATOR_RECHECK`] of the operator's removal or key change. Requests are checked each time
/// anyway; this ends the watches.
fn still_admitted(ctx: &LinkContext, conn: &mut Conn) -> bool {
    let LinkCaller::Operator { principal, key } = &conn.caller else {
        return true;
    };
    if conn.checked.elapsed() < OPERATOR_RECHECK {
        return true;
    }
    conn.checked = Instant::now();
    match ctx.app.security.enrolled_operator(principal, *key, false) {
        Ok(_) => true,
        Err(error) => {
            info!(link = %ctx.name, %error, "operator connection closed");
            false
        }
    }
}

/// A closed connection: its device is lost, its counters retired. Dropping `conn.out` ends its
/// I/O task.
fn drop_conn(ctx: &mut LinkContext, retired: &mut Retired, mut conn: Conn) {
    if let Some(client) = conn.client.take() {
        client.close(&ctx.app);
    }
    // As on a serial hang-up: the device is lost whether or not its snapshot was accepted yet, so
    // actions in flight on it fail at once.
    if let Some(device) = conn.session.device_name().map(str::to_owned) {
        let _ = ctx.handle(HostEvent::DeviceLost {
            device_name: device.into(),
        });
    }
    retire(retired, &conn.session);
}

fn retire(retired: &mut Retired, session: &HostSession<Stream>) {
    let host = session.stats();
    let decoder = session.decoder().stats();
    retired.frames_rx += u64::from(host.frames_received);
    retired.decode_errors += u64::from(host.decode_errors);
    retired.sessions += u64::from(host.sessions);
    retired.device_timeouts += u64::from(host.device_timeouts);
    retired.hello_rejects += u64::from(host.rejects_sent);
    retired.crc_errors += u64::from(decoder.crc_errors);
    retired.framing_errors += u64::from(decoder.framing_errors);
    retired.dropped += u64::from(decoder.dropped());
}

fn update_stats(ctx: &mut LinkContext, retired: &Retired, conns: &BTreeMap<u64, Conn>) {
    let mut total = Retired {
        frames_rx: retired.frames_rx,
        decode_errors: retired.decode_errors,
        sessions: retired.sessions,
        device_timeouts: retired.device_timeouts,
        hello_rejects: retired.hello_rejects,
        crc_errors: retired.crc_errors,
        framing_errors: retired.framing_errors,
        dropped: retired.dropped,
    };
    for conn in conns.values() {
        retire(&mut total, &conn.session);
    }
    let status = &mut ctx.status;
    status.frames_rx = total.frames_rx;
    status.decode_errors = total.decode_errors;
    status.sessions = total.sessions;
    status.device_timeouts = total.device_timeouts;
    status.hello_rejects = total.hello_rejects;
    status.crc_errors = total.crc_errors;
    status.framing_errors = total.framing_errors;
    status.dropped = total.dropped;
}

/// Frames counted for the rate limit: each `0x00` that ends a run of other bytes, so a frame counts
/// once whether its peer writes the delimiter before it, after it, or both.
struct FrameCounter {
    in_frame: bool,
}

impl FrameCounter {
    fn count(&mut self, bytes: &[u8]) -> u32 {
        let mut frames = 0u32;
        for &byte in bytes {
            if byte == 0 {
                if self.in_frame {
                    frames = frames.saturating_add(1);
                }
                self.in_frame = false;
            } else {
                self.in_frame = true;
            }
        }
        frames
    }
}

/// One connection's I/O: reads within the rate limit into the link task's channel, and writes what
/// the link task queued. Ends when the peer hangs up, on an I/O error, or when the link task drops
/// the connection.
/// What a connection's task shares with the link task.
struct Shared {
    /// Output bytes handed to the connection and not yet written.
    pending: Arc<AtomicUsize>,
    /// The session's waiting answers in bytes ([`Conn::backlog`]).
    backlog: Arc<AtomicUsize>,
    /// Notified when the backlog falls below [`ANSWER_BYTES`].
    room: Arc<Notify>,
    /// Notified when `pending` falls below [`QUEUE_BYTES`].
    drained: Arc<Notify>,
    buffers: Buffers,
}

async fn serve_conn<R: ConnRead, W: ConnWrite>(
    id: u64,
    mut reader: R,
    writer: W,
    out: mpsc::UnboundedReceiver<Vec<u8>>,
    shared: Shared,
    inbound: mpsc::Sender<Inbound>,
    limits: ConnLimits,
) {
    let Shared {
        pending,
        backlog,
        room,
        drained,
        buffers,
    } = shared;
    // Writes run on their own, so a peer slow to take its output never stops the reads (its
    // pings) or the held notices.
    let (write_end_tx, mut write_end) = oneshot::channel();
    let returned = buffers.clone();
    let writes = tokio::spawn(async move {
        let _ = write_end_tx.send(write_out(writer, out, &pending, &drained, &returned).await);
    });
    let mut buf = vec![0u8; READ_BUF];
    let mut counter = FrameCounter { in_frame: false };
    let mut budget = FrameBudget::new(limits, tokio::time::Instant::now());
    let mut told_held: Option<tokio::time::Instant> = None;
    let error = loop {
        let now = tokio::time::Instant::now();
        let resume = budget.wait_until(now);
        // Past its answer cap the session reads nothing more until the client takes its answers.
        let full = backlog.load(Ordering::Acquire) >= ANSWER_BYTES;
        let held = resume.is_some() || full;
        if held && told_held.is_none_or(|at| now >= at + HELD_NOTICE) {
            // Best effort: a full channel means the link task has plenty to do with this
            // connection already.
            let _ = inbound.try_send(Inbound::Held(id));
            told_held = Some(now);
        }
        // Wake at least every notice period while held, so the notices keep coming.
        let wake = held.then(|| resume.map_or(now + HELD_NOTICE, |at| at.min(now + HELD_NOTICE)));
        tokio::select! {
            biased;
            ended = &mut write_end => match ended {
                Ok(Some(error)) => break Some(error),
                // The link task dropped the connection.
                Ok(None) | Err(_) => return,
            },
            () = tokio::time::sleep_until(wake.unwrap_or(now)), if wake.is_some() => {}
            () = room.notified(), if full => {}
            read = reader.read_bytes(&mut buf), if !held => match read {
                Ok(0) => break None,
                Ok(n) => {
                    let chunk = buf.get(..n).unwrap_or_default();
                    budget.spend(counter.count(chunk));
                    let mut bytes = buffers.take();
                    bytes.extend_from_slice(chunk);
                    if inbound.send(Inbound::Bytes(id, bytes)).await.is_err() {
                        writes.abort();
                        return;
                    }
                }
                Err(error) => break Some(error),
            },
        }
    };
    writes.abort();
    let _ = inbound.send(Inbound::Closed(id, error)).await;
}

/// Writes the connection's output until the link task drops it (`None`) or a write fails or
/// takes longer than [`WRITE_TIMEOUT`].
async fn write_out<W: ConnWrite>(
    mut writer: W,
    mut out: mpsc::UnboundedReceiver<Vec<u8>>,
    pending: &AtomicUsize,
    drained: &Notify,
    buffers: &Buffers,
) -> Option<io::Error> {
    while let Some(frame) = out.recv().await {
        let len = frame.len();
        let written = tokio::time::timeout(WRITE_TIMEOUT, writer.write_bytes(&frame)).await;
        buffers.give(frame);
        let before = pending.fetch_sub(len, Ordering::AcqRel);
        if before >= QUEUE_BYTES && before - len < QUEUE_BYTES {
            drained.notify_one();
        }
        match written {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Some(error),
            Err(_) => {
                return Some(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "the peer took no output",
                ));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    fn shared() -> Shared {
        Shared {
            pending: Arc::new(AtomicUsize::new(0)),
            backlog: Arc::new(AtomicUsize::new(0)),
            room: Arc::new(Notify::new()),
            drained: Arc::new(Notify::new()),
            buffers: Buffers::default(),
        }
    }

    #[tokio::test]
    async fn a_session_at_its_answer_cap_is_not_read_until_the_answers_drain() {
        let (node_side, mut peer) = UnixStream::pair().expect("pair");
        let (_out, out_rx) = mpsc::unbounded_channel();
        let (inbound_tx, mut inbound) = mpsc::channel(64);
        let shared = shared();
        let (backlog, room) = (shared.backlog.clone(), shared.room.clone());
        backlog.store(ANSWER_BYTES, Ordering::Release);
        let (node_read, node_write) = node_side.into_split();
        tokio::spawn(serve_conn(
            1,
            node_read,
            node_write,
            out_rx,
            shared,
            inbound_tx,
            ConnLimits {
                max_frames_per_s: 1_000,
            },
        ));
        peer.write_all(&[1, 0]).await.expect("write");
        // Held: only notices for 300 ms, so the session is not timed out meanwhile.
        let held_until = tokio::time::Instant::now() + Duration::from_millis(300);
        let mut held = 0;
        while let Ok(event) = tokio::time::timeout_at(held_until, inbound.recv()).await {
            assert!(matches!(event, Some(Inbound::Held(1))), "read while held");
            held += 1;
        }
        assert!(held >= 2, "notices while held: {held}");
        backlog.store(0, Ordering::Release);
        room.notify_one();
        let read_at = tokio::time::Instant::now();
        loop {
            match inbound.recv().await {
                Some(Inbound::Held(1)) => {}
                Some(Inbound::Bytes(1, bytes)) => {
                    assert_eq!(bytes, [1, 0]);
                    break;
                }
                _ => panic!("expected the held frame"),
            }
        }
        assert!(
            read_at.elapsed() < Duration::from_millis(50),
            "the read resumes when the answers drain, not at the next notice"
        );
    }

    #[tokio::test]
    async fn frames_past_the_budget_wait_for_their_tokens_and_the_link_hears_of_it() {
        let (node_side, mut peer) = UnixStream::pair().expect("pair");
        let (_out, out_rx) = mpsc::unbounded_channel();
        let (inbound_tx, mut inbound) = mpsc::channel(64);
        // 20 frames a second: a burst of 20 reads at once, then one frame per 50 ms.
        let (node_read, node_write) = node_side.into_split();
        tokio::spawn(serve_conn(
            1,
            node_read,
            node_write,
            out_rx,
            shared(),
            inbound_tx,
            ConnLimits {
                max_frames_per_s: 20,
            },
        ));
        let started = tokio::time::Instant::now();
        let (mut frames, mut held) = (0usize, 0usize);
        let mut arrivals = Vec::new();
        for frame in 0..23u8 {
            peer.write_all(&[frame + 1, 0]).await.expect("write");
            loop {
                match inbound.recv().await {
                    Some(Inbound::Bytes(1, bytes)) => {
                        frames += bytes.iter().filter(|b| **b == 0).count();
                        arrivals.push(started.elapsed());
                        break;
                    }
                    Some(Inbound::Held(1)) => held += 1,
                    _ => panic!("expected bytes"),
                }
            }
        }
        assert_eq!(frames, 23);
        assert!(
            arrivals[19] < Duration::from_millis(40),
            "the burst: {arrivals:?}"
        );
        // Each frame past the burst waits about 50 ms for its token, never a window.
        for pair in arrivals[19..].windows(2) {
            let gap = pair[1] - pair[0];
            assert!(
                gap < Duration::from_millis(120),
                "gap {gap:?}: {arrivals:?}"
            );
        }
        assert!(
            arrivals[22] >= Duration::from_millis(120),
            "throttled: {arrivals:?}"
        );
        assert!(held >= 1, "the link task hears that the connection is held");
        drop(peer);
        loop {
            match inbound.recv().await {
                Some(Inbound::Held(1)) => {}
                other => {
                    assert!(matches!(other, Some(Inbound::Closed(1, None))));
                    break;
                }
            }
        }
    }

    #[test]
    fn a_frame_counts_once_whatever_delimiters_surround_it() {
        let mut counter = FrameCounter { in_frame: false };
        assert_eq!(counter.count(&[0, 1, 2, 0]), 1);
        assert_eq!(counter.count(&[0, 0, 3, 0, 4]), 1);
        assert_eq!(counter.count(&[5, 0]), 1);
        assert_eq!(counter.count(&[0, 0, 0]), 0);
    }
}

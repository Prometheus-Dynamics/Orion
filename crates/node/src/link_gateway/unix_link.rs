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
//! - **Rate limit.** Frames from a connection are read within the local control budget
//!   (`ORION_NODE_LOCAL_RATE_LIMIT_WINDOW_MS`, `_MAX_MESSAGES`): past it, the connection is not read
//!   until the window ends. Nothing is refused, so an action's final result is never lost.
//! - **Queues.** Output waiting for a connection is bounded in bytes ([`QUEUE_BYTES`]); past it, frames
//!   stay in the session, which bounds and counts its own queue. A connection that takes no output
//!   for [`WRITE_TIMEOUT`] is closed.
//! - Firmware transfers and `link.release` / `link.attach` do not apply: a Unix link has no port to
//!   hand over.

use super::UnixLinkConfig;
use super::events::{LEASE_REFRESH, LeaseAction, LinkContext, LinkSend, REOPEN_DELAY};
use super::unix_client::LinkClient;
use crate::NodeApp;
use crate::auth::{LocalAdmission, LocalAdmissionPolicy};
use orion::transport::ipc::UnixPeerIdentity;
use orion_link::Stream;
use orion_link::host::{ActionSendError, HostConfig, HostEvent, HostSession};
use orion_link::message::Roles;
use std::collections::BTreeMap;
use std::io;
use std::os::unix::fs::FileTypeExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Notify, mpsc, watch};
use tracing::{debug, info, warn};

/// Output bytes waiting for one connection above which no more frames are pulled from its session.
pub(super) const QUEUE_BYTES: usize = 64 * 1024;
/// A connection whose pending output does not move for this long is closed.
pub(super) const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
/// Inbound chunks buffered for the link task, across its connections.
const INBOUND_CHUNKS: usize = 64;
/// Read buffer per connection.
const READ_BUF: usize = 4096;

/// Per-connection limits, from the node's runtime tuning.
#[derive(Clone, Copy, Debug)]
pub(super) struct ConnLimits {
    pub(super) rate_window: Duration,
    pub(super) rate_max_frames: u32,
}

impl ConnLimits {
    pub(super) fn from_app(app: &NodeApp) -> Self {
        let tuning = &app.config.runtime_tuning;
        Self {
            rate_window: tuning.local_rate_limit_window.max(Duration::from_millis(1)),
            rate_max_frames: tuning.local_rate_limit_max_messages.max(1),
        }
    }
}

/// What a connection's I/O task tells the link task.
enum Inbound {
    Bytes(u64, Vec<u8>),
    Closed(u64, Option<io::Error>),
}

struct Conn {
    session: HostSession<Stream>,
    out: mpsc::UnboundedSender<Vec<u8>>,
    pending: Arc<AtomicUsize>,
    /// The caller's identity, for its session's client requests.
    identity: UnixPeerIdentity,
    /// The client of the current session, when it took the client role.
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

pub(super) async fn run(
    mut ctx: LinkContext,
    config: UnixLinkConfig,
    host: HostConfig,
    limits: ConnLimits,
    mut shutdown: watch::Receiver<bool>,
) {
    let policy = ctx.app.local_admission.clone();
    let mut tick = tokio::time::interval(LinkContext::tick_period(host.heartbeat_ms));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut desired = ctx.app.link_desired_changes();
    let mut listener: Option<UnixListener> = None;
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
            match bind(&config) {
                Ok(bound) => {
                    listener = Some(bound);
                    ctx.opened();
                }
                Err(error) => {
                    ctx.io_error(&format!("bind {}", config.path.display()), &error);
                    reopen_at = Instant::now() + REOPEN_DELAY;
                }
            }
        }

        let mut refresh = false;
        let mut closed: Vec<u64> = Vec::new();
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            event = inbound.recv() => match event {
                Some(Inbound::Bytes(id, bytes)) => {
                    if let Some(conn) = conns.get_mut(&id) {
                        ctx.status.bytes_rx += bytes.len() as u64;
                        conn.session.receive(&bytes);
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
            accepted = accept(listener.as_ref()) => match accepted {
                Ok(stream) => {
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
            _ = tick.tick() => {}
        }

        for id in closed {
            if let Some(conn) = conns.remove(&id) {
                drop_conn(&mut ctx, &mut retired, conn);
            }
        }

        let now = ctx.now_ms();
        let ids: Vec<u64> = conns.keys().copied().collect();
        for id in ids {
            let mut events = Vec::new();
            if let Some(conn) = conns.get_mut(&id) {
                conn.session.poll(now);
                while let Some(event) = conn.session.next_event() {
                    events.push(event);
                }
            }
            let mut duplicate = false;
            for event in events {
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
                    HostEvent::ClientRequest { .. } => {
                        if let HostEvent::ClientRequest { request, .. } = event
                            && let Some(client) = conn.client.as_mut()
                        {
                            client.request(&ctx.app, request);
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
                        if roles.contains(Roles::CLIENT) {
                            conn.client = Some(LinkClient::open(
                                &ctx.app,
                                &ctx.name,
                                device_name,
                                conn.identity.clone(),
                                client_wake.clone(),
                            ));
                        }
                    }
                    HostEvent::DeviceLost { .. } => {
                        if let Some(client) = conn.client.take() {
                            client.close(&ctx.app);
                        }
                    }
                    _ => {}
                }
                if let LeaseAction::Set(leases) = ctx.handle(event)
                    && let Some(conn) = conns.get_mut(&id)
                {
                    conn.session.set_leases(leases);
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
        }

        if refresh || Instant::now() >= lease_refresh_at {
            lease_refresh_at = Instant::now() + LEASE_REFRESH;
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
    if listener.take().is_some() {
        let _ = std::fs::remove_file(&config.path);
    }
    update_stats(&mut ctx, &retired, &conns);
    ctx.close();
}

/// Hands the session's frames to the connection while its queue has room; the rest wait in the
/// session. Returns the frames handed over.
fn transmit(ctx: &mut LinkContext, conn: &mut Conn) -> usize {
    let mut moved = 0;
    while conn.pending.load(Ordering::Acquire) < QUEUE_BYTES {
        let Some(bytes) = conn.session.transmit() else {
            break;
        };
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
fn bind(config: &UnixLinkConfig) -> io::Result<UnixListener> {
    match std::fs::symlink_metadata(&config.path) {
        Ok(meta) if meta.file_type().is_socket() => std::fs::remove_file(&config.path)?,
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "the path exists and is not a socket",
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    UnixListener::bind(&config.path)
}

async fn accept(listener: Option<&UnixListener>) -> io::Result<UnixStream> {
    match listener {
        Some(listener) => listener.accept().await.map(|(stream, _)| stream),
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
    let (out, out_rx) = mpsc::unbounded_channel();
    let pending = Arc::new(AtomicUsize::new(0));
    tokio::spawn(serve_conn(
        id,
        stream,
        out_rx,
        pending.clone(),
        inbound.clone(),
        drained.clone(),
        limits,
    ));
    Some(Conn {
        session: HostSession::stream(host.clone()),
        out,
        pending,
        identity,
        client: None,
    })
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
            device_name: device,
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
async fn serve_conn(
    id: u64,
    stream: UnixStream,
    mut out: mpsc::UnboundedReceiver<Vec<u8>>,
    pending: Arc<AtomicUsize>,
    inbound: mpsc::Sender<Inbound>,
    drained: Arc<Notify>,
    limits: ConnLimits,
) {
    let (mut reader, mut writer) = stream.into_split();
    let mut buf = vec![0u8; READ_BUF];
    let mut counter = FrameCounter { in_frame: false };
    let mut window_start = tokio::time::Instant::now();
    let mut window_frames = 0u32;
    let error = loop {
        let throttled = window_frames >= limits.rate_max_frames;
        tokio::select! {
            biased;
            frame = out.recv() => {
                let Some(frame) = frame else {
                    // The link task dropped the connection.
                    return;
                };
                let len = frame.len();
                let written = tokio::time::timeout(WRITE_TIMEOUT, writer.write_all(&frame)).await;
                let before = pending.fetch_sub(len, Ordering::AcqRel);
                if before >= QUEUE_BYTES && before - len < QUEUE_BYTES {
                    drained.notify_one();
                }
                match written {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => break Some(error),
                    Err(_) => break Some(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "the peer took no output",
                    )),
                }
            }
            () = tokio::time::sleep_until(window_start + limits.rate_window), if throttled => {
                window_start = tokio::time::Instant::now();
                window_frames = 0;
            }
            read = reader.read(&mut buf), if !throttled => match read {
                Ok(0) => break None,
                Ok(n) => {
                    let chunk = buf.get(..n).unwrap_or_default();
                    let now = tokio::time::Instant::now();
                    if now >= window_start + limits.rate_window {
                        window_start = now;
                        window_frames = 0;
                    }
                    window_frames = window_frames.saturating_add(counter.count(chunk));
                    if inbound.send(Inbound::Bytes(id, chunk.to_vec())).await.is_err() {
                        return;
                    }
                }
                Err(error) => break Some(error),
            },
        }
    };
    let _ = inbound.send(Inbound::Closed(id, error)).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn frames_past_the_budget_wait_for_the_next_window() {
        let (node_side, mut peer) = UnixStream::pair().expect("pair");
        let (_out, out_rx) = mpsc::unbounded_channel();
        let (inbound_tx, mut inbound) = mpsc::channel(16);
        let window = Duration::from_millis(300);
        tokio::spawn(serve_conn(
            1,
            node_side,
            out_rx,
            Arc::new(AtomicUsize::new(0)),
            inbound_tx,
            Arc::new(Notify::new()),
            ConnLimits {
                rate_window: window,
                rate_max_frames: 2,
            },
        ));
        let started = tokio::time::Instant::now();
        let mut arrivals = Vec::new();
        for frame in 0..3u8 {
            peer.write_all(&[frame + 1, 0]).await.expect("write");
            match inbound.recv().await {
                Some(Inbound::Bytes(1, bytes)) => assert_eq!(bytes, vec![frame + 1, 0]),
                _ => panic!("expected bytes"),
            }
            arrivals.push(started.elapsed());
        }
        assert!(arrivals[1] < window / 2, "within the budget: {arrivals:?}");
        assert!(
            arrivals[2] >= window - Duration::from_millis(20),
            "past the budget the frame waits for the window: {arrivals:?}"
        );
        drop(peer);
        assert!(matches!(
            inbound.recv().await,
            Some(Inbound::Closed(1, None))
        ));
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

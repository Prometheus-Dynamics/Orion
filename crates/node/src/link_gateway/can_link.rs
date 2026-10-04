//! The task serving one CAN bus (many devices, `HostBus`), over any [`CanIo`] socket.

use super::CanLinkConfig;
use super::events::{LEASE_REFRESH, LeaseAction, LinkContext, REOPEN_DELAY};
use orion_link::host::{BusFrame, HostBus, HostConfig};
use orion_link::{Packet, PacketStats};
use std::future::Future;
use std::io;
use std::time::{Duration, Instant};
use tokio::sync::watch;

/// Retry delay while the socket's transmit queue is full.
const TX_RETRY: Duration = Duration::from_millis(2);

/// One CAN or CAN FD data frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CanFrame {
    pub(crate) id: u32,
    pub(crate) extended: bool,
    pub(crate) fd: bool,
    pub(crate) data: Vec<u8>,
}

/// A CAN socket: SocketCAN in production, an in-memory bus in tests.
pub(crate) trait CanIo: Send + Sync + 'static {
    /// The next received data frame (remote and error frames are filtered out by the caller).
    /// Must be cancel-safe.
    fn recv(&self) -> impl Future<Output = io::Result<CanFrame>> + Send;
    /// Sends `frame`; `Ok(false)` if the transmit queue is full and it should be retried.
    fn try_send(&self, frame: &CanFrame) -> io::Result<bool>;
}

pub(super) async fn run<P: CanIo>(
    mut ctx: LinkContext,
    config: CanLinkConfig,
    host: HostConfig,
    mut open: impl FnMut() -> io::Result<P> + Send,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut tick = tokio::time::interval(LinkContext::tick_period(host.heartbeat_ms));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let transport = if config.fd {
        Packet::FD
    } else {
        Packet::CLASSIC
    };
    let mut bus = HostBus::new(host, config.base_ids(), transport, config.addresses.clone());
    let mut desired = ctx.app.link_desired_changes();
    let mut socket: Option<P> = None;
    let mut reopen_at = Instant::now();
    let mut lease_refresh_at = Instant::now() + LEASE_REFRESH;
    let mut pending: Option<BusFrame> = None;

    loop {
        if socket.is_none() && Instant::now() >= reopen_at {
            match open() {
                Ok(opened) => {
                    socket = Some(opened);
                    ctx.opened();
                }
                Err(error) => {
                    ctx.io_error(&format!("open {}", config.interface), &error);
                    reopen_at = Instant::now() + REOPEN_DELAY;
                }
            }
        }

        let mut refresh = false;
        let mut failure: Option<(&str, io::Error)> = None;
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            received = recv(socket.as_ref()) => match received {
                Ok(frame) => {
                    if bus.receive(frame.id, frame.extended, &frame.data) {
                        ctx.status.bytes_rx += frame.data.len() as u64;
                    }
                }
                Err(error) => failure = Some(("read", error)),
            },
            _ = retry(pending.is_some() && socket.is_some()) => {}
            changed = desired.changed() => refresh = changed.is_ok(),
            _ = tick.tick() => {}
        }

        bus.poll(ctx.now_ms());
        while let Some(event) = bus.next_event() {
            let address = event.address;
            if let LeaseAction::Set(leases) = ctx.handle(event.event) {
                bus.set_leases(address, leases);
            }
        }
        if refresh || Instant::now() >= lease_refresh_at {
            lease_refresh_at = Instant::now() + LEASE_REFRESH;
            let accepted: Vec<String> = ctx.accepted().cloned().collect();
            for name in accepted {
                if let (Some(address), Some(leases)) =
                    (bus.address_of(&name), ctx.refreshed_leases(&name))
                {
                    bus.set_leases(address, leases);
                }
            }
        }

        if failure.is_none()
            && let Some(open_socket) = socket.as_ref()
        {
            loop {
                let Some(frame) = pending.take().or_else(|| bus.next_frame()) else {
                    break;
                };
                let out = CanFrame {
                    id: frame.id,
                    extended: frame.extended,
                    fd: config.fd,
                    data: frame.segment.as_bytes().to_vec(),
                };
                match open_socket.try_send(&out) {
                    Ok(true) => {
                        ctx.status.frames_tx += 1;
                        ctx.status.bytes_tx += out.data.len() as u64;
                    }
                    Ok(false) => {
                        pending = Some(frame);
                        break;
                    }
                    Err(error) => {
                        failure = Some(("write", error));
                        break;
                    }
                }
            }
        } else {
            // Without a socket, frames are dropped as on a disconnected bus.
            while bus.next_frame().is_some() {}
        }
        if let Some((what, error)) = failure {
            ctx.io_error(what, &error);
            socket = None;
            pending = None;
            ctx.status.open = false;
            reopen_at = Instant::now() + REOPEN_DELAY;
        }
        update_stats(&mut ctx, &bus);
        ctx.publish_status();
    }
    drop(socket);
    update_stats(&mut ctx, &bus);
    ctx.close();
}

fn update_stats(ctx: &mut LinkContext, bus: &HostBus) {
    let mut frames_rx = 0u64;
    let mut decode_errors = 0u64;
    let mut sessions = 0u64;
    let mut device_timeouts = 0u64;
    let mut hello_rejects = 0u64;
    let mut decoder = PacketStats::default();
    let mut dropped = 0u64;
    for address in bus.addresses() {
        let Some(session) = bus.session(address) else {
            continue;
        };
        let host = session.stats();
        frames_rx += u64::from(host.frames_received);
        decode_errors += u64::from(host.decode_errors);
        sessions += u64::from(host.sessions);
        device_timeouts += u64::from(host.device_timeouts);
        hello_rejects += u64::from(host.rejects_sent);
        let stats = session.decoder().stats();
        decoder.crc_errors = decoder.crc_errors.wrapping_add(stats.crc_errors);
        decoder.framing_errors = decoder.framing_errors.wrapping_add(stats.framing_errors);
        dropped += u64::from(stats.dropped());
    }
    let status = &mut ctx.status;
    status.frames_rx = frames_rx;
    status.decode_errors = decode_errors;
    status.sessions = sessions;
    status.device_timeouts = device_timeouts;
    status.hello_rejects = hello_rejects;
    status.crc_errors = u64::from(decoder.crc_errors);
    status.framing_errors = u64::from(decoder.framing_errors);
    status.dropped = dropped;
}

async fn recv<P: CanIo>(socket: Option<&P>) -> io::Result<CanFrame> {
    match socket {
        Some(socket) => socket.recv().await,
        None => std::future::pending().await,
    }
}

async fn retry(wanted: bool) {
    if wanted {
        tokio::time::sleep(TX_RETRY).await;
    } else {
        std::future::pending::<()>().await;
    }
}

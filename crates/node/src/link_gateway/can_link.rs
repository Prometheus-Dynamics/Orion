//! The task serving one CAN bus (many devices, `HostBus`), over any [`CanIo`] socket.
//!
//! Besides the bus's traffic, the task takes device actions, firmware jobs and the released
//! devices from the node (`link.release` / `link.attach`). Release is per device: a released device
//! loses its session (nothing is routed to it, and what it sends is dropped) and its providers
//! become unavailable; the other devices on the bus are not affected. A firmware job runs per
//! device, so transfers to different devices can run at once.

use super::CanLinkConfig;
use super::events::{LEASE_REFRESH, LeaseAction, LinkContext, LinkSend, REOPEN_DELAY};
use super::firmware::{DeviceSeen, FirmwareJob};
use crate::actions::ActionOutcome;
use crate::app::{CanSlot, FirmwareCommand};
use orion_link::host::{ActionSendError, BusFrame, HostBus, HostConfig, HostEvent};
use orion_link::wire::max_chunk_for_frame;
use orion_link::{Packet, PacketStats};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::io;
use std::time::{Duration, Instant};
use tokio::sync::watch;

/// Retry delay while the socket's transmit queue is full.
const TX_RETRY: Duration = Duration::from_millis(2);

/// Segments one firmware chunk may take on the bus. A chunk is resent whole when a segment is lost,
/// and a long one holds the bus from the other devices sharing it.
const CHUNK_SEGMENTS: usize = 32;
/// Largest firmware chunk, whatever the segment budget allows (CAN FD).
const CHUNK_MAX: usize = 256;

/// The firmware chunk length on a bus with `transport`: the most payload that fits
/// [`CHUNK_SEGMENTS`] segments, less the chunk frame's own overhead, and at most [`CHUNK_MAX`]. The
/// device's and the frame's limits apply on top, in the driver.
fn firmware_chunk_len(transport: Packet) -> u16 {
    let budget = max_chunk_for_frame(CHUNK_SEGMENTS * transport.mtu.payload_len());
    u16::try_from(budget.min(CHUNK_MAX)).unwrap_or(u16::MAX)
}

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
    let chunk_len = firmware_chunk_len(transport);
    let mut bus = HostBus::new(host, config.base_ids(), transport, config.addresses.clone());
    let mut desired = ctx.app.link_desired_changes();
    let mut socket: Option<P> = None;
    let mut reopen_at = Instant::now();
    let mut lease_refresh_at = Instant::now() + LEASE_REFRESH;
    let mut pending: Option<BusFrame> = None;
    let mut commands = ctx.open_actions();
    let mut commands_open = true;
    let CanSlot {
        jobs: mut job_commands,
        released: mut released_set,
    } = ctx.open_can_slot();
    let mut jobs_open = true;
    let mut release_open = true;
    // The devices this task has applied as released.
    let mut held: BTreeSet<String> = BTreeSet::new();
    // What the task knows of each device it has seen (its firmware job replays it on start).
    let mut seen: BTreeMap<String, DeviceSeen> = BTreeMap::new();
    let mut running: BTreeMap<String, FirmwareJob> = BTreeMap::new();

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
            command = commands.recv(), if commands_open => match command {
                Some(command) => ctx.accept_action(command, |device, send| {
                    match bus.address_of(device) {
                        Some(address) => match send {
                            LinkSend::Request { id, name, payload } => {
                                bus.send_action(address, id, name, payload)
                            }
                            LinkSend::Cancel { id } => bus.send_action_cancel(address, id),
                        },
                        None => Err(ActionSendError::NotConnected),
                    }
                }),
                None => commands_open = false,
            },
            command = job_commands.recv(), if jobs_open => match command {
                Some(command) => start_job(
                    &mut running,
                    command,
                    &ctx.name,
                    &held,
                    &seen,
                    chunk_len,
                ),
                None => jobs_open = false,
            },
            changed = released_set.changed(), if release_open => {
                if changed.is_err() {
                    release_open = false;
                }
            }
            _ = tick.tick() => {}
        }

        // Release and attach, as the node's set now says.
        let wanted: BTreeSet<String> = released_set.borrow().clone();
        if wanted != held {
            let newly: Vec<String> = wanted.difference(&held).cloned().collect();
            held = wanted;
            for device in newly {
                if let Some(address) = bus.address_of(&device) {
                    // The session goes: nothing is routed to the device until it is attached.
                    bus.forget(address);
                    let _ = ctx.handle(HostEvent::DeviceLost {
                        device_name: device.clone(),
                    });
                    if pending
                        .as_ref()
                        .is_some_and(|frame| frame.address == address)
                    {
                        pending = None;
                    }
                }
                seen.remove(&device);
                if let Some(job) = running.remove(&device) {
                    job.finish(ActionOutcome::failed(
                        "device released during the firmware transfer",
                    ));
                }
            }
        }

        bus.poll(ctx.now_ms());
        while let Some(event) = bus.next_event() {
            let address = event.address;
            if let Some(device) = event_device(&event.event)
                && held.contains(device)
            {
                // A released device gets no session: its traffic is dropped, and a new session it
                // opens (its hello) is dropped the same way until it is attached.
                bus.forget(address);
                continue;
            }
            if let Some(device) = event_device(&event.event) {
                seen.entry(device.to_owned())
                    .or_default()
                    .on_event(&event.event);
            }
            for job in running.values_mut() {
                job.on_event(&event.event);
            }
            if let LeaseAction::Set(leases) = ctx.handle(event.event) {
                bus.set_leases(address, leases);
            }
        }

        // Firmware jobs: one step each; a finished job ends its action, and a handoff releases its
        // device (its bootloader owns the bus now).
        let now = ctx.now_ms();
        let mut ended = Vec::new();
        for (device, job) in &mut running {
            let address = bus.address_of(device);
            if let Some(end) = job.drive(now, |request| {
                address.is_some_and(|address| bus.send_xfer(address, request))
            }) {
                ended.push((device.clone(), end));
            }
        }
        for (device, end) in ended {
            if let Some(job) = running.remove(&device) {
                if end.handoff {
                    let _ = ctx.app.link_set_device_released(&ctx.name, &device, true);
                }
                job.finish(end.outcome);
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
            while let Some(frame) = pending.take().or_else(|| bus.next_frame()) {
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
    for job in std::mem::take(&mut running).into_values() {
        job.finish(ActionOutcome::failed(
            "link closed before the firmware transfer finished",
        ));
    }
    update_stats(&mut ctx, &bus);
    ctx.close();
}

/// Accepts a firmware job for its device, or refuses it: a released device, a transfer to the
/// device already running, or a device that is not connected now (the job does not wait for one).
fn start_job(
    running: &mut BTreeMap<String, FirmwareJob>,
    mut command: FirmwareCommand,
    link: &str,
    held: &BTreeSet<String>,
    seen: &BTreeMap<String, DeviceSeen>,
    chunk_len: u16,
) {
    let device = command.device.clone();
    let busy = running.contains_key(&device);
    let reason = if held.contains(&device) {
        format!("device `{device}` on link {link} is released; run link.attach first")
    } else if busy {
        format!("a firmware transfer to `{device}` is already running on link {link}")
    } else if let Some(device_seen) = seen.get(&device).filter(|seen| seen.is_connected(&device)) {
        command.driver.set_chunk_len(chunk_len);
        running.insert(device, FirmwareJob::start(command, device_seen));
        return;
    } else {
        format!("device `{device}` is not connected on link {link}")
    };
    command.reject(reason);
}

/// The device an event is about, if any.
fn event_device(event: &HostEvent) -> Option<&str> {
    match event {
        HostEvent::DeviceConnected { device_name, .. }
        | HostEvent::ProviderState { device_name, .. }
        | HostEvent::Status { device_name, .. }
        | HostEvent::DeviceLost { device_name }
        | HostEvent::DeviceInfo { device_name, .. }
        | HostEvent::Xfer { device_name, .. }
        | HostEvent::ActionResult { device_name, .. }
        | HostEvent::LeasesTooLarge { device_name, .. } => Some(device_name.as_str()),
        HostEvent::DeviceRejected { device_name, .. } => device_name.as_deref(),
        _ => None,
    }
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

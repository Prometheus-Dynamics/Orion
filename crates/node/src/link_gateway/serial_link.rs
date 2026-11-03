//! The task serving one serial link (one device, `HostSession<Stream>`).
//!
//! Besides the device's traffic, the task takes two things from the node: device actions (see
//! `actions`), and the link's control (`link.release` / `link.attach`, and firmware jobs). A
//! released link serves no port until it is attached; a firmware job runs in this task, because
//! it owns the session (`firmware.rs`).

use super::SerialLinkConfig;
use super::events::{LEASE_REFRESH, LeaseAction, LinkContext, REOPEN_DELAY};
use super::firmware::{DeviceSeen, FirmwareJob};
use super::serial::SerialPort;
use crate::actions::ActionOutcome;
use crate::app::{FirmwareCommand, SerialSlot};
use orion_link::Stream;
use orion_link::host::{ActionSendError, HostConfig, HostEvent, HostSession};
use std::time::Instant;
use tokio::sync::watch;

/// Pending output above which no more frames are pulled from the session (it queues and counts
/// drops itself).
const TX_HIGH_WATER: usize = 8 * 1024;

pub(super) async fn run(
    mut ctx: LinkContext,
    config: SerialLinkConfig,
    host: HostConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut tick = tokio::time::interval(LinkContext::tick_period(host.heartbeat_ms));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut session = HostSession::stream(host.clone());
    let mut desired = ctx.app.link_desired_changes();
    let mut port: Option<SerialPort> = None;
    let mut reopen_at = Instant::now();
    let mut lease_refresh_at = Instant::now() + LEASE_REFRESH;
    let mut tx: Vec<u8> = Vec::new();
    let mut buf = vec![0u8; 4096];
    let mut commands = ctx.open_actions();
    let mut commands_open = true;
    let SerialSlot {
        mut jobs,
        mut released,
    } = ctx.open_serial_slot();
    let mut jobs_open = true;
    let mut release_open = true;
    // The release state this task has applied (the node's flag may be ahead of it for a moment).
    let mut held = false;
    let mut seen = DeviceSeen::default();
    let mut job: Option<FirmwareJob> = None;

    loop {
        if port.is_none() && !held && Instant::now() >= reopen_at {
            match SerialPort::open(&config) {
                Ok(opened) => {
                    port = Some(opened);
                    ctx.opened();
                }
                Err(error) => {
                    ctx.io_error(&format!("open {}", config.path.display()), &error);
                    reopen_at = Instant::now() + REOPEN_DELAY;
                }
            }
        }

        let mut refresh = false;
        let mut failure: Option<(&str, std::io::Error)> = None;
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            read = read_port(port.as_ref(), &mut buf) => match read {
                Ok(0) => failure = Some(("read", std::io::Error::other("hang-up"))),
                Ok(n) => {
                    ctx.status.bytes_rx += n as u64;
                    session.receive(buf.get(..n).unwrap_or_default());
                }
                Err(error) => failure = Some(("read", error)),
            },
            written = write_port(port.as_ref(), &tx) => match written {
                Ok(n) => {
                    ctx.status.bytes_tx += n as u64;
                    tx.drain(..n.min(tx.len()));
                }
                Err(error) => failure = Some(("write", error)),
            },
            changed = desired.changed() => refresh = changed.is_ok(),
            command = commands.recv(), if commands_open => match command {
                Some(command) => ctx.accept_action(command, |device, id, name, payload| {
                    match session.device_name() {
                        Some(current) if current == device => session.send_action(id, name, payload),
                        _ => Err(ActionSendError::NotConnected),
                    }
                }),
                None => commands_open = false,
            },
            command = jobs.recv(), if jobs_open => match command {
                Some(command) => start_job(
                    &mut job,
                    command,
                    &ctx.name,
                    *released.borrow(),
                    &seen,
                ),
                None => jobs_open = false,
            },
            changed = released.changed(), if release_open => {
                if changed.is_err() {
                    release_open = false;
                }
            }
            _ = tick.tick() => {}
        }

        // Release or attach, as the node's flag now says.
        let wanted = *released.borrow();
        if wanted != held {
            held = wanted;
            if held {
                port = None;
                tx.clear();
                ctx.status.open = false;
                if let Some(device) = session.device_name().map(str::to_owned) {
                    // The device is lost to this node until it is attached again.
                    let lost = HostEvent::DeviceLost {
                        device_name: device,
                    };
                    seen.on_event(&lost);
                    ctx.handle(lost);
                }
                session = HostSession::stream(host.clone());
                if let Some(running) = job.take() {
                    running.finish(ActionOutcome::failed(
                        "link released during the firmware transfer",
                    ));
                }
            } else {
                reopen_at = Instant::now();
            }
        }

        session.poll(ctx.now_ms());
        while let Some(event) = session.next_event() {
            seen.on_event(&event);
            if let Some(running) = job.as_mut() {
                running.on_event(&event);
            }
            if let LeaseAction::Set(leases) = ctx.handle(event) {
                session.set_leases(leases);
            }
        }
        if let Some(mut running) = job.take() {
            match running.drive(ctx.now_ms(), |request| session.send_xfer(request)) {
                None => job = Some(running),
                Some(end) => {
                    if end.handoff {
                        // The device's bootloader owns the port now; it is not reopened until the
                        // link is attached again.
                        let _ = ctx
                            .app
                            .link_set_device_released(&ctx.name, running.device(), true);
                    }
                    running.finish(end.outcome);
                }
            }
        }
        if refresh || Instant::now() >= lease_refresh_at {
            lease_refresh_at = Instant::now() + LEASE_REFRESH;
            if let Some(leases) = session
                .device_name()
                .and_then(|name| ctx.refreshed_leases(name))
            {
                session.set_leases(leases);
            }
        }

        while tx.len() < TX_HIGH_WATER {
            let Some(bytes) = session.transmit() else {
                break;
            };
            ctx.status.frames_tx += 1;
            // Without an open port the frame is dropped, as on a disconnected cable.
            if port.is_some() {
                tx.extend_from_slice(&bytes);
            }
        }
        if failure.is_none()
            && let Some(open) = port.as_ref()
            && !tx.is_empty()
        {
            match open.try_write(&tx) {
                Ok(n) => {
                    ctx.status.bytes_tx += n as u64;
                    tx.drain(..n.min(tx.len()));
                }
                Err(error) => failure = Some(("write", error)),
            }
        }
        if let Some((what, error)) = failure {
            ctx.io_error(what, &error);
            port = None;
            tx.clear();
            ctx.status.open = false;
            reopen_at = Instant::now() + REOPEN_DELAY;
        }
        update_stats(&mut ctx, &session);
        ctx.publish_status();
    }
    drop(port);
    if let Some(running) = job.take() {
        running.finish(ActionOutcome::failed(
            "link closed before the firmware transfer finished",
        ));
    }
    update_stats(&mut ctx, &session);
    ctx.close();
}

/// Accepts a firmware job, or refuses it: a released link, a job already running, or a device that
/// is not connected now (the job does not wait for one; the action's deadline bounds a wait).
fn start_job(
    job: &mut Option<FirmwareJob>,
    command: FirmwareCommand,
    link: &str,
    released: bool,
    seen: &DeviceSeen,
) {
    let reason = if released {
        format!(
            "device `{}` on link {link} is released; run link.attach first",
            command.device
        )
    } else if job.is_some() {
        format!("a firmware transfer is already running on link {link}")
    } else if !seen.is_connected(&command.device) {
        format!(
            "device `{}` is not connected on link {link}",
            command.device
        )
    } else {
        *job = Some(FirmwareJob::start(command, seen));
        return;
    };
    command.reject(reason);
}

fn update_stats(ctx: &mut LinkContext, session: &HostSession<Stream>) {
    let host = session.stats();
    let decoder = session.decoder().stats();
    let status = &mut ctx.status;
    status.frames_rx = u64::from(host.frames_received);
    status.decode_errors = u64::from(host.decode_errors);
    status.sessions = u64::from(host.sessions);
    status.device_timeouts = u64::from(host.device_timeouts);
    status.hello_rejects = u64::from(host.rejects_sent);
    status.crc_errors = u64::from(decoder.crc_errors);
    status.framing_errors = u64::from(decoder.framing_errors);
    status.dropped = u64::from(decoder.dropped());
}

async fn read_port(port: Option<&SerialPort>, buf: &mut [u8]) -> std::io::Result<usize> {
    match port {
        Some(port) => port.read(buf).await,
        None => std::future::pending().await,
    }
}

async fn write_port(port: Option<&SerialPort>, bytes: &[u8]) -> std::io::Result<usize> {
    match port {
        Some(port) if !bytes.is_empty() => port.write(bytes).await,
        _ => std::future::pending().await,
    }
}

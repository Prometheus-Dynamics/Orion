//! The task serving one serial link (one device, `HostSession<Stream>`).

use super::SerialLinkConfig;
use super::events::{LEASE_REFRESH, LeaseAction, LinkContext, REOPEN_DELAY};
use super::serial::SerialPort;
use orion_link::Stream;
use orion_link::host::{HostConfig, HostSession};
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
    let mut session = HostSession::stream(host);
    let mut desired = ctx.app.link_desired_changes();
    let mut port: Option<SerialPort> = None;
    let mut reopen_at = Instant::now();
    let mut lease_refresh_at = Instant::now() + LEASE_REFRESH;
    let mut tx: Vec<u8> = Vec::new();
    let mut buf = vec![0u8; 4096];

    loop {
        if port.is_none() && Instant::now() >= reopen_at {
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
            _ = tick.tick() => {}
        }

        session.poll(ctx.now_ms());
        while let Some(event) = session.next_event() {
            if let LeaseAction::Set(leases) = ctx.handle(event) {
                session.set_leases(leases);
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
    update_stats(&mut ctx, &session);
    ctx.close();
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

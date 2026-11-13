//! Accepting WebSocket clients and running one connection: JSON control messages and MessagePack
//! frames in, queued messages out.

use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use orion_ws::{Heard, WsOptions, WsSocket};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio::time::Instant;

use super::state::{Outgoing, State, Sub};
use crate::codec::{Frame, for_each_frame};
use crate::error::{Error, Result};
use crate::mailbox::{self, CONTROL_CAPACITY};
use crate::message::{Control, parse_text};
use crate::{MAX_MESSAGE, SUBPROTOCOL_V4_0, SUBPROTOCOL_V4_1};

pub(super) fn lock(state: &Mutex<State>) -> MutexGuard<'_, State> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

/// When a client connection is considered gone.
#[derive(Clone, Copy, Debug)]
pub(super) struct Liveness {
    /// Silence after which the connection is closed.
    pub(super) timeout: Duration,
    /// WebSocket pings sent this often.
    pub(super) ping_every: Duration,
}

/// Accepts connections until the owning task is aborted; each connection runs in a `JoinSet`, so
/// aborting this loop also ends them.
pub(super) async fn accept_loop(
    listener: Option<TcpListener>,
    mut taken: tokio::sync::mpsc::UnboundedReceiver<WsSocket>,
    state: std::sync::Arc<Mutex<State>>,
    liveness: Liveness,
) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            accepted = async {
                match &listener {
                    Some(listener) => listener.accept().await,
                    // Only what is handed over.
                    None => std::future::pending().await,
                }
            } => match accepted {
                Ok((stream, from)) => {
                    crate::net::tune(&stream, liveness.timeout);
                    let state = std::sync::Arc::clone(&state);
                    connections.spawn(async move {
                        let _ = upgrade(stream, from, state, liveness).await;
                    });
                }
                // Typically EMFILE: back off instead of spinning.
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            },
            // A connection upgraded elsewhere and handed over.
            Some(socket) = taken.recv() => {
                let state = std::sync::Arc::clone(&state);
                connections.spawn(async move {
                    let _ = serve(socket, state, liveness).await;
                });
            }
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
    }
}

/// Upgrades a connection this server accepted, preferring v4.1 to 4.0, and runs it. A
/// connection that offers neither is turned away.
async fn upgrade(
    stream: TcpStream,
    from: std::net::SocketAddr,
    state: std::sync::Arc<Mutex<State>>,
    liveness: Liveness,
) -> Result<()> {
    let options = WsOptions {
        origins: Vec::new(),
        handshake: liveness.timeout,
        send_buffer: 0,
    };
    let protocols = [SUBPROTOCOL_V4_1, SUBPROTOCOL_V4_0];
    match orion_ws::accept(stream, from, &options, &protocols).await? {
        Some(socket) => serve(socket, state, liveness).await,
        None => Ok(()),
    }
}

/// Runs one client's connection, already upgraded, until it ends. The client's name is the
/// path it asked for after `/nt/`.
pub(super) async fn serve(
    socket: WsSocket,
    state: std::sync::Arc<Mutex<State>>,
    liveness: Liveness,
) -> Result<()> {
    let name = socket.path.trim_start_matches("/nt/").to_owned();
    let WsSocket {
        mut read,
        mut write,
        ..
    } = socket;

    let (tx, mut rx) = mailbox::channel(CONTROL_CAPACITY);
    let client = lock(&state).add_client(name, tx);
    let mut heard = Instant::now();
    let mut ping = tokio::time::interval(liveness.ping_every);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            incoming = read.next(MAX_MESSAGE) => match incoming {
                Ok(Some(heard_what)) => {
                    // A pong counts: the client's WebSocket answers the pings below.
                    heard = Instant::now();
                    match heard_what {
                        Heard::Text => match std::str::from_utf8(read.message()) {
                            Ok(text) => handle_text(&state, client, text),
                            Err(_) => lock(&state).warn(Some(client), "text that is not UTF-8".into()),
                        },
                        Heard::Binary => handle_binary(&state, client, read.message()),
                        Heard::Alive => {}
                    }
                }
                Ok(None) | Err(_) => break,
            },
            _ = ping.tick() => {
                if write.ping().await.is_err() {
                    break;
                }
            }
            () = tokio::time::sleep_until(heard + liveness.timeout) => break,
            outgoing = rx.recv() => {
                let sent = match outgoing {
                    Some(Outgoing::Text(text)) => write.send_text(&text).await,
                    Some(Outgoing::Binary(bytes)) => write.send_binary(&bytes).await,
                    None => break,
                };
                if sent.is_err() {
                    break;
                }
            }
        }
    }
    lock(&state).remove_client(client);
    Ok(())
}

fn handle_text(state: &Mutex<State>, client: u64, text: &str) {
    let mut state = lock(state);
    let parsed = match parse_text(text) {
        Ok(parsed) => parsed,
        Err(e) => return state.warn(Some(client), e.to_string()),
    };
    for item in parsed {
        let result = match item {
            Ok(Control::Publish(p)) => {
                state.client_publish(client, &p.name, &p.type_name, p.pubuid, p.properties)
            }
            Ok(Control::Unpublish(p)) => {
                state.client_unpublish(client, p.pubuid);
                Ok(())
            }
            Ok(Control::Setproperties(p)) => state.update_properties(&p.name, p.update),
            Ok(Control::Subscribe(p)) => {
                state.subscribe(
                    client,
                    p.subuid,
                    Sub {
                        topics: p.topics,
                        options: p.options,
                    },
                );
                Ok(())
            }
            Ok(Control::Unsubscribe(p)) => {
                state.unsubscribe(client, p.subuid);
                Ok(())
            }
            Ok(other) => Err(Error::Decode(format!(
                "server-only method from a client: {other:?}"
            ))),
            Err(message) => Err(Error::Decode(message)),
        };
        if let Err(e) = result {
            state.warn(Some(client), e.to_string());
        }
    }
}

fn handle_binary(state: &Mutex<State>, client: u64, bytes: &[u8]) {
    let mut state = lock(state);
    // One WebSocket message may batch several values; a malformed one drops the rest of it.
    let error = for_each_frame(bytes, |frame| handle_frame(&mut state, client, frame));
    if let Some(e) = error {
        state.warn(Some(client), e.to_string());
    }
}

fn handle_frame(state: &mut State, client: u64, frame: Frame) {
    if frame.id == -1 {
        if let Err(e) = state.reply_rtt(client, &frame.value) {
            state.warn(Some(client), e.to_string());
        }
        return;
    }
    let result = u32::try_from(frame.id)
        .map_err(|_| Error::Decode(format!("negative publisher uid {}", frame.id)))
        .and_then(|pubuid| state.client_value(client, pubuid, frame.value));
    if let Err(e) = result {
        state.warn(Some(client), e.to_string());
    }
}

//! Accepting WebSocket clients and running one connection: JSON control messages and MessagePack
//! frames in, queued messages out.

use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL;
use tokio_tungstenite::tungstenite::http::{HeaderValue, StatusCode};

use super::state::{Outgoing, State, Sub};
use crate::codec::{Frame, decode_frames};
use crate::error::{Error, Result};
use crate::message::{Control, parse_text};
use crate::{SUBPROTOCOL_V4_0, SUBPROTOCOL_V4_1};

pub(super) fn lock(state: &Mutex<State>) -> MutexGuard<'_, State> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Accepts connections until the owning task is aborted; each connection runs in a `JoinSet`, so
/// aborting this loop also ends them.
pub(super) async fn accept_loop(listener: TcpListener, state: std::sync::Arc<Mutex<State>>) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    let state = std::sync::Arc::clone(&state);
                    connections.spawn(async move {
                        let _ = serve(stream, state).await;
                    });
                }
                // Typically EMFILE: back off instead of spinning.
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            },
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
    }
}

/// Picks the NT4 subprotocol (v4.1 first, then 4.0) and records the client name from the path.
// The error type is fixed by tungstenite's `Callback` trait.
#[allow(clippy::result_large_err)]
fn handshake(
    req: &Request,
    mut response: Response,
    name: &mut String,
) -> std::result::Result<Response, ErrorResponse> {
    let offered: Vec<&str> = req
        .headers()
        .get_all(SEC_WEBSOCKET_PROTOCOL)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(str::trim)
        .collect();
    let chosen = if offered.contains(&SUBPROTOCOL_V4_1) {
        SUBPROTOCOL_V4_1
    } else if offered.contains(&SUBPROTOCOL_V4_0) {
        SUBPROTOCOL_V4_0
    } else {
        let mut rejection = ErrorResponse::new(Some("no NT4 subprotocol offered".to_owned()));
        *rejection.status_mut() = StatusCode::BAD_REQUEST;
        return Err(rejection);
    };
    response
        .headers_mut()
        .insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static(chosen));
    *name = req.uri().path().trim_start_matches("/nt/").to_owned();
    Ok(response)
}

// The handshake error type is fixed by tungstenite's `Callback` trait.
#[allow(clippy::result_large_err)]
async fn serve(stream: TcpStream, state: std::sync::Arc<Mutex<State>>) -> Result<()> {
    let mut name = String::new();
    let callback = |req: &Request, response: Response| handshake(req, response, &mut name);
    let mut socket = accept_hdr_async(stream, callback).await?;

    let (tx, mut rx) = mpsc::unbounded_channel();
    let client = lock(&state).add_client(name, tx);
    loop {
        tokio::select! {
            incoming = socket.next() => match incoming {
                Some(Ok(Message::Text(text))) => handle_text(&state, client, text.as_str()),
                Some(Ok(Message::Binary(bytes))) => handle_binary(&state, client, &bytes),
                Some(Ok(_)) => {}
                Some(Err(_)) | None => break,
            },
            outgoing = rx.recv() => {
                let sent = match outgoing {
                    Some(Outgoing::Text(text)) => socket.send(Message::Text(text.into())).await,
                    Some(Outgoing::Binary(bytes)) => {
                        socket.send(Message::Binary(bytes.to_vec().into())).await
                    }
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
    let (frames, error) = decode_frames(bytes);
    for frame in frames {
        handle_frame(&mut state, client, frame);
    }
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

//! The client connection task: connect, replay state, pump frames, reconnect with backoff.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep, sleep_until, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use super::subscription::{TopicEvent, TopicInfo};
use super::{ClientConfig, ClientEvent, Command, Shared};
use crate::codec::{decode_frame, encode_frame};
use crate::error::{Error, Result};
use crate::message::{
    AnnounceParams, Control, PublishParams, SetPropertiesParams, SubscribeOptions, SubscribeParams,
    UnpublishParams, UnsubscribeParams, encode_text, name_matches, parse_text,
};
use crate::time::now_micros;
use crate::value::Value;
use crate::{SUBPROTOCOL_V4_0, SUBPROTOCOL_V4_1};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Offered in this order; the server picks one and the client accepts either.
const OFFERED_SUBPROTOCOLS: &str = "v4.1.networktables.first.wpi.edu, networktables.first.wpi.edu";
/// Pings sent back to back after connecting, then one every `PING_INTERVAL`.
const BURST_PINGS: u32 = 5;
const BURST_INTERVAL: Duration = Duration::from_millis(200);
const PING_INTERVAL: Duration = Duration::from_secs(5);

/// Per-client state that survives reconnects.
#[derive(Default)]
struct State {
    subs: BTreeMap<u32, SubState>,
    pubs: BTreeMap<u32, Publication>,
}

struct SubState {
    topics: Vec<String>,
    options: SubscribeOptions,
    events: mpsc::UnboundedSender<TopicEvent>,
}

struct Publication {
    name: String,
    type_name: String,
    /// Last value, re-sent after a reconnect so the server has it again.
    last: Option<Value>,
}

/// Per-connection state, reset on every connect.
#[derive(Default)]
struct Link {
    /// Server topic id -> name, from announces.
    ids: HashMap<i32, String>,
    /// Topics this client publishes (announced with its `pubuid`). Their values are the client's
    /// own echo, delivered only to subscriptions with `all`.
    own: HashSet<i32>,
    /// Smallest RTT seen on this connection.
    best_rtt_us: Option<i64>,
    pings_sent: u32,
}

/// What a connection attempt ended with.
enum Outcome {
    Shutdown,
    Lost(String),
}

/// A message to put on the wire.
enum Outbound {
    Text(Control),
    Binary(Vec<u8>),
}

pub(crate) async fn run(
    config: ClientConfig,
    shared: Arc<Shared>,
    mut commands: mpsc::UnboundedReceiver<Command>,
    events: mpsc::UnboundedSender<ClientEvent>,
) {
    let mut state = State::default();
    let mut backoff = config.reconnect_min;
    loop {
        let outcome = match connect(&config).await {
            Ok(socket) => {
                backoff = config.reconnect_min;
                let _ = events.send(ClientEvent::Connected);
                serve(socket, &mut state, &mut commands, &events, &shared).await
            }
            Err(e) => Outcome::Lost(e.to_string()),
        };
        match outcome {
            Outcome::Shutdown => return,
            Outcome::Lost(reason) => {
                let _ = events.send(ClientEvent::Disconnected { reason });
            }
        }
        // Wait out the backoff, still recording commands so they go out on the next connect.
        let wait = sleep(backoff);
        tokio::pin!(wait);
        loop {
            tokio::select! {
                () = &mut wait => break,
                command = commands.recv() => match command {
                    Some(command) => {
                        // Offline: only the state changes; nothing goes on the wire.
                        let _ = apply(&mut state, command);
                    }
                    None => return,
                },
            }
        }
        backoff = (backoff * 2).min(config.reconnect_max);
    }
}

async fn connect(config: &ClientConfig) -> Result<Socket> {
    let host = if config.host.contains(':') && !config.host.starts_with('[') {
        format!("[{}]", config.host)
    } else {
        config.host.clone()
    };
    let url = format!("ws://{host}:{}/nt/{}", config.port, config.name);
    let mut request =
        tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(url)?;
    request.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static(OFFERED_SUBPROTOCOLS),
    );
    let (socket, response) = timeout(config.connect_timeout, connect_async(request))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "connect timed out"))??;
    let selected = response
        .headers()
        .get(SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok());
    match selected {
        Some(SUBPROTOCOL_V4_1 | SUBPROTOCOL_V4_0) => Ok(socket),
        _ => Err(Error::Subprotocol {
            offered: OFFERED_SUBPROTOCOLS.to_owned(),
        }),
    }
}

async fn serve(
    mut socket: Socket,
    state: &mut State,
    commands: &mut mpsc::UnboundedReceiver<Command>,
    events: &mpsc::UnboundedSender<ClientEvent>,
    shared: &Shared,
) -> Outcome {
    let mut link = Link::default();
    // Replay everything the server must know about this client.
    let mut replay: Vec<Outbound> = Vec::new();
    for (&subuid, sub) in &state.subs {
        replay.push(Outbound::Text(Control::Subscribe(SubscribeParams {
            topics: sub.topics.clone(),
            subuid,
            options: sub.options.clone(),
        })));
    }
    for (&pubuid, publication) in &state.pubs {
        replay.push(Outbound::Text(publish_message(pubuid, publication)));
        if let Some(value) = &publication.last {
            replay.push(Outbound::Binary(
                match encode_frame(pubuid as i32, now_micros(), value) {
                    Ok(bytes) => bytes,
                    Err(_) => continue,
                },
            ));
        }
    }
    for message in replay {
        if let Err(e) = send(&mut socket, message).await {
            return Outcome::Lost(e.to_string());
        }
    }

    let mut next_ping = Instant::now();
    loop {
        tokio::select! {
            incoming = socket.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        on_text(text.as_str(), state, &mut link, events);
                    }
                    Some(Ok(Message::Binary(bytes))) => {
                        on_binary(&bytes, state, &mut link, events, shared);
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        return Outcome::Lost("closed by server".into());
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Outcome::Lost(e.to_string()),
                }
            }
            command = commands.recv() => {
                let Some(command) = command else { return Outcome::Shutdown };
                match apply(state, command) {
                    Ok(Some(message)) => {
                        if let Err(e) = send(&mut socket, message).await {
                            return Outcome::Lost(e.to_string());
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        let _ = events.send(ClientEvent::ProtocolWarning(e.to_string()));
                    }
                }
            }
            () = sleep_until(next_ping) => {
                let t = now_micros();
                // A ping is `[-1, 0, int, client_time]`; the server echoes the client time back.
                match encode_frame(-1, 0, &Value::Int(t)) {
                    Ok(bytes) => {
                        if let Err(e) = send(&mut socket, Outbound::Binary(bytes)).await {
                            return Outcome::Lost(e.to_string());
                        }
                    }
                    Err(e) => {
                        let _ = events.send(ClientEvent::ProtocolWarning(e.to_string()));
                    }
                }
                link.pings_sent += 1;
                next_ping = Instant::now()
                    + if link.pings_sent < BURST_PINGS { BURST_INTERVAL } else { PING_INTERVAL };
            }
        }
    }
}

/// Updates the state for a command and returns the message to send now, if connected.
fn apply(state: &mut State, command: Command) -> Result<Option<Outbound>> {
    Ok(match command {
        Command::Subscribe {
            subuid,
            topics,
            options,
            events,
        } => {
            let message = Control::Subscribe(SubscribeParams {
                topics: topics.clone(),
                subuid,
                options: options.clone(),
            });
            state.subs.insert(
                subuid,
                SubState {
                    topics,
                    options,
                    events,
                },
            );
            Some(Outbound::Text(message))
        }
        Command::Unsubscribe { subuid } => {
            state.subs.remove(&subuid);
            Some(Outbound::Text(Control::Unsubscribe(UnsubscribeParams {
                subuid,
            })))
        }
        Command::Publish {
            pubuid,
            name,
            type_name,
        } => {
            let publication = Publication {
                name,
                type_name,
                last: None,
            };
            let message = publish_message(pubuid, &publication);
            state.pubs.insert(pubuid, publication);
            Some(Outbound::Text(message))
        }
        Command::Unpublish { pubuid } => {
            state.pubs.remove(&pubuid);
            Some(Outbound::Text(Control::Unpublish(UnpublishParams {
                pubuid,
            })))
        }
        Command::Set {
            pubuid,
            timestamp_us,
            value,
        } => match state.pubs.get_mut(&pubuid) {
            Some(publication) => {
                let bytes = encode_frame(pubuid as i32, timestamp_us, &value)?;
                publication.last = Some(value);
                Some(Outbound::Binary(bytes))
            }
            None => None,
        },
        Command::SetProperties { name, update } => Some(Outbound::Text(Control::Setproperties(
            SetPropertiesParams { name, update },
        ))),
    })
}

fn publish_message(pubuid: u32, publication: &Publication) -> Control {
    Control::Publish(PublishParams {
        name: publication.name.clone(),
        type_name: publication.type_name.clone(),
        pubuid,
        properties: Default::default(),
    })
}

async fn send(socket: &mut Socket, message: Outbound) -> Result<()> {
    let frame = match message {
        Outbound::Text(control) => Message::Text(encode_text(&[control])?.into()),
        Outbound::Binary(bytes) => Message::Binary(bytes.into()),
    };
    socket.send(frame).await?;
    Ok(())
}

fn on_text(
    text: &str,
    state: &mut State,
    link: &mut Link,
    events: &mpsc::UnboundedSender<ClientEvent>,
) {
    let parsed = match parse_text(text) {
        Ok(parsed) => parsed,
        Err(e) => {
            let _ = events.send(ClientEvent::ProtocolWarning(e.to_string()));
            return;
        }
    };
    for item in parsed {
        match item {
            Ok(Control::Announce(AnnounceParams {
                name,
                id,
                type_name,
                pubuid,
                properties,
            })) => {
                link.ids.insert(id, name.clone());
                if pubuid.is_some() {
                    link.own.insert(id);
                }
                let info = TopicInfo {
                    id,
                    name: name.clone(),
                    type_name,
                    properties,
                    pubuid,
                };
                route(state, &name, TopicEvent::Announced(info), false);
            }
            Ok(Control::Unannounce(p)) => {
                link.ids.remove(&p.id);
                link.own.remove(&p.id);
                route(
                    state,
                    &p.name,
                    TopicEvent::Unannounced {
                        id: p.id,
                        name: p.name.clone(),
                    },
                    false,
                );
            }
            Ok(Control::Properties(p)) => {
                let name = p.name.clone();
                route(
                    state,
                    &name,
                    TopicEvent::PropertiesChanged {
                        name: p.name,
                        update: p.update,
                    },
                    false,
                );
            }
            Ok(other) => {
                let _ = events.send(ClientEvent::ProtocolWarning(format!(
                    "server sent a client method: {other:?}"
                )));
            }
            Err(e) => {
                let _ = events.send(ClientEvent::ProtocolWarning(e));
            }
        }
    }
}

fn on_binary(
    bytes: &[u8],
    state: &mut State,
    link: &mut Link,
    events: &mpsc::UnboundedSender<ClientEvent>,
    shared: &Shared,
) {
    let frame = match decode_frame(bytes) {
        Ok(frame) => frame,
        Err(e) => {
            let _ = events.send(ClientEvent::ProtocolWarning(e.to_string()));
            return;
        }
    };
    if frame.id == -1 {
        on_pong(&frame, link, events, shared);
        return;
    }
    match link.ids.get(&frame.id) {
        Some(name) => {
            let name = name.clone();
            let event = TopicEvent::Value {
                id: frame.id,
                name: name.clone(),
                timestamp_us: frame.timestamp_us,
                value: frame.value,
            };
            let own = link.own.contains(&frame.id);
            route(state, &name, event, own);
        }
        None => {
            let _ = events.send(ClientEvent::ProtocolWarning(format!(
                "value for unknown topic id {}",
                frame.id
            )));
        }
    }
}

/// RTT sample: the server echoes our send time in `value` and stamps its own clock in `timestamp`.
/// The server clock at the midpoint of the round trip is `timestamp`, so
/// `offset = timestamp - (sent + rtt / 2)`. The sample with the smallest RTT wins.
fn on_pong(
    frame: &crate::codec::Frame,
    link: &mut Link,
    events: &mpsc::UnboundedSender<ClientEvent>,
    shared: &Shared,
) {
    let Value::Int(sent) = frame.value else {
        let _ = events.send(ClientEvent::ProtocolWarning(
            "RTT reply without an int".into(),
        ));
        return;
    };
    let rtt = now_micros() - sent;
    if rtt < 0 || link.best_rtt_us.is_some_and(|best| rtt >= best) {
        return;
    }
    link.best_rtt_us = Some(rtt);
    let offset = frame.timestamp_us - (sent + rtt / 2);
    shared.set_offset(offset);
    let _ = events.send(ClientEvent::TimeSync {
        offset_us: offset,
        rtt_us: rtt,
    });
}

/// Delivers a topic event to every subscription whose names or prefixes match `name`. `own` marks
/// the client's own values (echoes), which only `all` subscriptions receive. Subscriptions whose stream was
/// dropped are removed.
fn route(state: &mut State, name: &str, event: TopicEvent, own: bool) {
    let mut dead = Vec::new();
    for (&subuid, sub) in &state.subs {
        if own && !sub.options.all {
            continue;
        }
        if !name_matches(&sub.topics, sub.options.prefix, name) {
            continue;
        }
        if sub.events.send(event.clone()).is_err() {
            dead.push(subuid);
        }
    }
    for subuid in dead {
        state.subs.remove(&subuid);
    }
}

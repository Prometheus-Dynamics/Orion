//! The client connection task: connect, replay state, pump frames, reconnect with backoff.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use orion_ws::{Heard, WsSocket, WsWrite};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep, sleep_until, timeout};

use crate::mailbox::{self, CONTROL_CAPACITY, PushError};

use super::clock::{ClockFilter, Sample};
use super::subscription::{TopicEvent, TopicInfo};
use super::{ClientConfig, ClientEvent, Command, Shared};
use crate::codec::{Frame, encode_frame, encode_frames, for_each_frame};
use crate::error::{Error, Result};
use crate::message::{
    AnnounceParams, Control, PublishParams, SetPropertiesParams, SubscribeOptions, SubscribeParams,
    UnpublishParams, UnsubscribeParams, encode_text, name_matches, parse_text,
};
use crate::time::now_micros;
use crate::value::Value;
use crate::{MAX_MESSAGE, SUBPROTOCOL_V4_0, SUBPROTOCOL_V4_1};

type Socket = WsSocket;

/// Offered in this order; the server picks one and the client accepts either.
const OFFERED_SUBPROTOCOLS: [&str; 2] = [SUBPROTOCOL_V4_1, SUBPROTOCOL_V4_0];
/// Pings sent back to back after connecting, then one every `PING_INTERVAL`.
const BURST_PINGS: u32 = 5;
const BURST_INTERVAL: Duration = Duration::from_millis(200);
/// A server may answer only on its loop tick, which adds up to a whole period to the RTT. This
/// interval is not a multiple of a 20 ms loop: each ping lands 7.6 ms (0.382 of the loop) further
/// on, so pings cover the loop evenly and soon land just before a tick, where the RTT is lowest.
const PING_INTERVAL: Duration = Duration::from_micros(1_007_639);
/// Smallest offset change `ClientEvent::TimeSync` reports. Smaller ones are RTT noise; the
/// estimate still takes them.
const TIME_SYNC_EVENT_MIN_US: i64 = 250;

/// Per-client state that survives reconnects.
#[derive(Default)]
struct State {
    subs: BTreeMap<u32, SubState>,
    pubs: BTreeMap<u32, Publication>,
}

struct SubState {
    topics: Vec<String>,
    options: SubscribeOptions,
    events: mailbox::Sender<i32, TopicEvent>,
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
    clock: ClockFilter,
    /// Offset last reported in `ClientEvent::TimeSync`.
    reported_offset_us: Option<i64>,
    pings_sent: u32,
    /// The publications' last values went out, which waits for the first RTT sample.
    values_sent: bool,
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
    mut commands: mailbox::Receiver<u32, Command>,
    events: mpsc::UnboundedSender<ClientEvent>,
) {
    let mut state = State::default();
    let mut backoff = config.reconnect_min;
    loop {
        let outcome = match connect(&config).await {
            Ok(socket) => {
                backoff = config.reconnect_min;
                let _ = events.send(ClientEvent::Connected);
                serve(socket, &config, &mut state, &mut commands, &events, &shared).await
            }
            Err(e) => Outcome::Lost(e.to_string()),
        };
        match outcome {
            Outcome::Shutdown => return,
            Outcome::Lost(reason) => {
                // The next connection may reach a restarted server with another clock.
                shared.clear_offset();
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
                        let _ = apply(&mut state, command, None);
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
    let host = format!("{host}:{}", config.port);
    let path = format!("/nt/{}", config.name);
    let handshake = async {
        let stream = TcpStream::connect((config.host.as_str(), config.port)).await?;
        crate::net::tune(&stream, config.liveness_timeout);
        orion_ws::connect_on(stream, &host, &path, &OFFERED_SUBPROTOCOLS, None).await
    };
    let socket = timeout(config.connect_timeout, handshake)
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "connect timed out"))?;
    socket.map_err(|error| {
        // A server that takes the upgrade and names neither subprotocol is not an NT4 server.
        if error.kind() == std::io::ErrorKind::InvalidData {
            Error::Subprotocol {
                offered: OFFERED_SUBPROTOCOLS.join(", "),
            }
        } else {
            Error::Io(error)
        }
    })
}

async fn serve(
    socket: Socket,
    config: &ClientConfig,
    state: &mut State,
    commands: &mut mailbox::Receiver<u32, Command>,
    events: &mpsc::UnboundedSender<ClientEvent>,
    shared: &Shared,
) -> Outcome {
    let WsSocket {
        mut read,
        write: mut socket,
        ..
    } = socket;
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
    }
    for message in replay {
        if let Err(e) = send(&mut socket, message).await {
            return Outcome::Lost(e.to_string());
        }
    }

    let mut next_ping = Instant::now();
    // Anything from the server proves the link; it answers every ping.
    let mut heard = Instant::now();
    loop {
        tokio::select! {
            incoming = read.next(MAX_MESSAGE) => {
                let heard_what = match incoming {
                    Ok(Some(heard_what)) => heard_what,
                    Ok(None) => return Outcome::Lost("closed by server".into()),
                    Err(e) => return Outcome::Lost(e.to_string()),
                };
                heard = Instant::now();
                match heard_what {
                    Heard::Text => match std::str::from_utf8(read.message()) {
                        Ok(text) => on_text(text, state, &mut link, events),
                        Err(_) => {
                            let _ = events.send(ClientEvent::ProtocolWarning(
                                "text that is not UTF-8".into(),
                            ));
                        }
                    },
                    Heard::Binary => {
                        on_binary(read.message(), state, &mut link, events, shared);
                        if !link.values_sent && let Some(offset) = link.clock.offset_us() {
                            link.values_sent = true;
                            match last_values(state, now_micros() + offset) {
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
                    }
                    Heard::Alive => {}
                }
            }
            command = commands.recv() => {
                let Some(command) = command else { return Outcome::Shutdown };
                match apply(state, command, link.clock.offset_us()) {
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
            () = sleep_until(heard + config.liveness_timeout) => {
                return Outcome::Lost(format!(
                    "nothing from the server for {} ms",
                    config.liveness_timeout.as_millis()
                ));
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

/// Every publication's last value in one batched binary message, stamped `timestamp_us`.
fn last_values(state: &State, timestamp_us: i64) -> Result<Option<Outbound>> {
    let values: Vec<(i32, i64, &Value)> = state
        .pubs
        .iter()
        .filter_map(|(&pubuid, publication)| {
            let value = publication.last.as_ref()?;
            Some((pubuid as i32, timestamp_us, value))
        })
        .collect();
    if values.is_empty() {
        return Ok(None);
    }
    encode_frames(values).map(|bytes| Some(Outbound::Binary(bytes)))
}

/// Updates the state for a command and returns the message to send now, if connected. `offset_us`
/// is this connection's clock offset: without one a value is only kept, and goes out with the
/// last values after the first RTT sample, since no timestamp on the server's clock exists yet.
fn apply(state: &mut State, command: Command, offset_us: Option<i64>) -> Result<Option<Outbound>> {
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
                let stamp = offset_us.map(|offset| timestamp_us.unwrap_or(now_micros() + offset));
                let bytes = encode_frame(pubuid as i32, stamp.unwrap_or(0), &value)?;
                publication.last = Some(value);
                stamp.map(|_| Outbound::Binary(bytes))
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

async fn send(socket: &mut WsWrite, message: Outbound) -> Result<()> {
    match message {
        Outbound::Text(control) => socket.send_text(&encode_text(&[control])?).await?,
        Outbound::Binary(bytes) => socket.send_binary(&bytes).await?,
    }
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
                route(state, events, &name, TopicEvent::Announced(info), false);
            }
            Ok(Control::Unannounce(p)) => {
                link.ids.remove(&p.id);
                link.own.remove(&p.id);
                route(
                    state,
                    events,
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
                    events,
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
    // One WebSocket message may batch several values; a malformed one drops the rest of it.
    let error = for_each_frame(bytes, |frame| on_frame(frame, state, link, events, shared));
    if let Some(e) = error {
        let _ = events.send(ClientEvent::ProtocolWarning(e.to_string()));
    }
}

fn on_frame(
    frame: Frame,
    state: &mut State,
    link: &mut Link,
    events: &mpsc::UnboundedSender<ClientEvent>,
    shared: &Shared,
) {
    if frame.id == -1 {
        on_pong(&frame, link, events, shared);
        return;
    }
    match link.ids.get(&frame.id) {
        Some(name) => {
            let own = link.own.contains(&frame.id);
            let event = TopicEvent::Value {
                id: frame.id,
                name: name.clone(),
                timestamp_us: frame.timestamp_us,
                value: frame.value,
            };
            route(state, events, name, event, own);
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
/// `offset = timestamp - (sent + rtt / 2)`. [`ClockFilter`] picks the sample to use.
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
    let now = now_micros();
    let rtt = now - sent;
    if rtt < 0 {
        return;
    }
    let Some(best) = link.clock.add(Sample {
        at_us: now,
        rtt_us: rtt,
        offset_us: frame.timestamp_us - (sent + rtt / 2),
    }) else {
        return;
    };
    shared.set_offset(best.offset_us);
    if link
        .reported_offset_us
        .is_none_or(|reported| (best.offset_us - reported).abs() >= TIME_SYNC_EVENT_MIN_US)
    {
        link.reported_offset_us = Some(best.offset_us);
        let _ = events.send(ClientEvent::TimeSync {
            offset_us: best.offset_us,
            rtt_us: best.rtt_us,
        });
    }
}

/// Delivers a topic event to every subscription whose names or prefixes match `name`. `own` marks
/// the client's own values (echoes), which only `all` subscriptions receive. Subscriptions whose stream was
/// dropped are removed.
fn route(
    state: &mut State,
    events: &mpsc::UnboundedSender<ClientEvent>,
    name: &str,
    event: TopicEvent,
    own: bool,
) {
    let mut dead = Vec::new();
    // Values coalesce per topic id while queued.
    let (key, unannounced) = match &event {
        TopicEvent::Value { id, .. } => (Some(*id), None),
        TopicEvent::Unannounced { id, .. } => (None, Some(*id)),
        _ => (None, None),
    };
    let deliver = |subuid: u32, sub: &SubState, event: TopicEvent, dead: &mut Vec<u32>| match sub
        .events
        .push(event, key)
    {
        Ok(()) => {}
        Err(PushError::Closed(_)) => dead.push(subuid),
        Err(PushError::Full(_)) => {
            sub.events.abort();
            let _ = events.send(ClientEvent::ProtocolWarning(format!(
                "subscription {subuid} fell more than {CONTROL_CAPACITY} events behind; closed"
            )));
            dead.push(subuid);
        }
    };
    // Every matching subscription but the last gets a clone; the last takes the event.
    let mut previous: Option<(u32, &SubState)> = None;
    for (&subuid, sub) in &state.subs {
        if own && !sub.options.all {
            continue;
        }
        if !name_matches(&sub.topics, sub.options.prefix, name) {
            continue;
        }
        if let Some(id) = unannounced {
            // A value still queued for a topic that went away must not be delivered after it.
            sub.events.purge(&id);
        }
        if let Some((prev_uid, prev)) = previous.replace((subuid, sub)) {
            deliver(prev_uid, prev, event.clone(), &mut dead);
        }
    }
    if let Some((subuid, sub)) = previous {
        deliver(subuid, sub, event, &mut dead);
    }
    for subuid in dead {
        state.subs.remove(&subuid);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OFFSET_US: i64 = 5_000_000;

    fn state_with_publication() -> State {
        let mut state = State::default();
        let publish = Command::Publish {
            pubuid: 1,
            name: "/a".into(),
            type_name: "int".into(),
        };
        apply(&mut state, publish, None).unwrap();
        state
    }

    fn set(timestamp_us: Option<i64>) -> Command {
        Command::Set {
            pubuid: 1,
            timestamp_us,
            value: Value::Int(7),
        }
    }

    fn timestamp(message: Option<Outbound>) -> i64 {
        let Some(Outbound::Binary(bytes)) = message else {
            panic!("expected a binary message");
        };
        crate::codec::decode_frames(&bytes).0[0].timestamp_us
    }

    #[test]
    fn a_value_set_before_the_first_sample_is_kept_and_goes_out_on_the_server_clock() {
        let mut state = state_with_publication();
        assert!(apply(&mut state, set(None), None).unwrap().is_none());
        let stamp = OFFSET_US + 123;
        assert_eq!(timestamp(last_values(&state, stamp).unwrap()), stamp);
    }

    #[test]
    fn an_unstamped_value_gets_the_connections_offset() {
        let mut state = state_with_publication();
        let before = now_micros() + OFFSET_US;
        let message = apply(&mut state, set(None), Some(OFFSET_US)).unwrap();
        assert!(timestamp(message) >= before);
    }

    #[test]
    fn a_value_stamped_on_the_old_connection_waits_for_the_new_ones_clock() {
        let mut state = state_with_publication();
        assert!(apply(&mut state, set(Some(99)), None).unwrap().is_none());
    }

    #[test]
    fn a_stamped_value_keeps_its_timestamp() {
        let mut state = state_with_publication();
        let message = apply(&mut state, set(Some(99)), Some(OFFSET_US)).unwrap();
        assert_eq!(timestamp(message), 99);
    }

    #[test]
    fn nothing_is_sent_when_no_publication_has_a_value() {
        let state = state_with_publication();
        assert!(last_values(&state, 1).unwrap().is_none());
    }
}

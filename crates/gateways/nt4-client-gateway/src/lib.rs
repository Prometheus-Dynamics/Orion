//! The NT4 gateway, client form: the same mapping as `orion-nt4-gateway`, against an NT4
//! server that runs somewhere else. A robot controller runs the NT4 server its programs
//! talk to; a board beside it joins as one more client.
//!
//! - **The mapping is the same.** `nt4.publish` names the fields this board publishes on
//!   the server, `nt4.accept` the server's topics whose values are published here, whoever
//!   writes them there. Each is `topic:field=type@name`.
//! - **Which form runs is one setting.** `nt4.server` names the server to join. `nt4.listen`
//!   is the other form; a board is one or the other.
//! - **The link comes and goes.** The client reconnects on its own. What this board
//!   publishes is sent again when it is back, each topic's last value with it, and the
//!   accepted topics are subscribed to again and their values sent by the server.
//! - **A value that expires is withdrawn** as in the server form: the gateway stops
//!   publishing the NT4 topic, and publishes it again with the next sample.

#![cfg(target_os = "linux")]

use std::cell::RefCell;
use std::rc::Rc;

use orion_broker::Topics;
use orion_config::{ConfigError, Source};
use orion_nt4::{
    Client, ClientConfig, ClientEvent, ClientHandle, Publisher, SubscribeOptions, TopicEvent, Value,
};
use orion_nt4_gateway::{Counters, Entry, Far, Mapped, Mapping};
use tokio::task::JoinHandle;
use tracing::{debug, info};

/// What the client form of the gateway is told.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Nt4ClientConfig {
    /// `nt4.server`: the server's host, a name or an address.
    pub host: String,
    /// Its port, from `nt4.server` if it says one.
    pub port: u16,
    /// `nt4.name`: what this client calls itself there.
    pub name: String,
    /// The mapping.
    pub mapping: Mapping,
}

/// `host`, `host:port`, `[v6]` or `[v6]:port`.
fn host_and_port(text: &str) -> Result<(String, u16), String> {
    let port = |text: &str| {
        text.parse::<u16>()
            .map_err(|_| format!("{text:?} is not a port"))
    };
    if let Some(rest) = text.strip_prefix('[') {
        let (host, after) = rest.split_once(']').ok_or("a [ without its ]")?;
        return match after.strip_prefix(':') {
            Some(given) => Ok((host.to_owned(), port(given)?)),
            None if after.is_empty() => Ok((host.to_owned(), orion_nt4::DEFAULT_PORT)),
            None => Err(format!("{after:?} after the address")),
        };
    }
    match text.split_once(':') {
        // More than one colon and no brackets: an IPv6 address on its own.
        Some((_, rest)) if rest.contains(':') => Ok((text.to_owned(), orion_nt4::DEFAULT_PORT)),
        Some((host, given)) if !host.is_empty() => Ok((host.to_owned(), port(given)?)),
        None if !text.is_empty() => Ok((text.to_owned(), orion_nt4::DEFAULT_PORT)),
        _ => Err("no host".to_owned()),
    }
}

impl Nt4ClientConfig {
    /// The gateway's settings, or `None` if `nt4.server` is not set: no gateway of this
    /// form.
    ///
    /// # Errors
    ///
    /// A server that is not a host and a port, a mapping that does not hold together, or
    /// `nt4.listen` set as well: a board is a server or a client of one, not both.
    pub fn from_source(source: &mut Source) -> Result<Option<Self>, ConfigError> {
        let Some(server) = source.text("nt4.server").map(str::to_owned) else {
            return Ok(None);
        };
        let wrong = |message: String| ConfigError {
            key: "nt4.server".into(),
            message,
        };
        if source.text("nt4.listen").is_some() {
            return Err(wrong("nt4.listen is set too: one form or the other".into()));
        }
        let (host, port) = host_and_port(server.trim()).map_err(wrong)?;
        Ok(Some(Self {
            host,
            port,
            name: source.get("nt4.name", "orion".to_owned())?,
            mapping: Mapping::from_source(source)?,
        }))
    }
}

/// The NT4 side of the client form: this client's publications on the server.
struct Publications {
    handle: ClientHandle,
    /// One for each field that goes to NT4, while it is published.
    published: Vec<Option<Publisher>>,
}

impl Far for Publications {
    fn set(&mut self, at: usize, entry: &Entry, value: Value, first: bool) -> bool {
        let Some(slot) = self.published.get_mut(at) else {
            return false;
        };
        if first || slot.is_none() {
            *slot = self.handle.publish(&entry.nt_name, &entry.nt_type).ok();
        }
        slot.as_ref()
            .is_some_and(|publisher| publisher.set(value).is_ok())
    }

    fn withdraw(&mut self, at: usize, _entry: &Entry) {
        if let Some(publisher) = self.published.get_mut(at).and_then(Option::take) {
            let _ = publisher.unpublish();
        }
    }
}

/// The gateway. Dropping it stops it and leaves the server.
pub struct ClientGateway {
    state: Rc<RefCell<Shared>>,
    task: JoinHandle<()>,
}

struct Shared {
    mapped: Mapped,
    connected: bool,
}

impl std::fmt::Debug for ClientGateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ClientGateway(connected: {})", self.connected())
    }
}

impl Drop for ClientGateway {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl ClientGateway {
    /// Starts the NT4 client and the mapping. Call it inside the comm core's task set. The
    /// server need not be there yet: the client keeps trying.
    ///
    /// # Errors
    ///
    /// If a topic that comes from NT4 cannot be declared: its name is taken.
    pub fn start(topics: &Rc<RefCell<Topics>>, config: Nt4ClientConfig) -> std::io::Result<Self> {
        config.mapping.check().map_err(std::io::Error::other)?;
        let Mapping {
            period,
            publish,
            accept,
            max_payload,
        } = config.mapping;
        let fields = publish.len();
        let mapped = Mapped::new(topics, publish, &accept, max_payload)?;
        let mut client_config = ClientConfig::new(config.host.clone(), config.name);
        client_config.port = config.port;
        let mut client = Client::start(client_config);
        let handle = client.handle();
        // The accepted topics, each by its own name.
        let names: Vec<&str> = accept.iter().map(|entry| entry.nt_name.as_str()).collect();
        let mut accepted = if names.is_empty() {
            None
        } else {
            Some(
                handle
                    .subscribe(&names, SubscribeOptions::default())
                    .map_err(std::io::Error::other)?,
            )
        };
        let state = Rc::new(RefCell::new(Shared {
            mapped,
            connected: false,
        }));
        let running = state.clone();
        let (host, port) = (config.host, config.port);
        let task = tokio::task::spawn_local(async move {
            let mut far = Publications {
                handle,
                published: (0..fields).map(|_| None).collect(),
            };
            let mut period = tokio::time::interval(period);
            period.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = period.tick() => running.borrow_mut().mapped.turn(&mut far),
                    event = async {
                        match &mut accepted {
                            Some(accepted) => accepted.next().await,
                            None => std::future::pending().await,
                        }
                    } => match event {
                        Some(TopicEvent::Value { name, value, .. }) => {
                            running.borrow_mut().mapped.take(&name, &value);
                        }
                        Some(_) => {}
                        None => break,
                    },
                    event = client.next_event() => match event {
                        Some(ClientEvent::Connected) => {
                            info!("nt4: joined the server at {host}:{port}");
                            running.borrow_mut().connected = true;
                        }
                        Some(ClientEvent::Disconnected { reason }) => {
                            if std::mem::take(&mut running.borrow_mut().connected) {
                                info!("nt4: lost the server at {host}:{port}: {reason}");
                            } else {
                                debug!("nt4: no server at {host}:{port}: {reason}");
                            }
                        }
                        Some(_) => {}
                        None => break,
                    },
                }
            }
        });
        Ok(Self { state, task })
    }

    /// Whether the server is there.
    #[must_use]
    pub fn connected(&self) -> bool {
        self.state.borrow().connected
    }

    /// What has been counted.
    #[must_use]
    pub fn counters(&self) -> Counters {
        self.state.borrow().mapped.counters()
    }
}

#[cfg(test)]
mod tests {
    use super::host_and_port;

    #[test]
    fn a_server_is_a_host_and_maybe_a_port() {
        let good = |text: &str| host_and_port(text).expect(text);
        assert_eq!(good("10.0.0.2"), ("10.0.0.2".to_owned(), 5810));
        assert_eq!(
            good("controller.local:5811"),
            ("controller.local".to_owned(), 5811)
        );
        assert_eq!(good("[fe80::1]:5811"), ("fe80::1".to_owned(), 5811));
        assert_eq!(good("[fe80::1]"), ("fe80::1".to_owned(), 5810));
        assert_eq!(good("fe80::1"), ("fe80::1".to_owned(), 5810));
        for bad in [
            "",
            ":5810",
            "host:port",
            "host:70000",
            "[fe80::1",
            "[fe80::1]5810",
        ] {
            assert!(host_and_port(bad).is_err(), "{bad}");
        }
    }
}

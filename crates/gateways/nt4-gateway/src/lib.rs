//! The NT4 gateway: topics to NetworkTables 4 and back.
//!
//! It runs inside the comm core as an NT4 server. Programs that speak NT4 connect to it and
//! see the topics the configuration names, under the names and types the configuration
//! gives them; what they write to the NT4 topics the configuration accepts is published
//! here as topics of the comm core's own.
//!
//! - **The mapping is configuration.** `nt4.publish` and `nt4.accept` are lists of
//!   `topic:field=type@name`. Nothing here knows what a name means or who reads it.
//! - **Latest value, at a period.** NT4 carries the latest value of a topic, not a stream.
//!   Every `nt4.period_ms` the gateway looks at each mapped field and, if it has a newer
//!   sample, sets the NT4 topic to it. A sample replaced within one period is never sent.
//!   Nothing is read by a publisher's leave: the gateway is a reader of the ring like any
//!   other, and a publisher pays nothing for it.
//! - **A value that expires is withdrawn.** A topic declared with an expiry has no value
//!   once its newest sample is older than that. The gateway then deletes the NT4 topic, so
//!   that NT4 clients do not go on reading a value nobody stands behind, and announces it
//!   again with the next sample. Age is counted from when the gateway read the sample, on
//!   its own clock. A topic without an expiry keeps its last value in NT4, as its ring
//!   keeps it here.
//! - **What comes back is the comm core's own.** For each topic named in `nt4.accept` the
//!   gateway declares a topic with the mapped fields and publishes what NT4 clients write.
//!   A value of another type than the field's is dropped, and said once.

#![cfg(target_os = "linux")]

pub mod convert;
mod mapped;
mod mapping;

use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::Rc;

use orion_broker::Topics;
use orion_nt4::{Properties, Server, ServerConfig, ServerEvent, ServerHandle};
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;
use tracing::debug;

pub use mapped::{Counters, Far, Mapped};
pub use mapping::{Entry, Mapping, Nt4Config, field_type};

/// The NT4 side of the server form: the table of the server the gateway runs.
struct Table(ServerHandle);

impl Far for Table {
    fn set(&mut self, _at: usize, entry: &Entry, value: orion_nt4::Value, first: bool) -> bool {
        if first {
            let made = self
                .0
                .publish(&entry.nt_name, &entry.nt_type, Properties::new());
            if made.is_err() {
                return false;
            }
        }
        self.0.set_value(&entry.nt_name, value).is_ok()
    }

    fn withdraw(&mut self, _at: usize, entry: &Entry) {
        let _ = self.0.delete(&entry.nt_name);
    }
}

/// The gateway. Dropping it stops it and closes its NT4 server.
pub struct Gateway {
    state: Rc<RefCell<Mapped>>,
    local_addr: SocketAddr,
    task: JoinHandle<()>,
    /// Kept so that it serves for as long as the gateway lives.
    _server: Server,
}

impl std::fmt::Debug for Gateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Gateway({})", self.local_addr)
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Gateway {
    /// Starts the NT4 server and the mapping. Call it inside the comm core's task set.
    ///
    /// # Errors
    ///
    /// If the address cannot be listened on, or a topic that comes from NT4 cannot be
    /// declared: its name is taken.
    pub async fn start(topics: &Rc<RefCell<Topics>>, config: Nt4Config) -> std::io::Result<Self> {
        config.mapping.check().map_err(std::io::Error::other)?;
        let Mapping {
            period,
            publish,
            accept,
            max_payload,
        } = config.mapping;
        let mapped = Mapped::new(topics, publish, &accept, max_payload)?;
        let server = Server::start(ServerConfig {
            bind: config.listen,
            ..ServerConfig::default()
        })
        .await
        .map_err(std::io::Error::other)?;
        let local_addr = server.local_addr();
        let host = server.handle();
        let mut events = host.events();
        let state = Rc::new(RefCell::new(mapped));
        let running = state.clone();
        let task = tokio::task::spawn_local(async move {
            let mut table = Table(host);
            let mut period = tokio::time::interval(period);
            period.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = period.tick() => running.borrow_mut().turn(&mut table),
                    event = events.recv() => match event {
                        Ok(ServerEvent::ValueChanged { name, value, client: Some(_), .. }) => {
                            running.borrow_mut().take(&name, &value);
                        }
                        Ok(_) => {}
                        // Events were missed: what the topics hold now is what counts. Only
                        // clients write these: the gateway writes none of them itself.
                        Err(RecvError::Lagged(missed)) => {
                            debug!("nt4: {missed} events missed, reading every accepted topic");
                            let mut mapped = running.borrow_mut();
                            let names: Vec<String> = mapped.accepted().map(str::to_owned).collect();
                            for name in names {
                                if let Some(value) = table.0.value(&name) {
                                    mapped.take(&name, &value);
                                }
                            }
                        }
                        Err(RecvError::Closed) => break,
                    },
                }
            }
        });
        Ok(Self {
            state,
            local_addr,
            task,
            _server: server,
        })
    }

    /// Where NT4 clients connect.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// What has been counted.
    #[must_use]
    pub fn counters(&self) -> Counters {
        self.state.borrow().counters()
    }
}

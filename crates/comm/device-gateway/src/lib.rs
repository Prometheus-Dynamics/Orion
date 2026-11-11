//! A microcontroller on a link, as a resource.
//!
//! A device speaks the same wire as any program: it declares topics with the broker and
//! registers actions with node logic. What it cannot do on a few kilobytes is be a provider
//! too, writing a resource record that says what it is. The gateway does that for it.
//!
//! - [`Watch`] wraps a service (the broker, the relay) and notes what a session with the
//!   role `device` declared or registered through it, once the service took it.
//! - The gateway is a provider of its own, a session in memory, and writes one resource per
//!   device: named after the device's hello, of kind `device`, listing its topics and its
//!   actions. A client finds it, reads it and drives it like any other resource.
//! - **A device that goes is `Lost`, not gone.** Its record stays, so whoever used it sees
//!   why; when it comes back and declares again, it is `Ready` with what it declares now.
//! - A record is written when it changed, never on a timer. One that node logic could not
//!   take (it is restarting) is written again a second later, until it is.

mod provider;
mod watch;

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use orion_msg_records::ResourceState;
use orion_server::{Comm, SessionId};
use orion_types::NodeName;
use tokio::sync::Notify;

pub use watch::Watch;

/// What one device offers, as the gateway knows it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Offers {
    /// Its topics, by full name.
    pub topics: BTreeSet<String>,
    /// Its actions, by full name: the resource, a dot, the action.
    pub actions: BTreeSet<String>,
}

#[derive(Debug)]
struct Device {
    offers: Offers,
    state: ResourceState,
    /// The session it is on now, if it is connected.
    session: Option<SessionId>,
    /// What was last written for it, so an unchanged device is not written again.
    written: Option<(Offers, ResourceState)>,
}

#[derive(Debug, Default)]
pub(crate) struct State {
    devices: BTreeMap<String, Device>,
    sessions: BTreeMap<SessionId, String>,
}

impl State {
    /// `session`, a device named `name`, offers `offer`.
    fn offered(&mut self, session: SessionId, name: &str, offer: Offer) {
        let device = self
            .devices
            .entry(name.to_owned())
            .or_insert_with(|| Device {
                offers: Offers::default(),
                state: ResourceState::Ready,
                session: None,
                written: None,
            });
        if device.session != Some(session) {
            // A new session declares everything again: what it offered before is forgotten.
            if let Some(old) = device.session {
                self.sessions.remove(&old);
            }
            device.session = Some(session);
            device.offers = Offers::default();
            device.state = ResourceState::Ready;
            self.sessions.insert(session, name.to_owned());
        }
        match offer {
            Offer::Topic(topic) => device.offers.topics.insert(topic),
            Offer::Action(action) => device.offers.actions.insert(action),
        };
    }

    fn gone(&mut self, session: SessionId) -> bool {
        let Some(name) = self.sessions.remove(&session) else {
            return false;
        };
        if let Some(device) = self.devices.get_mut(&name) {
            device.session = None;
            device.state = ResourceState::Lost;
        }
        true
    }

    /// Devices whose record is not what was last written.
    fn changed(&self) -> Vec<(String, Offers, ResourceState)> {
        self.devices
            .iter()
            .filter(|(_, d)| d.written.as_ref() != Some(&(d.offers.clone(), d.state)))
            .map(|(name, d)| (name.clone(), d.offers.clone(), d.state))
            .collect()
    }

    fn written(&mut self, name: &str, offers: Offers, state: ResourceState) {
        if let Some(device) = self.devices.get_mut(name) {
            device.written = Some((offers, state));
        }
    }
}

/// Something a device offered.
#[derive(Debug)]
pub(crate) enum Offer {
    Topic(String),
    Action(String),
}

/// The gateway. Cheap to clone: clones share one table.
#[derive(Clone, Debug)]
pub struct DeviceGateway {
    state: Rc<RefCell<State>>,
    changed: Rc<Notify>,
    comm: Comm,
    node: NodeName,
    /// Whether the provider session is up. It starts with the first device: a board with
    /// none has no session for them and nothing that pings.
    started: Rc<Cell<bool>>,
}

impl DeviceGateway {
    /// The gateway for node `node` on `comm`. Its provider session starts with the first
    /// device, inside the comm core's `LocalSet`.
    #[must_use]
    pub fn start(comm: &Comm, node: NodeName) -> Self {
        Self {
            state: Rc::default(),
            changed: Rc::new(Notify::new()),
            comm: comm.clone(),
            node,
            started: Rc::new(Cell::new(false)),
        }
    }

    /// `service`, with what device sessions do through it noted here.
    pub fn watch<S>(&self, service: S) -> Watch<S> {
        Watch::new(service, self.clone())
    }

    /// What device `name` offers now, and whether it is connected. `None` for a device the
    /// gateway has not seen.
    #[must_use]
    pub fn device(&self, name: &str) -> Option<(Offers, ResourceState)> {
        let state = self.state.borrow();
        let device = state.devices.get(name)?;
        Some((device.offers.clone(), device.state))
    }

    pub(crate) fn offered(&self, session: SessionId, name: &str, offer: Offer) {
        self.state.borrow_mut().offered(session, name, offer);
        if !self.started.replace(true) {
            provider::start(&self.comm, self.node.clone(), self.clone());
        }
        self.changed.notify_one();
    }

    pub(crate) fn gone(&self, session: SessionId) {
        if self.state.borrow_mut().gone(session) {
            self.changed.notify_one();
        }
    }
}

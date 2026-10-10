//! Per-subscription streams: the typed topic events a subscription receives.

use tokio::sync::mpsc;

use super::ClientHandle;
use crate::message::Properties;
use crate::value::Value;

/// A topic as announced by the server.
#[derive(Clone, Debug, PartialEq)]
pub struct TopicInfo {
    /// Server-assigned id used by binary frames.
    pub id: i32,
    /// Topic name.
    pub name: String,
    /// NT4 type string.
    pub type_name: String,
    /// Topic properties.
    pub properties: Properties,
    /// Set when this client publishes the topic.
    pub pubuid: Option<u32>,
}

/// Something that happened to a topic matching a subscription.
#[derive(Clone, Debug, PartialEq)]
pub enum TopicEvent {
    /// The topic exists for this subscription (also sent again after a reconnect).
    Announced(TopicInfo),
    /// The topic is gone.
    Unannounced {
        /// Topic id it had.
        id: i32,
        /// Topic name.
        name: String,
    },
    /// The topic's properties changed.
    PropertiesChanged {
        /// Topic name.
        name: String,
        /// Changed keys (`null` means removed).
        update: Properties,
    },
    /// A new value.
    Value {
        /// Topic id.
        id: i32,
        /// Topic name.
        name: String,
        /// Server-clock timestamp of the value, in microseconds.
        timestamp_us: i64,
        /// The value.
        value: Value,
    },
}

/// A stream of [`TopicEvent`]s for one `subscribe` call. Dropping it unsubscribes.
#[derive(Debug)]
pub struct Subscription {
    handle: ClientHandle,
    subuid: u32,
    events: mpsc::UnboundedReceiver<TopicEvent>,
}

impl Subscription {
    pub(crate) fn new(
        handle: ClientHandle,
        subuid: u32,
        events: mpsc::UnboundedReceiver<TopicEvent>,
    ) -> Self {
        Self {
            handle,
            subuid,
            events,
        }
    }

    /// The subscription uid.
    pub fn subuid(&self) -> u32 {
        self.subuid
    }

    /// The next event, or `None` once the connection task has stopped.
    pub async fn next(&mut self) -> Option<TopicEvent> {
        self.events.recv().await
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        let _ = self.handle.unsubscribe(self.subuid);
    }
}

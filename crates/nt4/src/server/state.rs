//! The server's topic table, client table and fan-out rules. Every method runs under the
//! server mutex, so each change and the messages that announce it are ordered consistently.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};

use super::persist;
use super::{ClientInfo, ServerEvent, TopicOwner, TopicSnapshot};
use crate::codec::encode_frame;
use crate::error::{Error, Result};
use crate::message::{
    AnnounceParams, Control, Properties, PropertiesParams, SubscribeOptions, UnannounceParams,
    name_matches,
};
use crate::time::now_micros;
use crate::value::{Value, type_id_for_name};

/// A message queued for one client's connection task.
#[derive(Debug)]
pub(super) enum Outgoing {
    Text(String),
    Binary(Arc<[u8]>),
}

/// Who owns a topic's values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Owner {
    /// Retained or persistent, with no publisher right now.
    Unpublished,
    /// The host app, through the local API.
    Local,
    /// A connected client, by its publisher uid.
    Client { id: u64, pubuid: u32 },
}

pub(super) struct Topic {
    pub id: i32,
    pub type_name: String,
    pub type_id: u8,
    pub properties: Properties,
    pub value: Option<(i64, Value)>,
    pub owner: Owner,
}

/// One subscription of a client, as sent in `subscribe`.
#[derive(Clone, Debug)]
pub(super) struct Sub {
    pub topics: Vec<String>,
    pub options: SubscribeOptions,
}

pub(super) struct ClientEntry {
    pub name: String,
    pub tx: mpsc::UnboundedSender<Outgoing>,
    pub subs: BTreeMap<u32, Sub>,
    /// Publisher uid -> topic name.
    pub pubs: HashMap<u32, String>,
    /// Topic ids this client has been sent an announce for and not an unannounce.
    pub announced: HashSet<i32>,
}

pub(super) struct State {
    pub topics: BTreeMap<String, Topic>,
    names: HashMap<i32, String>,
    next_id: i32,
    pub clients: BTreeMap<u64, ClientEntry>,
    next_client: u64,
    persist_path: Option<PathBuf>,
    events: broadcast::Sender<ServerEvent>,
}

/// Whether `name` is selected by a subscription's topic list.
pub(super) fn matches(sub: &Sub, name: &str) -> bool {
    name_matches(&sub.topics, sub.options.prefix, name)
}

fn flag(properties: &Properties, key: &str) -> bool {
    properties
        .get(key)
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}

fn json(control: &Control) -> Result<String> {
    Ok(serde_json::to_string(&[control])?)
}

impl State {
    pub fn new(persist_path: Option<PathBuf>, events: broadcast::Sender<ServerEvent>) -> Self {
        Self {
            topics: BTreeMap::new(),
            names: HashMap::new(),
            next_id: 0,
            clients: BTreeMap::new(),
            next_client: 1,
            persist_path,
            events,
        }
    }

    pub fn event_receiver(&self) -> broadcast::Receiver<ServerEvent> {
        self.events.subscribe()
    }

    fn emit(&self, event: impl FnOnce() -> ServerEvent) {
        if self.events.receiver_count() > 0 {
            let _ = self.events.send(event());
        }
    }

    /// Restores persistent topics from the persistence file, if one is configured.
    pub fn load_persisted(&mut self) -> Result<()> {
        let Some(path) = self.persist_path.clone() else {
            return Ok(());
        };
        for entry in persist::load(&path)? {
            if type_id_for_name(&entry.type_name).is_none() {
                continue;
            }
            let value = entry.value.map(|v| (now_micros(), v));
            self.insert_topic(
                &entry.name,
                entry.type_name,
                entry.properties,
                value,
                Owner::Unpublished,
            );
        }
        Ok(())
    }

    fn insert_topic(
        &mut self,
        name: &str,
        type_name: String,
        properties: Properties,
        value: Option<(i64, Value)>,
        owner: Owner,
    ) -> i32 {
        let id = self.next_id;
        self.next_id += 1;
        let type_id = type_id_for_name(&type_name).unwrap_or(crate::value::RAW);
        self.names.insert(id, name.to_owned());
        self.topics.insert(
            name.to_owned(),
            Topic {
                id,
                type_name,
                type_id,
                properties,
                value,
                owner,
            },
        );
        id
    }

    pub fn add_client(&mut self, name: String, tx: mpsc::UnboundedSender<Outgoing>) -> u64 {
        let id = self.next_client;
        self.next_client += 1;
        self.clients.insert(
            id,
            ClientEntry {
                name: name.clone(),
                tx,
                subs: BTreeMap::new(),
                pubs: HashMap::new(),
                announced: HashSet::new(),
            },
        );
        self.emit(|| ServerEvent::ClientConnected { id, name });
        id
    }

    /// Drops a client. Topics it published are unpublished (kept only if retained or persistent).
    pub fn remove_client(&mut self, id: u64) {
        let Some(client) = self.clients.remove(&id) else {
            return;
        };
        let owned: Vec<String> = self
            .topics
            .iter()
            .filter(|(_, t)| matches!(t.owner, Owner::Client { id: owner, .. } if owner == id))
            .map(|(name, _)| name.clone())
            .collect();
        for name in owned {
            self.unpublish(&name);
        }
        let name = client.name;
        self.emit(|| ServerEvent::ClientDisconnected { id, name });
    }

    /// Creates or takes over a topic. Clients that knew the old definition get a fresh announce.
    pub fn publish(
        &mut self,
        name: &str,
        type_name: &str,
        properties: Properties,
        owner: Owner,
    ) -> Result<()> {
        let type_id = type_id_for_name(type_name)
            .ok_or_else(|| Error::UnsupportedType(type_name.to_owned()))?;
        self.unannounce_all(name);
        match self.topics.get_mut(name) {
            Some(topic) => {
                if topic.type_id != type_id {
                    topic.value = None;
                }
                topic.type_name = type_name.to_owned();
                topic.type_id = type_id;
                topic.owner = owner;
                topic.properties.extend(properties);
            }
            None => {
                self.insert_topic(name, type_name.to_owned(), properties, None, owner);
            }
        }
        if let Owner::Client { id, pubuid } = owner
            && let Some(client) = self.clients.get_mut(&id)
        {
            client.pubs.insert(pubuid, name.to_owned());
        }
        self.announce_all(name);
        self.emit(|| ServerEvent::TopicPublished {
            name: name.to_owned(),
            type_name: type_name.to_owned(),
        });
        self.persist();
        Ok(())
    }

    /// Gives up a topic's publisher. Retained or persistent topics stay (and stay announced);
    /// others are removed for everyone.
    pub fn unpublish(&mut self, name: &str) {
        let Some(topic) = self.topics.get_mut(name) else {
            return;
        };
        topic.owner = Owner::Unpublished;
        let keep = flag(&topic.properties, "retained") || flag(&topic.properties, "persistent");
        if !keep {
            self.delete(name);
            return;
        }
        self.emit(|| ServerEvent::TopicUnpublished {
            name: name.to_owned(),
        });
        self.persist();
    }

    /// Removes a topic completely, unannouncing it to every client.
    pub fn delete(&mut self, name: &str) {
        self.unannounce_all(name);
        if let Some(topic) = self.topics.remove(name) {
            self.names.remove(&topic.id);
        }
        self.emit(|| ServerEvent::TopicUnpublished {
            name: name.to_owned(),
        });
        self.persist();
    }

    pub fn client_publish(
        &mut self,
        client: u64,
        name: &str,
        type_name: &str,
        pubuid: u32,
        properties: Properties,
    ) -> Result<()> {
        self.publish(
            name,
            type_name,
            properties,
            Owner::Client { id: client, pubuid },
        )
    }

    pub fn client_unpublish(&mut self, client: u64, pubuid: u32) {
        let Some(name) = self
            .clients
            .get_mut(&client)
            .and_then(|c| c.pubs.remove(&pubuid))
        else {
            return;
        };
        let owned = matches!(
            self.topics.get(&name).map(|t| t.owner),
            Some(Owner::Client { id, pubuid: p }) if id == client && p == pubuid
        );
        if owned {
            self.unpublish(&name);
        }
    }

    /// A value from a client, on its publisher uid. Ignored (with an error) unless the client
    /// owns the topic and the type matches.
    pub fn client_value(&mut self, client: u64, pubuid: u32, value: Value) -> Result<()> {
        let name = self
            .clients
            .get(&client)
            .and_then(|c| c.pubs.get(&pubuid))
            .cloned()
            .ok_or_else(|| Error::Decode(format!("unknown publisher uid {pubuid}")))?;
        let topic = self
            .topics
            .get(&name)
            .ok_or_else(|| Error::UnknownTopic(name.clone()))?;
        if topic.owner != (Owner::Client { id: client, pubuid }) {
            return Err(Error::Decode(format!(
                "publisher uid {pubuid} no longer owns {name}"
            )));
        }
        if topic.type_id != value.type_id() {
            return Err(Error::TypeMismatch {
                topic: name,
                expected: topic.type_name.clone(),
                got: value.default_type_name(),
            });
        }
        self.store_value(&name, value, Some(client))
    }

    /// Stores a value (server time now) and fans it out to every client that wants it.
    pub fn store_value(
        &mut self,
        name: &str,
        value: Value,
        from_client: Option<u64>,
    ) -> Result<()> {
        let timestamp_us = now_micros();
        let (id, owner, persistent) = {
            let topic = self
                .topics
                .get(name)
                .ok_or_else(|| Error::UnknownTopic(name.into()))?;
            (topic.id, topic.owner, flag(&topic.properties, "persistent"))
        };
        let bytes: Arc<[u8]> = encode_frame(id, timestamp_us, &value)?.into();
        for (&cid, client) in &self.clients {
            if !client.announced.contains(&id) {
                continue;
            }
            let is_owner = matches!(owner, Owner::Client { id: o, .. } if o == cid);
            if wants_values(client, name, is_owner) {
                let _ = client.tx.send(Outgoing::Binary(Arc::clone(&bytes)));
            }
        }
        self.emit(|| ServerEvent::ValueChanged {
            name: name.to_owned(),
            value: value.clone(),
            timestamp_us,
            client: from_client,
        });
        if let Some(topic) = self.topics.get_mut(name) {
            topic.value = Some((timestamp_us, value));
        }
        if persistent {
            self.persist();
        }
        Ok(())
    }

    /// Merges into a topic's properties (`null` removes a key) and tells announced clients.
    pub fn update_properties(&mut self, name: &str, update: Properties) -> Result<()> {
        let topic = self
            .topics
            .get_mut(name)
            .ok_or_else(|| Error::UnknownTopic(name.into()))?;
        for (key, value) in &update {
            if value.is_null() {
                topic.properties.remove(key);
            } else {
                topic.properties.insert(key.clone(), value.clone());
            }
        }
        let id = topic.id;
        let message = Control::Properties(PropertiesParams {
            name: name.to_owned(),
            update: update.clone(),
        });
        let text = json(&message)?;
        for client in self.clients.values() {
            if client.announced.contains(&id) {
                let _ = client.tx.send(Outgoing::Text(text.clone()));
            }
        }
        self.emit(|| ServerEvent::PropertiesChanged {
            name: name.to_owned(),
            update,
        });
        self.persist();
        Ok(())
    }

    pub fn subscribe(&mut self, client: u64, subuid: u32, sub: Sub) {
        let Some(entry) = self.clients.get_mut(&client) else {
            return;
        };
        entry.subs.insert(subuid, sub.clone());
        let names: Vec<String> = self
            .topics
            .keys()
            .filter(|name| matches(&sub, name))
            .cloned()
            .collect();
        for name in names {
            self.announce_to(client, &name);
            self.send_current(client, &name);
        }
    }

    pub fn unsubscribe(&mut self, client: u64, subuid: u32) {
        if let Some(entry) = self.clients.get_mut(&client) {
            entry.subs.remove(&subuid);
        }
    }

    /// Sends a client a binary RTT reply (`[-1, now, int, client_time]`).
    pub fn reply_rtt(&self, client: u64, value: &Value) -> Result<()> {
        let bytes: Arc<[u8]> = encode_frame(-1, now_micros(), value)?.into();
        self.send_binary(client, bytes);
        Ok(())
    }

    fn send_binary(&self, client: u64, bytes: Arc<[u8]>) {
        if let Some(entry) = self.clients.get(&client) {
            let _ = entry.tx.send(Outgoing::Binary(bytes));
        }
    }

    fn announce_all(&mut self, name: &str) {
        let ids: Vec<u64> = self.clients.keys().copied().collect();
        for id in ids {
            self.announce_to(id, name);
        }
    }

    /// Announces `name` to one client if a subscription selects it and it was not announced yet.
    fn announce_to(&mut self, client: u64, name: &str) {
        let Some(topic) = self.topics.get(name) else {
            return;
        };
        let Some(entry) = self.clients.get_mut(&client) else {
            return;
        };
        if entry.announced.contains(&topic.id) || !entry.subs.values().any(|s| matches(s, name)) {
            return;
        }
        let pubuid = match topic.owner {
            Owner::Client { id, pubuid } if id == client => Some(pubuid),
            _ => None,
        };
        let message = Control::Announce(AnnounceParams {
            name: name.to_owned(),
            id: topic.id,
            type_name: topic.type_name.clone(),
            pubuid,
            properties: topic.properties.clone(),
        });
        entry.announced.insert(topic.id);
        if let Ok(text) = json(&message) {
            let _ = entry.tx.send(Outgoing::Text(text));
        }
    }

    fn unannounce_all(&mut self, name: &str) {
        let Some(id) = self.topics.get(name).map(|t| t.id) else {
            return;
        };
        let message = Control::Unannounce(UnannounceParams {
            name: name.to_owned(),
            id,
        });
        let Ok(text) = json(&message) else { return };
        for entry in self.clients.values_mut() {
            if entry.announced.remove(&id) {
                let _ = entry.tx.send(Outgoing::Text(text.clone()));
            }
        }
    }

    /// The current value to a client that just subscribed (late joiners get the last value).
    fn send_current(&self, client: u64, name: &str) {
        let Some(topic) = self.topics.get(name) else {
            return;
        };
        let Some((timestamp_us, value)) = &topic.value else {
            return;
        };
        let Some(entry) = self.clients.get(&client) else {
            return;
        };
        let is_owner = matches!(topic.owner, Owner::Client { id, .. } if id == client);
        if !entry.announced.contains(&topic.id) || !wants_values(entry, name, is_owner) {
            return;
        }
        if let Ok(bytes) = encode_frame(topic.id, *timestamp_us, value) {
            self.send_binary(client, Arc::from(bytes));
        }
    }

    pub fn snapshot(&self, name: &str, topic: &Topic) -> TopicSnapshot {
        TopicSnapshot {
            id: topic.id,
            name: name.to_owned(),
            type_name: topic.type_name.clone(),
            properties: topic.properties.clone(),
            value: topic.value.as_ref().map(|(_, v)| v.clone()),
            timestamp_us: topic.value.as_ref().map(|(t, _)| *t),
            owner: match topic.owner {
                Owner::Unpublished => TopicOwner::Unpublished,
                Owner::Local => TopicOwner::Local,
                Owner::Client { id, .. } => TopicOwner::Client(id),
            },
        }
    }

    pub fn client_infos(&self) -> Vec<ClientInfo> {
        self.clients
            .iter()
            .map(|(&id, c)| ClientInfo {
                id,
                name: c.name.clone(),
                subscriptions: c.subs.len(),
                publications: c.pubs.len(),
            })
            .collect()
    }

    pub fn warn(&self, client: Option<u64>, message: String) {
        self.emit(|| ServerEvent::ProtocolWarning { client, message });
    }

    /// Writes every persistent topic to the persistence file, if one is configured.
    pub fn persist(&self) {
        let Some(path) = &self.persist_path else {
            return;
        };
        let entries: Vec<persist::Entry> = self
            .topics
            .iter()
            .filter(|(_, t)| flag(&t.properties, "persistent"))
            .map(|(name, t)| persist::Entry {
                name: name.clone(),
                type_name: t.type_name.clone(),
                properties: t.properties.clone(),
                value: t.value.as_ref().map(|(_, v)| v.clone()),
            })
            .collect();
        if let Err(e) = persist::save(path, &entries) {
            self.emit(|| ServerEvent::PersistFailed {
                message: e.to_string(),
            });
        }
    }
}

/// Whether a client wants value updates for `name`. The owner of a topic gets its own values back
/// only with `all`. `topicsonly` subscriptions get announces but no values.
fn wants_values(client: &ClientEntry, name: &str, is_owner: bool) -> bool {
    client
        .subs
        .values()
        .any(|s| !s.options.topics_only && (!is_owner || s.options.all) && matches(s, name))
}
